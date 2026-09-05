//@ts-check
import http from "node:http";
import { WebSocket } from "ws";
import { spawn } from "child_process";
import { fileURLToPath } from "url";
import path from "path";

const CORE_WS_URL = process.env.CORE_WS_URL ?? "ws://localhost:8765";
const SECRET = process.env.RUNNER_SECRET ?? "";
const VOICE_SCRIPT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "index.js");

/** @param {string} msg */
function log(msg) {
  const time = new Date().toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  console.log(`[${time} RUNNER] ${msg}`);
}

/** @type {Map<string, import("child_process").ChildProcess>} */
const procs = new Map();
/** @type {WeakSet<import("child_process").ChildProcess>} */
const suppressedExitProcs = new WeakSet();

/**
 * @param {string} channelId
 * @param {string} reason
 */
function stopProc(channelId, reason) {
  const proc = procs.get(channelId);
  if (!proc) return;
  log(`Stopping existing bridge for channel ${channelId} (${reason})`);
  if (reason === "replaced by new spawn") {
    suppressedExitProcs.add(proc);
  }
  try { proc.kill("SIGTERM"); } catch {}
  procs.delete(channelId);
}

/** @type {import("ws").WebSocket | null} */
let coreWs = null;

function isCoreConnected() {
  return !!coreWs && coreWs.readyState === WebSocket.OPEN;
}

const HEALTHCHECK_ENABLED = !["0", "false", "no", "off"].includes(
  String(process.env.HEALTHCHECK_ENABLED ?? "1").toLowerCase(),
);
const HEALTHCHECK_PORT = Number(process.env.HEALTHCHECK_PORT ?? 8080) || 8080;
const HEALTHCHECK_HOST = process.env.HEALTHCHECK_HOST ?? "0.0.0.0";

if (HEALTHCHECK_ENABLED) {
  const server = http.createServer((req, res) => {
    const url = (req.url || "/").split("?")[0];

    if (req.method !== "GET" || (url !== "/health" && url !== "/live" && url !== "/ready")) {
      res.writeHead(404, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ status: "not_found" }));
      return;
    }

    const connected = isCoreConnected();
    const healthy = url === "/live" ? true : connected;

    res.writeHead(healthy ? 200 : 503, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        status: healthy ? "ok" : "degraded",
        core: connected ? "connected" : "disconnected",
        bridges: procs.size,
        uptime: Math.floor(process.uptime()),
        timestamp: new Date().toISOString(),
      }),
    );
  });

  server.on("error", (err) => log(`Healthcheck failed on ${HEALTHCHECK_HOST}:${HEALTHCHECK_PORT}: ${err.message}`));
  server.listen(HEALTHCHECK_PORT, HEALTHCHECK_HOST, () => {
    log(`Healthcheck listening on ${HEALTHCHECK_HOST}:${HEALTHCHECK_PORT} (/health)`);
  });
}

function connect() {
  log(`Connecting to ${CORE_WS_URL}`);
  const ws = new WebSocket(CORE_WS_URL, {
    headers: SECRET ? { "x-runner-secret": SECRET } : {},
  });
  coreWs = ws;

  ws.on("open", () => {
    log("Connected to core");
    ws.send(JSON.stringify({ type: "hello", region: process.env.RUNNER_REGION ?? "" }));
  });

  ws.on("message", (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.type === "spawn") {
      const { channelId, env } = msg;
      stopProc(channelId, "replaced by new spawn");
      log(`Spawning bridge for channel ${channelId}`);

      const proc = spawn("node", [VOICE_SCRIPT], {
        env: { ...process.env, ...env },
        stdio: ["ignore", "inherit", "inherit", "ipc"],
      });

      procs.set(channelId, proc);

      const send = (/** @type {object} */ payload) => {
        if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(payload));
      };

      proc.on("message", (ipcMsg) => send({ type: "message", channelId, msg: ipcMsg }));
      proc.on("error", (err) => send({ type: "error", channelId, message: err.message }));
      proc.on("exit", (code) => {
        if (procs.get(channelId) === proc) {
          procs.delete(channelId);
        }
        if (suppressedExitProcs.has(proc)) {
          suppressedExitProcs.delete(proc);
          return;
        }
        send({ type: "exit", channelId, code });
      });
    } else if (msg.type === "kill") {
      stopProc(msg.channelId, "kill requested by core");
    }
  });

  ws.on("close", () => {
    for (const [channelId, proc] of procs) {
      try { proc.kill("SIGTERM"); } catch {}
      procs.delete(channelId);
    }
    log("Disconnected from core, reconnecting in 5s");
    setTimeout(connect, 5000);
  });

  ws.on("error", (err) => log(`Error: ${err.message}`));
}

connect();
