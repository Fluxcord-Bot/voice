//@ts-check
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

function connect() {
  log(`Connecting to ${CORE_WS_URL}`);
  const ws = new WebSocket(CORE_WS_URL, {
    headers: SECRET ? { "x-runner-secret": SECRET } : {},
  });

  ws.on("open", () => {
    log("Connected to core");
    ws.send(JSON.stringify({ type: "hello", region: process.env.RUNNER_REGION ?? "" }));
  });

  ws.on("message", (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.type === "spawn") {
      const { channelId, env } = msg;
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
      proc.on("exit", (code) => { procs.delete(channelId); send({ type: "exit", channelId, code }); });
    } else if (msg.type === "kill") {
      const proc = procs.get(msg.channelId);
      if (proc) {
        try { proc.kill("SIGTERM"); } catch {}
        procs.delete(msg.channelId);
      }
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
