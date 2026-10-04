import http from "node:http";
import { WebSocket } from "ws";
import { spawn } from "child_process";
import { fileURLToPath } from "url";
import path from "path";
const CORE_WS_URL = process.env.CORE_WS_URL ?? "ws://localhost:8765";
const SECRET = process.env.RUNNER_SECRET ?? "";
const VOICE_SCRIPT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "index.js");
const MAX_BRIDGES = Number(process.env.MAX_BRIDGES ?? 60) || 60;
const SPAWN_GAP_MS = Number(process.env.SPAWN_GAP_MS ?? 1000) || 1000;
const BACKOFF_BASE_MS = Number(process.env.BACKOFF_BASE_MS ?? 5000) || 5000;
const BACKOFF_MAX_MS = Number(process.env.BACKOFF_MAX_MS ?? 300000) || 300000;
const bridgeMeta = new Map();
const spawnQueue = [];
let spawnPumpActive = false;
let lastSpawnAt = 0;
function getMeta(channelId) {
  let meta = bridgeMeta.get(channelId);
  if (!meta) {
    meta = { failures: 0, notBefore: 0 };
    bridgeMeta.set(channelId, meta);
  }
  return meta;
}
function recordResult(channelId, code) {
  const meta = getMeta(channelId);
  if (code === 0 || code === null) {
    meta.failures = 0;
    meta.notBefore = 0;
  } else {
    meta.failures += 1;
    const delay = Math.min(BACKOFF_MAX_MS, BACKOFF_BASE_MS * (2 ** Math.min(meta.failures - 1, 6)));
    meta.notBefore = Date.now() + delay;
  }
}
function pumpSpawnQueue(ws) {
  if (spawnPumpActive) return;
  spawnPumpActive = true;
  const tick = () => {
    if (spawnQueue.length === 0) {
      spawnPumpActive = false;
      return;
    }
    const now = Date.now();
    const gapWait = lastSpawnAt + SPAWN_GAP_MS - now;
    if (gapWait > 0) {
      setTimeout(tick, gapWait);
      return;
    }
    const item = spawnQueue[0];
    const meta = getMeta(item.channelId);
    if (meta.notBefore > now) {
      spawnQueue.shift();
      spawnQueue.push(item);
      setTimeout(tick, Math.min(meta.notBefore - now, 5000));
      return;
    }
    if (procs.size >= MAX_BRIDGES) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "busy", channelId: item.channelId }));
      }
      spawnQueue.shift();
      setTimeout(tick, SPAWN_GAP_MS);
      return;
    }
    spawnQueue.shift();
    lastSpawnAt = Date.now();
    doSpawn(ws, item.channelId, item.env);
    setTimeout(tick, 0);
  };
  tick();
}
function doSpawn(ws, channelId, env) {
  stopProc(channelId, "replaced by new spawn");
  log(`Spawning bridge for channel ${channelId}`);
  const proc = spawn("node", [VOICE_SCRIPT], {
    env: { ...process.env, ...env },
    stdio: ["ignore", "inherit", "inherit", "ipc"],
  });
  procs.set(channelId, proc);
  const send = (payload) => {
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
    recordResult(channelId, code);
    send({ type: "exit", channelId, code });
  });
}
function log(msg) {
  const time = new Date().toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  console.log(`[${time} RUNNER] ${msg}`);
}
const procs = new Map();
const suppressedExitProcs = new WeakSet();
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
      for (let i = spawnQueue.length - 1; i >= 0; i -= 1) {
        if (spawnQueue[i].channelId === channelId) spawnQueue.splice(i, 1);
      }
      spawnQueue.push({ channelId, env });
      pumpSpawnQueue(ws);
    } else if (msg.type === "kill") {
      const meta = getMeta(msg.channelId);
      meta.failures = 0;
      meta.notBefore = 0;
      for (let i = spawnQueue.length - 1; i >= 0; i -= 1) {
        if (spawnQueue[i].channelId === msg.channelId) spawnQueue.splice(i, 1);
      }
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
