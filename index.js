import {
  joinVoiceChannel,
  createAudioPlayer,
  createAudioResource,
  AudioPlayerStatus,
  EndBehaviorType,
  StreamType,
  VoiceConnectionStatus,
  entersState,
} from "@discordjs/voice";
import {
  Room,
  RoomEvent,
  AudioSource,
  LocalAudioTrack,
  AudioStream,
  TrackKind,
  TrackPublishOptions,
  TrackSource,
  AudioFrame,
} from "@livekit/rtc-node";
import DiscordOpus from "@discordjs/opus";
import prism from "prism-media";
import { PassThrough } from "node:stream";
const VERBOSE = process.env.VOICE_VERBOSE === "1";
const EXIT_STALE_ENDPOINT = 3;
const EXIT_STALE_TOKEN = 7;
function vlog(msg) {
  if (VERBOSE) log(msg);
}
function messageOf(error) {
  if (error instanceof Error) return error.message;
  return String(error);
}
function isStaleEndpointMessage(message) {
  const m = message.toLowerCase();
  return m.includes("enotfound") || m.includes("eai_again") || m.includes("getaddrinfo");
}
function isStaleTokenMessage(message) {
  const m = message.toLowerCase();
  return m.includes("401") || m.includes("unauthorized") || m.includes("expired") || m.includes("invalid token");
}
function withExitCode(error, code) {
  if (error instanceof Error) {
    error.exitCode = code;
  }
  return error;
}
function log(msg) {
  const time = new Date().toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  console.log(`[${time} VOICE] ${msg}`);
}
function formatError(error) {
  if (error instanceof AggregateError) {
    const parts = [];
    for (const inner of error.errors ?? []) {
      if (inner instanceof Error) {
        parts.push(inner.stack ?? inner.message);
      } else {
        parts.push(String(inner));
      }
    }
    return `AggregateError\n${parts.join("\n---\n")}`;
  }
  if (error instanceof Error) {
    return error.stack ?? error.message;
  }
  return String(error);
}
const {
  DISCORD_ENDPOINT = "",
  DISCORD_TOKEN = "",
  DISCORD_SESSION_ID = "",
  DISCORD_USER_ID = "",
  DISCORD_GUILD_ID = "",
  DISCORD_CHANNEL_ID = "",
  LIVEKIT_URL = "",
  LIVEKIT_TOKEN = "",
} = process.env;
function createStandaloneAdapter(endpoint, token, sessionId, guildId, userId) {
  return (methods) => {
    setImmediate(() => {
      methods.onVoiceStateUpdate({
        channel_id: DISCORD_CHANNEL_ID || null,
        guild_id: guildId,
        deaf: false,
        mute: false,
        self_deaf: false,
        self_mute: false,
        self_video: false,
        session_id: sessionId,
        suppress: false,
        user_id: userId,
        request_to_speak_timestamp: null,
      });
      methods.onVoiceServerUpdate({ token, guild_id: guildId, endpoint });
    });
    return { sendPayload: () => true, destroy: () => {} };
  };
}
const SAMPLE_RATE = 48_000;
const CHANNELS = 2;
const FRAME_SIZE = 960;
const FRAME_SAMPLES = FRAME_SIZE * CHANNELS;
const FRAME_DURATION_MS = 20;
const RECONNECT_TIMEOUT_MS = 45_000;
const MIN_BUFFER_DELAY = 3;
const STARTUP_BUFFER_DELAY = 4;
const MAX_BUFFER_DELAY = 8;
const MAX_BUFFER_DEPTH = 12;
const MAX_CONCEALMENT_FRAMES = 1;
const SPURT_GAP_MS = 100;
function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}
function assertNativeOpusAvailable() {
  const { OpusEncoder } = DiscordOpus;
  if (typeof OpusEncoder !== "function") {
    throw new Error("Native @discordjs/opus bindings are unavailable");
  }
  const encoder = new OpusEncoder(SAMPLE_RATE, CHANNELS);
  if (!encoder) {
    throw new Error("Native @discordjs/opus encoder initialization failed");
  }
  vlog("Using native @discordjs/opus codec");
}
function createMicrophonePublishOptions() {
  const options = new TrackPublishOptions();
  options.source = TrackSource.SOURCE_MICROPHONE;
  return options;
}
async function waitForDiscordReady(connection, timeoutMs, context) {
  try {
    await entersState(connection, VoiceConnectionStatus.Ready, timeoutMs);
  } catch (error) {
    if (
      error instanceof Error &&
      (error.name === "AbortError" || error.message.toLowerCase().includes("abort"))
    ) {
      const stuckAt = connection.state?.status ?? "unknown";
      throw new Error(
        `Timed out waiting for Discord voice Ready (${context}, ${timeoutMs}ms, stuck at ${stuckAt})`,
      );
    }
    throw error;
  }
}
class JitterBuffer {
  constructor(silenceFrame) {
    this.queue = [];
    this.silenceFrame = silenceFrame;
    this.lastFrame = silenceFrame;
    this.started = false;
    this.targetDelay = STARTUP_BUFFER_DELAY;
    this.lastArrivalAt = 0;
    this.jitterMs = 0;
    this.stableTicks = 0;
    this.concealmentFrames = 0;
  }
  push(frame) {
    const now = Date.now();
    if (this.lastArrivalAt !== 0) {
      const gap = now - this.lastArrivalAt;
      if (gap > SPURT_GAP_MS) {
        this.jitterMs = 0;
        this.targetDelay = MIN_BUFFER_DELAY;
        this.stableTicks = 0;
      } else {
        const deviation = Math.abs(gap - FRAME_DURATION_MS);
        this.jitterMs = this.jitterMs === 0 ? deviation : (this.jitterMs * 0.8) + (deviation * 0.2);
        const adaptiveDelay = clamp(
          STARTUP_BUFFER_DELAY + Math.ceil(this.jitterMs / FRAME_DURATION_MS),
          MIN_BUFFER_DELAY,
          MAX_BUFFER_DELAY,
        );
        if (adaptiveDelay > this.targetDelay) {
          this.targetDelay = adaptiveDelay;
          this.stableTicks = 0;
        }
      }
    }
    this.lastArrivalAt = now;
    if (this.queue.length >= MAX_BUFFER_DEPTH) {
      this.queue.shift();
      this.started = true;
      this.stableTicks = 0;
    }
    this.queue.push(frame);
    this.concealmentFrames = 0;
  }
  pull() {
    if (!this.started) {
      if (this.queue.length < this.targetDelay) return null;
      this.started = true;
    }
    const frame = this.queue.shift();
    if (frame) {
      if (this.queue.length > this.targetDelay) {
        this.stableTicks += 1;
        if (this.stableTicks >= 50 && this.targetDelay > MIN_BUFFER_DELAY) {
          this.targetDelay -= 1;
          this.stableTicks = 0;
        }
      } else {
        this.stableTicks = 0;
      }
      this.lastFrame = frame;
      this.concealmentFrames = 0;
      return frame;
    }
    if (this.concealmentFrames < MAX_CONCEALMENT_FRAMES) {
      this.concealmentFrames += 1;
      return concealFrame(this.lastFrame, this.silenceFrame);
    }
    this.started = false;
    this.stableTicks = 0;
    this.concealmentFrames = 0;
    return this.silenceFrame;
  }
}
function concealFrame(frame, silenceFrame) {
  if (frame === silenceFrame) return silenceFrame;
  const concealed = new Int16Array(frame.length);
  const n = frame.length;
  for (let i = 0; i < n; i++) {
    concealed[i] = Math.trunc(frame[i] * (1 - (i + 1) / n));
  }
  return concealed;
}
function mixFrames(frames) {
  const out = new Int16Array(FRAME_SAMPLES);
  const gain = frames.length > 1 ? 1 / frames.length : 1;
  for (const f of frames) {
    for (let i = 0; i < FRAME_SAMPLES; i++) {
      out[i] = Math.max(-32768, Math.min(32767, out[i] + (f[i] ?? 0) * gain));
    }
  }
  return out;
}
async function main() {
  assertNativeOpusAvailable();
  const INITIAL_JOIN_ATTEMPTS = 3;
  const INITIAL_JOIN_TIMEOUT_MS = 15_000;
  function createDiscordConnection() {
    return joinVoiceChannel({
      channelId: DISCORD_CHANNEL_ID,
      guildId: DISCORD_GUILD_ID,
      adapterCreator: createStandaloneAdapter(
        DISCORD_ENDPOINT,
        DISCORD_TOKEN,
        DISCORD_SESSION_ID,
        DISCORD_GUILD_ID,
        DISCORD_USER_ID,
      ),
      selfDeaf: false,
      selfMute: false,
    });
  }
  let discordConnection = createDiscordConnection();
  discordConnection.on("stateChange", (oldState, newState) => {
    if (oldState.status !== newState.status) {
      log(`Discord voice state ${oldState.status} -> ${newState.status}`);
    }
  });
  let joined = false;
  for (let attempt = 1; attempt <= INITIAL_JOIN_ATTEMPTS; attempt += 1) {
    try {
      await waitForDiscordReady(
        discordConnection,
        INITIAL_JOIN_TIMEOUT_MS,
        `initial connect attempt ${attempt}/${INITIAL_JOIN_ATTEMPTS}`,
      );
      joined = true;
      break;
    } catch (error) {
      const message = messageOf(error);
      log(`Discord voice join failed: ${message}`);
      if (isStaleEndpointMessage(message)) throw withExitCode(error, EXIT_STALE_ENDPOINT);
      if (attempt === INITIAL_JOIN_ATTEMPTS) throw error;
      try {
        discordConnection.destroy(false);
      } catch {}
      await new Promise((resolve) => setTimeout(resolve, 1000 * attempt));
      discordConnection.removeAllListeners("stateChange");
      discordConnection = createDiscordConnection();
      discordConnection.on("stateChange", (oldState, newState) => {
        if (oldState.status !== newState.status) {
          log(`Discord voice state ${oldState.status} -> ${newState.status}`);
        }
      });
    }
  }
  if (!joined) throw new Error("Discord voice join exhausted all attempts");
  log("Discord voice connected");
  let reconnectingDiscordVoice = false;
  let discordRecoveryTimer = null;
  discordConnection.removeAllListeners("stateChange");
  discordConnection.on("stateChange", async (oldState, newState) => {
    if (oldState.status !== newState.status) {
      log(`Discord voice state ${oldState.status} -> ${newState.status}`);
    }
    if (newState.status === VoiceConnectionStatus.Ready) {
      if (discordRecoveryTimer) {
        clearTimeout(discordRecoveryTimer);
        discordRecoveryTimer = null;
      }
    } else if (
      newState.status === VoiceConnectionStatus.Connecting ||
      newState.status === VoiceConnectionStatus.Signalling ||
      newState.status === VoiceConnectionStatus.Disconnected
    ) {
      if (!discordRecoveryTimer) {
        discordRecoveryTimer = setTimeout(() => {
          log("Discord voice did not recover in time, restarting bridge");
          process.exit(4);
        }, RECONNECT_TIMEOUT_MS);
      }
    }
    if (newState.status !== VoiceConnectionStatus.Disconnected || reconnectingDiscordVoice) return;
    reconnectingDiscordVoice = true;
    try {
      log("Discord voice disconnected, attempting in-process rejoin");
      const rejoinAccepted = discordConnection.rejoin();
      if (!rejoinAccepted) {
        throw new Error("Voice connection rejected rejoin");
      }
      await waitForDiscordReady(discordConnection, 15_000, "rejoin");
      log("Discord voice rejoined");
    } catch (error) {
      const message = messageOf(error);
      log(`Discord voice rejoin failed: ${message}`);
      if (isStaleEndpointMessage(message)) {
        withExitCode(error, EXIT_STALE_ENDPOINT);
      }
      process.exit(error.exitCode ?? 2);
    } finally {
      reconnectingDiscordVoice = false;
    }
  });
  const room = new Room();
  const lkSource = new AudioSource(SAMPLE_RATE, CHANNELS);
  const lkTrack = LocalAudioTrack.createAudioTrack("discord-audio", lkSource);
  let livekitConnected = false;
  let livekitTrackPublished = false;
  const receiver = discordConnection.receiver;
  const activeSubscriptions = new Set();
  const subscriptionPipelines = new Map();
  const SILENCE = new Int16Array(FRAME_SAMPLES);
  const discordBuffers = new Map();
  const discordAccum = new Map();
  const subscriptionIdleTimers = new Map();
  function cancelSubscriptionCleanup(userId) {
    const timer = subscriptionIdleTimers.get(userId);
    if (!timer) return;
    clearTimeout(timer);
    subscriptionIdleTimers.delete(userId);
  }
  function ensureDiscordSubscription(userId) {
    if (!userId || userId === DISCORD_USER_ID) return;
    cancelSubscriptionCleanup(userId);
    if (activeSubscriptions.has(userId)) return;
    activeSubscriptions.add(userId);
    discordBuffers.set(userId, new JitterBuffer(SILENCE));
    vlog(`Subscribed to Discord audio: ${userId}`);
    const opusStream = receiver.subscribe(userId, {
      end: { behavior: EndBehaviorType.Manual },
    });
    const decoder = new prism.opus.Decoder({
      rate: SAMPLE_RATE,
      channels: CHANNELS,
      frameSize: FRAME_SIZE,
    });
    subscriptionPipelines.set(userId, { opusStream, decoder, cleanedUp: false });
    opusStream.pipe(decoder);
    decoder.on("data", (pcm) => {
      const buffer = discordBuffers.get(userId);
      if (!buffer) return;
      const incoming = new Int16Array(pcm.buffer, pcm.byteOffset, pcm.byteLength / 2);
      let combined = discordAccum.get(userId) ?? new Int16Array(0);
      const merged = new Int16Array(combined.length + incoming.length);
      merged.set(combined);
      merged.set(incoming, combined.length);
      combined = merged;
      while (combined.length >= FRAME_SAMPLES) {
        buffer.push(combined.slice(0, FRAME_SAMPLES));
        combined = combined.slice(FRAME_SAMPLES);
      }
      discordAccum.set(userId, combined);
    });
    const cleanup = () => cleanupDiscordSubscription(userId);
    decoder.once("close", cleanup);
    decoder.once("error", cleanup);
    opusStream.once("close", cleanup);
    opusStream.once("end", cleanup);
    opusStream.once("error", cleanup);
  }
  function hydrateDiscordSpeakingUsers() {
    for (const userId of receiver.speaking.users.keys()) {
      ensureDiscordSubscription(userId);
    }
  }
  function cleanupDiscordSubscription(userId) {
    const pipeline = subscriptionPipelines.get(userId);
    if (pipeline) {
      if (pipeline.cleanedUp) return;
      pipeline.cleanedUp = true;
      subscriptionPipelines.delete(userId);
      pipeline.opusStream.unpipe(pipeline.decoder);
      pipeline.opusStream.destroy();
      pipeline.decoder.destroy();
    }
    activeSubscriptions.delete(userId);
    discordBuffers.delete(userId);
    cancelSubscriptionCleanup(userId);
    discordAccum.delete(userId);
  }
  receiver.speaking.on("start", (userId) => {
    ensureDiscordSubscription(userId);
  });
  receiver.speaking.on("end", (userId) => {
    cancelSubscriptionCleanup(userId);
    subscriptionIdleTimers.set(
      userId,
      setTimeout(() => {
        subscriptionIdleTimers.delete(userId);
        if (receiver.speaking.users.has(userId)) return;
        cleanupDiscordSubscription(userId);
      }, 30_000),
    );
  });
  const discordPlayer = createAudioPlayer();
  discordConnection.subscribe(discordPlayer);
  let pcmPassthrough = new PassThrough();
  function startPlaying() {
    pcmPassthrough = new PassThrough();
    discordPlayer.play(createAudioResource(pcmPassthrough, { inputType: StreamType.Raw }));
  }
  discordPlayer.on("stateChange", (_, next) => {
    if (next.status === AudioPlayerStatus.Idle) startPlaying();
  });
  startPlaying();
  const audioReaders = new Map();
  const fluxerBuffers = new Map();
  const fluxerAccum = new Map();
  let lkReady = false;
  let reconnectingFluxerRoom = false;
  function resetFluxerRemoteAudioState() {
    for (const [, reader] of audioReaders) {
      try {
        void reader.cancel();
      } catch {}
    }
    audioReaders.clear();
    fluxerBuffers.clear();
    fluxerAccum.clear();
  }
  async function ensureFluxerTrackPublished(reason) {
    const participant = room.localParticipant;
    if (!participant) {
      lkReady = false;
      log(`Can't publish Discord audio to Fluxer yet (${reason})`);
      return false;
    }
    if (livekitTrackPublished) {
      lkReady = true;
      log(`Discord audio already published (${reason})`);
      return true;
    }
    try {
      await participant.publishTrack(lkTrack, createMicrophonePublishOptions());
      livekitTrackPublished = true;
      lkReady = true;
      log(`Published Discord audio to Fluxer (${reason})`);
      return true;
    } catch (error) {
      lkReady = false;
      const message = error instanceof Error ? error.message : String(error);
      log(`Couldn't publish Discord audio to Fluxer (${reason}): ${message}`);
      return false;
    }
  }
  function attachFluxerAudioTrack(track, participant, source, reason) {
    if (track.kind !== TrackKind.KIND_AUDIO) return;
    if (source === TrackSource.SOURCE_SCREENSHARE_AUDIO || source === TrackSource.SOURCE_SCREENSHARE) return;
    if (!track.sid || audioReaders.has(track.sid)) return;
    vlog(`Subscribed to audio: ${participant.identity} (${source})`);
    const audioStream = new AudioStream(track, SAMPLE_RATE, CHANNELS);
    const reader = audioStream.getReader();
    audioReaders.set(track.sid, reader);
    fluxerAccum.set(track.sid, new Int16Array(0));
    fluxerBuffers.set(track.sid, new JitterBuffer(SILENCE));
    const sid = track.sid;
    (async () => {
      try {
        while (true) {
          const { done, value: frame } = await reader.read();
          if (done) break;
          const incoming = frame.data;
          const prev = fluxerAccum.get(sid) ?? new Int16Array(0);
          let combined = new Int16Array(prev.length + incoming.length);
          combined.set(prev);
          combined.set(incoming, prev.length);
          while (combined.length >= FRAME_SAMPLES) {
            const buffer = fluxerBuffers.get(sid);
            if (buffer) {
              buffer.push(combined.slice(0, FRAME_SAMPLES));
            }
            combined = combined.slice(FRAME_SAMPLES);
          }
          fluxerAccum.set(sid, combined);
        }
      } catch {}
      fluxerBuffers.delete(sid);
      fluxerAccum.delete(sid);
      audioReaders.delete(sid);
    })();
  }
  function hydrateFluxerAudioTracks(reason) {
    for (const participant of room.remoteParticipants.values()) {
      for (const publication of participant.trackPublications.values()) {
        if (publication.kind !== TrackKind.KIND_AUDIO) continue;
        if (!publication.subscribed) {
          publication.setSubscribed(true);
          vlog(`Requested audio subscription for ${participant.identity} (${reason})`);
        }
        if (publication.track) {
          attachFluxerAudioTrack(publication.track, participant, publication.source ?? TrackSource.SOURCE_MICROPHONE, reason);
        }
      }
    }
  }
  let mixTimer = null;
  let livekitRecoveryTimer = null;
  let shuttingDown = false;
  let nextMixAt = Date.now() + FRAME_DURATION_MS;
  const discordSpeakingSweep = setInterval(hydrateDiscordSpeakingUsers, 1000);
  const mixTick = () => {
    if (lkReady) {
      const dFrames = [];
      for (const [, buffer] of discordBuffers) {
        const frame = buffer.pull();
        if (frame) dFrames.push(frame);
      }
      const mixed = dFrames.length > 0 ? mixFrames(dFrames) : SILENCE;
      lkSource.captureFrame(new AudioFrame(mixed, SAMPLE_RATE, CHANNELS, FRAME_SIZE)).catch(() => {});
    }
    if (room.remoteParticipants.size > 0) {
      const fFrames = [];
      for (const [, buffer] of fluxerBuffers) {
        const frame = buffer.pull();
        if (frame) fFrames.push(frame);
      }
      const mixed = fFrames.length > 0 ? mixFrames(fFrames) : SILENCE;
      const buf = Buffer.from(mixed.buffer, mixed.byteOffset, mixed.byteLength);
      if (!pcmPassthrough.writableEnded) pcmPassthrough.write(buf);
    }
    nextMixAt += FRAME_DURATION_MS;
    const now = Date.now();
    if (nextMixAt < now - FRAME_DURATION_MS * 2) nextMixAt = now + FRAME_DURATION_MS;
    mixTimer = setTimeout(mixTick, Math.max(0, nextMixAt - now));
  };
  mixTimer = setTimeout(mixTick, FRAME_DURATION_MS);
  room.on(RoomEvent.TrackSubscribed, (track, pub, participant) => {
    attachFluxerAudioTrack(track, participant, pub.source ?? TrackSource.SOURCE_MICROPHONE, "track event");
  });
  room.on(RoomEvent.TrackUnsubscribed, (track) => {
    if (!track.sid) return;
    const reader = audioReaders.get(track.sid);
    if (reader) {
      reader.cancel();
      audioReaders.delete(track.sid);
    }
    fluxerBuffers.delete(track.sid);
    fluxerAccum.delete(track.sid);
  });
  room.on(RoomEvent.LocalTrackPublished, () => {
    livekitTrackPublished = true;
    log("Fluxer accepted the Discord audio track");
  });
  room.on(RoomEvent.LocalTrackUnpublished, () => {
    livekitTrackPublished = false;
    lkReady = false;
    log("Fluxer unpublished the Discord audio track");
  });
  room.on(RoomEvent.Connected, () => {
    livekitConnected = true;
    log(
      `Fluxer room connected (room=${room.name} participants=${room.remoteParticipants.size} remote participant${room.remoteParticipants.size === 1 ? "" : "s"})`,
    );
  });
  room.on(RoomEvent.Reconnecting, () => {
    livekitConnected = false;
    livekitTrackPublished = false;
    lkReady = false;
    resetFluxerRemoteAudioState();
    if (livekitRecoveryTimer) clearTimeout(livekitRecoveryTimer);
    livekitRecoveryTimer = null;
    log("Fluxer room reconnecting");
  });
  room.on(RoomEvent.Reconnected, async () => {
    if (livekitRecoveryTimer) {
      clearTimeout(livekitRecoveryTimer);
      livekitRecoveryTimer = null;
    }
    livekitConnected = true;
    log(
      `Fluxer room reconnected (${room.remoteParticipants.size} remote participant${room.remoteParticipants.size === 1 ? "" : "s"})`,
    );
    hydrateFluxerAudioTracks("reconnect");
    if (!(await ensureFluxerTrackPublished("reconnect"))) {
      log("Failed to restore Discord audio track after LiveKit reconnect");
    }
  });
  room.on(RoomEvent.Disconnected,  (reason) => {
    if (livekitRecoveryTimer) {
      clearTimeout(livekitRecoveryTimer);
      livekitRecoveryTimer = null;
    }
    livekitConnected = false;
    livekitTrackPublished = false;
    lkReady = false;
    resetFluxerRemoteAudioState();
    const detail = reason ? `: ${reason}` : "";
    log(`Fluxer room disconnected${detail}`);
    if (shuttingDown || reconnectingFluxerRoom) return;
    reconnectingFluxerRoom = true;
    void (async () => {
      try {
        log("Attempting in-process Fluxer room reconnect after hard disconnect");
        await room.connect(LIVEKIT_URL, LIVEKIT_TOKEN, { autoSubscribe: true });
        hydrateFluxerAudioTracks("hard reconnect");
        if (!(await ensureFluxerTrackPublished("hard reconnect"))) {
          throw new Error("Failed to restore Discord audio track after hard Fluxer reconnect");
        }
        livekitConnected = true;
        log(
          `Fluxer room reconnected after hard disconnect (${room.remoteParticipants.size} remote participant${room.remoteParticipants.size === 1 ? "" : "s"})`,
        );
      } catch (error) {
        const message = messageOf(error);
        log(`Fluxer reconnect failed: ${message}`);
        if (isStaleTokenMessage(message)) {
          process.exit(EXIT_STALE_TOKEN);
        }
        process.exit(6);
      } finally {
        reconnectingFluxerRoom = false;
      }
    })();
  });
  log(`Connecting to Fluxer at ${LIVEKIT_URL}`);
  try {
    await room.connect(LIVEKIT_URL, LIVEKIT_TOKEN, { autoSubscribe: true });
  } catch (error) {
    const message = messageOf(error);
    log(`Fluxer connect failed: ${message}`);
    if (isStaleTokenMessage(message)) throw withExitCode(error, EXIT_STALE_TOKEN);
    throw error;
  }
  hydrateFluxerAudioTracks("initial connect");
  livekitConnected = true;
  if (!(await ensureFluxerTrackPublished("initial connect"))) {
    throw new Error("Failed to publish Discord audio track to Fluxer");
  }
  log("Fluxer connected");
  process.send?.("bridge-ready");
  let startupTimeout = null;
  async function shutdown() {
    if (shuttingDown) return;
    shuttingDown = true;
    log("Shutting down");
    if (mixTimer) clearTimeout(mixTimer);
    if (livekitRecoveryTimer) clearTimeout(livekitRecoveryTimer);
    if (discordRecoveryTimer) clearTimeout(discordRecoveryTimer);
    clearInterval(discordSpeakingSweep);
    for (const timer of subscriptionIdleTimers.values()) clearTimeout(timer);
    subscriptionIdleTimers.clear();
    if (startupTimeout) clearTimeout(startupTimeout);
    for (const userId of [...activeSubscriptions]) {
      cleanupDiscordSubscription(userId);
    }
    discordConnection.destroy();
    await room.disconnect();
    process.exit(0);
  }
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
  startupTimeout = setTimeout(() => {
    if (livekitConnected && room.remoteParticipants.size === 0) process.send?.("fluxer-empty");
  }, 10_000);
  room.on(RoomEvent.ParticipantConnected, () => {
    clearTimeout(startupTimeout);
    if (livekitConnected) {
      log(`Fluxer participant count is now ${room.remoteParticipants.size}`);
    }
    process.send?.("fluxer-joined");
  });
  room.on(RoomEvent.ParticipantDisconnected, () => {
    if (livekitConnected) {
      log(`Fluxer participant count is now ${room.remoteParticipants.size}`);
    }
    if (livekitConnected && room.remoteParticipants.size === 0) {
      log("All Fluxer participants left");
      process.send?.("fluxer-empty");
    }
  });
}
main().catch((e) => {
  log(`Fatal: ${formatError(e)}`);
  process.exit(e && typeof e.exitCode === "number" ? e.exitCode : 1);
});
