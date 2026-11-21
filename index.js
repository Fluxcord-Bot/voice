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
  LIVEKIT_ROOMS = "",
} = process.env;
function parseLivekitRooms() {
  if (LIVEKIT_ROOMS) {
    try {
      const parsed = JSON.parse(LIVEKIT_ROOMS);
      if (Array.isArray(parsed)) {
        const rooms = parsed
          .filter(entry => entry && entry.url && entry.token)
          .map((entry, index) => ({
            url: entry.url,
            token: entry.token,
            label: entry.instanceId || String(index + 1),
          }));
        if (rooms.length > 0) return rooms;
      }
    } catch (error) {
      log(`Invalid LIVEKIT_ROOMS: ${messageOf(error)}`);
    }
  }
  return [{ url: LIVEKIT_URL, token: LIVEKIT_TOKEN, label: "default" }];
}
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
  const roomConfigs = parseLivekitRooms();
  const roomStates = roomConfigs.map((config, index) => {
    const source = new AudioSource(SAMPLE_RATE, CHANNELS);
    return {
      ...config,
      index,
      room: new Room(),
      source,
      track: LocalAudioTrack.createAudioTrack(`discord-audio-${index}`, source),
      connected: false,
      trackPublished: false,
      ready: false,
      reconnecting: false,
      recoveryTimer: null,
    };
  });
  const anyRoomWithParticipants = () =>
    roomStates.some(state => state.room.remoteParticipants.size > 0);
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
    log(`Subscribed to Discord audio: ${userId}`);
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
  function trackKey(state, sid) {
    return `${state.index}:${sid}`;
  }
  function resetFluxerRemoteAudioState(state) {
    const prefix = state ? `${state.index}:` : "";
    for (const [key, reader] of [...audioReaders]) {
      if (!key.startsWith(prefix)) continue;
      try {
        void reader.cancel();
      } catch {}
      audioReaders.delete(key);
      fluxerBuffers.delete(key);
      fluxerAccum.delete(key);
    }
  }
  async function ensureFluxerTrackPublished(state, reason) {
    const participant = state.room.localParticipant;
    if (!participant) {
      state.ready = false;
      log(`[${state.label}] Can't publish Discord audio to Fluxer yet (${reason})`);
      return false;
    }
    if (state.trackPublished) {
      state.ready = true;
      return true;
    }
    try {
      await participant.publishTrack(state.track, createMicrophonePublishOptions());
      state.trackPublished = true;
      state.ready = true;
      log(`[${state.label}] Published Discord audio to Fluxer (${reason})`);
      return true;
    } catch (error) {
      state.ready = false;
      log(`[${state.label}] Couldn't publish Discord audio to Fluxer (${reason}): ${messageOf(error)}`);
      return false;
    }
  }
  function attachFluxerAudioTrack(state, track, participant, source, reason) {
    if (track.kind !== TrackKind.KIND_AUDIO) return;
    if (source === TrackSource.SOURCE_SCREENSHARE_AUDIO || source === TrackSource.SOURCE_SCREENSHARE) return;
    if (!track.sid) return;
    const key = trackKey(state, track.sid);
    if (audioReaders.has(key)) return;
    log(`[${state.label}] Subscribed to audio: ${participant.identity} (${source})`);
    const audioStream = new AudioStream(track, SAMPLE_RATE, CHANNELS);
    const reader = audioStream.getReader();
    audioReaders.set(key, reader);
    fluxerAccum.set(key, new Int16Array(0));
    fluxerBuffers.set(key, new JitterBuffer(SILENCE));
    (async () => {
      try {
        while (true) {
          const { done, value: frame } = await reader.read();
          if (done) break;
          if (!state.fluxerAudioSeen) {
            state.fluxerAudioSeen = true;
            log(`[${state.label}] Receiving Fluxer audio from ${participant.identity}`);
          }
          const incoming = frame.data;
          const prev = fluxerAccum.get(key) ?? new Int16Array(0);
          let combined = new Int16Array(prev.length + incoming.length);
          combined.set(prev);
          combined.set(incoming, prev.length);
          while (combined.length >= FRAME_SAMPLES) {
            const buffer = fluxerBuffers.get(key);
            if (buffer) {
              buffer.push(combined.slice(0, FRAME_SAMPLES));
            }
            combined = combined.slice(FRAME_SAMPLES);
          }
          fluxerAccum.set(key, combined);
        }
      } catch {}
      fluxerBuffers.delete(key);
      fluxerAccum.delete(key);
      audioReaders.delete(key);
    })();
  }
  function hydrateFluxerAudioTracks(state, reason) {
    for (const participant of state.room.remoteParticipants.values()) {
      for (const publication of participant.trackPublications.values()) {
        if (publication.kind !== TrackKind.KIND_AUDIO) continue;
        if (!publication.subscribed) {
          publication.setSubscribed(true);
          vlog(`[${state.label}] Requested audio subscription for ${participant.identity} (${reason})`);
        }
        if (publication.track) {
          attachFluxerAudioTrack(
            state,
            publication.track,
            participant,
            publication.source ?? TrackSource.SOURCE_MICROPHONE,
            reason
          );
        }
      }
    }
  }
  let mixTimer = null;
  let shuttingDown = false;
  let nextMixAt = Date.now() + FRAME_DURATION_MS;
  const discordSpeakingSweep = setInterval(hydrateDiscordSpeakingUsers, 1000);
  let discordAudioSeen = false;
  let fluxerPlaybackSeen = false;
  const mixTick = () => {
    const dFrames = [];
    for (const [, buffer] of discordBuffers) {
      const frame = buffer.pull();
      if (frame) dFrames.push(frame);
    }
    if (dFrames.length > 0 && !discordAudioSeen) {
      discordAudioSeen = true;
      log(`Receiving Discord audio from ${dFrames.length} speaker${dFrames.length === 1 ? "" : "s"}`);
    }
    const discordMixed = dFrames.length > 0 ? mixFrames(dFrames) : SILENCE;
    const fluxerFramesByRoom = new Map();
    for (const [key, buffer] of fluxerBuffers) {
      const frame = buffer.pull();
      if (!frame) continue;
      const roomIndex = Number(key.slice(0, key.indexOf(":")));
      let frames = fluxerFramesByRoom.get(roomIndex);
      if (!frames) {
        frames = [];
        fluxerFramesByRoom.set(roomIndex, frames);
      }
      frames.push(frame);
    }
    for (const state of roomStates) {
      if (!state.ready) continue;
      const parts = [];
      if (dFrames.length > 0) parts.push(discordMixed);
      for (const [roomIndex, frames] of fluxerFramesByRoom) {
        if (roomIndex === state.index) continue;
        parts.push(...frames);
      }
      const outgoing = parts.length > 0 ? mixFrames(parts) : SILENCE;
      state.source
        .captureFrame(new AudioFrame(outgoing.slice(), SAMPLE_RATE, CHANNELS, FRAME_SIZE))
        .catch(error => {
          if (state.captureErrorLogged) return;
          state.captureErrorLogged = true;
          log(`[${state.label}] captureFrame failed: ${messageOf(error)}`);
        });
    }
    const allFluxerFrames = [...fluxerFramesByRoom.values()].flat();
    if (allFluxerFrames.length > 0 && !fluxerPlaybackSeen) {
      fluxerPlaybackSeen = true;
      log(`Playing Fluxer audio to Discord (${allFluxerFrames.length} stream${allFluxerFrames.length === 1 ? "" : "s"})`);
    }
    if (anyRoomWithParticipants()) {
      const mixed = allFluxerFrames.length > 0 ? mixFrames(allFluxerFrames) : SILENCE;
      const buf = Buffer.from(mixed.buffer, mixed.byteOffset, mixed.byteLength);
      if (!pcmPassthrough.writableEnded) pcmPassthrough.write(buf);
    }
    nextMixAt += FRAME_DURATION_MS;
    const now = Date.now();
    if (nextMixAt < now - FRAME_DURATION_MS * 2) nextMixAt = now + FRAME_DURATION_MS;
    mixTimer = setTimeout(mixTick, Math.max(0, nextMixAt - now));
  };
  mixTimer = setTimeout(mixTick, FRAME_DURATION_MS);
  function wireRoom(state) {
    const room = state.room;
    room.on(RoomEvent.TrackSubscribed, (track, pub, participant) => {
      attachFluxerAudioTrack(state, track, participant, pub.source ?? TrackSource.SOURCE_MICROPHONE, "track event");
    });
    room.on(RoomEvent.TrackUnsubscribed, (track) => {
      if (!track.sid) return;
      const key = trackKey(state, track.sid);
      const reader = audioReaders.get(key);
      if (reader) {
        reader.cancel();
        audioReaders.delete(key);
      }
      fluxerBuffers.delete(key);
      fluxerAccum.delete(key);
    });
    room.on(RoomEvent.TrackPublished, (publication, participant) => {
      if (publication.kind !== TrackKind.KIND_AUDIO) return;
      if (!publication.subscribed) publication.setSubscribed(true);
      if (publication.track) {
        attachFluxerAudioTrack(
          state,
          publication.track,
          participant,
          publication.source ?? TrackSource.SOURCE_MICROPHONE,
          "track published"
        );
      }
    });
    room.on(RoomEvent.TrackSubscriptionFailed, (trackSid, participant, reason) => {
      log(`[${state.label}] Track subscription failed for ${participant?.identity ?? trackSid}: ${reason ?? "unknown"}`);
    });
    room.on(RoomEvent.LocalTrackPublished, () => {
      state.trackPublished = true;
      log(`[${state.label}] Fluxer accepted the Discord audio track`);
    });
    room.on(RoomEvent.LocalTrackUnpublished, () => {
      state.trackPublished = false;
      state.ready = false;
      log(`[${state.label}] Fluxer unpublished the Discord audio track`);
    });
    room.on(RoomEvent.Connected, () => {
      state.connected = true;
      log(
        `[${state.label}] Fluxer room connected (participants=${room.remoteParticipants.size})`,
      );
    });
    room.on(RoomEvent.Reconnecting, () => {
      state.connected = false;
      state.trackPublished = false;
      state.ready = false;
      resetFluxerRemoteAudioState(state);
      if (state.recoveryTimer) clearTimeout(state.recoveryTimer);
      state.recoveryTimer = null;
      log(`[${state.label}] Fluxer room reconnecting`);
    });
    room.on(RoomEvent.Reconnected, async () => {
      if (state.recoveryTimer) {
        clearTimeout(state.recoveryTimer);
        state.recoveryTimer = null;
      }
      state.connected = true;
      log(
        `[${state.label}] Fluxer room reconnected (${room.remoteParticipants.size} remote participant${room.remoteParticipants.size === 1 ? "" : "s"})`,
      );
      hydrateFluxerAudioTracks(state, "reconnect");
      if (!(await ensureFluxerTrackPublished(state, "reconnect"))) {
        log(`[${state.label}] Failed to restore Discord audio track after LiveKit reconnect`);
      }
    });
    room.on(RoomEvent.Disconnected, (reason) => {
      if (state.recoveryTimer) {
        clearTimeout(state.recoveryTimer);
        state.recoveryTimer = null;
      }
      state.connected = false;
      state.trackPublished = false;
      state.ready = false;
      resetFluxerRemoteAudioState(state);
      const detail = reason ? `: ${reason}` : "";
      log(`[${state.label}] Fluxer room disconnected${detail}`);
      if (shuttingDown || state.reconnecting) return;
      state.reconnecting = true;
      void (async () => {
        try {
          log(`[${state.label}] Attempting in-process Fluxer room reconnect after hard disconnect`);
          await room.connect(state.url, state.token, { autoSubscribe: true });
          hydrateFluxerAudioTracks(state, "hard reconnect");
          if (!(await ensureFluxerTrackPublished(state, "hard reconnect"))) {
            throw new Error("Failed to restore Discord audio track after hard Fluxer reconnect");
          }
          state.connected = true;
          log(
            `[${state.label}] Fluxer room reconnected after hard disconnect (${room.remoteParticipants.size} remote participant${room.remoteParticipants.size === 1 ? "" : "s"})`,
          );
        } catch (error) {
          const message = messageOf(error);
          log(`[${state.label}] Fluxer reconnect failed: ${message}`);
          if (isStaleTokenMessage(message)) {
            process.exit(EXIT_STALE_TOKEN);
          }
          process.exit(6);
        } finally {
          state.reconnecting = false;
        }
      })();
    });
  }
  for (const state of roomStates) wireRoom(state);
  for (const state of roomStates) log(`Connecting to Fluxer at ${state.url} (${state.label})`);
  try {
    await Promise.all(roomStates.map(state => state.room.connect(state.url, state.token, { autoSubscribe: true })));
  } catch (error) {
    const message = messageOf(error);
    log(`Fluxer connect failed: ${message}`);
    if (isStaleTokenMessage(message)) throw withExitCode(error, EXIT_STALE_TOKEN);
    throw error;
  }
  for (const state of roomStates) {
    hydrateFluxerAudioTracks(state, "initial connect");
    state.connected = true;
    if (!(await ensureFluxerTrackPublished(state, "initial connect"))) {
      throw new Error(`Failed to publish Discord audio track to Fluxer (${state.label})`);
    }
  }
  log(`Fluxer connected (${roomStates.length} room${roomStates.length === 1 ? "" : "s"})`);
  process.send?.("bridge-ready");
  let startupTimeout = null;
  async function shutdown() {
    if (shuttingDown) return;
    shuttingDown = true;
    log("Shutting down");
    if (mixTimer) clearTimeout(mixTimer);
    if (discordRecoveryTimer) clearTimeout(discordRecoveryTimer);
    clearInterval(discordSpeakingSweep);
    for (const timer of subscriptionIdleTimers.values()) clearTimeout(timer);
    subscriptionIdleTimers.clear();
    if (startupTimeout) clearTimeout(startupTimeout);
    for (const state of roomStates) {
      if (state.recoveryTimer) clearTimeout(state.recoveryTimer);
    }
    for (const userId of [...activeSubscriptions]) {
      cleanupDiscordSubscription(userId);
    }
    discordConnection.destroy();
    await Promise.all(roomStates.map(state => state.room.disconnect()));
    process.exit(0);
  }
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
  function notifyFluxerPresence() {
    if (anyRoomWithParticipants()) process.send?.("fluxer-joined");
    else process.send?.("fluxer-empty");
  }
  startupTimeout = setTimeout(() => {
    if (!anyRoomWithParticipants()) process.send?.("fluxer-empty");
  }, 10_000);
  for (const state of roomStates) {
    state.room.on(RoomEvent.ParticipantConnected, participant => {
      clearTimeout(startupTimeout);
      log(
        `[${state.label}] Fluxer participant connected: ${participant?.identity ?? "unknown"} (count=${state.room.remoteParticipants.size})`
      );
      hydrateFluxerAudioTracks(state, "participant connected");
      notifyFluxerPresence();
    });
    state.room.on(RoomEvent.ParticipantDisconnected, () => {
      if (state.connected) {
        log(`[${state.label}] Fluxer participant count is now ${state.room.remoteParticipants.size}`);
      }
      notifyFluxerPresence();
    });
  }
}
main().catch((e) => {
  log(`Fatal: ${formatError(e)}`);
  process.exit(e && typeof e.exitCode === "number" ? e.exitCode : 1);
});
