//@ts-check
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
  TrackSource,
  AudioFrame,
} from "@livekit/rtc-node";
import prism from "prism-media";
import { PassThrough } from "node:stream";

/** @param {string} msg */
function log(msg) {
  const time = new Date().toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  console.log(`[${time} VOICE] ${msg}`);
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

/**
 * @param {string} endpoint
 * @param {string} token
 * @param {string} sessionId
 * @param {string} guildId
 * @param {string} userId
 * @returns {import("@discordjs/voice").DiscordGatewayAdapterCreator}
 */
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
const FRAME_SIZE = 960; // samples per channel per 20ms frame
const FRAME_SAMPLES = FRAME_SIZE * CHANNELS;
const FRAME_DURATION_MS = 20;
const MIN_BUFFER_DELAY = 2;
const STARTUP_BUFFER_DELAY = 3;
const MAX_BUFFER_DELAY = 8;
// Cap queues at ~240ms to prevent unbounded growth during long bursts.
const MAX_BUFFER_DEPTH = 12;

/**
 * @param {number} value
 * @param {number} min
 * @param {number} max
 * @returns {number}
 */
function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

class JitterBuffer {
  /**
   * @param {Int16Array} silenceFrame
   */
  constructor(silenceFrame) {
    /** @type {Int16Array[]} */
    this.queue = [];
    this.silenceFrame = silenceFrame;
    this.started = false;
    this.targetDelay = STARTUP_BUFFER_DELAY;
    this.lastArrivalAt = 0;
    this.jitterMs = 0;
    this.stableTicks = 0;
  }

  /**
   * @param {Int16Array} frame
   */
  push(frame) {
    const now = Date.now();
    if (this.lastArrivalAt !== 0) {
      const deviation = Math.abs((now - this.lastArrivalAt) - FRAME_DURATION_MS);
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
    this.lastArrivalAt = now;

    if (this.queue.length >= MAX_BUFFER_DEPTH) {
      this.queue.shift();
      this.started = true;
      this.targetDelay = Math.min(MAX_BUFFER_DELAY, this.targetDelay + 1);
      this.stableTicks = 0;
    }

    this.queue.push(frame);
  }

  /**
   * @returns {Int16Array | null}
   */
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
      return frame;
    }

    this.started = false;
    this.stableTicks = 0;
    this.targetDelay = Math.min(MAX_BUFFER_DELAY, this.targetDelay + 1);
    return this.silenceFrame;
  }
}

/** @param {Int16Array[]} frames */
function mixFrames(frames) {
  const out = new Int16Array(FRAME_SAMPLES);
  for (const f of frames) {
    for (let i = 0; i < FRAME_SAMPLES; i++) {
      out[i] = Math.max(-32768, Math.min(32767, out[i] + (f[i] ?? 0)));
    }
  }
  return out;
}

async function main() {
  const discordConnection = joinVoiceChannel({
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

  await entersState(discordConnection, VoiceConnectionStatus.Ready, 30_000);
  log("Discord voice connected");

  let reconnectingDiscordVoice = false;
  discordConnection.on("stateChange", async (_oldState, newState) => {
    if (newState.status !== VoiceConnectionStatus.Disconnected || reconnectingDiscordVoice) return;
    reconnectingDiscordVoice = true;
    try {
      const rejoinAccepted = discordConnection.rejoin();
      if (!rejoinAccepted) {
        throw new Error("Voice connection rejected rejoin");
      }
      await entersState(discordConnection, VoiceConnectionStatus.Ready, 15_000);
      log("Discord voice rejoined");
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      log(`Discord voice rejoin failed: ${message}`);
      process.exit(2);
    } finally {
      reconnectingDiscordVoice = false;
    }
  });

  const room = new Room();
  const lkSource = new AudioSource(SAMPLE_RATE, CHANNELS);
  const lkTrack = LocalAudioTrack.createAudioTrack("discord-audio", lkSource);

  const receiver = discordConnection.receiver;
  const activeSubscriptions = new Set();
  /** @type {Map<string, { opusStream: import("@discordjs/voice").AudioReceiveStream, decoder: prism.opus.Decoder, cleanedUp: boolean }>} */
  const subscriptionPipelines = new Map();
  const SILENCE = new Int16Array(FRAME_SAMPLES);

  /** @type {Map<string, JitterBuffer>} */
  const discordBuffers = new Map();

  /** @param {string} userId */
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
  }

  receiver.speaking.on("start", (userId) => {
    if (activeSubscriptions.has(userId)) return;
    activeSubscriptions.add(userId);
    discordBuffers.set(userId, new JitterBuffer(SILENCE));

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

    decoder.on("data", (/** @type {Buffer} */ pcm) => {
      const buffer = discordBuffers.get(userId);
      if (!buffer) return;
      const frame = new Int16Array(pcm.buffer, pcm.byteOffset, pcm.byteLength / 2);
      buffer.push(frame);
    });

    const cleanup = () => cleanupDiscordSubscription(userId);

    decoder.once("close", cleanup);
    decoder.once("error", cleanup);
    opusStream.once("close", cleanup);
    opusStream.once("end", cleanup);
    opusStream.once("error", cleanup);
  });

  receiver.speaking.on("end", (userId) => {
    cleanupDiscordSubscription(userId);
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

  /** @type {Map<string, ReadableStreamDefaultReader>} */
  const audioReaders = new Map();
  /** @type {Map<string, JitterBuffer>} */
  const fluxerBuffers = new Map();
  /** @type {Map<string, Int16Array>} */
  const fluxerAccum = new Map();
  let lkReady = false;

  const mixInterval = setInterval(() => {
    // Discord → Livekit
    if (lkReady) {
      const dFrames = [];
      for (const [, buffer] of discordBuffers) {
        const frame = buffer.pull();
        if (frame) dFrames.push(frame);
      }
      const mixed = dFrames.length > 0 ? mixFrames(dFrames) : SILENCE;
      lkSource.captureFrame(new AudioFrame(mixed, SAMPLE_RATE, CHANNELS, FRAME_SIZE)).catch(() => {});
    }

    // Livekit → Discord
    const fFrames = [];
    for (const [, buffer] of fluxerBuffers) {
      const frame = buffer.pull();
      if (frame) fFrames.push(frame);
    }
    const mixed = fFrames.length > 0 ? mixFrames(fFrames) : SILENCE;
    const buf = Buffer.from(mixed.buffer, mixed.byteOffset, mixed.byteLength);
    if (!pcmPassthrough.writableEnded) pcmPassthrough.write(buf);
  }, FRAME_DURATION_MS);

  room.on(RoomEvent.TrackSubscribed, (track, _pub, participant) => {
    if (track.kind !== TrackKind.KIND_AUDIO) return;
    if (!track.sid) return;
    log(`Subscribed to audio: ${participant.identity}`);
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
    })();
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

  log(`Connecting to Fluxer at ${LIVEKIT_URL}`);
  // @ts-ignore
  await room.connect(LIVEKIT_URL, LIVEKIT_TOKEN, { autoSubscribe: true });
  // @ts-ignore
  await room.localParticipant?.publishTrack(lkTrack, { source: TrackSource.SOURCE_MICROPHONE });
  lkReady = true;
  log("Fluxer connected");

  let shuttingDown = false;
  async function shutdown() {
    if (shuttingDown) return;
    shuttingDown = true;
    log("Shutting down");
    clearInterval(mixInterval);
    for (const userId of [...activeSubscriptions]) {
      cleanupDiscordSubscription(userId);
    }
    discordConnection.destroy();
    await room.disconnect();
    process.exit(0);
  }

  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);

  // If no Fluxer participants appear within 10s, signal the parent.
  const startupTimeout = setTimeout(() => {
    if (room.remoteParticipants.size === 0) process.send?.("fluxer-empty");
  }, 10_000);

  room.on(RoomEvent.ParticipantConnected, () => {
    clearTimeout(startupTimeout);
    process.send?.("fluxer-joined");
  });

  room.on(RoomEvent.ParticipantDisconnected, () => {
    if (room.remoteParticipants.size === 0) {
      log("All Fluxer participants left");
      process.send?.("fluxer-empty");
    }
  });
}

main().catch((e) => {
  log(`Fatal: ${e}`);
  process.exit(1);
});
