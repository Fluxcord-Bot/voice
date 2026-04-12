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
// Cap queues at ~200ms to prevent unbounded growth during long bursts
const MAX_QUEUE_DEPTH = 10;

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

  const room = new Room();
  const lkSource = new AudioSource(SAMPLE_RATE, CHANNELS);
  const lkTrack = LocalAudioTrack.createAudioTrack("discord-audio", lkSource);

  const receiver = discordConnection.receiver;
  const activeSubscriptions = new Set();

  /** @type {Map<string, Int16Array[]>} */
  const discordQueues = new Map();

  receiver.speaking.on("start", (userId) => {
    if (activeSubscriptions.has(userId)) return;
    activeSubscriptions.add(userId);
    discordQueues.set(userId, []);

    const opusStream = receiver.subscribe(userId, {
      end: { behavior: EndBehaviorType.Manual },
    });

    const decoder = new prism.opus.Decoder({
      rate: SAMPLE_RATE,
      channels: CHANNELS,
      frameSize: FRAME_SIZE,
    });

    opusStream.pipe(decoder);

    decoder.on("data", (/** @type {Buffer} */ pcm) => {
      const frame = new Int16Array(pcm.buffer, pcm.byteOffset, pcm.byteLength / 2);
      const queue = discordQueues.get(userId) ?? [];
      if (queue.length < MAX_QUEUE_DEPTH) queue.push(frame);
      discordQueues.set(userId, queue);
    });

    decoder.on("error", () => {});
    opusStream.on("error", () => {});
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
  /** @type {Map<string, Int16Array[]>} */
  const fluxerQueues = new Map();
  /** @type {Map<string, Int16Array>} */
  const fluxerAccum = new Map();
  let lkReady = false;

  const SILENCE = new Int16Array(FRAME_SAMPLES);

  setInterval(() => {
    // Discord → Livekit
    if (lkReady) {
      const dFrames = [];
      for (const [, queue] of discordQueues) {
        const frame = queue.shift();
        if (frame) dFrames.push(frame);
      }
      const mixed = dFrames.length > 0 ? mixFrames(dFrames) : SILENCE;
      lkSource.captureFrame(new AudioFrame(mixed, SAMPLE_RATE, CHANNELS, FRAME_SIZE)).catch(() => {});
    }

    // Livekit → Discord
    const fFrames = [];
    for (const [, queue] of fluxerQueues) {
      const frame = queue.shift();
      if (frame) fFrames.push(frame);
    }
    if (fFrames.length > 0) {
      const mixed = mixFrames(fFrames);
      const buf = Buffer.from(mixed.buffer, mixed.byteOffset, mixed.byteLength);
      if (!pcmPassthrough.writableEnded) pcmPassthrough.write(buf);
    }
  }, 20);

  room.on(RoomEvent.TrackSubscribed, (track, _pub, participant) => {
    if (track.kind !== TrackKind.KIND_AUDIO) return;
    if (!track.sid) return;
    log(`Subscribed to audio: ${participant.identity}`);
    const audioStream = new AudioStream(track, SAMPLE_RATE, CHANNELS);
    const reader = audioStream.getReader();
    audioReaders.set(track.sid, reader);
    fluxerAccum.set(track.sid, new Int16Array(0));
    fluxerQueues.set(track.sid, []);
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
            const queue = fluxerQueues.get(sid);
            if (queue && queue.length < MAX_QUEUE_DEPTH) {
              queue.push(combined.slice(0, FRAME_SAMPLES));
            }
            combined = combined.slice(FRAME_SAMPLES);
          }
          fluxerAccum.set(sid, combined);
        }
      } catch {}
      fluxerQueues.delete(sid);
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
    fluxerQueues.delete(track.sid);
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
