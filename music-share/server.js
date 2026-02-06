import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import dotenv from 'dotenv';
import multer from 'multer';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { Client, Storage, ID, Permission, Role, InputFile } from 'node-appwrite';
import fetch from 'node-fetch';

dotenv.config();

// Small safety: node-appwrite sometimes uses global fetch
if (global.fetch) delete global.fetch;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ─────────────────────────────────────────────────────────────
// App / Server / Socket.IO
// ─────────────────────────────────────────────────────────────

const app = express();
const httpServer = createServer(app);

const io = new Server(httpServer, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  transports: ['websocket', 'polling'],
  pingTimeout: 10000,
  pingInterval: 5000,
  maxHttpBufferSize: 1e8
});

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─────────────────────────────────────────────────────────────
// Appwrite setup
// ─────────────────────────────────────────────────────────────

const appwriteClient = new Client()
  .setEndpoint(process.env.APPWRITE_ENDPOINT)
  .setProject(process.env.APPWRITE_PROJECT_ID)
  .setKey(process.env.APPWRITE_API_KEY);

const storage = new Storage(appwriteClient);
const BUCKET_ID = process.env.APPWRITE_BUCKET_ID;

if (!BUCKET_ID || !process.env.APPWRITE_ENDPOINT || !process.env.APPWRITE_PROJECT_ID || !process.env.APPWRITE_API_KEY) {
  console.error('❌ Appwrite env vars missing');
  process.exit(1);
}

// ─────────────────────────────────────────────────────────────
// Multer (disk storage) for fast, safe upload
// ─────────────────────────────────────────────────────────────

const uploadDir = '/tmp/uploads';
if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });

const diskStorage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, uploadDir),
  filename: (_req, file, cb) => {
    const ext = path.extname(file.originalname);
    const base = path.basename(file.originalname, ext);
    cb(null, `${Date.now()}-${base}${ext}`);
  }
});

const upload = multer({
  storage: diskStorage,
  limits: { fileSize: 50 * 1024 * 1024 }, // 50 MB
  fileFilter: (_req, file, cb) => {
    if (!file.mimetype.startsWith('audio/')) return cb(new Error('Only audio files allowed'));
    cb(null, true);
  }
});

// ─────────────────────────────────────────────────────────────
// Room model
// ─────────────────────────────────────────────────────────────

/*
 room = {
   code,
   roomName,
   hostSocketId,
   hostName,
   hostDelay: ms,
   audioFileId,
   audioUrl,
   metadata: { title, artist, duration },
   isPlaying,
   scheduledStartTime,
   startOffset,
   globalVolume,
   listeners: Map<socketId, {
     id, name, volume, muted,
     ping, clockOffset, manualDelay,
     syncAccuracy, downloaded, synced
   }>,
   syncInterval,
   createdAt,
   autoDeleteTimer
 }
*/

const rooms = new Map();
const AUTO_DELETE_TIMEOUT = 30 * 60 * 1000;

function getHighPrecisionTime() {
  return Date.now() + (performance.now() % 1);
}

function generateRoomCode() {
  let code;
  do {
    code = Math.floor(1000 + Math.random() * 9000).toString();
  } while (rooms.has(code));
  return code;
}

function createRoom(hostSocketId, roomName, hostName) {
  const code = generateRoomCode();
  const room = {
    code,
    roomName: roomName || `Room ${code}`,
    hostSocketId,
    hostName: hostName || 'Host',
    hostDelay: 0,
    audioFileId: null,
    audioUrl: null,
    metadata: { title: 'No track loaded', artist: 'Unknown Artist', duration: 0 },
    isPlaying: false,
    scheduledStartTime: null,
    startOffset: 0,
    globalVolume: 1,
    listeners: new Map(),
    createdAt: Date.now(),
    syncInterval: null,
    autoDeleteTimer: null
  };

  rooms.set(code, room);

  // periodic sync broadcast when playing
  room.syncInterval = setInterval(() => {
    if (room.isPlaying) broadcastPreciseSync(code);
  }, 1000);

  // auto-delete after 30min inactivity (host gone)
  room.autoDeleteTimer = setTimeout(() => deleteRoom(code), AUTO_DELETE_TIMEOUT);

  broadcastRoomList();
  return room;
}

async function deleteRoom(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;

  if (room.syncInterval) clearInterval(room.syncInterval);
  if (room.autoDeleteTimer) clearTimeout(room.autoDeleteTimer);

  if (room.audioFileId) {
    try {
      await storage.deleteFile(BUCKET_ID, room.audioFileId);
    } catch (e) {
      console.warn('Appwrite delete error', e.message);
    }
  }

  io.to(roomCode).emit('room-closed');
  rooms.delete(roomCode);
  broadcastRoomList();
}

function broadcastRoomList() {
  const list = Array.from(rooms.values()).map(r => ({
    roomCode: r.code,
    roomName: r.roomName,
    hostName: r.hostName,
    listenerCount: r.listeners.size,
    isPlaying: r.isPlaying,
    hasAudio: !!r.audioUrl,
    trackTitle: r.metadata.title,
    createdAt: r.createdAt
  }));
  io.emit('room-list', list);
}

function broadcastRoomState(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;

  let currentPosition = room.startOffset;
  if (room.isPlaying && room.scheduledStartTime) {
    const elapsed = getHighPrecisionTime() - room.scheduledStartTime;
    currentPosition = room.startOffset + elapsed;
  }

  const state = {
    playing: room.isPlaying,
    position: currentPosition,
    duration: room.metadata.duration,
    globalVolume: room.globalVolume,
    metadata: room.metadata,
    hasFile: !!room.audioUrl,
    audioUrl: room.audioUrl,
    roomName: room.roomName,
    serverTime: getHighPrecisionTime()
  };

  io.to(roomCode).emit('room-state', state);
}

function broadcastListenerList(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;

  const listeners = Array.from(room.listeners.values()).map(l => ({
    id: l.id,
    name: l.name,
    volume: l.volume,
    muted: l.muted,
    ping: l.ping,
    clockOffset: l.clockOffset || 0,
    manualDelay: l.manualDelay || 0,
    syncAccuracy: l.syncAccuracy || 'unknown',
    downloaded: !!l.downloaded,
    synced: !!l.synced
  }));

  io.to(roomCode).emit('listener-list', listeners);
}

function broadcastPreciseSync(roomCode) {
  const room = rooms.get(roomCode);
  if (!room || !room.isPlaying) return;

  const now = getHighPrecisionTime();
  const elapsed = now - room.scheduledStartTime;
  const pos = room.startOffset + elapsed;

  io.to(roomCode).emit('sync:update', {
    serverTime: now,
    scheduledStartTime: room.scheduledStartTime,
    startOffset: room.startOffset,
    currentPosition: pos,
    roomCode
  });
}

// ─────────────────────────────────────────────────────────────
// Playback control
// ─────────────────────────────────────────────────────────────

function startPlayback(roomCode, position = 0) {
  const room = rooms.get(roomCode);
  if (!room || !room.audioUrl) return;

  const start = getHighPrecisionTime() + 1200; // ~1.2s buffer
  room.isPlaying = true;
  room.scheduledStartTime = start;
  room.startOffset = position;

  io.to(roomCode).emit('sync:play', {
    scheduledStartTime: start,
    offset: position,
    serverTime: getHighPrecisionTime(),
    roomCode
  });

  broadcastRoomState(roomCode);
  broadcastRoomList();
}

function pausePlayback(roomCode) {
  const room = rooms.get(roomCode);
  if (!room || !room.isPlaying) return;

  if (room.scheduledStartTime) {
    const elapsed = getHighPrecisionTime() - room.scheduledStartTime;
    room.startOffset += elapsed;
  }
  room.isPlaying = false;
  room.scheduledStartTime = null;

  io.to(roomCode).emit('sync:pause', {
    position: room.startOffset,
    serverTime: getHighPrecisionTime(),
    roomCode
  });

  broadcastRoomState(roomCode);
  broadcastRoomList();
}

function stopPlayback(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;

  room.isPlaying = false;
  room.startOffset = 0;
  room.scheduledStartTime = null;

  io.to(roomCode).emit('sync:stop', {
    serverTime: getHighPrecisionTime(),
    roomCode
  });

  broadcastRoomState(roomCode);
  broadcastRoomList();
}

function seekPlayback(roomCode, position) {
  const room = rooms.get(roomCode);
  if (!room) return;

  room.startOffset = position;

  if (room.isPlaying) {
    const start = getHighPrecisionTime() + 1000;
    room.scheduledStartTime = start;
    io.to(roomCode).emit('sync:play', {
      scheduledStartTime: start,
      offset: position,
      serverTime: getHighPrecisionTime(),
      roomCode
    });
  } else {
    io.to(roomCode).emit('sync:seek', {
      position,
      serverTime: getHighPrecisionTime(),
      roomCode
    });
  }

  broadcastRoomState(roomCode);
}

// ─────────────────────────────────────────────────────────────
// Socket.IO events
// ─────────────────────────────────────────────────────────────

io.on('connection', socket => {
  // NTP sync
  socket.on('ntp:sync', (clientSend, cb) => {
    const recv = getHighPrecisionTime();
    const send = getHighPrecisionTime();
    cb({ clientSendTime: clientSend, serverReceiveTime: recv, serverSendTime: send });
  });

  // room list
  socket.on('get-rooms', cb => {
    const list = Array.from(rooms.values()).map(r => ({
      roomCode: r.code,
      roomName: r.roomName,
      hostName: r.hostName,
      listenerCount: r.listeners.size,
      isPlaying: r.isPlaying,
      hasAudio: !!r.audioUrl,
      trackTitle: r.metadata.title
    }));
    cb(list);
  });

  // create room
  socket.on('create-room', ({ roomName, hostName }, cb) => {
    const room = createRoom(socket.id, roomName, hostName);
    socket.join(room.code);
    cb({ success: true, roomCode: room.code, roomName: room.roomName, hostName: room.hostName });
  });

  // host join
  socket.on('join-as-host', ({ roomCode }, cb) => {
    const room = rooms.get(roomCode);
    if (!room) return cb({ error: 'Room not found' });

    room.hostSocketId = socket.id;
    socket.join(roomCode);

    cb({
      success: true,
      roomState: {
        roomName: room.roomName,
        playing: room.isPlaying,
        position: room.startOffset,
        metadata: room.metadata,
        hasFile: !!room.audioUrl,
        audioUrl: room.audioUrl,
        serverTime: getHighPrecisionTime()
      }
    });

    broadcastListenerList(roomCode);
  });

  // listener join
  socket.on('join-as-listener', ({ roomCode, listenerName }, cb) => {
    const room = rooms.get(roomCode);
    if (!room) return cb({ error: 'Room not found' });

    socket.join(roomCode);

    const listener = {
      id: socket.id,
      name: listenerName || `Listener ${room.listeners.size + 1}`,
      volume: 1,
      muted: false,
      ping: 0,
      clockOffset: 0,
      manualDelay: 0,
      syncAccuracy: 'calibrating',
      downloaded: false,
      synced: false,
      joinedAt: Date.now()
    };
    room.listeners.set(socket.id, listener);

    let currentPosition = room.startOffset;
    if (room.isPlaying && room.scheduledStartTime) {
      const elapsed = getHighPrecisionTime() - room.scheduledStartTime;
      currentPosition = room.startOffset + elapsed;
    }

    cb({
      success: true,
      roomCode,
      roomName: room.roomName,
      hostName: room.hostName,
      audioUrl: room.audioUrl,
      currentState: {
        playing: room.isPlaying,
        position: currentPosition,
        scheduledStartTime: room.scheduledStartTime,
        startOffset: room.startOffset,
        metadata: room.metadata,
        serverTime: getHighPrecisionTime()
      }
    });

    broadcastListenerList(roomCode);
    broadcastRoomList();
  });

  // listener status events
  socket.on('download-complete', ({ roomCode }) => {
    const room = rooms.get(roomCode);
    const l = room?.listeners.get(socket.id);
    if (!l) return;
    l.downloaded = true;
    broadcastListenerList(roomCode);
  });

  socket.on('sync-ready', ({ roomCode }) => {
    const room = rooms.get(roomCode);
    const l = room?.listeners.get(socket.id);
    if (!l) return;
    l.synced = true;
    broadcastListenerList(roomCode);
  });

  socket.on('update-sync-stats', ({ roomCode, clockOffset, syncAccuracy }) => {
    const room = rooms.get(roomCode);
    const l = room?.listeners.get(socket.id);
    if (!l) return;
    l.clockOffset = clockOffset;
    l.syncAccuracy = syncAccuracy;
    broadcastListenerList(roomCode);
  });

  socket.on('update-ping', ({ roomCode, ping }) => {
    const room = rooms.get(roomCode);
    const l = room?.listeners.get(socket.id);
    if (!l) return;
    l.ping = ping;
    broadcastListenerList(roomCode);
  });

  // host controls
  socket.on('play', ({ roomCode, position }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    startPlayback(roomCode, position || 0);
  });

  socket.on('pause', ({ roomCode }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    pausePlayback(roomCode);
  });

  socket.on('stop', ({ roomCode }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    stopPlayback(roomCode);
  });

  socket.on('seek', ({ roomCode, position }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    seekPlayback(roomCode, position);
  });

  socket.on('set-global-volume', ({ roomCode, volume }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    room.globalVolume = Math.max(0, Math.min(1, volume));
    io.to(roomCode).emit('volume-update-global', { volume: room.globalVolume });
    broadcastRoomState(roomCode);
  });

  socket.on('set-listener-volume', ({ roomCode, listenerId, volume }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    const l = room.listeners.get(listenerId);
    if (!l) return;
    l.volume = Math.max(0, Math.min(1, volume));
    io.to(listenerId).emit('volume-update', { volume: l.volume });
    broadcastListenerList(roomCode);
  });

  socket.on('mute-listener', ({ roomCode, listenerId, muted }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    const l = room.listeners.get(listenerId);
    if (!l) return;
    l.muted = !!muted;
    io.to(listenerId).emit('mute-update', { muted: l.muted });
    broadcastListenerList(roomCode);
  });

  socket.on('set-listener-delay', ({ roomCode, listenerId, delay }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    const l = room.listeners.get(listenerId);
    if (!l) return;
    l.manualDelay = Math.max(-2500, Math.min(2500, delay));
    io.to(listenerId).emit('delay-update', { delay: l.manualDelay });
    broadcastListenerList(roomCode);
  });

  socket.on('set-host-delay', ({ roomCode, delay }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    room.hostDelay = Math.max(-2500, Math.min(2500, delay));
  });

  // resync snapshot for late listeners
  socket.on('request-sync', ({ roomCode }, cb) => {
    const room = rooms.get(roomCode);
    if (!room || !room.audioUrl) return cb({ ok: false });

    let pos = room.startOffset;
    if (room.isPlaying && room.scheduledStartTime) {
      const elapsed = getHighPrecisionTime() - room.scheduledStartTime;
      pos += elapsed;
    }

    cb({
      ok: true,
      playing: room.isPlaying,
      scheduledStartTime: room.scheduledStartTime,
      offset: room.startOffset,
      serverTime: getHighPrecisionTime(),
      positionNow: pos
    });
  });

  socket.on('disconnect', () => {
    for (const [roomCode, room] of rooms) {
      // host disconnect
      if (room.hostSocketId === socket.id) {
        setTimeout(() => {
          const current = rooms.get(roomCode);
          if (current && current.hostSocketId === socket.id) deleteRoom(roomCode);
        }, 30000);
      }
      // listener disconnect
      if (room.listeners.has(socket.id)) {
        room.listeners.delete(socket.id);
        broadcastListenerList(roomCode);
        broadcastRoomList();
      }
    }
  });
});

// ─────────────────────────────────────────────────────────────
// Upload route (fast, reliable)
// ─────────────────────────────────────────────────────────────

app.post('/upload', upload.single('audio'), async (req, res) => {
  const roomCode = req.body.roomCode;
  const room = rooms.get(roomCode);

  if (!room) {
    if (req.file?.path) fs.unlink(req.file.path, () => {});
    return res.status(404).json({ ok: false, error: 'Room not found' });
  }
  if (!req.file) {
    return res.status(400).json({ ok: false, error: 'No file uploaded' });
  }

  try {
    // delete old file
    if (room.audioFileId) {
      try {
        await storage.deleteFile(BUCKET_ID, room.audioFileId);
      } catch (e) {
        console.warn('Old file delete error', e.message);
      }
    }
    // stop current playback
    if (room.isPlaying) stopPlayback(roomCode);

    const fileId = ID.unique();
    const inputFile = InputFile.fromPath(req.file.path, req.file.originalname);
    const uploadedFile = await storage.createFile(
      BUCKET_ID,
      fileId,
      inputFile,
      [Permission.read(Role.any())]
    );
    fs.unlink(req.file.path, () => {});

    const actualFileId = uploadedFile.$id;
    const fileUrl = `${process.env.APPWRITE_ENDPOINT}/storage/buckets/${BUCKET_ID}/files/${actualFileId}/download?project=${process.env.APPWRITE_PROJECT_ID}`;

    room.audioFileId = actualFileId;
    room.audioUrl = fileUrl;
    room.metadata = {
      title: req.file.originalname.replace(/\.[^/.]+$/, ''),
      artist: 'Unknown Artist',
      duration: 0
    };
    room.startOffset = 0;
    room.isPlaying = false;
    room.scheduledStartTime = null;

    // reset listeners ready flags
    room.listeners.forEach(l => {
      l.downloaded = false;
      l.synced = false;
    });

    io.to(roomCode).emit('file:ready', { url: room.audioUrl, metadata: room.metadata });
    broadcastRoomState(roomCode);
    broadcastRoomList();
    broadcastListenerList(roomCode);

    res.json({ ok: true, audioUrl: room.audioUrl, metadata: room.metadata });
  } catch (e) {
    console.error('Upload error', e);
    if (req.file?.path) fs.unlink(req.file.path, () => {});
    res.status(500).json({ ok: false, error: 'Failed to upload audio' });
  }
});

// ─────────────────────────────────────────────────────────────
// Health
// ─────────────────────────────────────────────────────────────

app.get('/health', (_req, res) => {
  res.json({
    status: 'ok',
    timestamp: Date.now(),
    rooms: rooms.size,
    listeners: Array.from(rooms.values()).reduce((sum, r) => sum + r.listeners.size, 0)
  });
});

// ─────────────────────────────────────────────────────────────
// Start
// ─────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`Music sync server running on ${PORT}`);
});
