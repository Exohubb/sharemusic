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

dotenv.config();

// Fix for node-appwrite compatibility
if (global.fetch) delete global.fetch;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ═══════════════════════════════════════════════════════════
// EXPRESS + SOCKET.IO SETUP
// ═══════════════════════════════════════════════════════════

const app = express();
const httpServer = createServer(app);

const io = new Server(httpServer, {
  cors: { 
    origin: '*', 
    methods: ['GET', 'POST'],
    credentials: false
  },
  transports: ['websocket', 'polling'],
  pingTimeout: 10000,
  pingInterval: 5000,
  maxHttpBufferSize: 100 * 1024 * 1024, // 100MB
  upgradeTimeout: 10000,
  allowUpgrades: true
});

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ═══════════════════════════════════════════════════════════
// APPWRITE CONFIGURATION
// ═══════════════════════════════════════════════════════════

const APPWRITE_ENDPOINT = process.env.APPWRITE_ENDPOINT || 'https://cloud.appwrite.io/v1';
const APPWRITE_PROJECT_ID = process.env.APPWRITE_PROJECT_ID;
const APPWRITE_API_KEY = process.env.APPWRITE_API_KEY;
const BUCKET_ID = process.env.APPWRITE_BUCKET_ID;

if (!APPWRITE_PROJECT_ID || !APPWRITE_API_KEY || !BUCKET_ID) {
  console.error('❌ Missing Appwrite configuration in .env file');
  process.exit(1);
}

const appwriteClient = new Client()
  .setEndpoint(APPWRITE_ENDPOINT)
  .setProject(APPWRITE_PROJECT_ID)
  .setKey(APPWRITE_API_KEY);

const storage = new Storage(appwriteClient);

console.log('✅ Appwrite configured:', APPWRITE_ENDPOINT);

// ═══════════════════════════════════════════════════════════
// MULTER SETUP (DISK STORAGE FOR FAST UPLOAD)
// ═══════════════════════════════════════════════════════════

const uploadDir = path.join('/tmp', 'music-sync-uploads');
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

const diskStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => {
    const uniqueName = `${Date.now()}-${Math.random().toString(36).substring(7)}${path.extname(file.originalname)}`;
    cb(null, uniqueName);
  }
});

const upload = multer({
  storage: diskStorage,
  limits: { 
    fileSize: 50 * 1024 * 1024, // 50MB max
    files: 1
  },
  fileFilter: (req, file, cb) => {
    const allowedTypes = ['audio/mpeg', 'audio/mp3', 'audio/wav', 'audio/m4a', 'audio/mp4', 'audio/x-m4a'];
    if (allowedTypes.includes(file.mimetype) || file.originalname.match(/\.(mp3|m4a|wav|aac)$/i)) {
      cb(null, true);
    } else {
      cb(new Error('Only audio files are allowed (MP3, M4A, WAV, AAC)'));
    }
  }
});

// ═══════════════════════════════════════════════════════════
// ROOM DATA STRUCTURE
// ═══════════════════════════════════════════════════════════

const rooms = new Map();
const AUTO_DELETE_TIMEOUT = 30 * 60 * 1000; // 30 minutes

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

// ═══════════════════════════════════════════════════════════
// ROOM MANAGEMENT
// ═══════════════════════════════════════════════════════════

function createRoom(hostSocketId, roomName, hostName) {
  const code = generateRoomCode();
  
  const room = {
    code,
    roomName: roomName || `Room ${code}`,
    hostSocketId,
    hostName: hostName || 'Host',
    hostDelay: 0, // ✅ HOST DELAY CONTROL
    
    audioFileId: null,
    audioUrl: null,
    metadata: {
      title: 'No track loaded',
      artist: 'Unknown',
      duration: 0
    },
    
    isPlaying: false,
    scheduledStartTime: null,
    startOffset: 0,
    
    globalVolume: 1.0,
    listeners: new Map(),
    
    createdAt: Date.now(),
    lastActivityAt: Date.now(),
    syncInterval: null,
    autoDeleteTimer: null
  };
  
  rooms.set(code, room);
  
  // Continuous sync broadcast when playing
  room.syncInterval = setInterval(() => {
    if (room.isPlaying) {
      broadcastPreciseSync(code);
    }
  }, 1000);
  
  // Auto-delete after 30min of inactivity
  room.autoDeleteTimer = setTimeout(() => {
    console.log(`⏰ Auto-deleting inactive room: ${code}`);
    deleteRoom(code);
  }, AUTO_DELETE_TIMEOUT);
  
  console.log(`🏠 Room created: ${code} - ${room.roomName} by ${hostName}`);
  broadcastRoomList();
  
  return room;
}

async function deleteRoom(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;
  
  console.log(`🗑️ Deleting room: ${roomCode}`);
  
  // Clear intervals/timers
  if (room.syncInterval) clearInterval(room.syncInterval);
  if (room.autoDeleteTimer) clearTimeout(room.autoDeleteTimer);
  
  // ✅ DELETE AUDIO FILE FROM APPWRITE
  if (room.audioFileId) {
    try {
      await storage.deleteFile(BUCKET_ID, room.audioFileId);
      console.log(`   ✅ Deleted audio file: ${room.audioFileId}`);
    } catch (err) {
      console.warn(`   ⚠️ Could not delete audio file:`, err.message);
    }
  }
  
  // Notify all clients in room
  io.to(roomCode).emit('room-closed', { roomCode });
  
  // Remove from rooms map
  rooms.delete(roomCode);
  
  // Update room list for all clients
  broadcastRoomList();
  
  console.log(`   ✅ Room deleted: ${roomCode}`);
}

function updateRoomActivity(roomCode) {
  const room = rooms.get(roomCode);
  if (room) {
    room.lastActivityAt = Date.now();
  }
}

// ═══════════════════════════════════════════════════════════
// BROADCASTING FUNCTIONS
// ═══════════════════════════════════════════════════════════

function broadcastRoomList() {
  const roomList = Array.from(rooms.values()).map(room => ({
    roomCode: room.code,
    roomName: room.roomName,
    hostName: room.hostName,
    listenerCount: room.listeners.size,
    isPlaying: room.isPlaying,
    hasAudio: !!room.audioUrl,
    trackTitle: room.metadata.title,
    createdAt: room.createdAt
  }));
  
  // Broadcast to ALL connected clients instantly
  io.emit('room-list', roomList);
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
    scheduledStartTime: room.scheduledStartTime,
    startOffset: room.startOffset,
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
    ping: l.ping || 0,
    clockOffset: l.clockOffset || 0,
    manualDelay: l.manualDelay || 0,
    syncAccuracy: l.syncAccuracy || 'unknown',
    downloaded: l.downloaded || false,
    synced: l.synced || false,
    connected: true
  }));
  
  io.to(roomCode).emit('listener-list', listeners);
}

function broadcastPreciseSync(roomCode) {
  const room = rooms.get(roomCode);
  if (!room || !room.isPlaying) return;
  
  const now = getHighPrecisionTime();
  const elapsed = now - room.scheduledStartTime;
  const currentPosition = room.startOffset + elapsed;
  
  io.to(roomCode).emit('sync:update', {
    serverTime: now,
    scheduledStartTime: room.scheduledStartTime,
    startOffset: room.startOffset,
    currentPosition: currentPosition,
    roomCode: roomCode
  });
}

// ═══════════════════════════════════════════════════════════
// PLAYBACK CONTROL FUNCTIONS
// ═══════════════════════════════════════════════════════════

function startPlayback(roomCode, position = 0) {
  const room = rooms.get(roomCode);
  if (!room || !room.audioUrl) {
    console.warn(`Cannot start playback: room ${roomCode} has no audio`);
    return;
  }
  
  // ✅ GIVE 1500ms BUFFER FOR ALL CLIENTS TO PREPARE
  const scheduledStartTime = getHighPrecisionTime() + 1500;
  
  room.isPlaying = true;
  room.scheduledStartTime = scheduledStartTime;
  room.startOffset = position;
  room.lastActivityAt = Date.now();
  
  console.log(`▶️ Room ${roomCode}: Starting playback at ${scheduledStartTime} (position: ${position}ms)`);
  
  // Send to ALL clients in room (host + listeners)
  io.to(roomCode).emit('sync:play', {
    scheduledStartTime: scheduledStartTime,
    offset: position,
    serverTime: getHighPrecisionTime(),
    roomCode: roomCode
  });
  
  broadcastRoomState(roomCode);
  broadcastRoomList();
}

function pausePlayback(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;
  
  if (room.isPlaying && room.scheduledStartTime) {
    const elapsed = getHighPrecisionTime() - room.scheduledStartTime;
    room.startOffset = Math.max(0, room.startOffset + elapsed);
  }
  
  room.isPlaying = false;
  room.scheduledStartTime = null;
  room.lastActivityAt = Date.now();
  
  console.log(`⏸️ Room ${roomCode}: Paused at ${room.startOffset}ms`);
  
  io.to(roomCode).emit('sync:pause', {
    position: room.startOffset,
    serverTime: getHighPrecisionTime(),
    roomCode: roomCode
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
  room.lastActivityAt = Date.now();
  
  console.log(`⏹️ Room ${roomCode}: Stopped`);
  
  io.to(roomCode).emit('sync:stop', {
    serverTime: getHighPrecisionTime(),
    roomCode: roomCode
  });
  
  broadcastRoomState(roomCode);
  broadcastRoomList();
}

function seekPlayback(roomCode, position) {
  const room = rooms.get(roomCode);
  if (!room) return;
  
  room.startOffset = Math.max(0, position);
  room.lastActivityAt = Date.now();
  
  if (room.isPlaying) {
    const scheduledStartTime = getHighPrecisionTime() + 1000;
    room.scheduledStartTime = scheduledStartTime;
    
    io.to(roomCode).emit('sync:play', {
      scheduledStartTime: scheduledStartTime,
      offset: position,
      serverTime: getHighPrecisionTime(),
      roomCode: roomCode
    });
  } else {
    io.to(roomCode).emit('sync:seek', {
      position: position,
      serverTime: getHighPrecisionTime(),
      roomCode: roomCode
    });
  }
  
  broadcastRoomState(roomCode);
}

// ═══════════════════════════════════════════════════════════
// SOCKET.IO CONNECTION HANDLER
// ═══════════════════════════════════════════════════════════

io.on('connection', (socket) => {
  console.log(`🔌 Client connected: ${socket.id}`);
  
  // ──────────────────────────────────────────────────────────
  // NTP CLOCK SYNC (HIGH PRECISION)
  // ──────────────────────────────────────────────────────────
  
  socket.on('ntp:sync', (clientSendTime, callback) => {
    const serverReceiveTime = getHighPrecisionTime();
    const serverSendTime = getHighPrecisionTime();
    
    if (typeof callback === 'function') {
      callback({
        clientSendTime: clientSendTime,
        serverReceiveTime: serverReceiveTime,
        serverSendTime: serverSendTime
      });
    }
  });
  
  // ──────────────────────────────────────────────────────────
  // ROOM LIST
  // ──────────────────────────────────────────────────────────
  
  socket.on('get-rooms', (callback) => {
    const roomList = Array.from(rooms.values()).map(room => ({
      roomCode: room.code,
      roomName: room.roomName,
      hostName: room.hostName,
      listenerCount: room.listeners.size,
      isPlaying: room.isPlaying,
      hasAudio: !!room.audioUrl,
      trackTitle: room.metadata.title,
      createdAt: room.createdAt
    }));
    
    if (typeof callback === 'function') {
      callback(roomList);
    }
  });
  
  // ──────────────────────────────────────────────────────────
  // CREATE ROOM (HOST)
  // ──────────────────────────────────────────────────────────
  
  socket.on('create-room', ({ roomName, hostName }, callback) => {
    try {
      const room = createRoom(socket.id, roomName, hostName);
      socket.join(room.code);
      
      if (typeof callback === 'function') {
        callback({
          success: true,
          roomCode: room.code,
          roomName: room.roomName,
          hostName: room.hostName
        });
      }
    } catch (err) {
      console.error('Error creating room:', err);
      if (typeof callback === 'function') {
        callback({ success: false, error: 'Failed to create room' });
      }
    }
  });
  
  // ──────────────────────────────────────────────────────────
  // JOIN AS HOST (REJOIN)
  // ──────────────────────────────────────────────────────────
  
  socket.on('join-as-host', ({ roomCode }, callback) => {
    const room = rooms.get(roomCode);
    
    if (!room) {
      if (typeof callback === 'function') {
        callback({ success: false, error: 'Room not found' });
      }
      return;
    }
    
    room.hostSocketId = socket.id;
    room.lastActivityAt = Date.now();
    socket.join(roomCode);
    
    console.log(`👑 Host rejoined: ${roomCode}`);
    
    if (typeof callback === 'function') {
      callback({
        success: true,
        roomState: {
          roomName: room.roomName,
          playing: room.isPlaying,
          position: room.startOffset,
          metadata: room.metadata,
          hasFile: !!room.audioUrl,
          audioUrl: room.audioUrl,
          hostDelay: room.hostDelay,
          serverTime: getHighPrecisionTime()
        }
      });
    }
    
    setTimeout(() => broadcastListenerList(roomCode), 100);
  });
  
  // ──────────────────────────────────────────────────────────
  // JOIN AS LISTENER
  // ──────────────────────────────────────────────────────────
  
  socket.on('join-as-listener', ({ roomCode, listenerName }, callback) => {
    const room = rooms.get(roomCode);
    
    if (!room) {
      if (typeof callback === 'function') {
        callback({ success: false, error: 'Room not found' });
      }
      return;
    }
    
    socket.join(roomCode);
    room.lastActivityAt = Date.now();
    
    const listener = {
      id: socket.id,
      name: listenerName || `Listener ${room.listeners.size + 1}`,
      volume: 1.0,
      muted: false,
      ping: 0,
      clockOffset: 0,
      manualDelay: 0, // ✅ INDIVIDUAL DELAY CONTROL
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
    
    console.log(`👂 Listener joined: ${listener.name} → Room ${roomCode}`);
    
    if (typeof callback === 'function') {
      callback({
        success: true,
        roomCode: roomCode,
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
    }
    
    broadcastListenerList(roomCode);
    broadcastRoomList();
  });
  
  // ──────────────────────────────────────────────────────────
  // LISTENER STATUS UPDATES
  // ──────────────────────────────────────────────────────────
  
  socket.on('download-complete', ({ roomCode }) => {
    const room = rooms.get(roomCode);
    if (!room) return;
    
    const listener = room.listeners.get(socket.id);
    if (listener) {
      listener.downloaded = true;
      broadcastListenerList(roomCode);
    }
  });
  
  socket.on('sync-ready', ({ roomCode }) => {
    const room = rooms.get(roomCode);
    if (!room) return;
    
    const listener = room.listeners.get(socket.id);
    if (listener) {
      listener.synced = true;
      broadcastListenerList(roomCode);
    }
  });
  
  socket.on('update-sync-stats', ({ roomCode, clockOffset, syncAccuracy }) => {
    const room = rooms.get(roomCode);
    if (!room) return;
    
    const listener = room.listeners.get(socket.id);
    if (listener) {
      listener.clockOffset = clockOffset;
      listener.syncAccuracy = syncAccuracy;
      broadcastListenerList(roomCode);
    }
  });
  
  socket.on('update-ping', ({ roomCode, ping }) => {
    const room = rooms.get(roomCode);
    if (!room) return;
    
    const listener = room.listeners.get(socket.id);
    if (listener) {
      listener.ping = Math.round(ping);
      broadcastListenerList(roomCode);
    }
  });
  
  // ──────────────────────────────────────────────────────────
  // PLAYBACK CONTROLS (HOST ONLY)
  // ──────────────────────────────────────────────────────────
  
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
  
  // ──────────────────────────────────────────────────────────
  // VOLUME CONTROLS (HOST ONLY)
  // ──────────────────────────────────────────────────────────
  
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
    
    const listener = room.listeners.get(listenerId);
    if (listener) {
      listener.volume = Math.max(0, Math.min(1, volume));
      io.to(listenerId).emit('volume-update', { volume: listener.volume });
      broadcastListenerList(roomCode);
    }
  });
  
  socket.on('mute-listener', ({ roomCode, listenerId, muted }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    
    const listener = room.listeners.get(listenerId);
    if (listener) {
      listener.muted = !!muted;
      io.to(listenerId).emit('mute-update', { muted: listener.muted });
      broadcastListenerList(roomCode);
    }
  });
  
  // ──────────────────────────────────────────────────────────
  // ✅ DELAY CONTROLS (±2500ms)
  // ──────────────────────────────────────────────────────────
  
  // HOST DELAY
  socket.on('set-host-delay', ({ roomCode, delay }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    
    room.hostDelay = Math.max(-2500, Math.min(2500, delay));
    console.log(`⏱️ Host delay set: ${room.hostDelay}ms`);
  });
  
  // LISTENER DELAY (HOST CONTROLS THIS)
  socket.on('set-listener-delay', ({ roomCode, listenerId, delay }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    
    const listener = room.listeners.get(listenerId);
    if (listener) {
      listener.manualDelay = Math.max(-2500, Math.min(2500, delay));
      io.to(listenerId).emit('delay-update', { delay: listener.manualDelay });
      broadcastListenerList(roomCode);
      console.log(`⏱️ Listener delay set: ${listener.name} = ${listener.manualDelay}ms`);
    }
  });
  
  // ──────────────────────────────────────────────────────────
  // ✅ RESYNC REQUEST (FOR LATE LISTENERS)
  // ──────────────────────────────────────────────────────────
  
  socket.on('request-sync', ({ roomCode }, callback) => {
    const room = rooms.get(roomCode);
    
    if (!room || !room.audioUrl) {
      if (typeof callback === 'function') {
        callback({ ok: false, error: 'No audio in room' });
      }
      return;
    }
    
    let currentPosition = room.startOffset;
    if (room.isPlaying && room.scheduledStartTime) {
      const elapsed = getHighPrecisionTime() - room.scheduledStartTime;
      currentPosition = room.startOffset + elapsed;
    }
    
    if (typeof callback === 'function') {
      callback({
        ok: true,
        playing: room.isPlaying,
        scheduledStartTime: room.scheduledStartTime,
        offset: room.startOffset,
        currentPosition: currentPosition,
        serverTime: getHighPrecisionTime()
      });
    }
  });
  
  // ──────────────────────────────────────────────────────────
  // DISCONNECT HANDLER
  // ──────────────────────────────────────────────────────────
  
  socket.on('disconnect', () => {
    console.log(`🔌 Client disconnected: ${socket.id}`);
    
    for (const [roomCode, room] of rooms.entries()) {
      // Host disconnected
      if (room.hostSocketId === socket.id) {
        console.log(`⚠️ Host disconnected from room ${roomCode}, scheduling deletion...`);
        setTimeout(() => {
          const currentRoom = rooms.get(roomCode);
          if (currentRoom && currentRoom.hostSocketId === socket.id) {
            deleteRoom(roomCode);
          }
        }, 30000); // 30 second grace period
      }
      
      // Listener disconnected
      if (room.listeners.has(socket.id)) {
        const listener = room.listeners.get(socket.id);
        console.log(`👋 ${listener.name} left room ${roomCode}`);
        room.listeners.delete(socket.id);
        broadcastListenerList(roomCode);
        broadcastRoomList();
      }
    }
  });
});

// ═══════════════════════════════════════════════════════════
// FILE UPLOAD ROUTE (FAST & RELIABLE)
// ═══════════════════════════════════════════════════════════

app.post('/upload', (req, res, next) => {
  upload.single('audio')(req, res, (err) => {
    if (err) {
      console.error('❌ Multer error:', err);
      
      // Clean up file if it exists
      if (req.file && req.file.path) {
        fs.unlink(req.file.path, () => {});
      }
      
      if (err instanceof multer.MulterError) {
        if (err.code === 'LIMIT_FILE_SIZE') {
          return res.status(400).json({ ok: false, error: 'File too large (max 50MB)' });
        }
        return res.status(400).json({ ok: false, error: `Upload error: ${err.message}` });
      }
      
      return res.status(400).json({ ok: false, error: err.message });
    }
    
    next();
  });
}, async (req, res) => {
  const startTime = Date.now();
  const roomCode = req.body.roomCode;
  
  try {
    const room = rooms.get(roomCode);
    
    if (!room) {
      if (req.file && req.file.path) fs.unlink(req.file.path, () => {});
      return res.status(404).json({ ok: false, error: 'Room not found' });
    }
    
    if (!req.file) {
      return res.status(400).json({ ok: false, error: 'No file uploaded' });
    }
    
    console.log(`📤 Upload started: ${req.file.originalname} (${(req.file.size / 1024 / 1024).toFixed(2)} MB)`);
    
    // Stop current playback
    if (room.isPlaying) {
      stopPlayback(roomCode);
    }
    
    // Delete old file from Appwrite
    if (room.audioFileId) {
      try {
        await storage.deleteFile(BUCKET_ID, room.audioFileId);
        console.log(`   ✅ Deleted old file: ${room.audioFileId}`);
      } catch (err) {
        console.warn(`   ⚠️ Could not delete old file:`, err.message);
      }
    }
    
    // Upload new file to Appwrite
    const fileId = ID.unique();
    const inputFile = InputFile.fromPath(req.file.path, req.file.originalname);
    
    console.log(`   📡 Uploading to Appwrite...`);
    
    const uploadedFile = await storage.createFile(
      BUCKET_ID,
      fileId,
      inputFile,
      [Permission.read(Role.any())]
    );
    
    // Clean up temp file
    fs.unlink(req.file.path, (err) => {
      if (err) console.warn('Could not delete temp file:', err.message);
    });
    
    const actualFileId = uploadedFile.$id;
    const fileUrl = `${APPWRITE_ENDPOINT}/storage/buckets/${BUCKET_ID}/files/${actualFileId}/download?project=${APPWRITE_PROJECT_ID}`;
    
    // Update room data
    room.audioFileId = actualFileId;
    room.audioUrl = fileUrl;
    room.metadata = {
      title: req.file.originalname.replace(/\.[^/.]+$/, ''),
      artist: 'Unknown',
      duration: 0
    };
    room.startOffset = 0;
    room.isPlaying = false;
    room.scheduledStartTime = null;
    room.lastActivityAt = Date.now();
    
    // Reset all listener statuses
    room.listeners.forEach(listener => {
      listener.downloaded = false;
      listener.synced = false;
    });
    
    const uploadTime = Date.now() - startTime;
    console.log(`   ✅ Upload complete in ${uploadTime}ms`);
    console.log(`   🌐 URL: ${fileUrl}`);
    
    // Notify all clients in room
    io.to(roomCode).emit('file:ready', {
      url: room.audioUrl,
      metadata: room.metadata
    });
    
    // Update room state and list
    broadcastRoomState(roomCode);
    broadcastListenerList(roomCode);
    broadcastRoomList();
    
    // Send success response
    res.json({
      ok: true,
      audioUrl: room.audioUrl,
      metadata: room.metadata,
      fileId: actualFileId,
      uploadTime: uploadTime
    });
    
  } catch (err) {
    console.error('❌ Upload error:', err);
    
    // Clean up temp file
    if (req.file && req.file.path) {
      fs.unlink(req.file.path, () => {});
    }
    
    res.status(500).json({
      ok: false,
      error: 'Failed to upload audio',
      details: err.message
    });
  }
});

// ═══════════════════════════════════════════════════════════
// HEALTH CHECK
// ═══════════════════════════════════════════════════════════

app.get('/health', (req, res) => {
  const totalListeners = Array.from(rooms.values()).reduce((sum, r) => sum + r.listeners.size, 0);
  
  res.json({
    status: 'ok',
    timestamp: Date.now(),
    rooms: rooms.size,
    totalListeners: totalListeners,
    uptime: process.uptime()
  });
});

// ═══════════════════════════════════════════════════════════
// START SERVER
// ═══════════════════════════════════════════════════════════

const PORT = process.env.PORT || 3000;

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🚀 ═══════════════════════════════════════`);
  console.log(`   Music Sync Server`);
  console.log(`   ═══════════════════════════════════════`);
  console.log(`   🌐 Port: ${PORT}`);
  console.log(`   📦 Storage: Appwrite (${BUCKET_ID})`);
  console.log(`   ⏱️  Delay Range: ±2500ms`);
  console.log(`   🎯 Features: Host delay, instant sync`);
  console.log(`═══════════════════════════════════════\n`);
});

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n⏹️ Shutting down server...');
  
  // Delete all rooms (and their files)
  const deletePromises = Array.from(rooms.keys()).map(code => deleteRoom(code));
  await Promise.all(deletePromises);
  
  console.log('✅ Cleanup complete');
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\n⏹️ Received SIGTERM, shutting down...');
  
  const deletePromises = Array.from(rooms.keys()).map(code => deleteRoom(code));
  await Promise.all(deletePromises);
  
  process.exit(0);
});
