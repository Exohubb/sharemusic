import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import dotenv from 'dotenv';
import multer from 'multer';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { createClient } from '@supabase/supabase-js';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ═══════════════════════════════════════════════════════════
// SUPABASE SETUP
// ═══════════════════════════════════════════════════════════

const SUPABASE_URL = 'https://jnisacrqerzzkriayhxy.supabase.co';
const SUPABASE_SERVICE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpuaXNhY3JxZXJ6emtyaWF5aHh5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MDM4NDg2MywiZXhwIjoyMDg1OTYwODYzfQ.gk8VZ7TZxQyJmq1aAl09HpUIZNrbg--BYRT4Hd8zEq8';
const BUCKET_NAME = 'music-files';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

console.log('✅ Supabase initialized:', SUPABASE_URL);

// ═══════════════════════════════════════════════════════════
// EXPRESS + SOCKET.IO
// ═══════════════════════════════════════════════════════════

const app = express();
const httpServer = createServer(app);

const io = new Server(httpServer, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  transports: ['websocket', 'polling'],
  pingTimeout: 10000,
  pingInterval: 5000,
  maxHttpBufferSize: 100 * 1024 * 1024
});

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ═══════════════════════════════════════════════════════════
// MULTER (MEMORY STORAGE FOR FAST SUPABASE UPLOAD)
// ═══════════════════════════════════════════════════════════

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024 }
});

// ═══════════════════════════════════════════════════════════
// ROOM DATA STRUCTURE
// ═══════════════════════════════════════════════════════════

const rooms = new Map();

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
    hostDelay: 0,
    
    audioFileId: null,
    audioUrl: null,
    metadata: { title: 'No track loaded', artist: 'Unknown', duration: 0 },
    
    isPlaying: false,
    scheduledStartTime: null,
    startOffset: 0,
    globalVolume: 1.0,
    
    listeners: new Map(),
    createdAt: Date.now(),
    syncInterval: null
  };
  
  rooms.set(code, room);
  
  room.syncInterval = setInterval(() => {
    if (room.isPlaying) broadcastPreciseSync(code);
  }, 1000);
  
  console.log(`🏠 Room created: ${code} - ${room.roomName}`);
  broadcastRoomList();
  
  return room;
}

async function deleteRoom(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;
  
  console.log(`🗑️ Deleting room: ${roomCode}`);
  
  if (room.syncInterval) clearInterval(room.syncInterval);
  
  // ✅ DELETE FILE FROM SUPABASE
  if (room.audioFileId) {
    try {
      const { error } = await supabase.storage
        .from(BUCKET_NAME)
        .remove([room.audioFileId]);
      
      if (error) throw error;
      console.log(`   ✅ Deleted file from Supabase: ${room.audioFileId}`);
    } catch (err) {
      console.warn(`   ⚠️ Could not delete file:`, err.message);
    }
  }
  
  io.to(roomCode).emit('room-closed');
  rooms.delete(roomCode);
  broadcastRoomList();
}

// ═══════════════════════════════════════════════════════════
// BROADCASTING (OPTIMIZED)
// ═══════════════════════════════════════════════════════════

function broadcastRoomList() {
  const roomList = Array.from(rooms.values()).map(r => ({
    roomCode: r.code,
    roomName: r.roomName,
    hostName: r.hostName,
    listenerCount: r.listeners.size,
    isPlaying: r.isPlaying,
    hasAudio: !!r.audioUrl,
    trackTitle: r.metadata.title,
    createdAt: r.createdAt
  }));
  
  io.emit('room-list', roomList);
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
    synced: l.synced || false
  }));
  
  // Only send to host
  io.to(room.hostSocketId).emit('listener-list', listeners);
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

// ═══════════════════════════════════════════════════════════
// PLAYBACK CONTROL
// ═══════════════════════════════════════════════════════════

function startPlayback(roomCode, position = 0) {
  const room = rooms.get(roomCode);
  if (!room || !room.audioUrl) return;
  
  const scheduledStartTime = getHighPrecisionTime() + 1500;
  room.isPlaying = true;
  room.scheduledStartTime = scheduledStartTime;
  room.startOffset = position;
  
  console.log(`▶️ Room ${roomCode}: Play`);
  
  io.to(roomCode).emit('sync:play', {
    scheduledStartTime,
    offset: position,
    serverTime: getHighPrecisionTime(),
    roomCode
  });
  
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
  
  console.log(`⏸️ Room ${roomCode}: Pause`);
  
  io.to(roomCode).emit('sync:pause', {
    position: room.startOffset,
    serverTime: getHighPrecisionTime(),
    roomCode
  });
  
  broadcastRoomList();
}

function stopPlayback(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;
  
  room.isPlaying = false;
  room.startOffset = 0;
  room.scheduledStartTime = null;
  
  console.log(`⏹️ Room ${roomCode}: Stop`);
  
  io.to(roomCode).emit('sync:stop', {
    serverTime: getHighPrecisionTime(),
    roomCode
  });
  
  broadcastRoomList();
}

function seekPlayback(roomCode, position) {
  const room = rooms.get(roomCode);
  if (!room) return;
  
  room.startOffset = Math.max(0, position);
  
  if (room.isPlaying) {
    const scheduledStartTime = getHighPrecisionTime() + 1000;
    room.scheduledStartTime = scheduledStartTime;
    
    io.to(roomCode).emit('sync:play', {
      scheduledStartTime,
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
}

// ═══════════════════════════════════════════════════════════
// SOCKET.IO EVENTS
// ═══════════════════════════════════════════════════════════

io.on('connection', (socket) => {
  console.log(`🔌 Connected: ${socket.id}`);
  
  socket.on('ntp:sync', (clientSend, cb) => {
    const recv = getHighPrecisionTime();
    const send = getHighPrecisionTime();
    cb({ clientSendTime: clientSend, serverReceiveTime: recv, serverSendTime: send });
  });
  
  socket.on('get-rooms', (cb) => {
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
  
  socket.on('create-room', ({ roomName, hostName }, cb) => {
    const room = createRoom(socket.id, roomName, hostName);
    socket.join(room.code);
    cb({ success: true, roomCode: room.code, roomName: room.roomName });
  });
  
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
        hostDelay: room.hostDelay,
        serverTime: getHighPrecisionTime()
      }
    });
    
    setTimeout(() => broadcastListenerList(roomCode), 100);
  });
  
  socket.on('join-as-listener', ({ roomCode, listenerName }, cb) => {
    const room = rooms.get(roomCode);
    if (!room) return cb({ error: 'Room not found' });
    
    socket.join(roomCode);
    
    const listener = {
      id: socket.id,
      name: listenerName || `Listener ${room.listeners.size + 1}`,
      volume: 1.0,
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
    
    console.log(`👂 ${listener.name} joined room ${roomCode}`);
    
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
  
  socket.on('download-complete', ({ roomCode }) => {
    const room = rooms.get(roomCode);
    const l = room?.listeners.get(socket.id);
    if (l) {
      l.downloaded = true;
      broadcastListenerList(roomCode);
    }
  });
  
  socket.on('sync-ready', ({ roomCode }) => {
    const room = rooms.get(roomCode);
    const l = room?.listeners.get(socket.id);
    if (l) {
      l.synced = true;
      broadcastListenerList(roomCode);
    }
  });
  
  socket.on('update-sync-stats', ({ roomCode, clockOffset, syncAccuracy }) => {
    const room = rooms.get(roomCode);
    const l = room?.listeners.get(socket.id);
    if (l) {
      l.clockOffset = clockOffset;
      l.syncAccuracy = syncAccuracy;
      broadcastListenerList(roomCode);
    }
  });
  
  socket.on('update-ping', ({ roomCode, ping }) => {
    const room = rooms.get(roomCode);
    const l = room?.listeners.get(socket.id);
    if (l) {
      l.ping = Math.round(ping);
      broadcastListenerList(roomCode);
    }
  });
  
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
  
  socket.on('set-listener-volume', ({ roomCode, listenerId, volume }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    const l = room.listeners.get(listenerId);
    if (l) {
      l.volume = Math.max(0, Math.min(1, volume));
      io.to(listenerId).emit('volume-update', { volume: l.volume });
      broadcastListenerList(roomCode);
    }
  });
  
  socket.on('mute-listener', ({ roomCode, listenerId, muted }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    const l = room.listeners.get(listenerId);
    if (l) {
      l.muted = !!muted;
      io.to(listenerId).emit('mute-update', { muted: l.muted });
      broadcastListenerList(roomCode);
    }
  });
  
  socket.on('set-listener-delay', ({ roomCode, listenerId, delay }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    const l = room.listeners.get(listenerId);
    if (l) {
      l.manualDelay = Math.max(-2500, Math.min(2500, delay));
      io.to(listenerId).emit('delay-update', { delay: l.manualDelay });
      broadcastListenerList(roomCode);
    }
  });
  
  socket.on('set-host-delay', ({ roomCode, delay }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    room.hostDelay = Math.max(-2500, Math.min(2500, delay));
  });
  
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
      if (room.hostSocketId === socket.id) {
        setTimeout(() => {
          const current = rooms.get(roomCode);
          if (current && current.hostSocketId === socket.id) deleteRoom(roomCode);
        }, 30000);
      }
      if (room.listeners.has(socket.id)) {
        room.listeners.delete(socket.id);
        broadcastListenerList(roomCode);
        broadcastRoomList();
      }
    }
  });
});

// ═══════════════════════════════════════════════════════════
// ✅ FAST SUPABASE UPLOAD
// ═══════════════════════════════════════════════════════════

app.post('/upload', upload.single('audio'), async (req, res) => {
  const startTime = Date.now();
  const roomCode = req.body.roomCode;
  
  try {
    const room = rooms.get(roomCode);
    if (!room) return res.status(404).json({ ok: false, error: 'Room not found' });
    if (!req.file) return res.status(400).json({ ok: false, error: 'No file uploaded' });
    
    console.log(`📤 Uploading: ${req.file.originalname} (${(req.file.size / 1024 / 1024).toFixed(2)} MB)`);
    
    if (room.isPlaying) stopPlayback(roomCode);
    
    // Delete old file
    if (room.audioFileId) {
      try {
        await supabase.storage.from(BUCKET_NAME).remove([room.audioFileId]);
      } catch (err) {
        console.warn('Could not delete old file:', err.message);
      }
    }
    
    // ✅ UPLOAD TO SUPABASE
    const fileName = `${Date.now()}-${req.file.originalname}`;
    
    const { data, error } = await supabase.storage
      .from(BUCKET_NAME)
      .upload(fileName, req.file.buffer, {
        contentType: req.file.mimetype,
        cacheControl: '3600',
        upsert: false
      });
    
    if (error) throw error;
    
    // ✅ GET PUBLIC URL
    const { data: urlData } = supabase.storage
      .from(BUCKET_NAME)
      .getPublicUrl(fileName);
    
    const publicUrl = urlData.publicUrl;
    
    const uploadTime = Date.now() - startTime;
    console.log(`   ✅ Upload complete in ${uploadTime}ms`);
    console.log(`   🌐 URL: ${publicUrl}`);
    
    room.audioFileId = fileName;
    room.audioUrl = publicUrl;
    room.metadata = {
      title: req.file.originalname.replace(/\.[^/.]+$/, ''),
      artist: 'Unknown',
      duration: 0
    };
    room.startOffset = 0;
    room.isPlaying = false;
    room.scheduledStartTime = null;
    
    room.listeners.forEach(l => {
      l.downloaded = false;
      l.synced = false;
    });
    
    io.to(roomCode).emit('file:ready', { url: room.audioUrl, metadata: room.metadata });
    broadcastListenerList(roomCode);
    broadcastRoomList();
    
    res.json({ ok: true, audioUrl: publicUrl, metadata: room.metadata, uploadTime });
    
  } catch (err) {
    console.error('❌ Upload error:', err);
    res.status(500).json({ ok: false, error: 'Failed to upload', details: err.message });
  }
});

// ═══════════════════════════════════════════════════════════
// START SERVER
// ═══════════════════════════════════════════════════════════

const PORT = process.env.PORT || 3000;

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🚀 ═══════════════════════════════════════`);
  console.log(`   Music Sync Server (Supabase Edition)`);
  console.log(`   ═══════════════════════════════════════`);
  console.log(`   🌐 Port: ${PORT}`);
  console.log(`   📦 Storage: Supabase`);
  console.log(`   ⏱️  Delay Range: ±2500ms`);
  console.log(`═══════════════════════════════════════\n`);
});
