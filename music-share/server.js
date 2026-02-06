import express from 'express';
import { Server } from 'socket.io';
import { createServer } from 'http';
import multer from 'multer';
import { Client, Storage, ID, Permission, Role, InputFile } from 'node-appwrite';
import cors from 'cors';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import fetch from 'node-fetch';

if (global.fetch) {
  delete global.fetch;
}

dotenv.config();

const __dirname = dirname(fileURLToPath(import.meta.url));
const app = express();
const httpServer = createServer(app);

// ═══════════════════════════════════════════════════════════
// APPWRITE SETUP
// ═══════════════════════════════════════════════════════════

const appwriteClient = new Client()
  .setEndpoint(process.env.APPWRITE_ENDPOINT || 'https://cloud.appwrite.io/v1')
  .setProject(process.env.APPWRITE_PROJECT_ID)
  .setKey(process.env.APPWRITE_API_KEY);

const storage = new Storage(appwriteClient);
const BUCKET_ID = process.env.APPWRITE_BUCKET_ID;

console.log('\n🔧 Appwrite Configuration:');
console.log('   Endpoint:', process.env.APPWRITE_ENDPOINT);
console.log('   Project:', process.env.APPWRITE_PROJECT_ID);
console.log('   Bucket:', BUCKET_ID);

if (!BUCKET_ID || !process.env.APPWRITE_PROJECT_ID || !process.env.APPWRITE_API_KEY) {
  console.error('\n❌ Appwrite not configured!\n');
  process.exit(1);
}

// ═══════════════════════════════════════════════════════════
// SOCKET.IO WITH ENHANCED TIMING
// ═══════════════════════════════════════════════════════════

const io = new Server(httpServer, { 
  cors: { 
    origin: '*',
    methods: ['GET', 'POST']
  },
  perMessageDeflate: false,
  maxHttpBufferSize: 1e8,
  pingTimeout: 10000,
  pingInterval: 5000,
  // ✅ Enhanced for low latency
  transports: ['websocket', 'polling'],
  upgradeTimeout: 3000,
  allowUpgrades: true
});

// ═══════════════════════════════════════════════════════════
// MIDDLEWARE
// ═══════════════════════════════════════════════════════════

app.use(cors());
app.use(express.json());
app.use(express.static(join(__dirname, 'public')));

const upload = multer({ 
  storage: multer.memoryStorage(),
  limits: { fileSize: 100 * 1024 * 1024 }
});

// ═══════════════════════════════════════════════════════════
// ROOM SYSTEM WITH ENHANCED SYNC
// ═══════════════════════════════════════════════════════════

const rooms = new Map();
const AUTO_DELETE_TIMEOUT = 30 * 60 * 1000;

// ✅ HIGH-PRECISION SERVER TIME
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

function createRoom(hostSocketId, hostName) {
  const roomCode = generateRoomCode();
  
  const room = {
    code: roomCode,
    hostSocketId: hostSocketId,
    hostName: hostName || 'Host',
    listeners: new Map(),
    
    audioFileId: null,
    audioUrl: null,
    metadata: {
      title: 'Unknown Track',
      artist: 'Unknown Artist',
      duration: 0
    },
    
    isPlaying: false,
    scheduledStartTime: null,
    startOffset: 0,
    
    globalVolume: 1.0,
    quality: 'standard',
    
    // ✅ SYNC TRACKING
    lastSyncTime: null,
    syncInterval: null,
    
    createdAt: Date.now(),
    autoDeleteTimer: null
  };
  
  rooms.set(roomCode, room);
  console.log('🏠 Room created:', roomCode, 'by', hostName);
  
  room.autoDeleteTimer = setTimeout(() => {
    deleteRoom(roomCode);
  }, AUTO_DELETE_TIMEOUT);
  
  // ✅ START CONTINUOUS SYNC BROADCAST
  room.syncInterval = setInterval(() => {
    if (room.isPlaying) {
      broadcastPreciseSync(roomCode);
    }
  }, 1000); // Sync every second
  
  broadcastRoomList();
  return room;
}

async function deleteRoom(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;
  
  console.log('🗑️ Deleting room:', roomCode);
  
  if (room.syncInterval) {
    clearInterval(room.syncInterval);
  }
  
  if (room.audioFileId) {
    try {
      await storage.deleteFile(BUCKET_ID, room.audioFileId);
    } catch (err) {
      console.error('Error deleting file:', err.message);
    }
  }
  
  if (room.autoDeleteTimer) {
    clearTimeout(room.autoDeleteTimer);
  }
  
  io.to(roomCode).emit('room-closed');
  rooms.delete(roomCode);
  broadcastRoomList();
}

function broadcastRoomList() {
  const roomList = Array.from(rooms.values()).map(room => ({
    roomId: room.code,
    hostName: room.hostName,
    listenerCount: room.listeners.size,
    playing: room.isPlaying,
    hasFile: !!room.audioUrl,
    metadata: room.metadata
  }));
  
  io.emit('room-list', roomList);
}

// ═══════════════════════════════════════════════════════════
// ✅ ENHANCED PLAYBACK CONTROL WITH PRECISE TIMING
// ═══════════════════════════════════════════════════════════

function startPlayback(roomCode, position = 0) {
  const room = rooms.get(roomCode);
  if (!room || !room.audioUrl) return;

  // ✅ Schedule with buffer for network + audio loading
  const scheduledStartTime = getHighPrecisionTime() + 1000; // 1 second buffer
  
  room.isPlaying = true;
  room.scheduledStartTime = scheduledStartTime;
  room.startOffset = position;
  room.lastSyncTime = getHighPrecisionTime();

  console.log(`▶️  Room ${roomCode}: Play scheduled at ${scheduledStartTime}, position ${position}ms`);

  io.to(roomCode).emit('sync:play', {
    scheduledStartTime: scheduledStartTime,
    offset: position,
    serverTime: getHighPrecisionTime(),
    roomCode: roomCode
  });

  broadcastRoomState(roomCode);
}

function pausePlayback(roomCode) {
  const room = rooms.get(roomCode);
  if (!room || !room.isPlaying) return;

  if (room.isPlaying && room.scheduledStartTime) {
    const elapsed = getHighPrecisionTime() - room.scheduledStartTime;
    room.startOffset = room.startOffset + elapsed;
  }
  
  room.isPlaying = false;
  room.scheduledStartTime = null;

  console.log(`⏸️  Room ${roomCode}: Pause at ${room.startOffset}ms`);

  io.to(roomCode).emit('sync:pause', {
    position: room.startOffset,
    serverTime: getHighPrecisionTime(),
    roomCode: roomCode
  });

  broadcastRoomState(roomCode);
}

function stopPlayback(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;

  room.isPlaying = false;
  room.startOffset = 0;
  room.scheduledStartTime = null;

  console.log(`⏹️  Room ${roomCode}: Stop`);

  io.to(roomCode).emit('sync:stop', {
    serverTime: getHighPrecisionTime(),
    roomCode: roomCode
  });

  broadcastRoomState(roomCode);
}

function seekPlayback(roomCode, position) {
  const room = rooms.get(roomCode);
  if (!room) return;

  room.startOffset = position;

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

// ✅ NEW: CONTINUOUS SYNC BROADCAST
function broadcastPreciseSync(roomCode) {
  const room = rooms.get(roomCode);
  if (!room || !room.isPlaying) return;

  const currentServerTime = getHighPrecisionTime();
  const elapsed = currentServerTime - room.scheduledStartTime;
  const currentPosition = room.startOffset + elapsed;

  io.to(roomCode).emit('sync:update', {
    serverTime: currentServerTime,
    scheduledStartTime: room.scheduledStartTime,
    startOffset: room.startOffset,
    currentPosition: currentPosition,
    roomCode: roomCode
  });
}

// ═══════════════════════════════════════════════════════════
// BROADCASTING
// ═══════════════════════════════════════════════════════════

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
    quality: room.quality,
    metadata: room.metadata,
    hasFile: !!room.audioUrl,
    audioUrl: room.audioUrl,
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
    syncAccuracy: l.syncAccuracy || 'unknown',
    downloaded: l.downloaded || false,
    synced: l.synced || false
  }));

  io.to(roomCode).emit('listener-list', listeners);
}

// ═══════════════════════════════════════════════════════════
// SOCKET.IO EVENTS
// ═══════════════════════════════════════════════════════════

io.on('connection', (socket) => {
  console.log('🔌 Connected:', socket.id);

  // ✅ ENHANCED NTP SYNC (Multiple samples for accuracy)
  socket.on('ntp:sync', (clientSendTime, callback) => {
    const serverReceiveTime = getHighPrecisionTime();
    const serverSendTime = getHighPrecisionTime();
    
    callback({
      clientSendTime: clientSendTime,
      serverReceiveTime: serverReceiveTime,
      serverSendTime: serverSendTime
    });
  });

  socket.on('get-rooms', (callback) => {
    const roomList = Array.from(rooms.values()).map(room => ({
      roomId: room.code,
      hostName: room.hostName,
      listenerCount: room.listeners.size,
      playing: room.isPlaying,
      hasFile: !!room.audioUrl,
      metadata: room.metadata
    }));
    callback(roomList);
  });

  socket.on('create-room', ({ hostName }, callback) => {
    const room = createRoom(socket.id, hostName);
    socket.join(room.code);
    callback({ 
      success: true, 
      roomCode: room.code,
      hostName: room.hostName
    });
  });

  socket.on('join-as-host', ({ roomCode }, callback) => {
    const room = rooms.get(roomCode);
    
    if (!room) {
      callback({ error: 'Room not found' });
      return;
    }

    room.hostSocketId = socket.id;
    socket.join(roomCode);
    
    callback({
      success: true,
      roomState: {
        playing: room.isPlaying,
        position: room.startOffset,
        metadata: room.metadata,
        quality: room.quality,
        hasFile: !!room.audioUrl,
        audioUrl: room.audioUrl,
        serverTime: getHighPrecisionTime()
      }
    });

    console.log('👑 Host joined room:', roomCode);
    setTimeout(() => broadcastListenerList(roomCode), 100);
  });

  socket.on('join-as-listener', ({ roomCode, listenerName }, callback) => {
    const room = rooms.get(roomCode);
    
    if (!room) {
      callback({ error: 'Room not found' });
      return;
    }

    socket.join(roomCode);
    
    const listener = {
      id: socket.id,
      name: listenerName || `Listener ${room.listeners.size + 1}`,
      volume: 1.0,
      muted: false,
      ping: 0,
      clockOffset: 0,
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

    callback({
      success: true,
      roomCode,
      hostName: room.hostName,
      audioUrl: room.audioUrl,
      currentState: {
        playing: room.isPlaying,
        position: currentPosition,
        scheduledStartTime: room.scheduledStartTime,
        startOffset: room.startOffset,
        metadata: room.metadata,
        quality: room.quality,
        serverTime: getHighPrecisionTime()
      }
    });

    broadcastListenerList(roomCode);
    console.log(`👂 ${listener.name} joined room ${roomCode}`);
  });

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

  // ✅ NEW: Update listener sync stats
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
    const listener = room.listeners.get(listenerId);
    if (listener) {
      listener.volume = volume;
      io.to(listenerId).emit('volume-update', { volume });
      broadcastListenerList(roomCode);
    }
  });

  socket.on('mute-listener', ({ roomCode, listenerId, muted }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    const listener = room.listeners.get(listenerId);
    if (listener) {
      listener.muted = muted;
      io.to(listenerId).emit('mute-update', { muted });
      broadcastListenerList(roomCode);
    }
  });

  socket.on('set-quality', ({ roomCode, quality }) => {
    const room = rooms.get(roomCode);
    if (!room || socket.id !== room.hostSocketId) return;
    room.quality = quality;
    broadcastRoomState(roomCode);
  });

  socket.on('update-ping', ({ roomCode, ping }) => {
    const room = rooms.get(roomCode);
    if (!room) return;
    const listener = room.listeners.get(socket.id);
    if (listener) {
      listener.ping = ping;
      broadcastListenerList(roomCode);
    }
  });

  socket.on('ping-request', () => {
    socket.emit('pong-response');
  });

  socket.on('disconnect', () => {
    console.log('🔌 Disconnected:', socket.id);
    for (const [roomCode, room] of rooms) {
      if (room.hostSocketId === socket.id) {
        setTimeout(() => {
          const currentRoom = rooms.get(roomCode);
          if (currentRoom && currentRoom.hostSocketId === socket.id) {
            deleteRoom(roomCode);
          }
        }, 30000);
      }
      if (room.listeners.has(socket.id)) {
        room.listeners.delete(socket.id);
        broadcastListenerList(roomCode);
      }
    }
  });
});

// ═══════════════════════════════════════════════════════════
// FILE UPLOAD
// ═══════════════════════════════════════════════════════════

app.post('/upload', upload.single('audio'), async (req, res) => {
  try {
    const roomCode = req.body.roomCode;
    const room = rooms.get(roomCode);
    
    if (!room) {
      return res.status(404).json({ error: 'Room not found' });
    }

    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    console.log(`📤 Uploading: ${req.file.originalname} (${(req.file.size / 1024 / 1024).toFixed(2)} MB)`);

    if (room.audioFileId) {
      try {
        await storage.deleteFile(BUCKET_ID, room.audioFileId);
      } catch (err) {
        console.warn('Could not delete old file');
      }
    }

    const fileId = ID.unique();
    const inputFile = InputFile.fromBuffer(req.file.buffer, req.file.originalname);

    const uploadedFile = await storage.createFile(
      BUCKET_ID,
      fileId,
      inputFile,
      [Permission.read(Role.any())]
    );

    const actualFileId = uploadedFile.$id;
    const fileUrl = `${process.env.APPWRITE_ENDPOINT}/storage/buckets/${BUCKET_ID}/files/${actualFileId}/download?project=${process.env.APPWRITE_PROJECT_ID}`;

    room.audioFileId = actualFileId;
    room.audioUrl = fileUrl;
    room.metadata = {
      title: req.file.originalname.replace(/\.[^/.]+$/, ''),
      artist: 'Unknown Artist',
      duration: 0
    };

    io.to(roomCode).emit('file:ready', { 
      url: room.audioUrl,
      metadata: room.metadata 
    });

    res.json({ 
      success: true,
      metadata: room.metadata,
      audioUrl: room.audioUrl,
      fileId: actualFileId
    });

    broadcastRoomState(roomCode);
    broadcastRoomList();

    console.log('✅ Upload complete');

  } catch (err) {
    console.error('❌ Upload error:', err.message);
    res.status(500).json({ 
      error: err.message || 'Upload failed'
    });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: Date.now(),
    highPrecision: getHighPrecisionTime(),
    rooms: rooms.size
  });
});

// ═══════════════════════════════════════════════════════════
// START SERVER
// ═══════════════════════════════════════════════════════════

const PORT = process.env.PORT || 3000;

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log('🚀 ════════════════════════════════════════');
  console.log('   MUSIC SHARE SERVER (ENHANCED SYNC)');
  console.log('   ════════════════════════════════════════');
  console.log(`   🌐 Port: ${PORT}`);
  console.log(`   ⏱️  High-Precision Timing: Enabled`);
  console.log(`   🔄 Continuous Sync: Enabled`);
  console.log('════════════════════════════════════════\n');
});

process.on('SIGINT', () => {
  console.log('\n⏹️ Shutting down...');
  process.exit(0);
});
