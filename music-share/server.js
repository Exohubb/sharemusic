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

// ✅ FIX: Disable global fetch for Appwrite compatibility
if (global.fetch) {
  console.log('⚠️  Disabling global fetch for Appwrite compatibility');
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

// Validate configuration at startup
console.log('\n🔧 Checking Appwrite Configuration...');
console.log('   Endpoint:', process.env.APPWRITE_ENDPOINT);
console.log('   Project ID:', process.env.APPWRITE_PROJECT_ID);
console.log('   API Key:', process.env.APPWRITE_API_KEY ? `✅ Set (${process.env.APPWRITE_API_KEY.substring(0, 20)}...)` : '❌ MISSING');
console.log('   Bucket ID:', BUCKET_ID || '❌ MISSING');

if (!BUCKET_ID || !process.env.APPWRITE_PROJECT_ID || !process.env.APPWRITE_API_KEY) {
  console.error('\n❌ FATAL ERROR: Appwrite not properly configured!');
  console.error('Please check your .env file\n');
  process.exit(1);
}

console.log('✅ Appwrite initialized\n');

// ═══════════════════════════════════════════════════════════
// SOCKET.IO CONFIGURATION
// ═══════════════════════════════════════════════════════════

const io = new Server(httpServer, { 
  cors: { 
    origin: '*',
    methods: ['GET', 'POST']
  },
  perMessageDeflate: false,
  maxHttpBufferSize: 1e8,
  pingTimeout: 10000,
  pingInterval: 5000
});

// ═══════════════════════════════════════════════════════════
// MIDDLEWARE
// ═══════════════════════════════════════════════════════════

app.use(cors());
app.use(express.json());
app.use(express.static(join(__dirname, 'public')));

// Memory storage for multer
const upload = multer({ 
  storage: multer.memoryStorage(),
  limits: { fileSize: 100 * 1024 * 1024 }
});

// ═══════════════════════════════════════════════════════════
// ROOM SYSTEM
// ═══════════════════════════════════════════════════════════

const rooms = new Map();
const AUTO_DELETE_TIMEOUT = 30 * 60 * 1000;

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
    
    createdAt: Date.now(),
    autoDeleteTimer: null
  };
  
  rooms.set(roomCode, room);
  console.log('🏠 Room created:', roomCode, 'by', hostName);
  
  room.autoDeleteTimer = setTimeout(() => {
    deleteRoom(roomCode);
  }, AUTO_DELETE_TIMEOUT);
  
  broadcastRoomList();
  return room;
}

async function deleteRoom(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;
  
  console.log('🗑️ Deleting room:', roomCode);
  
  if (room.audioFileId) {
    try {
      await storage.deleteFile(BUCKET_ID, room.audioFileId);
      console.log('   ✅ Deleted Appwrite file:', room.audioFileId);
    } catch (err) {
      console.error('   ⚠️ Error deleting file:', err.message);
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
// PLAYBACK CONTROL
// ═══════════════════════════════════════════════════════════

function startPlayback(roomCode, position = 0) {
  const room = rooms.get(roomCode);
  if (!room || !room.audioUrl) return;

  const scheduledStartTime = Date.now() + 500;
  
  room.isPlaying = true;
  room.scheduledStartTime = scheduledStartTime;
  room.startOffset = position;

  console.log(`▶️  Room ${roomCode}: Play at ${scheduledStartTime} from ${position}ms`);

  io.to(roomCode).emit('sync:play', {
    scheduledStartTime: scheduledStartTime,
    offset: position,
    serverTime: Date.now()
  });

  broadcastRoomState(roomCode);
}

function pausePlayback(roomCode) {
  const room = rooms.get(roomCode);
  if (!room || !room.isPlaying) return;

  if (room.isPlaying && room.scheduledStartTime) {
    const elapsed = Date.now() - room.scheduledStartTime;
    room.startOffset = room.startOffset + elapsed;
  }
  
  room.isPlaying = false;
  room.scheduledStartTime = null;

  console.log(`⏸️  Room ${roomCode}: Pause at ${room.startOffset}ms`);

  io.to(roomCode).emit('sync:pause', {
    position: room.startOffset,
    serverTime: Date.now()
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
    serverTime: Date.now()
  });

  broadcastRoomState(roomCode);
}

function seekPlayback(roomCode, position) {
  const room = rooms.get(roomCode);
  if (!room) return;

  room.startOffset = position;

  if (room.isPlaying) {
    const scheduledStartTime = Date.now() + 500;
    room.scheduledStartTime = scheduledStartTime;
    
    io.to(roomCode).emit('sync:play', {
      scheduledStartTime: scheduledStartTime,
      offset: position,
      serverTime: Date.now()
    });
  } else {
    io.to(roomCode).emit('sync:seek', {
      position: position,
      serverTime: Date.now()
    });
  }

  broadcastRoomState(roomCode);
}

// ═══════════════════════════════════════════════════════════
// BROADCASTING
// ═══════════════════════════════════════════════════════════

function broadcastRoomState(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;

  let currentPosition = room.startOffset;
  if (room.isPlaying && room.scheduledStartTime) {
    const elapsed = Date.now() - room.scheduledStartTime;
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
    serverTime: Date.now()
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

  socket.on('ntp:sync', (clientSendTime, callback) => {
    callback({
      serverTime: Date.now(),
      clientSendTime: clientSendTime
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
        serverTime: Date.now()
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
      downloaded: false,
      synced: false,
      joinedAt: Date.now()
    };

    room.listeners.set(socket.id, listener);

    let currentPosition = room.startOffset;
    if (room.isPlaying && room.scheduledStartTime) {
      const elapsed = Date.now() - room.scheduledStartTime;
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
        metadata: room.metadata,
        quality: room.quality,
        serverTime: Date.now()
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
// ✅ FILE UPLOAD WITH ADVANCED ERROR HANDLING
// ═══════════════════════════════════════════════════════════

app.post('/upload', upload.single('audio'), async (req, res) => {
  const startTime = Date.now();
  
  try {
    const roomCode = req.body.roomCode;
    const room = rooms.get(roomCode);
    
    console.log('\n═══════════════════════════════════════════');
    console.log('📤 UPLOAD REQUEST RECEIVED');
    console.log('═══════════════════════════════════════════');
    console.log('Room Code:', roomCode);
    console.log('Room exists:', !!room);
    
    if (!room) {
      console.error('❌ Room not found:', roomCode);
      return res.status(404).json({ error: 'Room not found' });
    }

    if (!req.file) {
      console.error('❌ No file in request');
      return res.status(400).json({ error: 'No file uploaded' });
    }

    console.log('File Details:');
    console.log('  - Name:', req.file.originalname);
    console.log('  - Size:', (req.file.size / 1024 / 1024).toFixed(2), 'MB');
    console.log('  - MIME:', req.file.mimetype);
    console.log('  - Buffer:', req.file.buffer ? 'Present' : 'Missing');

    // Delete old file if exists
    if (room.audioFileId) {
      try {
        console.log('🗑️ Deleting old file:', room.audioFileId);
        await storage.deleteFile(BUCKET_ID, room.audioFileId);
        console.log('   ✅ Old file deleted');
      } catch (err) {
        console.warn('   ⚠️  Could not delete old file:', err.message);
      }
    }

    // Upload to Appwrite
    console.log('\n📡 UPLOADING TO APPWRITE');
    console.log('  - Endpoint:', process.env.APPWRITE_ENDPOINT);
    console.log('  - Project:', process.env.APPWRITE_PROJECT_ID);
    console.log('  - Bucket:', BUCKET_ID);
    
    const fileId = ID.unique();
    console.log('  - Generated File ID:', fileId);
    
    const inputFile = InputFile.fromBuffer(
      req.file.buffer,
      req.file.originalname
    );

    const uploadStartTime = Date.now();
    
    const uploadedFile = await storage.createFile(
      BUCKET_ID,
      fileId,
      inputFile,
      [Permission.read(Role.any())]
    );

    const uploadDuration = Date.now() - uploadStartTime;
    console.log('\n✅ APPWRITE UPLOAD SUCCESS');
    console.log('  - Duration:', uploadDuration, 'ms');
    console.log('  - File ID:', uploadedFile.$id);
    console.log('  - Size:', uploadedFile.sizeOriginal, 'bytes');

    const actualFileId = uploadedFile.$id;
    
    // Generate download URL
    const fileUrl = `${process.env.APPWRITE_ENDPOINT}/storage/buckets/${BUCKET_ID}/files/${actualFileId}/download?project=${process.env.APPWRITE_PROJECT_ID}`;
    
    console.log('\n🌐 GENERATED DOWNLOAD URL:');
    console.log(fileUrl);
    
    // Test if URL is accessible
    console.log('\n🧪 TESTING URL ACCESSIBILITY...');
    try {
      const testResponse = await fetch(fileUrl, {
        method: 'HEAD',
        headers: {
          'X-Appwrite-Project': process.env.APPWRITE_PROJECT_ID,
          'X-Appwrite-Key': process.env.APPWRITE_API_KEY
        }
      });
      console.log('  - Status:', testResponse.status, testResponse.statusText);
      console.log('  - Headers:', Object.fromEntries(testResponse.headers.entries()));
      
      if (testResponse.ok) {
        console.log('✅ URL is accessible');
      } else {
        console.error('⚠️  URL returned non-200 status');
      }
    } catch (testErr) {
      console.error('❌ URL test failed:', testErr.message);
    }

    // Update room
    room.audioFileId = actualFileId;
    room.audioUrl = fileUrl;
    room.metadata = {
      title: req.file.originalname.replace(/\.[^/.]+$/, ''),
      artist: 'Unknown Artist',
      duration: 0
    };

    console.log('\n✅ ROOM UPDATED');
    console.log('  - Audio File ID:', room.audioFileId);
    console.log('  - Audio URL:', room.audioUrl);
    console.log('  - Metadata:', room.metadata);

    // Notify clients
    console.log('\n📢 BROADCASTING TO CLIENTS');
    io.to(roomCode).emit('file:ready', { 
      url: room.audioUrl,
      metadata: room.metadata 
    });
    console.log('  - Emitted file:ready to room:', roomCode);

    broadcastRoomState(roomCode);
    broadcastRoomList();

    const totalDuration = Date.now() - startTime;
    console.log('\n✅ UPLOAD COMPLETE');
    console.log('  - Total Duration:', totalDuration, 'ms');
    console.log('═══════════════════════════════════════════\n');

    res.json({ 
      success: true,
      metadata: room.metadata,
      audioUrl: room.audioUrl,
      fileId: actualFileId,
      uploadTime: totalDuration
    });

  } catch (err) {
    const totalDuration = Date.now() - startTime;
    
    console.error('\n❌ UPLOAD ERROR');
    console.error('═══════════════════════════════════════════');
    console.error('Error Name:', err.name);
    console.error('Error Message:', err.message);
    console.error('Error Code:', err.code);
    console.error('Error Type:', err.type);
    console.error('Error Stack:', err.stack);
    
    if (err.response) {
      console.error('Response:', err.response);
      if (Buffer.isBuffer(err.response)) {
        console.error('Response (decoded):', err.response.toString());
      }
    }
    
    console.error('Duration before error:', totalDuration, 'ms');
    console.error('═══════════════════════════════════════════\n');
    
    res.status(500).json({ 
      error: err.message || 'Upload failed',
      type: err.type || 'Unknown',
      code: err.code,
      details: err.response ? err.response.toString() : null
    });
  }
});

// ═══════════════════════════════════════════════════════════
// PROXY ENDPOINT WITH ADVANCED ERROR HANDLING
// ═══════════════════════════════════════════════════════════

app.get('/audio/:roomCode', async (req, res) => {
  const startTime = Date.now();
  
  try {
    const roomCode = req.params.roomCode;
    const room = rooms.get(roomCode);
    
    console.log('\n═══════════════════════════════════════════');
    console.log('📡 AUDIO PROXY REQUEST');
    console.log('═══════════════════════════════════════════');
    console.log('Room Code:', roomCode);
    console.log('Room exists:', !!room);
    console.log('Audio File ID:', room?.audioFileId);
    console.log('Request Headers:', req.headers);
    
    if (!room || !room.audioFileId) {
      console.error('❌ Audio not found');
      console.error('═══════════════════════════════════════════\n');
      return res.status(404).json({ error: 'Audio not found' });
    }

    console.log('\n📥 FETCHING FROM APPWRITE');
    console.log('  - Method: Using node-fetch directly');
    console.log('  - Bucket:', BUCKET_ID);
    console.log('  - File ID:', room.audioFileId);

    // Use node-fetch to download from Appwrite
    const downloadUrl = `${process.env.APPWRITE_ENDPOINT}/storage/buckets/${BUCKET_ID}/files/${room.audioFileId}/download?project=${process.env.APPWRITE_PROJECT_ID}`;
    
    console.log('  - URL:', downloadUrl);
    
    const downloadStartTime = Date.now();
    
    const response = await fetch(downloadUrl, {
      headers: {
        'X-Appwrite-Project': process.env.APPWRITE_PROJECT_ID,
        'X-Appwrite-Key': process.env.APPWRITE_API_KEY
      }
    });

    const downloadDuration = Date.now() - downloadStartTime;
    
    console.log('\n📡 APPWRITE RESPONSE');
    console.log('  - Status:', response.status, response.statusText);
    console.log('  - Content-Type:', response.headers.get('content-type'));
    console.log('  - Content-Length:', response.headers.get('content-length'));
    console.log('  - Duration:', downloadDuration, 'ms');

    if (!response.ok) {
      const errorText = await response.text();
      console.error('❌ Appwrite returned error');
      console.error('  - Response:', errorText);
      throw new Error(`Appwrite error: ${response.status} - ${errorText}`);
    }

    const buffer = await response.buffer();
    console.log('  - Buffer size:', buffer.length, 'bytes');

    // Set headers
    res.setHeader('Content-Type', response.headers.get('content-type') || 'audio/mpeg');
    res.setHeader('Content-Length', buffer.length);
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('Cache-Control', 'public, max-age=3600');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Range, Content-Type');

    res.send(buffer);

    const totalDuration = Date.now() - startTime;
    console.log('\n✅ PROXY COMPLETE');
    console.log('  - Total Duration:', totalDuration, 'ms');
    console.log('═══════════════════════════════════════════\n');

  } catch (err) {
    const totalDuration = Date.now() - startTime;
    
    console.error('\n❌ PROXY ERROR');
    console.error('═══════════════════════════════════════════');
    console.error('Error Name:', err.name);
    console.error('Error Message:', err.message);
    console.error('Error Code:', err.code);
    console.error('Error Type:', err.type);
    console.error('Error Stack:', err.stack);
    console.error('Duration before error:', totalDuration, 'ms');
    console.error('═══════════════════════════════════════════\n');
    
    res.status(500).json({ 
      error: 'Failed to load audio',
      details: err.message 
    });
  }
});

// Handle OPTIONS for CORS preflight
app.options('/audio/:roomCode', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Range, Content-Type');
  res.sendStatus(200);
});

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: Date.now(),
    appwrite: !!BUCKET_ID,
    rooms: rooms.size,
    nodeVersion: process.version,
    platform: process.platform
  });
});

// ═══════════════════════════════════════════════════════════
// START SERVER
// ═══════════════════════════════════════════════════════════

const PORT = process.env.PORT || 3000;

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log('🚀 ════════════════════════════════════════');
  console.log('   MUSIC SHARE SERVER (RENDER + APPWRITE)');
  console.log('   ════════════════════════════════════════');
  console.log(`   🌐 Port: ${PORT}`);
  console.log(`   📦 Storage: Appwrite (${BUCKET_ID})`);
  console.log(`   🎯 NTP Sync: Enabled`);
  console.log(`   🔧 Node: ${process.version}`);
  console.log('════════════════════════════════════════\n');
});

process.on('SIGINT', () => {
  console.log('\n⏹️ Shutting down...');
  process.exit(0);
});
