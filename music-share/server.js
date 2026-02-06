import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import dotenv from 'dotenv';
import multer from 'multer';
import { fileURLToPath } from 'url';
import path from 'path';
import { createClient } from '@supabase/supabase-js';
import sql from './db.js';



// Add this after imports, before const app = express();
async function ensureTablesExist() {
  try {
    console.log('🔍 Checking database tables...');
    
    await sql`
      CREATE TABLE IF NOT EXISTS rooms (
        code VARCHAR(4) PRIMARY KEY,
        room_name VARCHAR(100) NOT NULL,
        host_name VARCHAR(50) NOT NULL,
        host_socket_id VARCHAR(50),
        audio_file_id VARCHAR(255),
        audio_url TEXT,
        track_title VARCHAR(255) DEFAULT 'No track loaded',
        track_artist VARCHAR(100) DEFAULT 'Unknown',
        is_playing BOOLEAN DEFAULT false,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `;
    
    await sql`
      CREATE TABLE IF NOT EXISTS listeners (
        id SERIAL PRIMARY KEY,
        room_code VARCHAR(4) REFERENCES rooms(code) ON DELETE CASCADE,
        socket_id VARCHAR(50) NOT NULL,
        name VARCHAR(50) NOT NULL,
        volume DECIMAL(3,2) DEFAULT 1.0,
        muted BOOLEAN DEFAULT false,
        manual_delay INTEGER DEFAULT 0,
        ping INTEGER DEFAULT 0,
        clock_offset INTEGER DEFAULT 0,
        sync_accuracy VARCHAR(20) DEFAULT 'calibrating',
        joined_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(room_code, socket_id)
      )
    `;
    
    await sql`CREATE INDEX IF NOT EXISTS idx_listeners_room ON listeners(room_code)`;
    await sql`CREATE INDEX IF NOT EXISTS idx_listeners_socket ON listeners(socket_id)`;
    
    console.log('✅ Database tables ready');
  } catch (err) {
    console.error('❌ Database setup error:', err.message);
  }
}





dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ═══════════════════════════════════════════════════════════
// SUPABASE STORAGE
// ═══════════════════════════════════════════════════════════

const SUPABASE_URL = 'https://jnisacrqerzzkriayhxy.supabase.co';
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpuaXNhY3JxZXJ6emtyaWF5aHh5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAzODQ4NjMsImV4cCI6MjA4NTk2MDg2M30.Dirt_VYOV9QIp0TR5-UmrNXrzvMINujYFZiOL-znP7E';

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
const BUCKET_NAME = 'music-sync-files';

console.log('✅ Supabase Storage initialized');
console.log('✅ PostgreSQL connected');

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
// MULTER
// ═══════════════════════════════════════════════════════════

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const allowedTypes = ['audio/mpeg', 'audio/mp3', 'audio/wav', 'audio/m4a', 'audio/mp4', 'audio/x-m4a', 'audio/aac'];
    if (allowedTypes.includes(file.mimetype) || file.originalname.match(/\.(mp3|m4a|wav|aac)$/i)) {
      cb(null, true);
    } else {
      cb(new Error('Only audio files allowed'));
    }
  }
});

// ═══════════════════════════════════════════════════════════
// IN-MEMORY CACHE (for real-time state)
// ═══════════════════════════════════════════════════════════

const roomsCache = new Map(); // For real-time playback state

function getHighPrecisionTime() {
  return Date.now() + (performance.now() % 1);
}

function generateRoomCode() {
  return Math.floor(1000 + Math.random() * 9000).toString();
}

// ═══════════════════════════════════════════════════════════
// DATABASE FUNCTIONS
// ═══════════════════════════════════════════════════════════

async function createRoomInDB(code, roomName, hostName, hostSocketId) {
  try {
    await sql`
      INSERT INTO rooms (code, room_name, host_name, host_socket_id)
      VALUES (${code}, ${roomName}, ${hostName}, ${hostSocketId})
    `;
    
    // Create cache entry
    roomsCache.set(code, {
      code,
      roomName,
      hostName,
      hostSocketId,
      hostDelay: 0,
      isPlaying: false,
      scheduledStartTime: null,
      startOffset: 0,
      syncInterval: null,
      listeners: new Map()
    });
    
    console.log(`🏠 Room created in DB: ${code}`);
    return true;
  } catch (err) {
    console.error('❌ Failed to create room:', err);
    return false;
  }
}

async function getRoomFromDB(code) {
  try {
    const [room] = await sql`
      SELECT * FROM rooms WHERE code = ${code}
    `;
    
    if (!room) return null;
    
    // Load listeners
    const listeners = await sql`
      SELECT * FROM listeners WHERE room_code = ${code}
    `;
    
    return {
      ...room,
      listeners: listeners
    };
  } catch (err) {
    console.error('❌ Failed to get room:', err);
    return null;
  }
}

async function updateRoomAudio(code, fileId, audioUrl, title) {
  try {
    await sql`
      UPDATE rooms 
      SET audio_file_id = ${fileId},
          audio_url = ${audioUrl},
          track_title = ${title},
          updated_at = NOW()
      WHERE code = ${code}
    `;
    return true;
  } catch (err) {
    console.error('❌ Failed to update room audio:', err);
    return false;
  }
}

async function deleteRoomFromDB(code) {
  try {
    // Get audio file ID before deleting
    const [room] = await sql`SELECT audio_file_id FROM rooms WHERE code = ${code}`;
    
    // Delete from database (listeners will be deleted by CASCADE)
    await sql`DELETE FROM rooms WHERE code = ${code}`;
    
    // Delete from Supabase Storage
    if (room?.audio_file_id) {
      try {
        await supabase.storage.from(BUCKET_NAME).remove([room.audio_file_id]);
        console.log(`   ✅ Deleted file: ${room.audio_file_id}`);
      } catch (err) {
        console.warn('   ⚠️ Could not delete file:', err.message);
      }
    }
    
    // Remove from cache
    const cached = roomsCache.get(code);
    if (cached?.syncInterval) clearInterval(cached.syncInterval);
    roomsCache.delete(code);
    
    console.log(`🗑️ Room deleted: ${code}`);
    return true;
  } catch (err) {
    console.error('❌ Failed to delete room:', err);
    return false;
  }
}

async function addListenerToDB(roomCode, socketId, name) {
  try {
    await sql`
      INSERT INTO listeners (room_code, socket_id, name)
      VALUES (${roomCode}, ${socketId}, ${name})
      ON CONFLICT (room_code, socket_id) 
      DO UPDATE SET name = ${name}, joined_at = NOW()
    `;
    
    // Add to cache
    const cached = roomsCache.get(roomCode);
    if (cached) {
      cached.listeners.set(socketId, {
        id: socketId,
        name,
        volume: 1.0,
        muted: false,
        ping: 0,
        clockOffset: 0,
        manualDelay: 0,
        syncAccuracy: 'calibrating',
        downloaded: false,
        synced: false
      });
    }
    
    return true;
  } catch (err) {
    console.error('❌ Failed to add listener:', err);
    return false;
  }
}

async function removeListenerFromDB(roomCode, socketId) {
  try {
    await sql`
      DELETE FROM listeners 
      WHERE room_code = ${roomCode} AND socket_id = ${socketId}
    `;
    
    // Remove from cache
    const cached = roomsCache.get(roomCode);
    if (cached) {
      cached.listeners.delete(socketId);
    }
    
    return true;
  } catch (err) {
    console.error('❌ Failed to remove listener:', err);
    return false;
  }
}

async function getAllRooms() {
  try {
    const rooms = await sql`
      SELECT 
        r.*,
        COUNT(l.id) as listener_count
      FROM rooms r
      LEFT JOIN listeners l ON r.code = l.room_code
      GROUP BY r.code
      ORDER BY r.created_at DESC
    `;
    
    return rooms.map(r => ({
      roomCode: r.code,
      roomName: r.room_name,
      hostName: r.host_name,
      listenerCount: parseInt(r.listener_count) || 0,
      isPlaying: r.is_playing,
      hasAudio: !!r.audio_url,
      trackTitle: r.track_title,
      createdAt: r.created_at
    }));
  } catch (err) {
    console.error('❌ Failed to get rooms:', err);
    return [];
  }
}

// ═══════════════════════════════════════════════════════════
// BROADCASTING
// ═══════════════════════════════════════════════════════════

async function broadcastRoomList() {
  const rooms = await getAllRooms();
  io.emit('room-list', rooms);
}

function broadcastListenerList(roomCode) {
  const cached = roomsCache.get(roomCode);
  if (!cached) return;
  
  const listeners = Array.from(cached.listeners.values()).map(l => ({
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
  
  io.to(cached.hostSocketId).emit('listener-list', listeners);
}

function broadcastPreciseSync(roomCode) {
  const cached = roomsCache.get(roomCode);
  if (!cached || !cached.isPlaying) return;
  
  const now = getHighPrecisionTime();
  const elapsed = now - cached.scheduledStartTime;
  const pos = cached.startOffset + elapsed;
  
  io.to(roomCode).emit('sync:update', {
    serverTime: now,
    scheduledStartTime: cached.scheduledStartTime,
    startOffset: cached.startOffset,
    currentPosition: pos,
    roomCode
  });
}

// ═══════════════════════════════════════════════════════════
// PLAYBACK CONTROLS
// ═══════════════════════════════════════════════════════════

function startPlayback(roomCode, position = 0) {
  const cached = roomsCache.get(roomCode);
  if (!cached) return;
  
  const scheduledStartTime = getHighPrecisionTime() + 1500;
  cached.isPlaying = true;
  cached.scheduledStartTime = scheduledStartTime;
  cached.startOffset = position;
  
  io.to(roomCode).emit('sync:play', {
    scheduledStartTime,
    offset: position,
    serverTime: getHighPrecisionTime(),
    roomCode
  });
  
  broadcastRoomList();
}

function pausePlayback(roomCode) {
  const cached = roomsCache.get(roomCode);
  if (!cached) return;
  
  if (cached.isPlaying && cached.scheduledStartTime) {
    const elapsed = getHighPrecisionTime() - cached.scheduledStartTime;
    cached.startOffset = Math.max(0, cached.startOffset + elapsed);
  }
  
  cached.isPlaying = false;
  cached.scheduledStartTime = null;
  
  io.to(roomCode).emit('sync:pause', {
    position: cached.startOffset,
    serverTime: getHighPrecisionTime(),
    roomCode
  });
  
  broadcastRoomList();
}

function stopPlayback(roomCode) {
  const cached = roomsCache.get(roomCode);
  if (!cached) return;
  
  cached.isPlaying = false;
  cached.startOffset = 0;
  cached.scheduledStartTime = null;
  
  io.to(roomCode).emit('sync:stop', {
    serverTime: getHighPrecisionTime(),
    roomCode
  });
  
  broadcastRoomList();
}

function seekPlayback(roomCode, position) {
  const cached = roomsCache.get(roomCode);
  if (!cached) return;
  
  cached.startOffset = Math.max(0, position);
  
  if (cached.isPlaying) {
    const scheduledStartTime = getHighPrecisionTime() + 1000;
    cached.scheduledStartTime = scheduledStartTime;
    
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
  
  socket.on('get-rooms', async (cb) => {
    const rooms = await getAllRooms();
    cb(rooms);
  });
  
  socket.on('create-room', async ({ roomName, hostName }, cb) => {
    const code = generateRoomCode();
    const success = await createRoomInDB(code, roomName, hostName, socket.id);
    
    if (success) {
      socket.join(code);
      
      // Start sync interval
      const cached = roomsCache.get(code);
      cached.syncInterval = setInterval(() => {
        if (cached.isPlaying) broadcastPreciseSync(code);
      }, 1000);
      
      await broadcastRoomList();
      cb({ success: true, roomCode: code, roomName });
    } else {
      cb({ success: false, error: 'Failed to create room' });
    }
  });
  
  socket.on('join-as-host', async ({ roomCode }, cb) => {
    const room = await getRoomFromDB(roomCode);
    if (!room) return cb({ success: false, error: 'Room not found' });
    
    // Update socket ID
    await sql`UPDATE rooms SET host_socket_id = ${socket.id} WHERE code = ${roomCode}`;
    
    let cached = roomsCache.get(roomCode);
    if (!cached) {
      cached = {
        code: roomCode,
        roomName: room.room_name,
        hostName: room.host_name,
        hostSocketId: socket.id,
        hostDelay: 0,
        isPlaying: false,
        scheduledStartTime: null,
        startOffset: 0,
        syncInterval: null,
        listeners: new Map()
      };
      roomsCache.set(roomCode, cached);
      
      // Start sync interval
      cached.syncInterval = setInterval(() => {
        if (cached.isPlaying) broadcastPreciseSync(roomCode);
      }, 1000);
    } else {
      cached.hostSocketId = socket.id;
    }
    
    socket.join(roomCode);
    
    cb({
      success: true,
      roomState: {
        roomName: room.room_name,
        playing: cached.isPlaying,
        position: cached.startOffset,
        metadata: {
          title: room.track_title,
          artist: room.track_artist,
          duration: 0
        },
        hasFile: !!room.audio_url,
        audioUrl: room.audio_url,
        hostDelay: cached.hostDelay,
        serverTime: getHighPrecisionTime()
      }
    });
    
    setTimeout(() => broadcastListenerList(roomCode), 100);
  });
  
  socket.on('join-as-listener', async ({ roomCode, listenerName }, cb) => {
    const room = await getRoomFromDB(roomCode);
    if (!room) return cb({ success: false, error: 'Room not found' });
    
    socket.join(roomCode);
    
    await addListenerToDB(roomCode, socket.id, listenerName);
    
    const cached = roomsCache.get(roomCode);
    let currentPosition = cached?.startOffset || 0;
    if (cached?.isPlaying && cached.scheduledStartTime) {
      const elapsed = getHighPrecisionTime() - cached.scheduledStartTime;
      currentPosition = cached.startOffset + elapsed;
    }
    
    console.log(`👂 ${listenerName} joined ${roomCode}`);
    
    cb({
      success: true,
      roomCode,
      roomName: room.room_name,
      hostName: room.host_name,
      audioUrl: room.audio_url,
      currentState: {
        playing: cached?.isPlaying || false,
        position: currentPosition,
        scheduledStartTime: cached?.scheduledStartTime,
        startOffset: cached?.startOffset || 0,
        metadata: {
          title: room.track_title,
          artist: room.track_artist,
          duration: 0
        },
        serverTime: getHighPrecisionTime()
      }
    });
    
    broadcastListenerList(roomCode);
    await broadcastRoomList();
  });
  
  socket.on('download-complete', ({ roomCode }) => {
    const cached = roomsCache.get(roomCode);
    const l = cached?.listeners.get(socket.id);
    if (l) {
      l.downloaded = true;
      broadcastListenerList(roomCode);
    }
  });
  
  socket.on('sync-ready', ({ roomCode }) => {
    const cached = roomsCache.get(roomCode);
    const l = cached?.listeners.get(socket.id);
    if (l) {
      l.synced = true;
      broadcastListenerList(roomCode);
    }
  });
  
  socket.on('update-sync-stats', ({ roomCode, clockOffset, syncAccuracy }) => {
    const cached = roomsCache.get(roomCode);
    const l = cached?.listeners.get(socket.id);
    if (l) {
      l.clockOffset = clockOffset;
      l.syncAccuracy = syncAccuracy;
      broadcastListenerList(roomCode);
    }
  });
  
  socket.on('update-ping', ({ roomCode, ping }) => {
    const cached = roomsCache.get(roomCode);
    const l = cached?.listeners.get(socket.id);
    if (l) {
      l.ping = Math.round(ping);
      broadcastListenerList(roomCode);
    }
  });
  
  socket.on('play', ({ roomCode, position }) => {
    const cached = roomsCache.get(roomCode);
    if (!cached || socket.id !== cached.hostSocketId) return;
    startPlayback(roomCode, position || 0);
  });
  
  socket.on('pause', ({ roomCode }) => {
    const cached = roomsCache.get(roomCode);
    if (!cached || socket.id !== cached.hostSocketId) return;
    pausePlayback(roomCode);
  });
  
  socket.on('stop', ({ roomCode }) => {
    const cached = roomsCache.get(roomCode);
    if (!cached || socket.id !== cached.hostSocketId) return;
    stopPlayback(roomCode);
  });
  
  socket.on('seek', ({ roomCode, position }) => {
    const cached = roomsCache.get(roomCode);
    if (!cached || socket.id !== cached.hostSocketId) return;
    seekPlayback(roomCode, position);
  });
  
  socket.on('set-listener-volume', ({ roomCode, listenerId, volume }) => {
    const cached = roomsCache.get(roomCode);
    if (!cached || socket.id !== cached.hostSocketId) return;
    const l = cached.listeners.get(listenerId);
    if (l) {
      l.volume = Math.max(0, Math.min(1, volume));
      io.to(listenerId).emit('volume-update', { volume: l.volume });
      broadcastListenerList(roomCode);
    }
  });
  
  socket.on('mute-listener', ({ roomCode, listenerId, muted }) => {
    const cached = roomsCache.get(roomCode);
    if (!cached || socket.id !== cached.hostSocketId) return;
    const l = cached.listeners.get(listenerId);
    if (l) {
      l.muted = !!muted;
      io.to(listenerId).emit('mute-update', { muted: l.muted });
      broadcastListenerList(roomCode);
    }
  });
  
  socket.on('set-listener-delay', ({ roomCode, listenerId, delay }) => {
    const cached = roomsCache.get(roomCode);
    if (!cached || socket.id !== cached.hostSocketId) return;
    const l = cached.listeners.get(listenerId);
    if (l) {
      l.manualDelay = Math.max(-2500, Math.min(2500, delay));
      io.to(listenerId).emit('delay-update', { delay: l.manualDelay });
      broadcastListenerList(roomCode);
    }
  });
  
  socket.on('set-host-delay', ({ roomCode, delay }) => {
    const cached = roomsCache.get(roomCode);
    if (!cached || socket.id !== cached.hostSocketId) return;
    cached.hostDelay = Math.max(-2500, Math.min(2500, delay));
  });
  
  socket.on('request-sync', ({ roomCode }, cb) => {
    const cached = roomsCache.get(roomCode);
    if (!cached) return cb({ ok: false });
    
    let pos = cached.startOffset;
    if (cached.isPlaying && cached.scheduledStartTime) {
      const elapsed = getHighPrecisionTime() - cached.scheduledStartTime;
      pos += elapsed;
    }
    
    cb({
      ok: true,
      playing: cached.isPlaying,
      scheduledStartTime: cached.scheduledStartTime,
      offset: cached.startOffset,
      serverTime: getHighPrecisionTime(),
      positionNow: pos
    });
  });
  
  socket.on('disconnect', async () => {
    // Check all rooms for this socket
    for (const [roomCode, cached] of roomsCache) {
      // Host disconnected
      if (cached.hostSocketId === socket.id) {
        setTimeout(async () => {
          const current = roomsCache.get(roomCode);
          if (current && current.hostSocketId === socket.id) {
            await deleteRoomFromDB(roomCode);
            io.to(roomCode).emit('room-closed');
            await broadcastRoomList();
          }
        }, 30000);
      }
      
      // Listener disconnected
      if (cached.listeners.has(socket.id)) {
        await removeListenerFromDB(roomCode, socket.id);
        broadcastListenerList(roomCode);
        await broadcastRoomList();
      }
    }
  });
});

// ═══════════════════════════════════════════════════════════
// FILE UPLOAD
// ═══════════════════════════════════════════════════════════

app.post('/upload', upload.single('audio'), async (req, res) => {
  const startTime = Date.now();
  const roomCode = req.body.roomCode;
  
  try {
    const room = await getRoomFromDB(roomCode);
    if (!room) return res.status(404).json({ ok: false, error: 'Room not found' });
    if (!req.file) return res.status(400).json({ ok: false, error: 'No file' });
    
    console.log(`📤 Upload: ${req.file.originalname} (${(req.file.size / 1024 / 1024).toFixed(2)} MB)`);
    
    // Stop playback
    const cached = roomsCache.get(roomCode);
    if (cached?.isPlaying) stopPlayback(roomCode);
    
    // Delete old file
    if (room.audio_file_id) {
      try {
        await supabase.storage.from(BUCKET_NAME).remove([room.audio_file_id]);
      } catch (err) {
        console.warn('Could not delete old file:', err.message);
      }
    }
    
    // Upload to Supabase
    const timestamp = Date.now();
    const randomStr = Math.random().toString(36).substring(2, 8);
    const ext = path.extname(req.file.originalname);
    const fileName = `room-${roomCode}-${timestamp}-${randomStr}${ext}`;
    
    const { data, error } = await supabase.storage
      .from(BUCKET_NAME)
      .upload(fileName, req.file.buffer, {
        contentType: req.file.mimetype,
        cacheControl: '3600',
        upsert: false
      });
    
    if (error) {
      console.error('❌ Supabase error:', error);
      return res.status(500).json({ ok: false, error: error.message });
    }
    
    const { data: urlData } = supabase.storage
      .from(BUCKET_NAME)
      .getPublicUrl(fileName);
    
    const publicUrl = urlData.publicUrl;
    const title = req.file.originalname.replace(/\.[^/.]+$/, '');
    
    // Update database
    await updateRoomAudio(roomCode, fileName, publicUrl, title);
    
    const uploadTime = Date.now() - startTime;
    console.log(`✅ Upload complete: ${uploadTime}ms`);
    
    // Reset listeners
    if (cached) {
      cached.listeners.forEach(l => {
        l.downloaded = false;
        l.synced = false;
      });
    }
    
    // Notify clients
    io.to(roomCode).emit('file:ready', {
      url: publicUrl,
      metadata: { title, artist: 'Unknown', duration: 0 }
    });
    
    broadcastListenerList(roomCode);
    await broadcastRoomList();
    
    res.json({ ok: true, audioUrl: publicUrl, metadata: { title, artist: 'Unknown' }, uploadTime });
    
  } catch (err) {
    console.error('❌ Upload error:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════
// START SERVER
// ═══════════════════════════════════════════════════════════

const PORT = process.env.PORT || 3000;

httpServer.listen(PORT, '0.0.0.0', async () => {
  console.log(`\n🚀 ═══════════════════════════════════════`);
  console.log(`   Music Sync Server`);
  console.log(`   ═══════════════════════════════════════`);
  console.log(`   🌐 Port: ${PORT}`);
  console.log(`   📦 Storage: Supabase`);
  console.log(`   🗄️  Database: PostgreSQL`);
  console.log(`   🪣 Bucket: ${BUCKET_NAME}`);
  console.log(`═══════════════════════════════════════\n`);
  
  // Test database
  try {
    await sql`SELECT 1`;
    console.log('✅ Database connected\n');
  } catch (err) {
    console.error('❌ Database connection failed:', err.message, '\n');
  }
});

