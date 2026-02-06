import sql from './db.js';

async function initDatabase() {
  console.log('🔧 Initializing database...\n');

  try {
    // Create rooms table
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
    console.log('✅ Rooms table ready');

    // Create listeners table
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
    console.log('✅ Listeners table ready');

    // Create indexes for performance
    await sql`CREATE INDEX IF NOT EXISTS idx_listeners_room ON listeners(room_code)`;
    await sql`CREATE INDEX IF NOT EXISTS idx_listeners_socket ON listeners(socket_id)`;
    
    console.log('✅ Indexes created');

    console.log('\n🎉 Database initialization complete!\n');
    process.exit(0);

  } catch (err) {
    console.error('❌ Database initialization failed:', err);
    process.exit(1);
  }
}

initDatabase();
