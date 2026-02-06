import postgres from 'postgres';

// FORCE pooler connection (port 6543) - IPv4 compatible
const connectionString = process.env.DATABASE_URL 
  ? process.env.DATABASE_URL.replace(':5432', ':6543').replace('db.jnisacrqerzzkriayhxy.supabase.co', 'aws-0-ap-south-1.pooler.supabase.com')
  : 'postgresql://postgres.jnisacrqerzzkriayhxy:m4WDHRaLfuqWCxne@aws-0-ap-south-1.pooler.supabase.com:6543/postgres';

console.log('🔗 Database connection:', connectionString.replace(/:[^:@]+@/, ':****@'));

const sql = postgres(connectionString, {
  max: 10,
  idle_timeout: 20,
  connect_timeout: 10,
  prepare: false, // Required for pooler
  ssl: false // Pooler doesn't need SSL
});

export default sql;
