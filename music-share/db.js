import postgres from 'postgres';

// Session Pooler - IPv4 Compatible
const connectionString = process.env.DATABASE_URL || 
  'postgresql://postgres.jnisacrqerzzkriayhxy:m4WDHRaLfuqWCxne@aws-1-ap-south-1.pooler.supabase.com:5432/postgres';

console.log('🔗 Connecting to Supabase Session Pooler...');

const sql = postgres(connectionString, {
  max: 10,
  idle_timeout: 20,
  connect_timeout: 10
});

export default sql;
