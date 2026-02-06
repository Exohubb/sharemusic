import postgres from 'postgres';

const connectionString = process.env.DATABASE_URL || 'postgresql://postgres:m4WDHRaLfuqWCxne@db.jnisacrqerzzkriayhxy.supabase.co:5432/postgres';

const sql = postgres(connectionString, {
  max: 10,
  idle_timeout: 20,
  connect_timeout: 10
});

export default sql;
