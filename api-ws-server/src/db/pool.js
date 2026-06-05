const { Pool } = require('pg');
require('dotenv').config();

const connectionString = process.env.DATABASE_URL;

const pool = new Pool({
  connectionString,
});



pool.on('error', (err) => {
  console.error('[DB]: Unexpected idle client database error:', err.message);
});

module.exports = {
  query: (text, params) => pool.query(text, params),
  pool,
};
