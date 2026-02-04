/**
 * revamp_db.js
 * MySQL pool for Live & UAT (SSL required)
 */


const path = require('path');
require('dotenv').config({
  path: path.resolve(__dirname, '../.env') // ← correct
});

const mysql = require('mysql2/promise');

// ---------- Base config (COMMON) ----------
const baseConfig = {
  host: process.env.OLD_DB_HOST,
  user: process.env.OLD_DB_USER,
  password: process.env.OLD_DB_PASSWORD,
  port: Number(process.env.OLD_DB_PORT),

  // 🔑 THIS IS THE FIX
  ssl: { rejectUnauthorized: false },

  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
};

// ---------- Debug ----------


// ---------- Pools ----------
const liveDB = mysql.createPool({
  ...baseConfig,
  database: process.env.DB_SCHEMA_LIVE
});

const uatDB = mysql.createPool({
  ...baseConfig,
  database: process.env.DB_SCHEMA_UAT
});


module.exports = { liveDB, uatDB };
