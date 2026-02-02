const mysql = require("mysql2/promise");

const baseConfig = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  waitForConnections: process.env.DB_WAIT_FOR_CONNECTIONS === "true",
  connectionLimit: Number(process.env.DB_CONNECTION_LIMIT),
  queueLimit: Number(process.env.DB_QUEUE_LIMIT)
};

const liveDB = mysql.createPool({
  ...baseConfig,
  database: process.env.DB_SCHEMA_LIVE
});

const uatDB = mysql.createPool({
  ...baseConfig,
  database: process.env.DB_SCHEMA_UAT
});

module.exports = {
  liveDB,
  uatDB
};
