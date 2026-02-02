
const axios = require("axios");

const kafka = require('../../config/kafka_config');
const db = require('../../config/revamp_db');
const { encrypt } = require("../common/encryption");

/* =========================
   CONSUMER CONFIG
========================= */
const consumer = kafka.consumer({
  groupId: "feedback-track-bridge-migration-v2",
  sessionTimeout: 60000,
  heartbeatInterval: 3000
});

const MIGRATION_STEP = "feedback_track_bridge_migration";

// Dual-schema target tables
const TARGET_TABLES = [
  "UAT_mytvs_bridge.feedback_track",
  "mytvs_bridge.feedback_track"
];

const SOURCE_TABLE = "feedback_track";
const DB_BATCH_SIZE = 500; // Optional batching if needed

/* ---------- Error Logger ---------- */
async function logError(data, msg) {
  try {
    await mysql.query(
      `
      INSERT INTO migration_error_log
      (source_table, target_table, source_primary_key, failed_data, error_message, migration_step)
      VALUES (?, ?, ?, ?, ?, ?)
      `,
      [
        SOURCE_TABLE,
        TARGET_TABLES.join(","),
        data?.id || null,
        JSON.stringify(data || {}),
        msg,
        MIGRATION_STEP
      ]
    );
  } catch (_) {}
}

/* =========================
   CONSUMER RUN
========================= */
async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: "service_buddy_booking_migration", fromBeginning: true });

  console.log("✅ Feedback Track Dual-Schema Consumer Started");

  await consumer.run({
    autoCommit: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }) => {
      const rows = [];

      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;

        let data;
        try {
          data = JSON.parse(message.value.toString());
        } catch {
          await logError({}, "INVALID_JSON");
          resolveOffset(message.offset);
          continue;
        }

        try {
          // Optional: encrypt or normalize fields if needed
          const crmLogId = data.crm_update_id ? encrypt(String(data.crm_update_id)) : null;

          rows.push([
            data.id,
            data.booking_id,
            data.b2b_booking_id,
            crmLogId,
            data.created_log
          ]);

        } catch (err) {
          await logError(data, err.message);
          resolveOffset(message.offset);
        }

        resolveOffset(message.offset);
      }

      if (!rows.length) return;

      // Insert / update in both schemas
      const SQL = `
        INSERT INTO ?? 
        (id, booking_id, b2b_booking_id, crm_log_id, created_log)
        VALUES ?
        ON DUPLICATE KEY UPDATE
          booking_id = VALUES(booking_id),
          b2b_booking_id = VALUES(b2b_booking_id),
          crm_log_id = VALUES(crm_log_id),
          created_log = VALUES(created_log)
      `;

      try {
        for (const table of TARGET_TABLES) {
          for (let i = 0; i < rows.length; i += DB_BATCH_SIZE) {
            await mysql.query(SQL, [table, rows.slice(i, i + DB_BATCH_SIZE)]);
            await heartbeat();
          }
        }

        await commitOffsetsIfNecessary();
      } catch (err) {
        console.error("❌ DB batch failed:", err.message);
      }
    }
  });
}

/* ---------- Graceful Shutdown ---------- */
async function shutdown(signal) {
  console.log(`⚠️ Consumer shutdown: ${signal}`);
  try {
    await consumer.disconnect();
  } finally {
    process.exit(0);
  }
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

run().catch(err => {
  console.error("❌ Consumer fatal:", err);
  process.exit(1);
});
