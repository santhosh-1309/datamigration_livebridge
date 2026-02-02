/**
 * b2b_booking_tbl - Kafka Consumer
 * Dual-schema migration, optimized batch insert
 */

require("dotenv").config();

const kafka = require("../../config/kafka_config");
const mysql = require("../../config/revamp_db");

/* ===============================
   CONFIG
================================ */
const TOPIC = "b2b_booking_bridge_migration";
const GROUP_ID = "b2b-booking-bridge-migration";
const TARGET_TABLE = [
  "UAT_mytvs_bridge.b2b_booking_tbl",
  "mytvs_bridge.b2b_booking_tbl"
];
const MIGRATION_STEP = "b2b_booking_bridge_migration";

// ⏱ Same date filter (NO logic change)
const MIN_DATE = new Date("2024-01-01T00:00:00Z");

// Batch insert size
const DB_BATCH_SIZE = 500;

/* ===============================
   KAFKA CONSUMER
================================ */
const consumer = kafka.consumer({
  groupId: GROUP_ID,
  sessionTimeout: 30000,
  maxBytesPerPartition: 10 * 1024 * 1024
});

/* ===============================
   ERROR LOGGER (NON-BLOCKING)
================================ */
async function logError(data, errorMsg) {
  try {
    await mysql.query(
      `
      INSERT INTO migration_error_log
      (source_table, target_table, source_primary_key, failed_data, error_message, migration_step)
      VALUES (?, ?, ?, ?, ?, ?)
      `,
      [
        "b2b_booking_work",
        TARGET_TABLE.join(","),
        data?.b2b_booking_id || null,
        JSON.stringify(data || {}),
        errorMsg,
        MIGRATION_STEP
      ]
    );
  } catch (_) {
    // never block consumer
  }
}

/* ===============================
   CONSUMER RUN
================================ */
async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  console.log("🚀 b2b_booking_tbl consumer running");

  await consumer.run({
    autoCommit: false,

    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
      const rows = [];

      for (const message of batch.messages) {
        let data;

        try {
          data = JSON.parse(message.value.toString());
        } catch {
          await logError({}, "INVALID_JSON");
          resolveOffset(message.offset);
          continue;
        }

        try {
          // ⛔ SAME filter logic
          if (!data.goaxled_log_date || new Date(data.goaxled_log_date) < MIN_DATE) {
            resolveOffset(message.offset);
            continue;
          }

          rows.push([
            data.b2b_booking_id,
            data.booking_id,
            data.pickup_date || null,
            data.pickup_time || null,
            data.tvs_job_card_no || null,
            data.goaxled_log_date
          ]);

        } catch (err) {
          await logError(data, err.message);
        }

        resolveOffset(message.offset);
      }

      if (!rows.length) return;

      const SQL = `
        INSERT INTO ??
        (b2b_booking_id, booking_id, pickup_date, pickup_time, tvs_job_card_no, goaxled_log_date)
        VALUES ?
        ON DUPLICATE KEY UPDATE
          booking_id = VALUES(booking_id),
          pickup_date = VALUES(pickup_date),
          pickup_time = VALUES(pickup_time),
          tvs_job_card_no = VALUES(tvs_job_card_no),
          goaxled_log_date = VALUES(goaxled_log_date)
      `;

      try {
        // Insert/update for both schemas
        for (const table of TARGET_TABLE) {
          for (let i = 0; i < rows.length; i += DB_BATCH_SIZE) {
            await mysql.query(SQL, [table, rows.slice(i, i + DB_BATCH_SIZE)]);
            await heartbeat();
          }
        }

        await commitOffsetsIfNecessary();
      } catch (err) {
        console.error("❌ DB batch failed:", err.message);
        throw err; // DO NOT COMMIT OFFSETS
      }
    }
  });
}

/* ===============================
   GRACEFUL SHUTDOWN
================================ */
async function shutdown(signal) {
  console.log(`⚠️ b2b_booking_tbl consumer shutdown: ${signal}`);
  try {
    await consumer.disconnect();
  } finally {
    process.exit(0);
  }
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

run().catch(err => {
  console.error("❌ b2b_booking_tbl consumer fatal:", err);
  process.exit(1);
});
