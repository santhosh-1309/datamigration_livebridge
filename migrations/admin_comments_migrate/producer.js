const kafka = require("../common/kafka");
const mysql = require("../config/newDB");

const consumer = kafka.consumer({
  groupId: "admin-comments-bridge-migration",
  sessionTimeout: 60000,
  heartbeatInterval: 3000,
  maxBytesPerPartition: 10 * 1024 * 1024
});

/* =========================
   CONFIG
========================= */
const TOPIC = "admin_comments_migration";
const SOURCE_TABLE = "admin_comments_tbl";
const TARGET_TABLE = [
  "UAT_mytvs_bridge.admin_comments_stage",
  "mytvs_bridge.admin_comments_stage"
];
const MIGRATION_STEP = "admin_comments_migration";
const COMMENT_MAX_BYTES = 255;

// ✅ DATE FILTER BASED ON user_booking_tb.log
const MIN_DATE = new Date("2024-01-01T00:00:00Z");

/* =========================
   HELPERS
========================= */
function safeTruncateUtf8(str, maxBytes) {
  if (!str) return null;
  const buf = Buffer.from(str, "utf8");
  return buf.length <= maxBytes
    ? str
    : buf.slice(0, maxBytes).toString("utf8");
}

/* =========================
   ERROR LOGGER (NON-BLOCKING)
========================= */
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
        TARGET_TABLE,
        data?.com_id || null,
        JSON.stringify(data || {}),
        msg,
        MIGRATION_STEP
      ]
    );
  } catch (_) {
    // never block consumer
  }
}

/* =========================
   CONSUMER
========================= */
async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  console.log("🚀 Admin comments consumer running");

  await consumer.run({
    autoCommit: false,

    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      commitOffsetsIfNecessary,
      isRunning,
      isStale
    }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;

        let r;

        try {
          r = JSON.parse(message.value.toString());

          if (!r.book_id) {
            resolveOffset(message.offset);
            continue;
          }

          /* =========================
             FETCH BOOKING (SOURCE OF TRUTH)
          ========================= */
          const [[booking]] = await mysql.query(
            `
            SELECT booking_id, log
            FROM user_booking_tb
            WHERE booking_id = ?
            LIMIT 1
            `,
            [r.book_id]
          );

          // ❌ Booking does not exist
          if (!booking) {
            resolveOffset(message.offset);
            continue;
          }

          // ❌ Booking older than MIN_DATE
          if (new Date(booking.log) < MIN_DATE) {
            resolveOffset(message.offset);
            continue;
          }

          /* =========================
             INSERT / UPDATE COMMENT
          ========================= */
          const comment = safeTruncateUtf8(
            r.comments?.toString().trim(),
            COMMENT_MAX_BYTES
          );

          await mysql.query(
            `
            INSERT INTO admin_comments_stage
            (
              id,
              booking_id,
              comment,
              followup_date,
              booking_status,
              created_log,
              mod_log
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
              comment = VALUES(comment),
              followup_date = VALUES(followup_date),
              booking_status = VALUES(booking_status),
              mod_log = VALUES(mod_log)
            `,
            [
              r.com_id,
              booking.booking_id,
              comment,
              r.Follow_up_date || null,
              r.booking_status || null,
              r.comment_log || null,
              r.update_log || null
            ]
          );

        } catch (err) {
          await logError(r, err.message);
        }

        resolveOffset(message.offset);
        await heartbeat();
      }

      await commitOffsetsIfNecessary();
    }
  });
}

/* =========================
   GRACEFUL SHUTDOWN
========================= */
async function shutdown(signal) {
  console.log(`⚠️ Admin comments consumer shutdown: ${signal}`);
  try {
    await consumer.disconnect();
  } finally {
    process.exit(0);
  }
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

run().catch(err => {
  console.error("❌ Admin comments consumer fatal:", err);
  process.exit(1);
});
