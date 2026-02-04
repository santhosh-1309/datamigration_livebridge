/**
 * feedback_track_bridge - Kafka Consumer (Safe & Unified Version)
 */

const { feedbackConsumer } = require('../../config/kafka_config');
const { liveDB, uatDB } = require('../../config/revamp_db');
const { encrypt } = require('../../common/encryption');

console.log("🚀 Feedback Track Consumer script started");

/* ---------- Constants ---------- */
const SOURCE_TABLE = 'feedback_track';
const TARGET_TABLE = [
  "UAT_mytvs_bridge.service_buddy_booking_uat",
  "mytvs_bridge.feedback_track"
];
const MIGRATION_STEP = 'feedback_track_bridge_migration';
const MIN_DATE = new Date('2023-12-31T23:59:59Z');

/* ---------- Safe Encryption ---------- */
function safeEncrypt(input) {
  if (!input) return null;
  try {
    return encrypt(input);
  } catch (err) {
    console.error('❌ Encryption failed for:', input, err.message);
    return null;
  }
}

/* ---------- Safe Error Logger ---------- */
async function logError(data, msg, table = TARGET_TABLE.join(',')) {
  try {
    if (!uatDB) return;
    const jsonData = (() => { try { return JSON.stringify(data || {}); } catch { return '{}'; } })();

    await uatDB.query(
      `INSERT INTO migration_error_log
       (source_table, target_table, source_primary_key, failed_data, error_message, migration_step)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [SOURCE_TABLE, table, data?.id || null, jsonData, msg, MIGRATION_STEP]
    );
  } catch (err) {
    console.error('❌ Migration error log failed:', err.message || err);
  }
}

/* ---------- Consumer ---------- */
async function runFeedbackTrackConsumer() {
  await feedbackConsumer.connect();
  await feedbackConsumer.subscribe({ topic: 'feedback_track_migration', fromBeginning: true });
  console.log('✅ Feedback Track Consumer Started');

  await feedbackConsumer.run({
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {

      for (const message of batch.messages) {
        let data;
        try {
          data = JSON.parse(message.value.toString());
        } catch {
          await logError({}, 'INVALID_JSON');
          resolveOffset(message.offset);
          continue;
        }

        // Skip rows before MIN_DATE
        if (!data.created_log || new Date(data.created_log) <= MIN_DATE) {
          resolveOffset(message.offset);
          continue;
        }

        try {
          const crmLogId = safeEncrypt(data.crm_update_id ? String(data.crm_update_id) : null);

          for (const table of TARGET_TABLE) {
            const insertSQL = `
              INSERT INTO ${table}
              (id, booking_id, b2b_booking_id, crm_update_id, created_log)
              VALUES (?, ?, ?, ?, ?)
              ON DUPLICATE KEY UPDATE
                booking_id = VALUES(booking_id),
                b2b_booking_id = VALUES(b2b_booking_id),
                crm_log_id = VALUES(crm_update_id),
                created_log = VALUES(created_log)
            `;

            const params = [
              data.id,
              data.booking_id,
              data.b2b_booking_id,
              crmLogId,
              data.created_log || null
            ];

            try {
              await Promise.all([
                liveDB.query(insertSQL, params),
                uatDB.query(insertSQL, params)
              ]);
            } catch (err) {
              await logError(data, 'INSERT_FAILED: ' + (err.message || err), table);
            }
          }

          resolveOffset(message.offset);
          await heartbeat();

        } catch (err) {
          await logError(data, 'PROCESSING_FAILED: ' + (err.message || err));
          resolveOffset(message.offset);
        }
      }

      await commitOffsetsIfNecessary();
    }
  });
}

/* ---------- Run Consumer ---------- */
runFeedbackTrackConsumer().catch(err => {
  console.error("❌ Consumer failed:", err);
  process.exit(1);
});

module.exports = runFeedbackTrackConsumer;
