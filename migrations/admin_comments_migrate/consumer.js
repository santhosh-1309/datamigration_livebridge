/**
 * admin_comments_bridge - Kafka Consumer
 * Structure-aligned with user_vehicle consumer
 */

const { feedbackConsumer } = require("../../config/kafka_config");
const { liveDB, uatDB } = require("../../config/revamp_db");

console.log("🚀 Admin Comments Consumer script started");

/* ---------- Constants ---------- */
const SOURCE_TABLE = "go_bumpr.admin_comments_tbl";
const TARGET_TABLE = [
  "UAT_mytvs_bridge.admin_comments_uat",
  "mytvs_bridge.admin_comments"
];
const MIGRATION_STEP = "admin_comments_bridge_migration";
const TOPIC = "admin_comments_migration";
const MIN_DATE = new Date('2023-12-31T23:59:59Z');

/* ---------- Safe Error Logger ---------- */
async function logError(data, msg, table = TARGET_TABLE.join(',')) {
  try {
    if (!uatDB) return;

    const jsonData = (() => {
      try { return JSON.stringify(data || {}); }
      catch { return "{}"; }
    })();

    await uatDB.query(
      `INSERT INTO migration_error_log
       (source_table, target_table, source_primary_key, failed_data, error_message, migration_step)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [
        SOURCE_TABLE,
        table,
        data?.com_id || null,
        jsonData,
        msg,
        MIGRATION_STEP
      ]
    );
  } catch (err) {
    console.error("❌ Migration error log failed:", err.message || err);
  }
}

/* ---------- Consumer ---------- */
async function runAdminCommentsConsumer() {
  await feedbackConsumer.connect();
  await feedbackConsumer.subscribe({ topic: TOPIC, fromBeginning: true });

  console.log("✅ Admin Comments Consumer Started");

  await feedbackConsumer.run({
    eachBatchAutoResolve: false,

    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {

      for (const message of batch.messages) {
        let data;

        try {
          data = JSON.parse(message.value.toString());
        } catch {
          await logError({}, "INVALID_JSON");
          resolveOffset(message.offset);
          continue;
        }

        if (!data?.com_id || !data?.book_id) {
          resolveOffset(message.offset);
          await heartbeat();
          continue;
        }

        try {
          // Skip rows before the MIN_DATE
          if (!data.created_log || new Date(data.created_log) <= MIN_DATE) {
            resolveOffset(message.offset);
            continue;
          }

          for (const table of TARGET_TABLE) {
            const [rows] = await uatDB.query(
              `SELECT id FROM ${table} WHERE id = ?`,
              [data.com_id]
            );

            if (!rows.length) {
              const insertSQL = `
                INSERT INTO ${table}
                (
                  id,
                  booking_id,
                  comment,
                  followup_date,
                  booking_status,
                  created_log,
                  mod_log,
                  crm_log_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
              `;

              const params = [
                data.com_id,
                data.book_id,
                data.comments || null,
                data.Follow_up_date || null,
                data.status || null,
                data.log || null,
                data.update_log || null,
                data.crm_log_id || null
              ];

              try {
                await Promise.all([
                  liveDB.query(insertSQL, params),
                  uatDB.query(insertSQL, params)
                ]);
              } catch (err) {
                await logError(data, "INSERT_FAILED: " + (err.message || err), table);
              }

            } else {
              const updateSQL = `
                UPDATE ${table}
                SET
                  comment = IFNULL(?, comment),
                  followup_date = IFNULL(?, followup_date),
                  booking_status = IFNULL(?, booking_status),
                  mod_log = IFNULL(?, mod_log)
                WHERE id = ?
              `;

              const params = [
                data.comments || null,
                data.Follow_up_date || null,
                data.status || null,
                data.update_log || null,
                data.com_id
              ];

              try {
                await Promise.all([
                  liveDB.query(updateSQL, params),
                  uatDB.query(updateSQL, params)
                ]);
              } catch (err) {
                await logError(data, "UPDATE_FAILED: " + (err.message || err), table);
              }
            }
          }

          resolveOffset(message.offset);
          await heartbeat();

        } catch (err) {
          await logError(data, "PROCESSING_FAILED: " + (err.message || err));
          resolveOffset(message.offset);
        }
      }

      await commitOffsetsIfNecessary();
    }
  });
}

/* ---------- Bootstrap ---------- */
runAdminCommentsConsumer().catch(err => {
  console.error("❌ Admin Comments Consumer failed:", err);
  process.exit(1);
});

module.exports = runAdminCommentsConsumer;
