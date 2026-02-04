/**
 * b2b_booking_bridge - Kafka Consumer
 * Fully uniform with user_register consumer
 */

const { b2bBookingConsumer } = require('../../config/kafka_config');
const { liveDB, uatDB } = require('../../config/revamp_db');

console.log('🚀 B2B Booking Consumer started');

/* ---------- Constants ---------- */
const SOURCE_TABLE = 'b2b_booking_work';
const TARGET_TABLE = [
  'UAT_mytvs_bridge.b2b_booking_tbl_uat',
  'mytvs_bridge.b2b_booking_tbl'
];
const MIGRATION_STEP = 'b2b_booking_bridge_migration';
const TOPIC = 'b2b_booking_bridge_migration';
const MIN_DATE = new Date('2023-12-31T23:59:59Z');

/* ---------- Safe Error Logger ---------- */
async function logError(data, msg, table = TARGET_TABLE.join(',')) {
  try {
    if (!uatDB) return;

    const jsonData = (() => {
      try { return JSON.stringify(data || {}); }
      catch { return '{}'; }
    })();

    await uatDB.query(
      `INSERT INTO migration_error_log
       (source_table, target_table, source_primary_key, failed_data, error_message, migration_step)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [
        SOURCE_TABLE,
        table,
        data?.b2b_booking_id || null,
        jsonData,
        msg,
        MIGRATION_STEP
      ]
    );
  } catch (err) {
    console.error('❌ Migration error log failed:', err.message || err);
  }
}

/* ---------- Consumer ---------- */
async function runB2BBookingConsumer() {
  await b2bBookingConsumer.connect();
  await b2bBookingConsumer.subscribe({ topic: TOPIC, fromBeginning: true });

  console.log('✅ B2B Booking Consumer running');

  await b2bBookingConsumer.run({
    eachBatchAutoResolve: false,

    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
      for (const message of batch.messages) {
        let data;

        /* ---------- JSON Parse ---------- */
        try {
          data = JSON.parse(message.value.toString());
        } catch {
          await logError({}, 'INVALID_JSON');
          resolveOffset(message.offset);
          continue;
        }

        // Skip rows before MIN_DATE
        if (!data.b2b_log || new Date(data.b2b_log) <= MIN_DATE) {
          resolveOffset(message.offset);
          continue;
        }

        try {
          for (const table of TARGET_TABLE) {
            const insertSQL = `
              INSERT INTO ${table}
              (b2b_booking_id, booking_id, pickup_date, pickup_time, tvs_job_card_no, goaxled_log_date, migrated_data)
              VALUES (?, ?, ?, ?, ?, ?, ?)
              ON DUPLICATE KEY UPDATE
                b2b_booking_id = VALUES(b2b_booking_id),
                booking_id = VALUES(booking_id),
                pickup_date = VALUES(pickup_date),
                pickup_time = VALUES(pickup_time),
                tvs_job_card_no = VALUES(tvs_job_card_no),
                goaxled_log_date = VALUES(goaxled_log_date),
                migrated_data = VALUES(migrated_data)
            `;

            const params = [
              data.b2b_booking_id,
              data.gb_booking_id,
              data.pickup_date || null,
              data.pickup_time || null,
              data.tvs_job_card_no || null,
              data.goaxled_log_date || null,
              data.jsonData
            ];

            try {
              await Promise.all([
                liveDB.query(insertSQL, params),
                uatDB.query(insertSQL, params)
              ]);
            } catch (err) {
              await logError(data, 'UPSERT_FAILED: ' + (err.message || err), table);
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

/* ---------- Bootstrap ---------- */
runB2BBookingConsumer().catch(err => {
  console.error('❌ B2B Booking Consumer fatal:', err);
  process.exit(1);
});

module.exports = runB2BBookingConsumer;
