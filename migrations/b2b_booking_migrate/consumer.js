/**
 * b2b_booking_bridge - Kafka Consumer
 * Fast batch insert (500) – Orchestrator compatible
 */

const { b2bBookingConsumer } = require('../../config/kafka_config');
const { liveDB, uatDB } = require('../../config/revamp_db');

console.log('🚀 B2B Booking Consumer started');

/* ---------- Constants ---------- */
const SOURCE_TABLE = 'b2b_booking_work';

const TARGET_TABLE = [
  { db: uatDB, table: 'UAT_mytvs_bridge.b2b_booking_tbl_uat' },
  { db: liveDB, table: 'mytvs_bridge.b2b_booking_tbl' }
];

const TOPIC = 'b2b_booking_bridge_migration';
const MIGRATION_STEP = 'b2b_booking_bridge_migration';
const BATCH_SIZE = 500;
const MIN_DATE = new Date('2023-12-31T23:59:59Z');

/* ---------- Error Logger ---------- */
async function logError(data, msg, table = TARGET_TABLE.map(t => t.table).join(',')) {
  try {
    if (!uatDB) return;

    await uatDB.query(
      `
      INSERT INTO migration_error_log
      (source_table, target_table, source_primary_key, failed_data, error_message, migration_step)
      VALUES (?, ?, ?, ?, ?, ?)
      `,
      [
        SOURCE_TABLE,
        table,
        data?.b2b_booking_id || null,
        JSON.stringify(data || {}),
        msg,
        MIGRATION_STEP
      ]
    );
  } catch {
    // silent
  }
}

/* ---------- Consumer ---------- */
async function runB2BBookingConsumer() {
  await b2bBookingConsumer.connect();
  await b2bBookingConsumer.subscribe({
    topic: TOPIC,
    fromBeginning: true
  });

  console.log('✅ B2B Booking Consumer Running');

  await b2bBookingConsumer.run({
    eachBatchAutoResolve: false,

    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      commitOffsetsIfNecessary
    }) => {

      const batchData = TARGET_TABLE.map(target => ({
        db: target.db,
        table: target.table,
        inserts: []
      }));

      for (const message of batch.messages) {
        let data;

        try {
          data = JSON.parse(message.value.toString());
        } catch {
          await logError({}, 'INVALID_JSON');
          resolveOffset(message.offset);
          continue;
        }

        // Skip old rows
        if (!data.b2b_log || new Date(data.b2b_log) <= MIN_DATE) {
          resolveOffset(message.offset);
          continue;
        }

        batchData.forEach(batchItem => {
          batchItem.inserts.push([
            data.b2b_booking_id,
            data.gb_booking_id || null,
            data.pickup_date || null,
            data.pickup_time || null,
            data.tvs_job_card_no || null,
            data.goaxled_log_date || null,
            data.jsonData || null
          ]);
        });

        resolveOffset(message.offset);
        await heartbeat();

        // Flush when batch size reached
        for (const batchItem of batchData) {
          if (batchItem.inserts.length >= BATCH_SIZE) {
            await insertBatch(batchItem);
            batchItem.inserts = [];
          }
        }
      }

      // Insert remaining rows
      for (const batchItem of batchData) {
        if (batchItem.inserts.length > 0) {
          await insertBatch(batchItem);
        }
      }

      await commitOffsetsIfNecessary();
    }
  });
}

/* ---------- Batch Insert ---------- */
async function insertBatch({ db, table, inserts }) {
  const SQL = `
    INSERT INTO ${table}
    (
      b2b_booking_id,
      booking_id,
      pickup_date,
      pickup_time,
      tvs_job_card_no,
      goaxled_log_date,
      migrated_data
    )
    VALUES ?
    ON DUPLICATE KEY UPDATE
      booking_id = VALUES(booking_id),
      pickup_date = VALUES(pickup_date),
      pickup_time = VALUES(pickup_time),
      tvs_job_card_no = VALUES(tvs_job_card_no),
      goaxled_log_date = VALUES(goaxled_log_date),
      migrated_data = VALUES(migrated_data)
  `;

  try {
    await db.query(SQL, [inserts]);
  } catch (err) {
    console.error(`❌ Batch insert failed for ${table}:`, err.message);
    for (const row of inserts) {
      await logError(
        { b2b_booking_id: row[0] },
        'BATCH_INSERT_FAILED: ' + err.message,
        table
      );
    }
  }
}

/* ---------- Bootstrap ---------- */
runB2BBookingConsumer().catch(err => {
  console.error('❌ B2B Booking Consumer fatal:', err);
  process.exit(1);
});

module.exports = runB2BBookingConsumer;
