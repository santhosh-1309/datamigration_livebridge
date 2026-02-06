/**
 * user_booking_bridge - Kafka Consumer
 * Fast batch insert (500) – Orchestrator compatible
 */

const { userBookingConsumer } = require('../../config/kafka_config');
const { liveDB, uatDB } = require('../../config/revamp_db');

console.log('🚀 User Booking Consumer started');

/* ---------- Constants ---------- */
const SOURCE_TABLE = 'user_booking_tb';

const TARGET_TABLE = [
  { db: uatDB, table: 'UAT_mytvs_bridge.user_booking_uat' },
  { db: liveDB, table: 'mytvs_bridge.user_booking' }
];

const TOPIC = 'user_booking_tb_migration';
const MIGRATION_STEP = 'user_booking_bridge_migration';
const BATCH_SIZE = 500;

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
        data?.booking_id || null,
        JSON.stringify(data || {}),
        msg,
        MIGRATION_STEP
      ]
    );
  } catch (_) {
    // silent
  }
}

/* ---------- Consumer ---------- */
async function runUserBookingConsumer() {
  await userBookingConsumer.connect();
  await userBookingConsumer.subscribe({
    topic: TOPIC,
    fromBeginning: true
  });

  console.log('✅ User Booking Consumer Running');

  await userBookingConsumer.run({
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

        batchData.forEach(batchItem => {
          batchItem.inserts.push([
            data.booking_id,
            data.reg_id,
            data.garage_code || null,
            data.user_veh_id || null,
            data.service_type || null,
            data.created_log,
            data.booking_status || null,
            data.source || null,
            data.crm_update_id || null,
            data.city_id || null,
            data.city || null
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
      booking_id,
      reg_id,
      garage_code,
      user_veh_id,
      actual_service_type_migration,
      created_log,
      booking_status,
      source,
      crm_update_id,
      city_id,
      city
    )
    VALUES ?
    ON DUPLICATE KEY UPDATE
      reg_id = VALUES(reg_id),
      garage_code = VALUES(garage_code),
      user_veh_id = VALUES(user_veh_id),
      actual_service_type_migration = VALUES(actual_service_type_migration),
      booking_status = VALUES(booking_status),
      source = VALUES(source),
      crm_update_id = VALUES(crm_update_id),
      city_id = VALUES(city_id),
      city = VALUES(city)
  `;

  try {
    await db.query(SQL, [inserts]);
  } catch (err) {
    console.error(`❌ Batch insert failed for ${table}:`, err.message);
    for (const row of inserts) {
      await logError({ booking_id: row[0] }, 'BATCH_INSERT_FAILED: ' + err.message, table);
    }
  }
}

/* ---------- Bootstrap ---------- */
runUserBookingConsumer().catch(err => {
  console.error('❌ User Booking Consumer fatal:', err);
  process.exit(1);
});

module.exports = runUserBookingConsumer;
