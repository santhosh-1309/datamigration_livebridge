/**
 * user_booking_bridge - Kafka Consumer
 * Fully unified with other migration consumers
 */

const { userBookingConsumer } = require('../../config/kafka_config');
const { liveDB, uatDB } = require('../../config/revamp_db');

console.log('🚀 User Booking Consumer started');

/* ===============================
   CONSTANTS
================================ */
const SOURCE_TABLE = 'user_booking_tb';
const TARGET_TABLE = [
  '`UAT_mytvs_bridge`.`user_booking_uat`',
  '`mytvs_bridge`.`user_booking`'
];

const TOPIC = 'user_booking_tb_migration';
const MIGRATION_STEP = 'user_booking_bridge_migration';
const MIN_DATE = new Date('2024-01-01T00:00:00Z');
const DB_BATCH_SIZE = 500;

/* ===============================
   ERROR LOGGER
================================ */
async function logError(data, msg, table = TARGET_TABLE.join(',')) {
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

/* ===============================
   CONSUMER RUN
================================ */
async function runUserBookingConsumer() {
  await userBookingConsumer.connect();
  await userBookingConsumer.subscribe({ topic: TOPIC, fromBeginning: true });

  console.log('✅ User Booking Consumer running');

  await userBookingConsumer.run({
    eachBatchAutoResolve: false,

    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      commitOffsetsIfNecessary
    }) => {

      const rows = [];

      for (const message of batch.messages) {
        let data;

        try {
          data = JSON.parse(message.value.toString());
        } catch {
          await logError({}, 'INVALID_JSON');
          resolveOffset(message.offset);
          continue;
        }

        // Skip old data
        if (!data.created_log || new Date(data.created_log) < MIN_DATE) {
          resolveOffset(message.offset);
          continue;
        }

        try {
          rows.push([
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
        } catch (err) {
          await logError(data, err.message);
        }

        resolveOffset(message.offset);
      }

      if (!rows.length) {
        await commitOffsetsIfNecessary();
        return;
      }

      for (const table of TARGET_TABLE) {
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
          for (let i = 0; i < rows.length; i += DB_BATCH_SIZE) {
            await Promise.all([
              liveDB.query(SQL, [rows.slice(i, i + DB_BATCH_SIZE)]),
              uatDB.query(SQL, [rows.slice(i, i + DB_BATCH_SIZE)])
            ]);
            await heartbeat();
          }
        } catch (err) {
          console.error(`❌ DB batch failed (${table}):`, err.message);
          await logError({}, 'BATCH_INSERT_FAILED: ' + err.message, table);
          throw err; // offsets NOT committed
        }
      }

      await commitOffsetsIfNecessary();
    }
  });
}

/* ===============================
   SHUTDOWN
================================ */
async function shutdown(signal) {
  console.log(`⚠️ User Booking consumer shutdown: ${signal}`);
  try {
    await userBookingConsumer.disconnect();
  } finally {
    process.exit(0);
  }
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

/* ===============================
   BOOTSTRAP
================================ */
runUserBookingConsumer().catch(err => {
  console.error('❌ User Booking consumer fatal:', err);
  process.exit(1);
});

module.exports = runUserBookingConsumer;
