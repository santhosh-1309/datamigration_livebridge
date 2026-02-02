const kafka = require('../../config/kafka_config');
const mysql = require('../../config/revamp_db');

/* ===============================
   CONFIG
================================ */
const TOPIC = 'user_booking_tb_migration';
const GROUP_ID = 'user-booking-bridge-migration';
const TARGET_TABLE = [
  "UAT_mytvs_bridge.user_booking_tb",
  "mytvs_bridge.user_booking_tb"
];
const MIGRATION_STEP = 'user_booking_bridge_migration';

// ⏱ Same date filter (NO logic change)
const MIN_DATE = new Date('2024-01-01T00:00:00Z');

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
        'user_booking_tb',
        TARGET_TABLE,
        data?.booking_id || null,
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

  console.log('🚀 User Booking consumer running');

  await consumer.run({
    autoCommit: false,

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

        try {
          // ⛔ SAME filter logic
          if (!data.created_log || new Date(data.created_log) < MIN_DATE) {
            resolveOffset(message.offset);
            continue;
          }

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
            data.city || null,
            data.log || new Date()
          ]);

        } catch (err) {
          await logError(data, err.message);
        }

        resolveOffset(message.offset);
      }

      if (!rows.length) return;

      const SQL = `
        INSERT INTO ${TARGET_TABLE}
        (
          booking_id,
          reg_id,
          garage_code,
          user_veh_id,
          service_type,
          created_log,
          booking_status,
          source,
          crm_update_id,
          city_id,
          city,
          created_log
        )
        VALUES ?
        ON DUPLICATE KEY UPDATE
          reg_id = VALUES(reg_id),
          garage_code = VALUES(garage_code),
          user_veh_id = VALUES(user_veh_id),
          service_type = VALUES(service_type),
          booking_status = VALUES(booking_status),
          source = VALUES(source),
          crm_update_id = VALUES(crm_update_id),
          city_id = VALUES(city_id),
          city = VALUES(city),
          created_log = VALUES(created_log)
      `;

      try {
        for (let i = 0; i < rows.length; i += DB_BATCH_SIZE) {
          await mysql.query(SQL, [rows.slice(i, i + DB_BATCH_SIZE)]);
          await heartbeat();
        }

        await commitOffsetsIfNecessary();
      } catch (err) {
        console.error('❌ DB batch failed:', err.message);
        throw err; // DO NOT COMMIT OFFSETS
      }
    }
  });
}

/* ===============================
   GRACEFUL SHUTDOWN
================================ */
async function shutdown(signal) {
  console.log(`⚠️ User Booking consumer shutdown: ${signal}`);
  try {
    await consumer.disconnect();
  } finally {
    process.exit(0);
  }
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

run().catch(err => {
  console.error('❌ User Booking consumer fatal:', err);
  process.exit(1);
});
