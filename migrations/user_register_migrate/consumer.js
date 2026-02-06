/**
 * user_vehicle_bridge - Kafka Consumer (Batch Insert)
 */

const { uservechicleConsumer } = require('../../config/kafka_config');
const { liveDB, uatDB } = require('../../config/revamp_db');

console.log("🚀 User Vehicle Consumer started");

/* ---------- Constants ---------- */
const SOURCE_TABLE = 'go_bumpr.user_vehicle';

const TARGET_TABLE = [
  { db: uatDB, table: 'UAT_mytvs_bridge.user_vehicle_uat' },
  { db: liveDB, table: 'mytvs_bridge.user_vehicle' }
];

const MIGRATION_STEP = 'user_vehicle_bridge_migration';
const TOPIC = 'user_vehicle_bridge_migration';
const BATCH_SIZE = 500;

/* ---------- Error Logger ---------- */
async function logError(data, msg, table = TARGET_TABLE.map(t => t.table).join(',')) {
  try {
    if (!uatDB) return;

    await uatDB.query(
      `INSERT INTO migration_error_log
       (source_table, target_table, source_primary_key, failed_data, error_message, migration_step)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [
        SOURCE_TABLE,
        table,
        data?.vehicle_id || null,
        JSON.stringify(data || {}),
        msg,
        MIGRATION_STEP
      ]
    );
  } catch (err) {
    console.error('❌ Error log failed:', err.message);
  }
}

/* ---------- Batch Insert ---------- */
async function insertBatch({ db, table, inserts }) {
  const sql = `
    INSERT INTO ${table}
    (
      vehicle_id,
      reg_id,
      vehicle_number,
      brand,
      model,
      fuel_type,
      created_log
    )
    VALUES ?
    ON DUPLICATE KEY UPDATE
      brand = VALUES(brand),
      model = VALUES(model),
      fuel_type = VALUES(fuel_type),
      mod_log = CURRENT_TIMESTAMP
  `;

  try {
    await db.query(sql, [inserts]);
  } catch (err) {
    console.error(`❌ Batch insert failed for ${table}:`, err.message);

    for (const row of inserts) {
      await logError(
        { vehicle_id: row[0], reg_id: row[1] },
        'BATCH_INSERT_FAILED: ' + err.message,
        table
      );
    }
  }
}

/* ---------- Consumer ---------- */
async function runUserVehicleConsumer() {
  await uservechicleConsumer.connect();
  await uservechicleConsumer.subscribe({
    topic: TOPIC,
    fromBeginning: true
  });

  console.log('✅ User Vehicle Consumer Running');

  await uservechicleConsumer.run({
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

        // Prepare insert rows
        batchData.forEach(b => {
          b.inserts.push([
            data.vehicle_id,
            data.reg_id,
            data.vehicle_number || null,
            data.brand || null,
            data.model || null,
            data.fuel_type || null,
            data.log
          ]);
        });

        resolveOffset(message.offset);
        await heartbeat();

        // Insert once batch hits size
        for (const b of batchData) {
          if (b.inserts.length >= BATCH_SIZE) {
            await insertBatch(b);
            b.inserts = [];
          }
        }
      }

      // Insert remaining rows
      for (const b of batchData) {
        if (b.inserts.length > 0) {
          await insertBatch(b);
        }
      }

      await commitOffsetsIfNecessary();
    }
  });
}

/* ---------- Bootstrap ---------- */
runUserVehicleConsumer().catch(err => {
  console.error("❌ User Vehicle Consumer fatal:", err);
  process.exit(1);
});

module.exports = runUserVehicleConsumer;
