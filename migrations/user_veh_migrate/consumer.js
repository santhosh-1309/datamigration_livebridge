require("dotenv").config();

const kafka = require("../common/kafka");
const { liveDB, uatDB } = require("../config/revamp_db");
const { encrypt } = require("../common/encryption");

const consumer = kafka.consumer({
  groupId: "user-vehicle-bridge-migration",
  sessionTimeout: 30000,
  maxBytesPerPartition: 5 * 1024 * 1024
});

const TOPIC = "user_vechicle_bridge_migration";
const TARGET_TABLE = "user_vehicle_migration_stage";
const MIGRATION_STEP = "user_vehicle_bridge_migration";

const DB_BATCH_SIZE = 500;

/* ------------------------------
   ERROR LOGGER (NON-BLOCKING)
-------------------------------*/
async function logError(data, errorMsg) {
  try {
    await liveDB.query(
      `INSERT INTO migration_error_log
       (source_table, target_table, source_primary_key, failed_data, error_message, migration_step)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [
        "user_vehicle",
        TARGET_TABLE,
        data?.id || null,
        JSON.stringify(data || {}),
        errorMsg,
        MIGRATION_STEP
      ]
    );
  } catch (_) {
    // never block consumer
  }
}

/* ------------------------------
   REG_ID CACHE (LOAD ONCE)
-------------------------------*/
const regIdCache = new Map();

async function loadRegIdCache() {
  const [rows] = await liveDB.query(
    `SELECT reg_id, old_reg_id FROM user_register_bridge`
  );

  for (const r of rows) {
    if (!r.old_reg_id) continue;
    r.old_reg_id
      .split(",")
      .map(x => x.trim())
      .filter(Boolean)
      .forEach(oldId => regIdCache.set(oldId, r.reg_id));
  }

  console.log(`✅ reg_id cache loaded: ${regIdCache.size}`);
}

/* ------------------------------
   BULK UPSERT (DUAL SCHEMA)
-------------------------------*/
async function bulkUpsert(rows, heartbeat) {
  const SQL = `
    INSERT INTO user_vehicle_migration_stage
    (id, reg_id, old_reg_id, veh_reg_no, fuel_type, odometerReading, make_name, model_name, created_log)
    VALUES ?
    ON DUPLICATE KEY UPDATE
      reg_id = VALUES(reg_id),
      old_reg_id = VALUES(old_reg_id),
      veh_reg_no = VALUES(veh_reg_no),
      fuel_type = VALUES(fuel_type),
      odometerReading = VALUES(odometerReading),
      make_name = VALUES(make_name),
      model_name = VALUES(model_name),
      created_log = VALUES(created_log)
  `;

  for (let i = 0; i < rows.length; i += DB_BATCH_SIZE) {
    const chunk = rows.slice(i, i + DB_BATCH_SIZE);

    // 🔁 Write to BOTH schemas
    await liveDB.query(SQL, [chunk]);
    await uatDB.query(SQL, [chunk]);

    await heartbeat();
  }
}

/* ------------------------------
   CONSUMER RUN
-------------------------------*/
async function run() {
  await loadRegIdCache();

  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  console.log("🚀 User Vehicle consumer running (DUAL SCHEMA)");

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
          await logError({}, "INVALID_JSON");
          resolveOffset(message.offset);
          continue;
        }

        try {
          const finalRegId =
            regIdCache.get(String(data.reg_id)) || data.reg_id;

          const oldRegId =
            finalRegId !== data.reg_id ? String(data.reg_id) : null;

          rows.push([
            data.id,
            finalRegId,
            oldRegId,
            data.veh_reg_no ? encrypt(data.veh_reg_no) : null,
            data.fuel_type,
            data.odometerReading,
            data.make,
            data.model,
            data.log
          ]);
        } catch (err) {
          await logError(data, err.message);
        }

        resolveOffset(message.offset);
      }

      if (!rows.length) return;

      try {
        await bulkUpsert(rows, heartbeat);
        await commitOffsetsIfNecessary();
      } catch (err) {
        console.error("❌ Dual-schema DB batch failed:", err.message);
        throw err; // offsets NOT committed
      }
    }
  });
}

/* ------------------------------
   GRACEFUL SHUTDOWN
-------------------------------*/
async function shutdown(signal) {
  console.log(`⚠️ Consumer shutdown: ${signal}`);
  try {
    await consumer.disconnect();
  } finally {
    process.exit(0);
  }
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

run().catch(err => {
  console.error("❌ Consumer fatal:", err);
  process.exit(1);
});
