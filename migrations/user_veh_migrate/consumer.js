/**
 * user_vehicle_bridge - Kafka Consumer
 * Dynamic-table version with batch inserts and updates + 10-sec inactivity catch
 */

const { uservechicleConsumer } = require("../../config/kafka_config");
const { liveDB, uatDB } = require("../../config/revamp_db");
const { encrypt } = require("../../common/encryption");

console.log("🚀 User Vehicle Consumer started");

/* ---------- Constants ---------- */
const SOURCE_TABLE = "go_bumpr.user_vehicle_table";

const TARGET_TABLE = [
  { db: uatDB, table: "UAT_mytvs_bridge.user_vehicle_uat" },
  { db: liveDB, table: "mytvs_bridge.user_vehicle" }
];

const MIGRATION_STEP = "user_vehicle_bridge_migration";
const TOPIC = "user_vechicle_bridge_migration"; 

// Max rows per batch insert to DB
const BATCH_SIZE = 500;

/* ---------- Safe Encryption ---------- */
function safeEncrypt(input) {
  if (!input) return null;
  try {
    return encrypt(input);
  } catch (err) {
    console.error("❌ Encryption failed for:", input, err.message);
    return null;
  }
}

/* ---------- Safe Error Logger ---------- */
async function logError(data, msg, table = TARGET_TABLE.map(t => t.table).join(",")) {
  try {
    if (!uatDB) return;
    await uatDB.query(
      `INSERT INTO migration_error_log
       (source_table, target_table, source_primary_key, failed_data, error_message, migration_step)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [
        SOURCE_TABLE,
        table,
        data?.id || null,
        JSON.stringify(data || {}),
        msg,
        MIGRATION_STEP
      ]
    );
  } catch (err) {
    console.error("❌ Migration error log failed:", err.message || err);
  }
}

/* ---------- Helper: chunk array ---------- */
function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

/* ---------- Consumer ---------- */
async function runUserVehicleConsumer() {
  await uservechicleConsumer.connect();
  await uservechicleConsumer.subscribe({ topic: TOPIC, fromBeginning: true });

  console.log("✅ User Vehicle Consumer Running");

  let lastMessageTime = Date.now();

  await uservechicleConsumer.run({
    eachBatchAutoResolve: false,

    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
      if (!batch.messages.length) return;

      // Parse messages
      const messages = batch.messages.map(m => {
        try {
          return JSON.parse(m.value.toString());
        } catch {
          logError({}, "INVALID_JSON");
          return null;
        }
      }).filter(Boolean);

      if (messages.length > 0) lastMessageTime = Date.now();

      for (const target of TARGET_TABLE) {
        const { db, table } = target;

        // Separate inserts and updates
        const inserts = [];
        const updates = [];

        for (const data of messages) {
          if (data.type !== "4w") continue;
          const encVehRegNo = safeEncrypt(data.reg_no);

          const [rows] = await db.query(`SELECT id FROM ${table} WHERE id = ?`, [data.id]);

          if (!rows.length) {
            inserts.push([
              data.id,
              data.user_id || null,
              encVehRegNo,
              data.fuel_type || null,
              data.odo_reading || null,
              data.brand || null,
              data.model || null,
              data.log,
              data.vehicle_id
            ]);
          } else {
            updates.push([
              data.user_id,
              encVehRegNo,
              data.fuel_type,
              data.odo_reading,
              data.brand,
              data.model,
              data.vehicle_id,
              data.id
            ]);
          }
        }

        // Batch inserts
        for (const chunk of chunkArray(inserts, BATCH_SIZE)) {
          if (chunk.length === 0) continue;
          const insertSQL = `
            INSERT INTO ${table}
            (id, reg_id, veh_reg_no, fuel_type, odometerReading,
             make_name, model_name, created_log, vehicle_id)
            VALUES ?
          `;
          try {
            await db.query(insertSQL, [chunk]);
          } catch (err) {
            for (const row of chunk) await logError({ id: row[0] }, "INSERT_FAILED: " + err.message, table);
          }
        }

        // Batch updates
        for (const chunk of chunkArray(updates, BATCH_SIZE)) {
          if (chunk.length === 0) continue;
          const updateSQL = `
            UPDATE ${table}
            SET
              reg_id = IFNULL(?, reg_id),
              veh_reg_no = IFNULL(?, veh_reg_no),
              fuel_type = IFNULL(?, fuel_type),
              odometerReading = IFNULL(?, odometerReading),
              make_name = IFNULL(?, make_name),
              model_name = IFNULL(?, model_name),
              vehicle_id = IFNULL(?, vehicle_id),
              mod_log = CURRENT_TIMESTAMP
            WHERE id = ?
          `;
          for (const params of chunk) {
            try {
              await db.query(updateSQL, params);
            } catch (err) {
              await logError({ id: params[7] }, "UPDATE_FAILED: " + err.message, table);
            }
          }
        }
      }

      // Resolve offsets
      for (const message of batch.messages) resolveOffset(message.offset);
      await heartbeat();
      await commitOffsetsIfNecessary();

      // 10-second inactivity catch
      if (Date.now() - lastMessageTime > 10000) {
        console.log("⏱ 10 seconds no new messages, exiting batch to move to next table...");
        process.exit(0);
      }
    }
  });
}

/* ---------- Bootstrap ---------- */
runUserVehicleConsumer().catch(err => {
  console.error("❌ User Vehicle Consumer fatal:", err);
  process.exit(1);
});

module.exports = runUserVehicleConsumer;
