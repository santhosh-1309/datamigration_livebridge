/**
 * user_vehicle_bridge - Kafka Consumer
 * Dynamic-table version (same as user_register)
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

/* ---------- Consumer ---------- */
async function runUserVehicleConsumer() {
  await uservechicleConsumer.connect();
  await uservechicleConsumer.subscribe({ topic: TOPIC, fromBeginning: true });

  console.log("✅ User Vehicle Consumer Running");

  await uservechicleConsumer.run({
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

        // Business rule unchanged
        if (data.type !== "4w") {
          resolveOffset(message.offset);
          await heartbeat();
          continue;
        }

        try {
          const encVehRegNo = safeEncrypt(data.reg_no);

          for (const target of TARGET_TABLE) {
            const { db, table } = target;

            const [rows] = await db.query(
              `SELECT id FROM ${table} WHERE id = ?`,
              [data.id]
            );

            const insertSQL = `
              INSERT INTO ${table}
              (id, reg_id, veh_reg_no, fuel_type, odometerReading,
               make_name, model_name, created_log, vehicle_id)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            `;

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

            if (!rows.length) {
              const params = [
                data.id,
                data.user_id || null,
                encVehRegNo,
                data.fuel_type || null,
                data.odo_reading || null,
                data.brand || null,
                data.model || null,
                data.log,
                data.vehicle_id
              ];

              try {
                await db.query(insertSQL, params);
              } catch (err) {
                await logError(data, "INSERT_FAILED: " + err.message, table);
              }

            } else {
              const params = [
                data.user_id,
                encVehRegNo,
                data.fuel_type,
                data.odo_reading,
                data.brand,
                data.model,
                data.vehicle_id,
                data.id
              ];

              try {
                await db.query(updateSQL, params);
              } catch (err) {
                await logError(data, "UPDATE_FAILED: " + err.message, table);
              }
            }
          }

          resolveOffset(message.offset);
          await heartbeat();

        } catch (err) {
          await logError(data, "PROCESSING_FAILED: " + err.message);
          resolveOffset(message.offset);
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
