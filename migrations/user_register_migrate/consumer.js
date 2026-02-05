/**
 * user_register_bridge - Kafka Consumer (Safe Encryption + Batch Insert)
 * Dynamic target table support
 */

const { userRegisterConsumer } = require('../../config/kafka_config');
const { liveDB, uatDB } = require('../../config/revamp_db');
const { encrypt } = require('../../common/encryption');

console.log("🚀 User Register Consumer started");

/* ---------- Constants ---------- */
const SOURCE_TABLE = 'go_bumpr.user_register';

const TARGET_TABLE = [
  { db: uatDB, table: 'UAT_mytvs_bridge.user_register_uat' },
  { db: liveDB, table: 'mytvs_bridge.user_register' }
];

const MIGRATION_STEP = 'user_register_bridge_migration';

/* ---------- Helpers ---------- */
function normalizeMobile(mobile) {
  if (!mobile) return null;
  const digits = mobile.replace(/\D/g, '');
  return digits.length >= 10 ? digits.slice(-10) : null;
}

function cleanEmail(email) {
  if (!email) return null;
  const e = email.trim().toLowerCase();
  return e.includes('@') ? e : null;
}

/* ---------- Safe Encryption ---------- */
function safeEncrypt(input) {
  if (!input) return null;
  try {
    return encrypt(input);
  } catch (err) {
    console.error('❌ Encryption failed for:', input, err.message);
    return null;
  }
}

/* ---------- Safe Error Logger ---------- */
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
        data?.reg_id || null,
        JSON.stringify(data || {}),
        msg,
        MIGRATION_STEP
      ]
    );
  } catch (err) {
    console.error('❌ Migration error log failed:', err.message || err);
  }
}

/* ---------- Consumer ---------- */
async function runUserRegisterConsumer() {
  await userRegisterConsumer.connect();
  await userRegisterConsumer.subscribe({
    topic: 'user_register_bridge_migration',
    fromBeginning: true
  });

  console.log('✅ User Register Consumer Running');

  const BATCH_SIZE = 500; // Adjust for performance

  await userRegisterConsumer.run({
    eachBatchAutoResolve: false,

    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {

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

        const mobile = normalizeMobile(data.mobile_number);
        if (!mobile) {
          await logError(data, 'INVALID_MOBILE');
          resolveOffset(message.offset);
          continue;
        }

        const encMobile = safeEncrypt(mobile);
        const altMobile = normalizeMobile(data.mobile_number2);
        const encAlt = safeEncrypt(altMobile);
        const email = cleanEmail(data.email_id);
        const encEmail = safeEncrypt(email);

        // Prepare batch insert for all target tables
        batchData.forEach(batchItem => {
          batchItem.inserts.push([
            data.reg_id,
            data.name || null,
            encMobile,
            encAlt,
            encEmail,
            mobile,
            data.City || null,
            data.source || null,
            data.crm_log_id || null,
            data.log
          ]);
        });

        resolveOffset(message.offset);
        await heartbeat();

        // If batch size reached, insert
        for (const batchItem of batchData) {
          if (batchItem.inserts.length >= BATCH_SIZE) {
            await insertBatch(batchItem);
            batchItem.inserts = [];
          }
        }
      }

      // Insert remaining records
      for (const batchItem of batchData) {
        if (batchItem.inserts.length > 0) {
          await insertBatch(batchItem);
        }
      }

      await commitOffsetsIfNecessary();
    }
  });
}

/* ---------- Batch Insert Helper ---------- */
async function insertBatch({ db, table, inserts }) {
  const insertSQL = `
    INSERT INTO ${table}
    (reg_id, name, mobile_number, alternative_mobile_number,
     email_id, normalized_mobile, actual_city, actual_source, crm_log_id, created_log)
    VALUES ?
    ON DUPLICATE KEY UPDATE
      name = VALUES(name),
      email_id = VALUES(email_id),
      alternative_mobile_number = VALUES(alternative_mobile_number),
      crm_log_id = VALUES(crm_log_id),
      mod_log = CURRENT_TIMESTAMP
  `;
  try {
    await db.query(insertSQL, [inserts]);
  } catch (err) {
    console.error(`❌ Batch insert failed for ${table}:`, err.message);
    // Log each row individually
    for (const row of inserts) {
      await logError({
        reg_id: row[0],
        mobile: row[2],
        email: row[4]
      }, 'BATCH_INSERT_FAILED: ' + err.message, table);
    }
  }
}

/* ---------- Bootstrap ---------- */
runUserRegisterConsumer().catch(err => {
  console.error("❌ User Register Consumer fatal:", err);
  process.exit(1);
});

module.exports = runUserRegisterConsumer;
