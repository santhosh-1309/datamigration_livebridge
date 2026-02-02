
const kafka = require('../../config/kafka_config');
const db = require('../../config/revamp_db');
const { encrypt } = require('../../common/encryption');

const consumer = kafka.consumer({
  groupId: 'user-register-bridge-migration-v8',
  sessionTimeout: 60000,
  heartbeatInterval: 3000
});

const SOURCE_TABLE = 'go_bumpr.user_register';
const TARGET_TABLE = [
  "UAT_mytvs_bridge.user_register_bridge",
  "mytvs_bridge.user_register_bridge"
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

/* ---------- Error Logger ---------- */
async function logError(data, msg) {
  await db.query(
    `INSERT INTO migration_error_log
     (source_table, target_table, source_primary_key, failed_data, error_message, migration_step)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [
      SOURCE_TABLE,
      TARGET_TABLE,
      data?.reg_id || null,
      JSON.stringify(data || {}),
      msg,
      MIGRATION_STEP
    ]
  );
}

/* ---------- Consumer ---------- */
async function runUserRegisterConsumer() {
  await consumer.connect();
  await consumer.subscribe({
    topic: 'user_register_bridge_migration',
    fromBeginning: true
  });

  console.log('✅ User Register Consumer Started');

  await consumer.run({
    eachBatchAutoResolve: false,

    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      commitOffsetsIfNecessary
    }) => {
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
          const mobile = normalizeMobile(data.mobile_number);
          if (!mobile) {
            await logError(data, 'INVALID_MOBILE');
            resolveOffset(message.offset);
            continue;
          }

          const encMobile = encrypt(mobile);
          const altMobile = normalizeMobile(data.mobile_number2);
          const encAlt = altMobile ? encrypt(altMobile) : null;
          const email = cleanEmail(data.email_id);
          const encEmail = email ? encrypt(email) : null;

          const [rows] = await db.query(
            `SELECT reg_id, old_reg_id
             FROM user_register_bridge
             WHERE normalized_mobile = ?`,
            [mobile]
          );

          /* ---------- INSERT ---------- */
          if (!rows.length) {
            await db.query(
              `INSERT INTO user_register_bridge
               (reg_id, name, mobile_number, alternative_mobile_number,
                email_id, normalized_mobile, city_id, source, crm_log_id, created_log)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
              [
                data.reg_id,
                data.name || null,
                encMobile,
                encAlt,
                encEmail,
                mobile,
                data.city_id || null,
                data.source || null,
                data.crm_log_id || null,
                data.log || new Date()
              ]
            );

            resolveOffset(message.offset);
            await heartbeat();
            continue;
          }

          /* ---------- UPDATE ---------- */
          const currentRegId = rows[0].reg_id;
          let oldIds = rows[0].old_reg_id
            ? rows[0].old_reg_id.split(',')
            : [];

          if (data.reg_id !== currentRegId) {
            oldIds.unshift(currentRegId.toString());
          }

          oldIds = [...new Set(oldIds)];

          await db.query(
            `UPDATE user_register_bridge
             SET
               reg_id = ?,
               old_reg_id = ?,
               name = IFNULL(?, name),
               email_id = IFNULL(?, email_id),
               alternative_mobile_number = IFNULL(?, alternative_mobile_number),
               crm_log_id = IFNULL(?, crm_log_id),
               mod_log = CURRENT_TIMESTAMP
             WHERE normalized_mobile = ?`,
            [
              data.reg_id,
              oldIds.join(','),
              data.name,
              encEmail,
              encAlt,
              data.crm_log_id,
              mobile
            ]
          );

          resolveOffset(message.offset);
          await heartbeat();

        } catch (err) {
          await logError(data, err.message);
          resolveOffset(message.offset);
        }
      }

      await commitOffsetsIfNecessary();
    }
  });
}

module.exports = runUserRegisterConsumer;
