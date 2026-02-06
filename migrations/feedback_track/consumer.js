/**
 * feedback_track_bridge - Kafka Consumer
 * Fast batch insert (500) – Orchestrator compatible
 */

const { feedbackConsumer } = require('../../config/kafka_config');
const { liveDB, uatDB } = require('../../config/revamp_db');
const { encrypt } = require('../../common/encryption');

console.log('🚀 Feedback Track Consumer started');

/* ---------- Constants ---------- */
const SOURCE_TABLE = 'feedback_track';

const TARGET_TABLE = [
  { db: uatDB, table: 'UAT_mytvs_bridge.service_buddy_booking_uat' },
  { db: liveDB, table: 'mytvs_bridge.feedback_track' }
];

const TOPIC = 'feedback_track_migration';
const MIGRATION_STEP = 'feedback_track_bridge_migration';
const BATCH_SIZE = 500;
const MIN_DATE = new Date('2023-12-31T23:59:59Z');

/* ---------- Safe Encryption ---------- */
function safeEncrypt(input) {
  if (!input) return null;
  try {
    return encrypt(String(input));
  } catch {
    return null;
  }
}

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
        data?.id || null,
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
async function runFeedbackTrackConsumer() {
  await feedbackConsumer.connect();
  await feedbackConsumer.subscribe({
    topic: TOPIC,
    fromBeginning: true
  });

  console.log('✅ Feedback Track Consumer Running');

  await feedbackConsumer.run({
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
        if (!data.created_log || new Date(data.created_log) <= MIN_DATE) {
          resolveOffset(message.offset);
          continue;
        }

        const crmLogId = safeEncrypt(data.crm_update_id);

        batchData.forEach(batchItem => {
          batchItem.inserts.push([
            data.id,
            data.booking_id || null,
            data.b2b_booking_id || null,
            crmLogId,
            data.created_log || null
          ]);
        });

        resolveOffset(message.offset);
        await heartbeat();

        // Flush batch
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
      id,
      booking_id,
      b2b_booking_id,
      crm_update_id,
      created_log
    )
    VALUES ?
    ON DUPLICATE KEY UPDATE
      booking_id = VALUES(booking_id),
      b2b_booking_id = VALUES(b2b_booking_id),
      crm_update_id = VALUES(crm_update_id),
      created_log = VALUES(created_log)
  `;

  try {
    await db.query(SQL, [inserts]);
  } catch (err) {
    console.error(`❌ Batch insert failed for ${table}:`, err.message);
    for (const row of inserts) {
      await logError({ id: row[0] }, 'BATCH_INSERT_FAILED: ' + err.message, table);
    }
  }
}

/* ---------- Bootstrap ---------- */
runFeedbackTrackConsumer().catch(err => {
  console.error('❌ Feedback Track Consumer fatal:', err);
  process.exit(1);
});

module.exports = runFeedbackTrackConsumer;
