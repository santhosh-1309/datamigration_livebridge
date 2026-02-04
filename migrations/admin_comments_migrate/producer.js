/**
 * admin_comments_bridge - Kafka Producer
 * Aligned with user_register producer (production safe)
 */

const axios = require('axios');
const { producer } = require('../../config/kafka_config');

// Utility delay
const delay = ms => new Promise(res => setTimeout(res, ms));

// Chunk array into Kafka-safe batches
function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

async function runAdminCommentsProducer() {
  const API_BATCH_SIZE = 10000;     // fetch 10k rows at a time
  const KAFKA_BATCH_SIZE = 200;     // send 200 msgs per Kafka batch
  const TOPIC = 'admin_comments_migration';
  const TABLE = 'admin_comments_tbl';

  let offset = 0;
  let totalSent = 0;

  try {
    await producer.connect();
    console.log('✅ Admin Comments Producer connected');

    while (true) {
      const res = await axios.get(
        'https://bridge.gobumpr.com/api/csv/get_csv.php',
        {
          params: {
            table: TABLE,
            limit: API_BATCH_SIZE,
            offset
          },
          timeout: 30000
        }
      );

      const data = res.data;

      if (!Array.isArray(data) || data.length === 0) {
        console.log('✅ No more admin_comments records');
        break;
      }

      // Build Kafka messages (robust key handling)
      const messages = data
        .filter(row => row)
        .map(row => {
          let key = row.com_id;

          // Kafka key must always be valid
          if (!key || typeof key !== 'number' || key <= 0) {
            key = `temp_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
          }

          return {
            key: String(key),
            value: JSON.stringify(row)
          };
        });

      const kafkaChunks = chunkArray(messages, KAFKA_BATCH_SIZE);

      for (const chunk of kafkaChunks) {
        try {
          await producer.send({
            topic: TOPIC,
            messages: chunk
          });
          totalSent += chunk.length;
        } catch (err) {
          console.error('❌ Kafka send failed for a batch:', err.message);
        }
      }

      console.log(`📦 Sent ${messages.length} messages (Total ${totalSent})`);
      offset += API_BATCH_SIZE;
      await delay(100);
    }

  } catch (err) {
    console.error('❌ Admin Comments Producer error:', err.message);
  } finally {
    try {
      await producer.disconnect();
      console.log('🔌 Admin Comments Producer disconnected');
    } catch (err) {
      console.error('❌ Error disconnecting producer:', err.message);
    }
  }
}

// Run directly
if (require.main === module) {
  runAdminCommentsProducer().catch(err => {
    console.error('❌ Fatal Producer Error:', err);
    process.exit(1);
  });
}

module.exports = runAdminCommentsProducer;
