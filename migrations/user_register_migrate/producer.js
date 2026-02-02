/**
 * user_register_bridge - Kafka Producer
 * Safe for large API migrations
 */

const axios = require('axios');
const kafka = require('../../config/kafka_config');

// Kafka producer
const producer = kafka.producer();

// Utility delay
const delay = ms => new Promise(res => setTimeout(res, ms));

// Chunk array into smaller Kafka-safe batches
function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

async function runUserRegisterProducer() {
  const API_BATCH_SIZE = 10000;
  const KAFKA_BATCH_SIZE = 200;
  const TOPIC = 'user_register_bridge_migration';
  const TABLE = 'user_register';

  let offset = 0;
  let totalSent = 0;

  try {
    await producer.connect();
    console.log('✅ User Register Producer connected');

    while (true) {
      const res = await axios.get(
        'https://bridge.gobumpr.com/api/csv/get_csv.php',
        {
          params: { table: TABLE, limit: API_BATCH_SIZE, offset },
          timeout: 30000
        }
      );

      const data = res.data;

      if (!Array.isArray(data) || data.length === 0) {
        console.log('✅ No more user_register records');
        break;
      }

      const messages = data.map(row => ({
        key: row.user_id ? String(row.user_id) : null,
        value: JSON.stringify(row)
      }));

      const kafkaChunks = chunkArray(messages, KAFKA_BATCH_SIZE);

      for (const chunk of kafkaChunks) {
        await producer.send({ topic: TOPIC, messages: chunk });
        totalSent += chunk.length;
      }

      console.log(`📦 Sent ${messages.length} (Total ${totalSent})`);

      offset += API_BATCH_SIZE;
      await delay(100);
    }

  } catch (err) {
    console.error('❌ User Register Producer error:', err);
  } finally {
    await producer.disconnect();
    console.log('🔌 User Register Producer disconnected');
  }
}

module.exports = runUserRegisterProducer;