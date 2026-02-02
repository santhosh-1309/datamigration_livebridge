/**
 * feedback_track - Kafka Producer
 * Safe for large CSV/API migrations
 */

const axios = require('axios');
const kafka = require('../common/kafka');

// ⚙️ Kafka producer
const producer = kafka.producer();

// ⏳ Utility delay
const delay = ms => new Promise(res => setTimeout(res, ms));

// 📦 Chunk array into smaller Kafka-safe batches
function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

(async () => {
  const API_BATCH_SIZE = 10000; // API fetch size
  const KAFKA_BATCH_SIZE = 200; // Kafka safe batch
  const TOPIC = 'feedback_track_migration';
  const TABLE = 'feedback_track';

  let offset = 0;
  let totalSent = 0;

  try {
    await producer.connect();
    console.log(`[${new Date().toISOString()}] ✅ Kafka producer connected.`);

    while (true) {
      console.log(`[${new Date().toISOString()}] ⏳ Fetching from API: offset=${offset}, limit=${API_BATCH_SIZE}`);

      const res = await axios.get(
        `https://bridge.gobumpr.com/api/csv/get_csv.php`,
        {
          params: { table: TABLE, limit: API_BATCH_SIZE, offset },
          timeout: 30000
        }
      );

      const data = res.data;
      if (!data || !Array.isArray(data) || data.length === 0) {
        console.log(`[${new Date().toISOString()}] ✅ No more data to process.`);
        break;
      }

      // 🔁 Convert rows to Kafka messages
      const messages = data.map(row => ({
        key: row.id ? String(row.id) : undefined,
        value: JSON.stringify(row)
      }));

      // 🔪 Split into Kafka-safe chunks
      const kafkaChunks = chunkArray(messages, KAFKA_BATCH_SIZE);

      for (const chunk of kafkaChunks) {
        await producer.send({
          topic: TOPIC,
          messages: chunk
        });
        totalSent += chunk.length;
      }

      console.log(`[${new Date().toISOString()}] 📦 Sent ${messages.length} rows (Total sent: ${totalSent})`);

      offset += API_BATCH_SIZE;
      await delay(100); // small throttle
    }

    console.log(`[${new Date().toISOString()}] 🎉 Migration completed. Total records sent: ${totalSent}`);
  } catch (err) {
    console.error(`[${new Date().toISOString()}] ❌ Producer fatal error:`, err);
  } finally {
    try {
      await producer.disconnect();
      console.log(`[${new Date().toISOString()}] 🔌 Kafka producer disconnected.`);
    } catch (e) {
      console.error('❌ Error disconnecting producer:', e);
    }
  }
})();
