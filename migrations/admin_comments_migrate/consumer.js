/**
 * admin_comments_tbl - Kafka Producer
 * Uses common API (get_csv.php)
 * Safe for large migrations
 */

const axios = require('axios');
const kafka = require('../common/kafka');

// Kafka producer (same default config as others)
const producer = kafka.producer();

// Utility delay
const delay = ms => new Promise(res => setTimeout(res, ms));

// Chunk helper (Kafka-safe)
function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

(async () => {
  // Tunables (same pattern)
  const API_BATCH_SIZE = 10000;
  const KAFKA_BATCH_SIZE = 200;

  const TOPIC = 'admin_comments_migration';
  const TABLE = 'admin_comments_tbl';

  let offset = 0;
  let totalSent = 0;

  try {
    await producer.connect();
    console.log(`[${new Date().toISOString()}] ✅ Kafka producer connected`);

    while (true) {
      console.log(
        `[${new Date().toISOString()}] ⏳ Fetching admin comments | offset=${offset}, limit=${API_BATCH_SIZE}`
      );

      // Fetch from common API
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

      // Exit condition
      if (!data || !Array.isArray(data) || data.length === 0) {
        console.log(`[${new Date().toISOString()}] ✅ No more data`);
        break;
      }

      // Convert to Kafka messages
      const messages = data.map(row => ({
        key: row.id ? String(row.id) : undefined,
        value: JSON.stringify(row)
      }));

      // Chunk for Kafka
      const kafkaChunks = chunkArray(messages, KAFKA_BATCH_SIZE);

      // Send chunks
      for (const chunk of kafkaChunks) {
        await producer.send({
          topic: TOPIC,
          messages: chunk
        });

        totalSent += chunk.length;
      }

      console.log(
        `[${new Date().toISOString()}] 📦 Sent ${messages.length} records (Total: ${totalSent})`
      );

      offset += API_BATCH_SIZE;
      await delay(100); // throttle
    }

    console.log(
      `[${new Date().toISOString()}] 🎉 Admin comments migration completed. Total sent: ${totalSent}`
    );

  } catch (err) {
    console.error(
      `[${new Date().toISOString()}] ❌ Admin comments producer fatal:`,
      err
    );
  } finally {
    try {
      await producer.disconnect();
      console.log(`[${new Date().toISOString()}] 🔌 Kafka producer disconnected`);
    } catch (e) {
      console.error('❌ Error during producer disconnect:', e.message);
    }
  }
})();
