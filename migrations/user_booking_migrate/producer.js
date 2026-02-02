/**
 * user_booking_tb - Kafka Producer
 * Safe for large API-based migrations
 */

const axios = require('axios');
const kafka = require('../../config/kafka_config');

// Kafka producer
const producer = kafka.producer();

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

async function runUserBookingProducer() {
  // 🔢 Tunable constants (same pattern)
  const API_BATCH_SIZE = 10000;
  const KAFKA_BATCH_SIZE = 200;

  const TABLE = 'user_booking_tb';
  const TOPIC = 'user_booking_tb_migration';

  let offset = 0;
  let totalSent = 0;

  try {
    await producer.connect();
    console.log('✅ User Booking Producer connected');

    while (true) {
      console.log(
        `⏳ Fetching user_booking_tb: offset=${offset}, limit=${API_BATCH_SIZE}`
      );

      // 🌐 Fetch from same CSV API
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

      // 🛑 Exit condition
      if (!Array.isArray(data) || data.length === 0) {
        console.log('✅ No more user_booking_tb records');
        break;
      }

      // 🔁 Convert rows to Kafka messages
      const messages = data.map(row => ({
        // booking_id preferred as Kafka key
        key: row.booking_id ? String(row.booking_id) : null,
        value: JSON.stringify(row)
      }));

      // 🔪 Kafka-safe chunking
      const kafkaChunks = chunkArray(messages, KAFKA_BATCH_SIZE);

      // 🚀 Send chunks
      for (const chunk of kafkaChunks) {
        await producer.send({
          topic: TOPIC,
          messages: chunk
        });

        totalSent += chunk.length;
      }

      console.log(
        `📦 Sent ${messages.length} records (Total sent: ${totalSent})`
      );

      offset += API_BATCH_SIZE;
      await delay(100); // throttle
    }

    console.log(
      `🎉 user_booking_tb migration producer completed. Total sent: ${totalSent}`
    );

  } catch (err) {
    console.error('❌ User Booking Producer error:', err);
  } finally {
    try {
      await producer.disconnect();
      console.log('🔌 User Booking Producer disconnected');
    } catch (e) {
      console.error('Error disconnecting producer:', e.message);
    }
  }
}

module.exports = runUserBookingProducer;
