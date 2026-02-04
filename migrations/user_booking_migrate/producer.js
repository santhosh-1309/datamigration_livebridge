/**
 * <table_name> - Kafka Producer
 * Production-safe large migration producer
 */

const axios = require('axios');
const kafka = require('../../config/kafka_config');

const { producer } = require('../../config/kafka_config');
const delay = ms => new Promise(res => setTimeout(res, ms));

function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

async function runProducer() {
  const API_BATCH_SIZE = 10000;
  const KAFKA_BATCH_SIZE = 200;

  const TABLE = 'user_booking_tb';              // 🔁 change only this
  const TOPIC = 'user_booking_tb_migration';    // 🔁 change only this
  const KEY_FIELD = 'booking_id';               // 🔁 change only this

  let offset = 0;
  let totalSent = 0;

  try {
    await producer.connect();
    console.log(`✅ Producer connected for ${TABLE}`);

    while (true) {
      console.log(`⏳ Fetching ${TABLE} offset=${offset}`);

      const res = await axios.get(
        'https://bridge.gobumpr.com/api/csv/get_csv.php',
        {
          params: { table: TABLE, limit: API_BATCH_SIZE, offset },
          timeout: 30000,
        }
      );

      const data = res.data;

      if (!Array.isArray(data) || data.length === 0) {
        console.log(`✅ No more records for ${TABLE}`);
        break;
      }

      const messages = data
        .filter(Boolean)
        .map(row => {
          let key = row[KEY_FIELD];

          if (!key) {
            key = `temp_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
          }

          return {
            key: String(key),
            value: JSON.stringify(row),
          };
        });

      const kafkaChunks = chunkArray(messages, KAFKA_BATCH_SIZE);

      for (const chunk of kafkaChunks) {
        try {
          await producer.send({
            topic: TOPIC,
            messages: chunk,
          });
          totalSent += chunk.length;
        } catch (err) {
          console.error(`❌ Kafka batch failed (${TABLE})`, err.message);
        }
      }

      console.log(`📦 Sent ${messages.length} records (Total ${totalSent})`);
      offset += API_BATCH_SIZE;
      await delay(100);
    }

    console.log(`🎉 ${TABLE} producer completed. Total sent: ${totalSent}`);
  } catch (err) {
    console.error(`❌ Producer fatal error (${TABLE}):`, err);
  } finally {
    try {
      await producer.disconnect();
      console.log(`🔌 Producer disconnected (${TABLE})`);
    } catch (err) {
      console.error('❌ Disconnect error:', err.message);
    }
  }
}

if (require.main === module) {
  runProducer().catch(err => {
    console.error('❌ Fatal Producer Error:', err);
    process.exit(1);
  });
}

module.exports = runProducer;
