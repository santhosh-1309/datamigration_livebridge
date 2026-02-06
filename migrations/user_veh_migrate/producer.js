/**
 * user_vehicle_bridge - Kafka Producer
 * Safe for large API migrations
 * Moves to next table if no new data for 10 seconds
 */

const axios = require('axios');
const { producer } = require('../../config/kafka_config');

const delay = ms => new Promise(res => setTimeout(res, ms));

// Chunk array into Kafka-safe batches
function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

async function runUserVehicleProducer() {
  const API_BATCH_SIZE = 10000;   // fetch 10k at a time
  const KAFKA_BATCH_SIZE = 200;   // send 200 messages per Kafka batch
  const TOPIC = 'user_vechicle_bridge_migration';
  const TABLE = 'user_vehicle_table';

  let offset = 0;
  let totalSent = 0;
  let lastInsertTime = Date.now();

  try {
    await producer.connect();
    console.log('✅ User Vehicle Producer connected');

    while (true) {
      const res = await axios.get(
        'https://bridge.gobumpr.com/api/csv/get_csv.php',
        {
          params: { table: TABLE, limit: API_BATCH_SIZE, offset },
          timeout: 30000,
        }
      );

      const data = res.data;

      if (!Array.isArray(data) || data.length === 0) {
        console.log('✅ No more user_vehicle records');
        break;
      }

      // update activity timestamp
      if (data.length > 0) lastInsertTime = Date.now();

      const messages = data
        .filter(row => row)
        .map(row => {
          let key = row.id;
          if (!key || typeof key !== 'number' || key <= 0) {
            key = `temp_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
          }
          return { key: String(key), value: JSON.stringify(row) };
        });

      const kafkaChunks = chunkArray(messages, KAFKA_BATCH_SIZE);

      for (const chunk of kafkaChunks) {
        try {
          await producer.send({ topic: TOPIC, messages: chunk });
          totalSent += chunk.length;
        } catch (err) {
          console.error('❌ Kafka send failed for a batch:', err);
        }
      }

      console.log(`📦 Sent ${messages.length} messages (Total ${totalSent})`);

      offset += API_BATCH_SIZE;
      await delay(100);

      // ⏱ inactivity check (CRITICAL)
      if (Date.now() - lastInsertTime > 10000) {
        console.log('⏱ 10 seconds no new data, moving to next table...');
        break;
      }
    }
  } catch (err) {
    console.error('❌ User Vehicle Producer error:', err);
  } finally {
    try {
      await producer.disconnect();
      console.log('🔌 User Vehicle Producer disconnected');
    } catch (err) {
      console.error('❌ Error disconnecting producer:', err);
    }
  }
}

// Run directly if executed
if (require.main === module) {
  runUserVehicleProducer().catch(err => {
    console.error('❌ Fatal Producer Error:', err);
    process.exit(1);
  });
}

module.exports = runUserVehicleProducer;
