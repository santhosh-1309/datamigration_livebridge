/**
 * feedback_track - Kafka Producer
 * IDENTICAL logic to user_register producer
 */

const axios = require('axios');
const { producer } = require('../../config/kafka_config');

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

async function runFeedbackTrackProducer() {
  const API_BATCH_SIZE = 10000;
  const KAFKA_BATCH_SIZE = 200;
  const TOPIC = 'feedback_track_migration';
  const TABLE = 'feedback_track';

  let offset = 0;
  let totalSent = 0;

  try {
    await producer.connect();
    console.log('✅ Feedback Track Producer connected');

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
        console.log('✅ No more feedback_track records');
        break;
      }

      const messages = data
        .filter(row => row)
        .map(row => {
          let key = row.id;

          // SAME robust key logic as user_register
          if (!key || typeof key !== 'number' || key <= 0) {
            key = `temp_${Date.now()}_${Math.floor(Math.random() * 1000000)}`;
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
          console.error('❌ Kafka send failed for a batch:', err);
        }
      }

      console.log(`📦 Sent ${messages.length} messages (Total ${totalSent})`);

      offset += API_BATCH_SIZE;
      await delay(100);
    }

  } catch (err) {
    console.error('❌ Feedback Track Producer error:', err);
  } finally {
    try {
      await producer.disconnect();
      console.log('🔌 Feedback Track Producer disconnected');
    } catch (err) {
      console.error('❌ Error disconnecting producer:', err);
    }
  }
}

// Run directly if executed
if (require.main === module) {
  runFeedbackTrackProducer().catch(err => {
    console.error('❌ Fatal Producer Error:', err);
    process.exit(1);
  });
}

module.exports = runFeedbackTrackProducer;
