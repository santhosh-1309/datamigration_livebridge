/**
 * b2b_booking_bridge - Kafka Producer
 * Safe for large API migrations
 */

const axios = require("axios");
const { producer } = require("../../config/kafka_config");

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

async function runB2BBookingProducer() {
  const API_BATCH_SIZE = 10000;   // fetch 10k at a time
  const KAFKA_BATCH_SIZE = 200;   // send 200 messages per Kafka batch
  const TOPIC = "b2b_booking_bridge_migration";
  const TABLE = "b2b.b2b_booking_tbl";

  let offset = 0;
  let totalSent = 0;

  try {
    await producer.connect();
    console.log("✅ B2B Booking Producer connected");

    while (true) {
      const res = await axios.get(
        "https://bridge.gobumpr.com/api/csv/get_csv.php",
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
        console.log("✅ No more b2b_booking records");
        break;
      }

      // ✅ Robust Kafka key handling (IDENTICAL LOGIC)
      const messages = data
        .filter(row => row)
        .map(row => {
          let key = row.b2b_booking_id;

          // Ensure key is valid
          if (!key || typeof key !== "number" || key <= 0) {
            key = `temp_${Date.now()}_${Math.floor(Math.random() * 1_000_000)}`;
          }

          return {
            key: String(key),             // always string
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
          console.error("❌ Kafka send failed for a batch:", err.message);
        }
      }

      console.log(`📦 Sent ${messages.length} messages (Total ${totalSent})`);

      offset += API_BATCH_SIZE;
      await delay(100);
    }

  } catch (err) {
    console.error("❌ B2B Booking Producer error:", err);
  } finally {
    try {
      await producer.disconnect();
      console.log("🔌 B2B Booking Producer disconnected");
    } catch (err) {
      console.error("❌ Error disconnecting producer:", err);
    }
  }
}

// Run directly if executed
if (require.main === module) {
  runB2BBookingProducer().catch(err => {
    console.error("❌ Fatal Producer Error:", err);
    process.exit(1);
  });
}

module.exports = runB2BBookingProducer;
