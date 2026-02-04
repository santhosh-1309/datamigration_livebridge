/**
 * user_vehicle_table - Kafka Producer
 * Safe + High performance migration
 */

require("dotenv").config();

const axios = require("axios");
const { producer } = require("../../config/kafka_config");


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

async function runUserVehicleProducer() {
  const API_BATCH_SIZE = 10000;     // API fetch size
  const KAFKA_BATCH_SIZE = 1000;    // Kafka send batch
  const TOPIC = "user_vechicle_bridge_migration";
  const TABLE = "user_vehicle_table";

  let offset = 0;
  let totalSent = 0;

  try {
    await producer.connect();
    console.log("✅ User Vehicle Producer connected");

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
        console.log("✅ No more user_vehicle_table records");
        break;
      }

      // 🔐 Robust Kafka-safe message creation
      const messages = data
        .filter(row => row)
        .map(row => {
          let key = row.id;

          // Fallback for invalid keys (CRITICAL)
          if (!key || typeof key !== "number" || key <= 0) {
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
          console.error("❌ Kafka batch send failed:", err);
        }
      }

      console.log(
        `📦 Sent ${messages.length} records (offset ${offset}, total ${totalSent})`
      );

      offset += API_BATCH_SIZE;
      await delay(100);
    }
  } catch (err) {
    console.error("❌ User Vehicle Producer error:", err);
  } finally {
    try {
      await producer.disconnect();
      console.log("🔌 User Vehicle Producer disconnected");
    } catch (err) {
      console.error("❌ Error disconnecting producer:", err);
    }
  }
}

// Run directly if executed
if (require.main === module) {
  runUserVehicleProducer().catch(err => {
    console.error("❌ Fatal Producer Error:", err);
    process.exit(1);
  });
}

module.exports = runUserVehicleProducer;
