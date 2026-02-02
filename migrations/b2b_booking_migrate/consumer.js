/**
 * b2b_booking_tbl - Kafka Producer
 * Dual-schema migration safe
 */

const axios = require("axios");
const kafka = require("../common/kafka");
const { CompressionTypes } = require("kafkajs");

const producer = kafka.producer({
  idempotent: true,
  maxInFlightRequests: 5,
  allowAutoTopicCreation: false
});

const delay = ms => new Promise(res => setTimeout(res, ms));

const API_BATCH = 10000;       // API fetch batch
const KAFKA_BATCH = 1000;      // Kafka send batch
const TOPIC = "b2b_booking_bridge_migration";
const TABLE = "b2b.b2b_booking_tbl";

(async () => {
  try {
    await producer.connect();
    console.log("✅ Kafka producer connected for b2b_booking_tbl");

    let offset = 0;
    while (true) {
      // Fetch from API
      const res = await axios.get(
        `https://bridge.gobumpr.com/api/csv/get_csv.php`,
        { params: { table: TABLE, limit: API_BATCH, offset }, timeout: 30000 }
      );

      const data = res.data;
      if (!data || !data.length) break;

      // Convert rows to Kafka messages
      const messages = data.map(row => ({
        key: String(row.booking_id),
        value: JSON.stringify(row)
      }));

      // Split into Kafka-safe chunks
      for (let i = 0; i < messages.length; i += KAFKA_BATCH) {
        await producer.send({
          topic: TOPIC,
          compression: CompressionTypes.Snappy,
          messages: messages.slice(i, i + KAFKA_BATCH)
        });
      }

      console.log(`📦 Sent ${messages.length} rows (offset ${offset})`);

      offset += API_BATCH;
      await delay(100); // small throttle
    }

    console.log("🎉 Migration completed for b2b_booking_tbl");

  } catch (err) {
    console.error("❌ Producer fatal:", err);
  } finally {
    await producer.disconnect();
    console.log("🔌 Kafka producer disconnected");
  }
})();
