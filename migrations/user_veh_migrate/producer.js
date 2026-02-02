const axios = require("axios");
const kafka = require("../common/kafka");
const { CompressionTypes } = require("kafkajs");

const producer = kafka.producer({
  idempotent: true,
  maxInFlightRequests: 5,
  allowAutoTopicCreation: false
});

const delay = ms => new Promise(res => setTimeout(res, ms));

const API_BATCH = 10000;
const KAFKA_BATCH = 1000;
const TOPIC = "user_vechicle_bridge_migration";
const TABLE = "user_vehicle_table";

(async () => {
  try {
    await producer.connect();
    console.log("✅ Kafka producer connected");

    let offset = 0;

    while (true) {
      const res = await axios.get(
        `https://bridge.gobumpr.com/api/csv/get_csv.php`,
        {
          params: { limit: API_BATCH, offset, table: TABLE },
          timeout: 30000
        }
      );

      const data = res.data;
      if (!data || !data.length) break;

      const messages = data.map(row => ({
        key: String(row.id), // CRITICAL: consistent partitioning
        value: JSON.stringify(row)
      }));

      for (let i = 0; i < messages.length; i += KAFKA_BATCH) {
        await producer.send({
          topic: TOPIC,
          compression: CompressionTypes.Snappy,
          messages: messages.slice(i, i + KAFKA_BATCH)
        });
      }

      console.log(`📦 Sent ${messages.length} rows (offset ${offset})`);

      offset += API_BATCH;
      await delay(100);
    }
  } catch (err) {
    console.error("❌ Producer fatal:", err);
  } finally {
    await producer.disconnect();
    console.log("🔌 Kafka producer disconnected");
  }
})();

