// migrations/user_veh_migrate/producer.js
const axios = require('axios');
const { producer } = require('../../config/kafka_config');
const { CompressionTypes } = require('kafkajs');

// Delay helper
const delay = ms => new Promise(res => setTimeout(res, ms));

(async () => {
  try {
    await producer.connect();
    console.log(`[${new Date().toISOString()}] ✅ Kafka producer connected.`);

    // Config
    const API_BATCH_SIZE = 2000;   // smaller batch to avoid API timeout
    const KAFKA_CHUNK_SIZE = 500;  // smaller chunk to avoid Kafka max message size
    const API_TIMEOUT = 120000;    // 2 minutes
    const table = "user_vehicle_table";

    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      console.log(
        `[${new Date().toISOString()}] ⏳ Fetching API data | offset=${offset}, limit=${API_BATCH_SIZE}`
      );

      // Fetch data from API
      const res = await axios.get(
        `https://bridge.gobumpr.com/api/csv/get_csv.php`,
        {
          params: { limit: API_BATCH_SIZE, offset, table },
          timeout: API_TIMEOUT
        }
      );

      const data = res.data;

      if (!data || !data.length) {
        console.log(`[${new Date().toISOString()}] ✅ No more data to process. Exiting loop.`);
        hasMore = false;
        break;
      }

      // Map API rows to Kafka messages
      const messages = data.map(row => ({
        key: row.user_id ? String(row.user_id) : undefined,
        value: JSON.stringify(row)
      }));

      // Send messages to Kafka in safe chunks
      for (let i = 0; i < messages.length; i += KAFKA_CHUNK_SIZE) {
        const chunk = messages.slice(i, i + KAFKA_CHUNK_SIZE);

        await producer.send({
          topic: 'user_vechicle_bridge_migration',
          messages: chunk,
          compression: CompressionTypes.GZIP
        });

        console.log(
          `[${new Date().toISOString()}] 📦 Sent ${chunk.length} messages (offset: ${offset})`
        );
      }

      offset += API_BATCH_SIZE;

      // Short delay to prevent overwhelming API
      await delay(100);
    }

    console.log(`[${new Date().toISOString()}] 🎉 All data processed successfully.`);

  } catch (err) {
    console.error(`[${new Date().toISOString()}] ❌ Producer failed`, err);
  } finally {
    await producer.disconnect();
    console.log(`[${new Date().toISOString()}] 🔌 Kafka producer disconnected.`);
  }
})();
