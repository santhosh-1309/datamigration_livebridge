const axios = require('axios');
const { producer } = require('../../config/kafka_config'); // 🔹 same as consumer.js

const delay = ms => new Promise(res => setTimeout(res, ms));

(async () => {
  try {
    await producer.connect();
    console.log(`[${new Date().toISOString()}] ✅ Kafka producer connected.`);

    const batchSize = 10000;
    let offset = 0;
    const table = "user_vehicle_table";
    let hasMore = true;

    while (hasMore) {
      console.log(`[${new Date().toISOString()}] ⏳ Fetching API data | offset=${offset}, limit=${batchSize}`);

      // 🔹 Fetch data from API
      const res = await axios.get(
        `https://bridge.gobumpr.com/api/csv/get_csv.php`,
        {
          params: { limit: batchSize, offset, table },
          timeout: 120000 // increased timeout to avoid ECONNABORTED
        }
      );

      const data = res.data;

      if (!data || !data.length) {
        console.log(`[${new Date().toISOString()}] ✅ No more data to process. Exiting loop.`);
        hasMore = false;
        break;
      }

      // 🔹 Map API data to Kafka messages
      const messages = data.map(row => ({
        key: row.user_id ? String(row.user_id) : undefined,
        value: JSON.stringify(row)
      }));

      await producer.send({
        topic: 'user_vechicle_bridge_migration',
        messages
      });

      console.log(
        `[${new Date().toISOString()}] 📦 Sent batch of ${messages.length} messages (offset: ${offset})`
      );

      offset += batchSize;
      await delay(100); // small delay between batches
    }

  } catch (err) {
    console.error(`[${new Date().toISOString()}] ❌ Fatal error in producer:`, err.message || err);
  } finally {
    await producer.disconnect();
    console.log(`[${new Date().toISOString()}] 🔌 Kafka producer disconnected.`);
  }
})();
