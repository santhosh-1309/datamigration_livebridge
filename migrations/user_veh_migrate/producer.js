const axios = require('axios');
const kafka = require('../common/kafka');
const producer = kafka.producer();

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
      console.log(`[${new Date().toISOString()}] ⏳ Fetching data from API: offset=${offset}, limit=${batchSize}`);

      const res = await axios.get(
        `https://bridge.gobumpr.com/api/csv/get_csv.php?limit=${batchSize}&offset=${offset}&table=${table}`,
        { timeout: 30000 }
      );

      const data = res.data;

      if (!data || !data.length) {
        console.log(`[${new Date().toISOString()}] ✅ No more data to process. Exiting loop.`);
        hasMore = false;
        break;
      }

      const messages = data.map(row => ({
        key: row.user_id ? String(row.user_id) : undefined, // 🔹 same logic as DB script
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
      await delay(100);
    }

  } catch (err) {
    console.error(`[${new Date().toISOString()}] ❌ Fatal error in producer:`, err);
  } finally {
    await producer.disconnect();
    console.log(`[${new Date().toISOString()}] 🔌 Kafka producer disconnected.`);
  }
})();
