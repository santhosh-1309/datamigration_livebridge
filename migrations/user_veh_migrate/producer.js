const axios = require('axios');
const { producer } = require('../../config/kafka_config');

const delay = ms => new Promise(res => setTimeout(res, ms));

/**
 * Split array into smaller chunks
 */
function chunkArray(array, size) {
  const chunks = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

(async () => {
  try {
    await producer.connect();
    console.log(`[${new Date().toISOString()}] ✅ Kafka producer connected.`);

    const apiBatchSize = 2000; // number of rows to fetch per API call
    const kafkaChunkSize = 500; // number of messages per Kafka send
    const table = 'user_vehicle_table';
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      console.log(`[${new Date().toISOString()}] ⏳ Fetching data from API: offset=${offset}, limit=${apiBatchSize}`);

      const res = await axios.get(
        `https://bridge.gobumpr.com/api/csv/get_csv.php?limit=${apiBatchSize}&offset=${offset}&table=${table}`,
        { timeout: 120000 } // 2 min timeout for large API calls
      );

      const data = res.data;

      if (!data || !data.length) {
        console.log(`[${new Date().toISOString()}] ✅ No more data to process. Exiting loop.`);
        hasMore = false;
        break;
      }

      // Split into smaller chunks for Kafka
      const chunks = chunkArray(data.map(row => ({
        key: row.user_id ? String(row.user_id) : undefined,
        value: JSON.stringify(row)
      })), kafkaChunkSize);

      for (const chunk of chunks) {
        await producer.send({
          topic: 'user_vechicle_bridge_migration',
          messages: chunk,
        });
        console.log(`[${new Date().toISOString()}] 📦 Sent Kafka chunk of ${chunk.length} messages (offset: ${offset})`);
        await delay(50); // short pause to avoid overloading
      }

      offset += apiBatchSize;
      await delay(100); // small delay between API calls
    }

  } catch (err) {
    console.error(`[${new Date().toISOString()}] ❌ Fatal error in producer:`, err);
  } finally {
    await producer.disconnect();
    console.log(`[${new Date().toISOString()}] 🔌 Kafka producer disconnected.`);
  }
})();
