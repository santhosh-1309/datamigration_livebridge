const axios = require('axios');
const { producer } = require('../../config/kafka_config');

const delay = ms => new Promise(res => setTimeout(res, ms));

// 🔧 CONFIG
const API_BATCH_SIZE = 10000;     // API fetch size
const KAFKA_CHUNK_SIZE = 500;     // Kafka safe chunk size
const API_TIMEOUT = 30000;
const TOPIC = 'user_vechicle_bridge_migration';
const TABLE = 'user_vehicle_table';

(async () => {
  let offset = 0;

  try {
    // 🔌 CONNECT PRODUCER
    await producer.connect();
    console.log(`[${new Date().toISOString()}] ✅ Kafka producer connected`);

    while (true) {
      console.log(
        `[${new Date().toISOString()}] ⏳ Fetching API data | offset=${offset}, limit=${API_BATCH_SIZE}`
      );

      // 🌐 FETCH DATA FROM API
      const response = await axios.get(
        `https://bridge.gobumpr.com/api/csv/get_csv.php`,
        {
          params: {
            limit: API_BATCH_SIZE,
            offset,
            table: TABLE
          },
          timeout: API_TIMEOUT
        }
      );

      const rows = response.data;

      if (!rows || rows.length === 0) {
        console.log(`[${new Date().toISOString()}] ✅ No more data. Migration completed.`);
        break;
      }

      // 🧾 PREPARE KAFKA MESSAGES
      const messages = rows.map(row => ({
        key: row.user_id ? String(row.user_id) : null,
        value: JSON.stringify(row)
      }));

      // 📦 SEND IN SAFE KAFKA CHUNKS
      for (let i = 0; i < messages.length; i += KAFKA_CHUNK_SIZE) {
        const chunk = messages.slice(i, i + KAFKA_CHUNK_SIZE);

        await producer.send({
          topic: TOPIC,
          messages: chunk
        });
      }

      console.log(
        `[${new Date().toISOString()}] 📤 Sent ${messages.length} messages | offset=${offset}`
      );

      offset += API_BATCH_SIZE;

      // ⏸ SMALL DELAY TO AVOID BROKER OVERLOAD
      await delay(100);
    }

  } catch (error) {
    console.error(
      `[${new Date().toISOString()}] ❌ Producer failed`,
      error
    );
  } finally {
    // 🔌 DISCONNECT SAFELY
    try {
      await producer.disconnect();
      console.log(`[${new Date().toISOString()}] 🔌 Kafka producer disconnected`);
    } catch (e) {
      console.error('❌ Error during producer disconnect', e);
    }
  }
})();
