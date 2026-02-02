const delay = ms => new Promise(r => setTimeout(r, ms));

const jobs = [
  {
    name: "user_register",
    producer: require("./user_register_migrate/producer"),
    consumer: require("./user_register_migrate/consumer")
  },
  {
    name: "user_vehicle",
    producer: require("./user_veh_migrate/producer"),
    consumer: require("./user_veh_migrate/consumer")
  },
  {
    name: "user_booking_tb",
    producer: require("./user_booking_migrate/producer"),
    consumer: require("./user_booking_migrate/consumer")

  },
   {
    name: "b2b_booking_tb",
    producer: require("./b2b_booking_migrate/producer"),
    consumer: require("./b2b_booking_migrate/consumer")
  },
   {
    name: "admin_comments_tbl",
    producer: require("./admin_comments_migrate/producer"),
    consumer: require("./admin_comments_migrate/consumer")

  },
  {
    name: "feedback_track",
    producer: require("./feedback_track/producer"),
    consumer: require("./feedback_tarck/consumer")

  }
];

async function runCycle() {
  while (true) {
    for (const job of jobs) {
      console.log(`🚀 Starting ${job.name}`);

      await Promise.all([
        job.consumer(), // start consumer first
        job.producer()
      ]);

      console.log(`✅ Finished ${job.name}`);
      await delay(3000);
    }

    console.log("🔁 Cycle completed. Restarting...");
    await delay(10000);
  }
}

runCycle().catch(err => {
  console.error("❌ Scheduler failed", err);
  process.exit(1);
});
