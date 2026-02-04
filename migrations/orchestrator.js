const { spawn, execSync } = require("child_process");
const path = require("path");
const fs = require("fs");

const config = require("./orchestrator.config.json");
const BASE_DIR = __dirname;
const CYCLE_DELAY = config.cycleDelayMs || 60000; // delay between cycles
const MODE = config.mode || "once"; // "once" or "cycle"

// Sleep utility
function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

// Run a Node.js process with low CPU impact
function runProcess(script, label, cwd) {
  return new Promise((resolve, reject) => {
    const fullPath = path.join(BASE_DIR, cwd || "", script);

    if (!fs.existsSync(fullPath)) {
      return reject(new Error(`File not found: ${fullPath}`));
    }

    console.log(`▶ ${label}: ${fullPath}`);

    const proc = spawn("node", [fullPath], {
      stdio: "inherit",
      shell: true, // required on Windows
    });

    proc.on("exit", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${label} failed (${fullPath})`));
    });
  });
}


// Get total Kafka lag for a consumer group
function getKafkaLag(group) {
  try {
    const output = execSync(
      `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group ${group}`,
      { stdio: "pipe" }
    ).toString();

    const lag = output
      .split("\n")
      .slice(1)
      .map((line) => line.trim().split(/\s+/)[6])
      .filter(Boolean)
      .reduce((acc, v) => acc + Number(v), 0);

    return lag;
  } catch {
    return null; // group not ready yet
  }
}

// Wait until Kafka lag = 0 with live progress logging
async function waitForKafkaLag(group) {
  console.log(`⏳ Waiting for Kafka lag = 0 for group: ${group}`);
  const startTime = Date.now();

  while (true) {
    const lag = getKafkaLag(group);
    const elapsed = Math.floor((Date.now() - startTime) / 1000);

    if (lag !== null) {
      console.log(`📉 Lag: ${lag} | ⏱ Elapsed: ${elapsed}s`);
      if (lag === 0) break;
    } else {
      console.log(`⚠ Kafka group not ready yet | ⏱ Elapsed: ${elapsed}s`);
    }

    await sleep(20000); // check every 20s
  }

  console.log(`✅ Kafka drained for group: ${group} | Total time: ${Math.floor((Date.now() - startTime)/1000)}s`);
}

// Run a single cycle: all tables one by one
async function runSingleCycle(cycleNo) {
  console.log(`\n🔁 STARTING CYCLE #${cycleNo}`);

  for (const step of config.steps) {
    console.log(`\n📦 TABLE: ${step.name}`);
    const groupName = `migration_${step.name}_group`;
    const tableStartTime = Date.now();

    // 1️⃣ Start consumer
    console.log(`▶ Consumer: ${path.join(step.path, step.consumer)}`);
    const consumerStartTime = Date.now();
    await runProcess(step.consumer, `Consumer ${step.name}`, step.path);
    const consumerTime = Math.floor((Date.now() - consumerStartTime) / 1000);

    // 2️⃣ Start producer (blocking)
    console.log(`▶ Producer: ${path.join(step.path, step.producer)}`);
    const producerStartTime = Date.now();
    await runProcess(step.producer, `Producer ${step.name}`, step.path);
    const producerTime = Math.floor((Date.now() - producerStartTime) / 1000);

    // 3️⃣ Wait for Kafka lag = 0 with progress
    console.log(`⏳ Waiting for Kafka lag = 0 for group: ${groupName}`);
    const kafkaStartTime = Date.now();
    await waitForKafkaLag(groupName);
    const kafkaTime = Math.floor((Date.now() - kafkaStartTime) / 1000);

    // 4️⃣ Stop consumer using PM2
    try {
      execSync(`pm2 stop consumer_${step.name}`);
      execSync(`pm2 delete consumer_${step.name}`);
      console.log(`✅ Consumer stopped for ${step.name}`);
    } catch {
      console.log("⚠ Consumer not running or already stopped");
    }

    const tableElapsed = Math.floor((Date.now() - tableStartTime) / 1000);
    console.log(
      `🎯 TABLE DONE: ${step.name} | Total time: ${tableElapsed}s (Consumer: ${consumerTime}s, Producer: ${producerTime}s, Kafka drain: ${kafkaTime}s)`
    );
  }

  console.log(`🎯 CYCLE #${cycleNo} COMPLETED`);
}


// Start orchestrator
async function startOrchestrator() {
  let cycle = 1;

  while (true) {
    try {
      await runSingleCycle(cycle);
    } catch (err) {
      console.error("💥 Error in orchestrator:", err.message);
    }

    if (MODE !== "cycle") {
      console.log("🛑 Single-run mode completed. Exiting.");
      process.exit(0);
    }

    console.log(`⏸ Waiting ${CYCLE_DELAY / 1000}s before next cycle...`);
    await sleep(CYCLE_DELAY);
    cycle++;
  }
}

startOrchestrator();
 // start pm2 start orchestrator.js --name migration_orchestrator

 // stop pm2 stop migration_orchestrator
 
 // restart pm2 restart migration_orchestrator

 // pm2 logs migration_orchestrator
