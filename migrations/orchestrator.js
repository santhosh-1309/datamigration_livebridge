const { spawn, execSync } = require("child_process");
const path = require("path");
const fs = require("fs");

const config = require("./orchestrator.config.json");

const BASE_DIR = __dirname;
const CYCLE_DELAY = config.cycleDelayMs || 60000;
const MODE = config.mode || "once";

// --------------------
// Helpers
// --------------------
function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

function resolvePath(p) {
  return path.resolve(BASE_DIR, p);
}

// --------------------
// Run node script (blocking)
// --------------------
function runProcess(script, label, cwd = "") {
  return new Promise((resolve, reject) => {
    const fullPath = resolvePath(path.join(cwd, script));

    if (!fs.existsSync(fullPath)) {
      return reject(new Error(`File not found: ${fullPath}`));
    }

    console.log(`▶ ${label}: ${fullPath}`);

    const proc = spawn("node", [fullPath], {
      stdio: "inherit",
      shell: true,
    });

    proc.on("exit", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${label} failed (${code})`));
    });
  });
}

// --------------------
// Start consumer in background (PM2)
// --------------------
function startBackgroundProcess(script, name, cwd = "") {
  const fullPath = resolvePath(path.join(cwd, script));

  if (!fs.existsSync(fullPath)) {
    throw new Error(`File not found: ${fullPath}`);
  }

  console.log(`▶ Starting background ${name}: ${fullPath}`);

  execSync(
    `pm2 start ${fullPath} --name ${name} --interpreter node`,
    { stdio: "inherit" }
  );
}

// --------------------
// Kafka status helper (FIXED)
// --------------------
function getKafkaStatus(group) {
  try {
    const output = execSync(
      `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group ${group}`,
      { stdio: "pipe" }
    ).toString();

    const lines = output.split("\n").slice(1).filter(Boolean);

    let totalLag = 0;
    let totalLogEnd = 0;

    for (const line of lines) {
      const cols = line.trim().split(/\s+/);

      const logEnd = cols[5] === "-" ? 0 : Number(cols[5]);
      const lag = cols[6] === "-" ? 0 : Number(cols[6]);

      totalLogEnd += logEnd;
      totalLag += lag;
    }

    return { totalLag, totalLogEnd };
  } catch {
    return null;
  }
}

// --------------------
// Wait for Kafka drain (SAFE)
// --------------------
async function waitForKafkaLag(group) {
  console.log(`⏳ Waiting for Kafka drain | group: ${group}`);
  const start = Date.now();

  while (true) {
    const status = getKafkaStatus(group);
    const elapsed = Math.floor((Date.now() - start) / 1000);

    if (!status) {
      console.log(`⚠ Group not ready | ⏱ ${elapsed}s`);
    } else {
      const { totalLag, totalLogEnd } = status;

      console.log(
        `📊 logEnd=${totalLogEnd}, lag=${totalLag} | ⏱ ${elapsed}s`
      );

      // ✅ Either no data OR fully consumed
      if (totalLogEnd === 0 || totalLag === 0) break;
    }

    // Optional safety timeout (6 hours)
    if (elapsed > 6 * 60 * 60) {
      throw new Error(`Kafka drain timeout for group ${group}`);
    }

    await sleep(60000);
  }

  console.log(
    `✅ Kafka drained | ${Math.floor((Date.now() - start) / 1000)}s`
  );
}

// --------------------
// Run single cycle
// --------------------
async function runSingleCycle(cycleNo) {
  console.log(`\n🔁 STARTING CYCLE #${cycleNo}`);

  for (const step of config.steps) {
    console.log(`\n📦 TABLE: ${step.name}`);
    const groupName = `migration_${step.name}_group`;
    const tableStart = Date.now();

    // 1️⃣ Start consumer
    startBackgroundProcess(
      step.consumer,
      `consumer_${step.name}`,
      step.path
    );

    // 2️⃣ Run producer
    await runProcess(
      step.producer,
      `Producer ${step.name}`,
      step.path
    );

    // 3️⃣ Wait for Kafka drain
    await waitForKafkaLag(groupName);

    // 4️⃣ Stop consumer
    try {
      execSync(`pm2 stop consumer_${step.name}`);
      execSync(`pm2 delete consumer_${step.name}`);
      console.log(`✅ Consumer stopped: ${step.name}`);
    } catch {
      console.log(`⚠ Consumer already stopped: ${step.name}`);
    }

    console.log(
      `🎯 TABLE DONE: ${step.name} | ${Math.floor(
        (Date.now() - tableStart) / 1000
      )}s`
    );
  }

  console.log(`🎯 CYCLE #${cycleNo} COMPLETED`);
}

// --------------------
// Orchestrator
// --------------------
async function startOrchestrator() {
  let cycle = 1;

  while (true) {
    try {
      await runSingleCycle(cycle);
    } catch (err) {
      console.error("💥 Orchestrator error:", err.message);
    }

    if (MODE !== "cycle") {
      console.log("🛑 Single-run mode completed. Exiting.");
      process.exit(0);
    }

    console.log(`⏸ Waiting ${CYCLE_DELAY / 1000}s before next cycle`);
    await sleep(CYCLE_DELAY);
    cycle++;
  }
}

startOrchestrator();

/*
PM2 Commands:

pm2 start orchestrator.js --name kafka-migration-orchestrator
pm2 stop kafka-migration-orchestrator
pm2 restart kafka-migration-orchestrator
pm2 logs kafka-migration-orchestrator
*/
