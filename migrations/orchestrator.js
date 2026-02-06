const { spawn, execSync } = require("child_process");
const path = require("path");
const fs = require("fs");

const config = require("./orchestrator.config.json");

const BASE_DIR = __dirname;
const CYCLE_DELAY = config.cycleDelayMs || 60000;
const MODE = config.mode || "once";

const PRODUCER_RETRY = 1;
const KAFKA_TIMEOUT = 6 * 60 * 60; // 6 hours

// --------------------
// Helpers
// --------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const resolvePath = (p) => path.resolve(BASE_DIR, p);

// --------------------
// Run producer (blocking, retry)
// --------------------
async function runProducer(script, label, cwd = "") {
  const fullPath = resolvePath(path.join(cwd, script));

  for (let attempt = 1; attempt <= PRODUCER_RETRY + 1; attempt++) {
    try {
      console.log(`▶ ${label} (attempt ${attempt})`);

      await new Promise((resolve, reject) => {
        const p = spawn("node", [fullPath], {
          stdio: "inherit",
          shell: true,
        });

        p.on("exit", (code) =>
          code === 0 ? resolve() : reject(new Error(`Exit ${code}`))
        );
      });

      return true;
    } catch (e) {
      console.error(`⚠ Producer failed: ${e.message}`);
      if (attempt > PRODUCER_RETRY) return false;
      await sleep(5000);
    }
  }
}

// --------------------
// Consumer (PM2 safe start)
// --------------------
function startConsumer(script, name, cwd = "") {
  const fullPath = resolvePath(path.join(cwd, script));

  try {
    execSync(`pm2 describe ${name}`, { stdio: "ignore" });
    console.log(`ℹ Consumer already running: ${name}`);
  } catch {
    execSync(`pm2 start ${fullPath} --name ${name} --interpreter node`, {
      stdio: "inherit",
    });
  }
}

function stopConsumer(name) {
  try {
    execSync(`pm2 stop ${name}`);
    execSync(`pm2 delete ${name}`);
  } catch {}
}

// --------------------
// Kafka helpers
// --------------------
function getKafkaStatus(group) {
  try {
    const out = execSync(
      `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group ${group}`,
      { stdio: "pipe" }
    ).toString();

    const lines = out.split("\n").slice(1).filter(Boolean);

    let lag = 0,
      logEnd = 0;

    for (const l of lines) {
      const c = l.trim().split(/\s+/);
      logEnd += c[5] === "-" ? 0 : Number(c[5]);
      lag += c[6] === "-" ? 0 : Number(c[6]);
    }

    return { lag, logEnd };
  } catch {
    return null;
  }
}

// --------------------
// Wait Kafka drain (timeout safe)
// --------------------
async function waitForKafka(group) {
  const start = Date.now();

  while (true) {
    const s = getKafkaStatus(group);
    const elapsed = (Date.now() - start) / 1000;

    if (s) {
      console.log(`📊 logEnd=${s.logEnd}, lag=${s.lag}`);
      if (s.logEnd === 0 || s.lag === 0) return true;
    } else {
      console.log("⚠ Kafka group not ready");
    }

    if (elapsed > KAFKA_TIMEOUT) {
      console.error("⏱ Kafka timeout — skipping table");
      return false;
    }

    await sleep(60000);
  }
}

// --------------------
// Run cycle
// --------------------
async function runSingleCycle(cycle) {
  console.log(`\n🔁 STARTING CYCLE #${cycle}`);

  for (const step of config.steps) {
    console.log(`\n📦 TABLE: ${step.name}`);
    const group = `migration_${step.name}_group`;
    const consumerName = `consumer_${step.name}`;
    const start = Date.now();

    try {
      startConsumer(step.consumer, consumerName, step.path);

      const producerOk = await runProducer(
        step.producer,
        `Producer ${step.name}`,
        step.path
      );

      if (producerOk) {
        await waitForKafka(group);
      } else {
        console.error(`⚠ Producer skipped for ${step.name}`);
      }
    } catch (e) {
      console.error(`💥 Table error: ${e.message}`);
    } finally {
      stopConsumer(consumerName);
      console.log(
        `🎯 TABLE DONE: ${step.name} | ${Math.floor(
          (Date.now() - start) / 1000
        )}s`
      );
    }
  }
}

// --------------------
// Orchestrator
// --------------------
async function start() {
  let cycle = 1;

  while (true) {
    await runSingleCycle(cycle);

    if (MODE !== "cycle") {
      console.log("🛑 Migration finished");
      process.exit(0);
    }

    await sleep(CYCLE_DELAY);
    cycle++;
  }
}

start();
