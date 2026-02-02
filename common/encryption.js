const crypto = require("crypto");

const ALGO = process.env.ENCRYPTION_ALGO;
const KEY = Buffer.from(process.env.ENCRYPTION_KEY);
const IV_LENGTH = Number(process.env.ENCRYPTION_IV_LENGTH);

function encrypt(text) {
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv(ALGO, KEY, iv);

  let encrypted = cipher.update(text, "utf8", "hex");
  encrypted += cipher.final("hex");

  const tag = cipher.getAuthTag().toString("hex");

  return `${iv.toString("hex")}:${tag}:${encrypted}`;
}

module.exports = { encrypt };
