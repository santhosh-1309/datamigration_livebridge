const crypto = require("crypto");

const algorithm = "aes-256-cbc";
const key = Buffer.from(
  "949bd1da5962dcf478f4fd7b625143d7949bd1da5962dcf478f4fd7b625143d7",
  "hex"
);
const iv = Buffer.from("00000000000000000000000000000000", "hex");

exports.encrypt = (text) => {
  if (!text) return null;

  const cipher = crypto.createCipheriv(algorithm, key, iv);
  const encrypted = Buffer.concat([
    cipher.update(text, "utf8"),
    cipher.final()
  ]);

  // Return HEX (to be stored using UNHEX in MySQL)
  return encrypted.toString("hex");
};

exports.decrypt = (encryptedText) => {
  if (!encryptedText) return null;

  try {
    const encryptedBuffer = Buffer.isBuffer(encryptedText)
      ? encryptedText
      : Buffer.from(encryptedText, "hex");

    const decipher = crypto.createDecipheriv(algorithm, key, iv);
    const decrypted = Buffer.concat([
      decipher.update(encryptedBuffer),
      decipher.final()
    ]);

    return decrypted.toString("utf8");
  } catch (error) {
    console.error("❌ Decryption error:", error.message);
    return null;
  }
};
