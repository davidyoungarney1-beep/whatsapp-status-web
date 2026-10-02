import "dotenv/config";
import express from "express";
import cors from "cors";
import pino from "pino";
import fs from "fs";
import path from "path";
import { MongoClient } from "mongodb";
import { useMongoAuthState } from "./mongo-auth.js";

import {
  makeWASocket,
  Browsers,
  DisconnectReason,
  downloadMediaMessage,
  sendGroupStatusText,
  sendGroupStatusImage,
  sendGroupStatusVideo,
  sendGroupStatusAudio,
  sendGroupStatusSticker
} from "@kaels/casileys";

const app = express();

app.use(cors());
app.use(express.json({ limit: "20mb" }));

const PORT = process.env.PORT || 3000;

const mongoClient = new MongoClient(process.env.MONGODB_URI);
let mongoDb = null;

async function initializeMongoDB() {
  if (!process.env.MONGODB_URI) {
    throw new Error("MONGODB_URI is missing from .env");
  }

  await mongoClient.connect();

  mongoDb = mongoClient.db(
    process.env.MONGODB_DB || "whatsapp_status_web"
  );

  console.log("✅ MongoDB connected successfully");
  console.log(`📦 Database: ${mongoDb.databaseName}`);
}

const ROOT = process.cwd();
const DATA_DIR = path.join(ROOT, "data");
const SESSIONS_DIR = path.join(ROOT, "sessions");
const ACCOUNTS_FILE = path.join(DATA_DIR, "accounts.json");

fs.mkdirSync(DATA_DIR, { recursive: true });
fs.mkdirSync(SESSIONS_DIR, { recursive: true });

if (!fs.existsSync(ACCOUNTS_FILE)) {
  fs.writeFileSync(ACCOUNTS_FILE, "[]");
}

const logger = pino({ level: "silent" });

const connections = new Map();
const pairingWaiters = new Map();
const reconnecting = new Set();

let accountsCache = [];

function readAccounts() {
  return accountsCache;
}

function saveAccounts(accounts) {
  accountsCache = accounts;

  if (!mongoDb) {
    console.error("❌ MongoDB is not initialized");
    return;
  }

  mongoDb.collection("accounts").updateOne(
    { _id: "accounts" },
    {
      $set: {
        accounts,
        updatedAt: new Date()
      }
    },
    { upsert: true }
  ).catch(error => {
    console.error(
      "❌ Failed saving accounts to MongoDB:",
      error?.message || error
    );
  });

  // Keep the local file as a temporary fallback/migration copy.
  fs.writeFileSync(
    ACCOUNTS_FILE,
    JSON.stringify(accounts, null, 2)
  );
}

async function initializeAccounts() {
  if (!mongoDb) {
    throw new Error("MongoDB is not initialized");
  }

  const collection = mongoDb.collection("accounts");

  const stored = await collection.findOne({
    _id: "accounts"
  });

  if (stored && Array.isArray(stored.accounts)) {
    accountsCache = stored.accounts;

    fs.writeFileSync(
      ACCOUNTS_FILE,
      JSON.stringify(accountsCache, null, 2)
    );

    console.log(
      `📦 Loaded ${accountsCache.length} accounts from MongoDB`
    );

    return;
  }

  // First run: migrate any existing local accounts.json.
  let localAccounts = [];

  try {
    localAccounts = JSON.parse(
      fs.readFileSync(ACCOUNTS_FILE, "utf8")
    );

    if (!Array.isArray(localAccounts)) {
      localAccounts = [];
    }
  } catch {
    localAccounts = [];
  }

  accountsCache = localAccounts;

  if (accountsCache.length > 0) {
    await collection.updateOne(
      { _id: "accounts" },
      {
        $set: {
          accounts: accountsCache,
          updatedAt: new Date()
        }
      },
      { upsert: true }
    );

    console.log(
      `📤 Migrated ${accountsCache.length} local accounts to MongoDB`
    );
  } else {
    await collection.updateOne(
      { _id: "accounts" },
      {
        $set: {
          accounts: [],
          updatedAt: new Date()
        }
      },
      { upsert: true }
    );

    console.log("📦 No existing accounts to migrate");
  }
}

function normalizePhone(phone) {
  let value = String(phone || "").replace(/\D/g, "");

  if (value.startsWith("0")) {
    value = "234" + value.slice(1);
  }

  return value;
}

function normalizeJid(jid) {
  if (!jid) return "";

  return String(jid)
    .replace(/:[0-9]+@/, "@")
    .trim();
}

function sameJid(a, b) {
  return normalizeJid(a) === normalizeJid(b);
}

function getOwnJids(sock) {
  const result = new Set();

  if (sock?.user?.id) {
    result.add(normalizeJid(sock.user.id));
  }

  if (sock?.user?.lid) {
    result.add(normalizeJid(sock.user.lid));
  }

  return [...result].filter(Boolean);
}

function unwrapMessage(message) {
  let msg = message;

  for (let i = 0; i < 8; i++) {
    if (!msg) break;

    if (msg.ephemeralMessage?.message) {
      msg = msg.ephemeralMessage.message;
      continue;
    }

    if (msg.viewOnceMessage?.message) {
      msg = msg.viewOnceMessage.message;
      continue;
    }

    if (msg.viewOnceMessageV2?.message) {
      msg = msg.viewOnceMessageV2.message;
      continue;
    }

    if (msg.viewOnceMessageV2Extension?.message) {
      msg = msg.viewOnceMessageV2Extension.message;
      continue;
    }

    if (msg.documentWithCaptionMessage?.message) {
      msg = msg.documentWithCaptionMessage.message;
      continue;
    }

    break;
  }

  return msg;
}

function getText(message) {
  const msg = unwrapMessage(message?.message);

  if (!msg) return null;

  return (
    msg.conversation ||
    msg.extendedTextMessage?.text ||
    msg.imageMessage?.caption ||
    msg.videoMessage?.caption ||
    null
  );
}

function getMessageType(message) {
  const msg = unwrapMessage(message?.message);

  if (!msg) return null;

  if (msg.imageMessage) return "image";
  if (msg.videoMessage) return "video";
  if (msg.audioMessage) return "audio";
  if (msg.stickerMessage) return "sticker";
  if (msg.conversation || msg.extendedTextMessage) {
    return "text";
  }

  return null;
}

function isOwnStatus(message, sock) {
  const remoteJid =
    normalizeJid(message?.key?.remoteJid);

  const remoteJidAlt =
    normalizeJid(
      message?.key?.remoteJidAlt
    );

  const isStatus =
    remoteJid === "status@broadcast" ||
    remoteJid?.endsWith("@broadcast") ||
    remoteJidAlt === "status@broadcast" ||
    remoteJidAlt?.endsWith("@broadcast");

  if (!isStatus) {
    return false;
  }

  const ownJids = getOwnJids(sock);

  const possibleSenders = [
    message?.key?.participant,
    message?.participant,
    message?.key?.senderPn,
    message?.key?.senderLid
  ]
    .filter(Boolean)
    .map(normalizeJid);

  return possibleSenders.some(sender =>
    ownJids.some(own => sameJid(sender, own))
  );
}

function accountPublic(account) {
  const conn = connections.get(account.id);

  return {
    id: account.id,
    phone: account.phone,
    createdAt: account.createdAt,
    forwardingEnabled:
      account.forwardingEnabled !== false,
    selectedGroups:
      account.selectedGroups || [],
    connected:
      !!conn?.connected,
    pairingCode:
      conn?.pairingCode || null
  };
}

function updateAccount(id, changes) {
  const accounts = readAccounts();

  const index = accounts.findIndex(
    a => a.id === id
  );

  if (index === -1) return null;

  accounts[index] = {
    ...accounts[index],
    ...changes
  };

  saveAccounts(accounts);

  return accounts[index];
}

async function forwardStatus(
  account,
  sock,
  message
) {
  if (account.forwardingEnabled === false) {
    return;
  }

  const selectedGroups =
    account.selectedGroups || [];

  if (!selectedGroups.length) {
    console.log(
      `ℹ️ No selected groups for ${account.phone}`
    );
    return;
  }

  if (!isOwnStatus(message, sock)) {
    return;
  }

  const type = getMessageType(message);

  if (!type) {
    return;
  }

  console.log(
    `📲 Own Status detected from ${account.phone} (${type})`
  );

  console.log(
    `📤 Selected groups (${selectedGroups.length}):`,
    selectedGroups
  );

  const text = getText(message);

  for (const groupId of selectedGroups) {
    try {
      if (type === "text") {
        await sendGroupStatusText(
          sock,
          groupId,
          text || ""
        );
      }

      else if (type === "image") {
        const buffer =
          await downloadMediaMessage(
            message,
            "buffer",
            {},
            {
              logger,
              reuploadRequest:
                sock.updateMediaMessage
            }
          );

        const msg =
          unwrapMessage(message.message);

        await sendGroupStatusImage(
          sock,
          groupId,
          buffer,
          msg?.imageMessage?.caption || ""
        );
      }

      else if (type === "video") {
        const buffer =
          await downloadMediaMessage(
            message,
            "buffer",
            {},
            {
              logger,
              reuploadRequest:
                sock.updateMediaMessage
            }
          );

        const msg =
          unwrapMessage(message.message);

        await sendGroupStatusVideo(
          sock,
          groupId,
          buffer,
          msg?.videoMessage?.caption || ""
        );
      }

      else if (type === "audio") {
        const buffer =
          await downloadMediaMessage(
            message,
            "buffer",
            {},
            {
              logger,
              reuploadRequest:
                sock.updateMediaMessage
            }
          );

        const msg =
          unwrapMessage(message.message);

        const audio =
          msg?.audioMessage;

        await sendGroupStatusAudio(
          sock,
          groupId,
          buffer,
          {
            mimetype:
              audio?.mimetype ||
              "audio/ogg; codecs=opus",
            ptt: !!audio?.ptt
          }
        );
      }

      else if (type === "sticker") {
        const buffer =
          await downloadMediaMessage(
            message,
            "buffer",
            {},
            {
              logger,
              reuploadRequest:
                sock.updateMediaMessage
            }
          );

        await sendGroupStatusSticker(
          sock,
          groupId,
          buffer
        );
      }

      console.log(
        `✅ Status forwarded to ${groupId}`
      );

    } catch (error) {
      console.error(
        `❌ Failed forwarding to ${groupId}:`,
        error?.message || error
      );
    }
  }
}
async function connectWhatsApp(
  account,
  phoneForPairing = null
) {
  if (connections.has(account.id)) {
    const existing =
      connections.get(account.id);

    if (existing.sock) {
      return existing.sock;
    }
  }

  if (!mongoDb) {
    throw new Error("MongoDB is not initialized");
  }

  const {
    state,
    saveCreds
  } = await useMongoAuthState(
    mongoDb,
    account.id
  );

  const entry = {
    sock: null,
    connected: false,
    pairingCode: null,
    pairingRequested: false
  };

  connections.set(
    account.id,
    entry
  );

  const sock =
    makeWASocket({
      logger,
      auth: state,
      browser:
        Browsers.windows("Chrome"),
      markOnlineOnConnect: false,
      syncFullHistory: false
    });

  entry.sock = sock;

  sock.ev.on(
    "creds.update",
    saveCreds
  );

  sock.ev.on(
    "connection.update",
    async update => {
      const {
        connection,
        lastDisconnect
      } = update;

      console.log(
        `🔄 ${account.id} connection:`,
        connection || "update"
      );

      /*
       * Wait for the WhatsApp socket to begin
       * connecting before requesting pairing code.
       */
      if (
        connection === "connecting" &&
        !state.creds.registered &&
        phoneForPairing &&
        !entry.pairingRequested
      ) {
        entry.pairingRequested = true;

        try {
          console.log(
            `🔗 Preparing pairing code for ${account.phone}...`
          );

          await new Promise(
            resolve =>
              setTimeout(resolve, 1500)
          );

          const code =
            await sock.requestPairingCode(
              phoneForPairing
            );

          entry.pairingCode =
            code;

          console.log(
            `🔐 Pairing code for ${account.phone}: ${code}`
          );

          const waiter =
            pairingWaiters.get(
              account.id
            );

          if (waiter) {
            waiter.resolve(code);

            pairingWaiters.delete(
              account.id
            );
          }

        } catch (error) {
          console.error(
            "❌ Pairing code error:",
            error?.message || error
          );

          entry.pairingRequested =
            false;

          const waiter =
            pairingWaiters.get(
              account.id
            );

          if (waiter) {
            waiter.reject(error);

            pairingWaiters.delete(
              account.id
            );
          }
        }
      }

      if (connection === "open") {
        entry.connected = true;

        updateAccount(
          account.id,
          {
            connected: true
          }
        );

        console.log(
          `✅ WhatsApp Connected: ${account.phone}`
        );
      }

      if (connection === "close") {
        entry.connected = false;

        updateAccount(
          account.id,
          {
            connected: false
          }
        );

        const statusCode =
          lastDisconnect
            ?.error
            ?.output
            ?.statusCode;

        const shouldReconnect =
          statusCode !==
          DisconnectReason.loggedOut;

        console.log(
          `⚠️ WhatsApp disconnected: ${account.id}`
        );

        console.log(
          `❗ Disconnect status code: ${statusCode ?? "unknown"}`
        );

        console.log(
          "❗ Disconnect error:",
          lastDisconnect?.error?.message ||
          lastDisconnect?.error ||
          "unknown"
        );

        console.log(
          `🔄 Reconnect: ${shouldReconnect}`
        );

        if (shouldReconnect) {
          setTimeout(
            async () => {
              try {
                connections.delete(
                  account.id
                );

                await connectWhatsApp(
                  account,
                  null
                );

              } catch (error) {
                console.error(
                  `❌ Reconnect failed for ${account.phone}:`,
                  error?.message ||
                    error
                );
              }
            },
            3000
          );

        } else {
          connections.delete(
            account.id
          );
        }
      }
    }
  );

  /*
   * Detect WhatsApp Status updates.
   */
  sock.ev.on(
    "messages.upsert",
    async ({ messages }) => {
      for (const message of messages) {
        try {
          if (!message?.message) {
            continue;
          }

          const remoteJid =
            normalizeJid(
              message.key?.remoteJid
            );

          const remoteJidAlt =
            normalizeJid(
              message.key?.remoteJidAlt
            );

          const isStatusEvent =
            remoteJid === "status@broadcast" ||
            remoteJid?.endsWith("@broadcast") ||
            remoteJidAlt === "status@broadcast" ||
            remoteJidAlt?.endsWith("@broadcast");

          if (!isStatusEvent) {
            continue;
          }

          console.log(
            `📡 Possible Status event for ${account.phone}`
          );

          console.log(
            `📡 Status event received for ${account.phone}`
          );

          if (
            !isOwnStatus(
              message,
              sock
            )
          ) {
            console.log(
              "ℹ️ Status belongs to another contact"
            );

            continue;
          }

          await forwardStatus(
            account,
            sock,
            message
          );

        } catch (error) {
          console.error(
            "❌ Status handler error:",
            error?.message ||
              error
          );
        }
      }
    }
  );

  return sock;
}


/*
|--------------------------------------------------------------------------
| HOME
|--------------------------------------------------------------------------
*/

app.get(
  "/",
  (req, res) => {
    res.json({
      success: true,
      service:
        "WhatsApp Status Web Backend",
      status: "running"
    });
  }
);


/*
|--------------------------------------------------------------------------
| CONNECT WHATSAPP
|--------------------------------------------------------------------------
*/

app.post(
  "/api/accounts/connect",
  async (req, res) => {
    try {
      const phone =
        normalizePhone(
          req.body?.phone
        );

      if (!phone) {
        return res.status(400).json({
          success: false,
          error:
            "Phone number is required"
        });
      }

      if (phone.length < 10) {
        return res.status(400).json({
          success: false,
          error:
            "Invalid phone number"
        });
      }

      let accounts =
        readAccounts();

      /*
       * Reuse an existing account
       * instead of creating duplicates.
       */
      let account =
        accounts.find(
          a => a.phone === phone
        );

      if (!account) {
        account = {
          id:
            "account_" +
            Date.now(),

          phone,

          createdAt:
            new Date().toISOString(),

          forwardingEnabled: true,

          selectedGroups: [],

          connected: false
        };

        accounts.push(account);

        saveAccounts(accounts);

        console.log(
          `➕ New account created: ${phone}`
        );

      } else {
        console.log(
          `♻️ Existing account found: ${phone}`
        );
      }

      let connection =
        connections.get(
          account.id
        );

      /*
       * Already connected.
       */
      if (
        connection?.connected
      ) {
        return res.json({
          success: true,
          alreadyConnected: true,
          account:
            accountPublic(account)
        });
      }

      /*
       * Pairing already in progress.
       */
      if (
        connection?.pairingRequested
      ) {
        return res.json({
          success: true,
          waiting: true,
          account:
            accountPublic(account),

          message:
            "Pairing code is already being generated."
        });
      }

      /*
       * Create a Promise that will be
       * resolved when the socket generates
       * the pairing code.
       */
      const pairingPromise =
        new Promise(
          (resolve, reject) => {
            pairingWaiters.set(
              account.id,
              {
                resolve,
                reject
              }
            );

            setTimeout(
              () => {
                if (
                  pairingWaiters.has(
                    account.id
                  )
                ) {
                  pairingWaiters.delete(
                    account.id
                  );

                  reject(
                    new Error(
                      "Timed out waiting for WhatsApp pairing connection"
                    )
                  );
                }
              },
              30000
            );
          }
        );

      await connectWhatsApp(
        account,
        phone
      );

      let code;

      try {
        code =
          await pairingPromise;

      } catch (error) {
        return res.status(500).json({
          success: false,
          error:
            error?.message ||
            "Could not generate pairing code"
        });
      }

      connection =
        connections.get(
          account.id
        );

      return res.json({
        success: true,

        account:
          accountPublic(account),

        pairingCode: code,

        message:
          "Enter this code in WhatsApp → Linked Devices → Link a Device → Link with phone number instead."
      });

    } catch (error) {
      console.error(
        "❌ Connect error:",
        error?.message ||
          error
      );

      return res.status(500).json({
        success: false,
        error:
          error?.message ||
          "Failed to connect WhatsApp"
      });
    }
  }
);


/*
|--------------------------------------------------------------------------
| LIST ACCOUNTS
|--------------------------------------------------------------------------
*/

app.get(
  "/api/accounts",
  (req, res) => {
    const accounts =
      readAccounts();

    res.json({
      success: true,

      accounts:
        accounts.map(
          accountPublic
        )
    });
  }
);


/*
|--------------------------------------------------------------------------
| SINGLE ACCOUNT
|--------------------------------------------------------------------------
*/

app.get(
  "/api/accounts/:accountId",
  (req, res) => {
    const accounts =
      readAccounts();

    const account =
      accounts.find(
        a =>
          a.id ===
          req.params.accountId
      );

    if (!account) {
      return res.status(404).json({
        success: false,
        error:
          "Account not found"
      });
    }

    res.json({
      success: true,

      account:
        accountPublic(account)
    });
  }
);


/*
|--------------------------------------------------------------------------
| GET GROUPS
|--------------------------------------------------------------------------
*/

app.get(
  "/api/accounts/:accountId/groups",
  async (req, res) => {
    try {
      const accounts =
        readAccounts();

      const account =
        accounts.find(
          a =>
            a.id ===
            req.params.accountId
        );

      if (!account) {
        return res.status(404).json({
          success: false,
          error:
            "Account not found"
        });
      }

      const connection =
        connections.get(
          account.id
        );

      if (!connection?.sock) {
        return res.status(400).json({
          success: false,
          error:
            "WhatsApp account is not connected"
        });
      }

      const groups =
        await connection.sock
          .groupFetchAllParticipating();

      const list =
        Object.values(
          groups
        ).map(group => ({
          id: group.id,

          name:
            group.subject ||
            group.name ||
            "Unnamed Group",

          participants:
            group.participants
              ?.length || 0,

          selected:
            (account.selectedGroups || [])
              .includes(group.id)
        }));

      list.sort(
        (a, b) =>
          a.name.localeCompare(
            b.name
          )
      );

      res.json({
        success: true,
        groups: list
      });

    } catch (error) {
      console.error(
        "❌ Group fetch error:",
        error?.message ||
          error
      );

      res.status(500).json({
        success: false,
        error:
          error?.message ||
          "Failed to fetch groups"
      });
    }
  }
);


/*
|--------------------------------------------------------------------------
| SAVE SELECTED GROUPS
|--------------------------------------------------------------------------
*/

app.post(
  "/api/accounts/:accountId/groups",
  (req, res) => {
    try {
      const accountId =
        req.params.accountId;

      const groupIds =
        Array.isArray(
          req.body?.groups
        )
          ? req.body.groups
          : [];

      const accounts =
        readAccounts();

      const account =
        accounts.find(
          a =>
            a.id ===
            accountId
        );

      if (!account) {
        return res.status(404).json({
          success: false,
          error:
            "Account not found"
        });
      }

      account.selectedGroups =
        groupIds.filter(
          Boolean
        );

      saveAccounts(
        accounts
      );

      res.json({
        success: true,

        account:
          accountPublic(account)
      });

    } catch (error) {
      res.status(500).json({
        success: false,
        error:
          error?.message ||
          "Failed to save groups"
      });
    }
  }
);


/*
|--------------------------------------------------------------------------
| FORWARDING ON / OFF
|--------------------------------------------------------------------------
*/

app.post(
  "/api/accounts/:accountId/forwarding",
  (req, res) => {
    try {
      const accountId =
        req.params.accountId;

      const enabled =
        req.body?.enabled !== false;

      const account =
        updateAccount(
          accountId,
          {
            forwardingEnabled:
              enabled
          }
        );

      if (!account) {
        return res.status(404).json({
          success: false,
          error:
            "Account not found"
        });
      }

      console.log(
        `🔀 Forwarding ${
          enabled
            ? "ON"
            : "OFF"
        } for ${account.phone}`
      );

      res.json({
        success: true,

        account:
          accountPublic(account)
      });

    } catch (error) {
      res.status(500).json({
        success: false,
        error:
          error?.message ||
          "Failed to update forwarding"
      });
    }
  }
);


/*
|--------------------------------------------------------------------------
| START SERVER
|--------------------------------------------------------------------------
*/

async function startServer() {
  try {
    await initializeMongoDB();
    await initializeAccounts();

    app.listen(
      PORT,
      "0.0.0.0",
      () => {
        console.log("");
        console.log(
          "======================================"
        );
        console.log(
          "🚀 WhatsApp Status Web Backend"
        );
        console.log(
          `🌐 Port: ${PORT}`
        );
        console.log(
          "======================================"
        );
        console.log("");

        const accounts =
          readAccounts();

        console.log(
          `📱 Saved accounts: ${accounts.length}`
        );

        /*
         * Restore saved WhatsApp sessions.
         */
        for (const account of accounts) {
          connectWhatsApp(
            account,
            null
          ).catch(error => {
            console.error(
              `❌ Failed restoring ${account.phone}:`,
              error?.message ||
              error
            );
          });
        }
      }
    );
  } catch (error) {
    console.error(
      "❌ Startup initialization failed:",
      error?.message ||
      error
    );

    process.exit(1);
  }
}

startServer();
