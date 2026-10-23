import "dotenv/config";
import express from "express";
import cors from "cors";
import pino from "pino";
import fs from "fs";
import path from "path";
import { MongoClient } from "mongodb";
import { useMongoAuthState } from "./mongo-auth.js";
import { getUrlInfo } from "@kaels/casileys/lib/Utils/link-preview.js";

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

// Serve the current frontend from Render too, so the fixed QR UI is available
// even if the separate workers.dev static deployment has not refreshed.
app.use(express.static(path.join(process.cwd(), "frontend")));

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

  console.log("â MongoDB connected successfully");
  console.log(`ð¦ Database: ${mongoDb.databaseName}`);
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
const directStatusEchoes = new Map();

let accountsCache = [];

function readAccounts() {
  return accountsCache;
}

function saveAccounts(accounts) {
  accountsCache = accounts;

  if (!mongoDb) {
    console.error("â MongoDB is not initialized");
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
      "â Failed saving accounts to MongoDB:",
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
      `ð¦ Loaded ${accountsCache.length} accounts from MongoDB`
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
      `ð¤ Migrated ${accountsCache.length} local accounts to MongoDB`
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

    console.log("ð¦ No existing accounts to migrate");
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

  // WhatsApp can emit our own Status update without a participant/sender JID,
  // especially when it was posted from the linked phone. In that case `fromMe`
  // is the reliable signal; requiring a sender JID caused manual statuses to be
  // incorrectly classified as someone else's and silently ignored.
  if (message?.key?.fromMe === true) {
    return true;
  }

  const ownJids = getOwnJids(sock);
  const possibleSenders = [
    message?.key?.participant,
    message?.participant,
    message?.key?.senderPn,
    message?.key?.senderLid,
    message?.key?.remoteJidAlt
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
      conn?.pairingCode || null,
    qrAvailable:
      !!conn?.qr,
    forwardingStatus:
      conn?.forwardingStatus || {
        state: "idle",
        current: 0,
        total: 0,
        message: "Waiting"
      }
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

async function getUnlockedGroups(sock, groupIds) {
  let metadata = {};
  try { metadata = await sock.groupFetchAllParticipating(); }
  catch (error) { console.warn("Could not pre-check group restrictions; runtime checks will be used:", error?.message || error); }
  const unlocked = [], skipped = [];
  for (const id of groupIds) {
    if (metadata?.[id]?.announce === true) skipped.push({ id, reason: "admin-only/locked group" });
    else unlocked.push(id);
  }
  return { unlocked, skipped };
}
function isLockedGroupError(error) {
  const message = String(error?.message || error || "").toLowerCase();
  return /not.?authori[sz]ed|forbidden|admin.?only|group.?locked|group.?restricted|not.?allowed|permission denied|not a participant|not.?participant|status.?not.?allowed/.test(message);
}

async function sendTextStatusWithPreview(sock, jid, text, preparedUrlInfo) {
  const urlInfo = preparedUrlInfo ?? await getUrlInfo(text, {
    thumbnailWidth: 192,
    fetchOpts: { timeout: 3000 },
    logger,
    uploadImage: sock.waUploadToServer
  });

  const extendedTextMessage = { text };

  if (urlInfo) {
    extendedTextMessage.matchedText =
      urlInfo["matched-text"];
    extendedTextMessage.jpegThumbnail =
      urlInfo.jpegThumbnail;
    extendedTextMessage.description =
      urlInfo.description;
    extendedTextMessage.title =
      urlInfo.title;
    extendedTextMessage.previewType =
      urlInfo.previewType ?? 0;
    extendedTextMessage.linkPreviewMetadata =
      urlInfo.linkPreviewMetadata;

    const img = urlInfo.highQualityThumbnail;

    if (img) {
      extendedTextMessage.thumbnailDirectPath =
        img.directPath;
      extendedTextMessage.mediaKey =
        img.mediaKey;
      extendedTextMessage.mediaKeyTimestamp =
        img.mediaKeyTimestamp;
      extendedTextMessage.thumbnailWidth =
        img.width;
      extendedTextMessage.thumbnailHeight =
        img.height;
      extendedTextMessage.thumbnailSha256 =
        img.fileSha256;
      extendedTextMessage.thumbnailEncSha256 =
        img.fileEncSha256;
    }
  }

  await sock.relayMessage(
    jid,
    {
      groupStatusMessageV2: {
        message: {
          extendedTextMessage
        }
      }
    },
    {}
  );
}


function rememberDirectStatusEcho(accountId, sentMessage, text) {
  const keyId = sentMessage?.key?.id;
  if (!keyId) return;
  directStatusEchoes.set(accountId, { keyId, text, expiresAt: Date.now() + 2 * 60 * 1000 });
}

function consumeDirectStatusEcho(accountId, message) {
  const saved = directStatusEchoes.get(accountId);
  if (!saved) return false;
  if (Date.now() > saved.expiresAt) { directStatusEchoes.delete(accountId); return false; }
  if (message?.key?.id !== saved.keyId) return false;
  directStatusEchoes.delete(accountId);
  return true;
}


async function forwardStatus(
  account,
  sock,
  message
) {
  const latestAccounts = readAccounts();
  const latestAccount =
    latestAccounts.find(
      a => a.id === account.id
    ) || account;

  account = latestAccount;

  if (account.forwardingEnabled === false) {
    return;
  }

  const requestedGroups = account.selectedGroups || [];
  const groupCheck = await getUnlockedGroups(sock, requestedGroups);
  const selectedGroups = groupCheck.unlocked;
  let skippedLocked = groupCheck.skipped.length;

  if (!requestedGroups.length) {
    console.log(
      `â¹ï¸ No selected groups for ${account.phone}`
    );
    return;
  }

  if (!selectedGroups.length) {
    const lockedEntry = connections.get(account.id);
    if (lockedEntry) lockedEntry.forwardingStatus = {
      state: "forwarded", current: 0, total: 0, attempting: 0,
      skipped: skippedLocked, startedAt: Date.now(), etaSeconds: 0,
      message: `Forwarded 0/0 unlocked groups; skipped ${skippedLocked} locked`
    };
    console.log(`All selected groups are locked/admin-only for ${account.phone}; skipped ${skippedLocked}.`);
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
    `ð² Own Status detected from ${account.phone} (${type})`
  );

  console.log(
    `ð¤ Selected groups (${selectedGroups.length}):`,
    selectedGroups
  );

  const entry = connections.get(account.id);

  if (entry?.forwardingStatus?.state === "forwarding") {
    console.log(
      `â³ Forwarding already in progress for ${account.phone}`
    );
    return;
  }

  if (entry) {
    entry.stopForwardingRequested = false;
    entry.forwardingStatus = {
      state: "forwarding",
      current: 0,
      total: selectedGroups.length,
      attempting: 0,
      skipped: skippedLocked,
      startedAt: Date.now(),
      etaSeconds: selectedGroups.length * 30,
      message: `Forwarded 0/${selectedGroups.length}`
    };
  }

  const text = getText(message);

  // Prepare expensive payload work once per status, not once per destination.
  // Link previews and media downloads were previously repeated for every group.
  let preparedUrlInfo = null;
  let mediaBuffer = null;
  if (type === "text") {
    try {
      preparedUrlInfo = await getUrlInfo(text || "", {
        thumbnailWidth: 192, fetchOpts: { timeout: 3000 }, logger,
        uploadImage: sock.waUploadToServer
      });
    } catch (error) {
      console.warn("Link preview preparation failed; forwarding text without cached preview:", error?.message || error);
    }
  } else if (["image", "video", "audio", "sticker"].includes(type)) {
    mediaBuffer = await downloadMediaMessage(message, "buffer", {}, {
      logger, reuploadRequest: sock.updateMediaMessage
    });
  }

  let successCount = 0;
  let failureCount = 0;

  let wasStopped = false;
  for (let i = 0; i < selectedGroups.length; i++) {
    if (entry?.stopForwardingRequested) { wasStopped = true; break; }
    const groupId = selectedGroups[i];

    console.log(`ð¯ ATTEMPTING GROUP: ${groupId}`);

    if (entry) {
      entry.forwardingStatus = {
        state: "forwarding",
        current: successCount,
        total: selectedGroups.length,
        attempting: i + 1,
        startedAt: entry.forwardingStatus.startedAt,
        etaSeconds: selectedGroups.length * 30,
        message: `Forwarded ${successCount}/${selectedGroups.length}; processing ${i + 1}/${selectedGroups.length}`
      };
    }

    try {
      if (type === "text") {
        await sendTextStatusWithPreview(
          sock, groupId, text || "", preparedUrlInfo
        );
      }

      else if (type === "image") {
        const buffer = mediaBuffer;

        const msg =
          unwrapMessage(message.message);

        await sendGroupStatusImage(
          sock,
          groupId,
          buffer,
          {
            caption:
              msg?.imageMessage?.caption || ""
          }
        );
      }

      else if (type === "video") {
        const buffer = mediaBuffer;

        const msg =
          unwrapMessage(message.message);

        await sendGroupStatusVideo(
          sock,
          groupId,
          buffer,
          {
            caption:
              msg?.videoMessage?.caption || ""
          }
        );
      }

      else if (type === "audio") {
        const buffer = mediaBuffer;

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
        const buffer = mediaBuffer;

        await sendGroupStatusSticker(
          sock,
          groupId,
          buffer
        );
      }

      successCount++;

      console.log(
        `â Status forwarded to ${groupId}`
      );

    } catch (error) {
      if (isLockedGroupError(error)) {
        skippedLocked++;
        console.log(`Skipping locked/restricted group ${groupId}: ${error?.message || error}`);
      } else {
        failureCount++;
        console.error(`Failed forwarding to ${groupId}:`, error?.message || error);
      }
    }
  }
  if (entry) {
    let finalState = "failed";
    let finalMessage = `Failed 0/${selectedGroups.length} groups`;

    if (wasStopped) {
      finalState = "stopped";
      finalMessage = `Stopped after forwarding to ${successCount}/${selectedGroups.length} groups`;
    } else if (successCount === selectedGroups.length) {
      finalState = "forwarded";
      finalMessage = `Forwarded ${successCount}/${selectedGroups.length} groups`;
    } else if (successCount > 0) {
      finalState = "partial";
      finalMessage = `Forwarded ${successCount}/${selectedGroups.length} groups`;
    }

    entry.forwardingStatus = {
      state: finalState,
      current: successCount,
      total: selectedGroups.length,
      attempting: selectedGroups.length,
      skipped: skippedLocked,
      startedAt: entry.forwardingStatus.startedAt,
      etaSeconds: 0,
      message: `${finalMessage}; skipped ${skippedLocked} locked; ${failureCount} failed`
    };
  }
}

let cachedWhatsAppVersion = null;
let cachedWhatsAppVersionAt = 0;

async function getCurrentWhatsAppVersion() {
  const now = Date.now();
  if (cachedWhatsAppVersion && now - cachedWhatsAppVersionAt < 5 * 60 * 1000) {
    return cachedWhatsAppVersion;
  }

  // This fork ships the live-version helper internally but does not export it
  // from its package root. Import the exact internal module instead of using
  // its stale bundled protocol version (which WhatsApp rejects with status 405).
  const versionTools = await import("@kaels/casileys/lib/Utils/generics.js");
  if (typeof versionTools.fetchLatestWaWebVersion !== "function") {
    throw new Error("The installed WhatsApp library is missing its live-version helper.");
  }

  const result = await versionTools.fetchLatestWaWebVersion();
  if (result?.isLatest !== true || !Array.isArray(result?.version) || result.version.length !== 3) {
    throw new Error("Could not retrieve a valid current WhatsApp Web version. Please retry shortly.");
  }

  cachedWhatsAppVersion = result.version;
  cachedWhatsAppVersionAt = now;
  console.log(`ð Using live WhatsApp Web version ${result.version.join(".")}`);
  return cachedWhatsAppVersion;
}

async function connectWhatsApp(
  account,
  phoneForPairing = null
) {
  if (connections.has(account.id)) {
    const existing =
      connections.get(account.id);

    if (existing.sock) {
      /*
       * Startup restoration creates the socket without a phone number.
       * If the user later requests pairing for that same saved account,
       * the original connection listener cannot see the new phone argument.
       * Request the code on the existing unregistered socket instead.
       */
      if (
        phoneForPairing &&
        existing.registered === false &&
        !existing.pairingRequested
      ) {
        existing.pairingRequested = true;

        (async () => {
          try {
            await new Promise(resolve => setTimeout(resolve, 1500));

            const code = await existing.sock.requestPairingCode(
              phoneForPairing
            );
            existing.pairingCode = code;

            console.log(
              `ð Pairing code for ${account.phone}: ${code}`
            );

            const waiter = pairingWaiters.get(account.id);
            if (waiter) {
              pairingWaiters.delete(account.id);
              waiter.resolve(code);
            }
          } catch (error) {
            existing.pairingRequested = false;
            console.error(
              `â Pairing code error for ${account.phone}:`,
              error?.message || error
            );

            const waiter = pairingWaiters.get(account.id);
            if (waiter) {
              pairingWaiters.delete(account.id);
              waiter.reject(error);
            }
          }
        })();
      }

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
    registered: Boolean(state.creds.registered),
    pairingCode: null,
    pairingRequested: false,
    qr: null,
    forwardingStatus: {
      state: "idle",
      current: 0,
      total: 0,
      message: "Waiting"
    },
    statusRecipients: new Set()
  };

  connections.set(
    account.id,
    entry
  );

  const waVersion = await getCurrentWhatsAppVersion();
  const sock =
    makeWASocket({
      version: waVersion,
      logger,
      auth: state,
      browser:
        Browsers.windows("Chrome"),
      markOnlineOnConnect: false,
      syncFullHistory: false
    });

  entry.sock = sock;

  const cacheStatusContacts = (contacts = []) => {
    for (const contact of contacts || []) {
      const jid = normalizeJid(contact?.id || contact?.jid || contact?.phoneNumber);
      if (!jid || getOwnJids(sock).includes(jid)) continue;
      if (jid.endsWith("@s.whatsapp.net") || jid.endsWith("@lid")) {
        entry.statusRecipients.add(jid);
      }
    }
  };

  sock.ev.on("contacts.upsert", cacheStatusContacts);
  sock.ev.on("contacts.update", cacheStatusContacts);
  sock.ev.on("messaging-history.set", ({ contacts = [] } = {}) => {
    cacheStatusContacts(contacts);
  });

  sock.ev.on(
    "creds.update",
    saveCreds
  );

  sock.ev.on(
    "connection.update",
    async update => {
      const {
        connection,
        lastDisconnect,
        qr
      } = update;

      if (qr) {
        entry.qr = qr;
        console.log(`ð· QR code ready for ${account.id}`);
      }

      console.log(
        `ð ${account.id} connection:`,
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
            `ð Preparing pairing code for ${account.phone}...`
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
            `ð Pairing code for ${account.phone}: ${code}`
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
            "â Pairing code error:",
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

      if (connection === "close") {
        entry.qr = null;
        entry.lastDisconnectCode = lastDisconnect?.error?.output?.statusCode || lastDisconnect?.error?.statusCode || null;
      }

      if (connection === "open") {
        entry.lastDisconnectCode = null;
        entry.connected = true;
        entry.registered = true;
        entry.qr = null;

        const linkedPhone = String(sock.user?.id || "")
          .split("@")[0]
          .split(":")[0];
        if (String(account.phone || "").startsWith("QR pending ") && /^\d{8,15}$/.test(linkedPhone)) {
          account.phone = linkedPhone;
          updateAccount(account.id, {
            phone: linkedPhone,
            connected: true
          });
        } else {
          updateAccount(
            account.id,
            {
              connected: true
            }
          );
        }

        console.log(
          `â WhatsApp Connected: ${account.phone}`
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
          `â ï¸ WhatsApp disconnected: ${account.id}`
        );

        console.log(
          `â Disconnect status code: ${statusCode ?? "unknown"}`
        );

        console.log(
          "â Disconnect error:",
          lastDisconnect?.error?.message ||
          lastDisconnect?.error ||
          "unknown"
        );

        console.log(
          `ð Reconnect: ${shouldReconnect}`
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
                  `â Reconnect failed for ${account.phone}:`,
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
            `ð¡ Possible Status event for ${account.phone}`
          );

          console.log(
            `ð¡ Status event received for ${account.phone}`
          );

          if (
            !isOwnStatus(
              message,
              sock
            )
          ) {
            console.log(
              "â¹ï¸ Status belongs to another contact"
            );

            continue;
          }

          if (consumeDirectStatusEcho(account.id, message)) {
            console.log(`Skipping already-forwarded direct Status echo for ${account.phone}.`);
            continue;
          }

          await forwardStatus(
            account,
            sock,
            message
          );

        } catch (error) {
          console.error(
            "â Status handler error:",
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
      if (req.body?.method === "qr") {
        const oldQrAccounts = readAccounts().filter(item =>
          String(item.phone || "").startsWith("QR pending ")
        );
        for (const oldAccount of oldQrAccounts) {
          const oldConnection = connections.get(oldAccount.id);
          try { oldConnection?.sock?.end?.(new Error("Replacing expired QR session")); } catch {}
          connections.delete(oldAccount.id);
          pairingWaiters.delete(oldAccount.id);
          reconnecting.delete(oldAccount.id);
          try {
            await mongoDb.collection("whatsapp_auth_creds").deleteMany({ accountId: oldAccount.id });
            await mongoDb.collection("whatsapp_auth_keys").deleteMany({ accountId: oldAccount.id });
          } catch (cleanupError) {
            console.warn("Could not clean temporary QR auth data:", cleanupError?.message || cleanupError);
          }
        }
        if (oldQrAccounts.length) {
          const oldIds = new Set(oldQrAccounts.map(item => item.id));
          saveAccounts(readAccounts().filter(item => !oldIds.has(item.id)));
        }
        const account = {
          id: "account_" + Date.now() + "_qr",
          phone: "QR pending " + Date.now(),
          createdAt: new Date().toISOString(),
          forwardingEnabled: true,
          selectedGroups: [],
          connected: false
        };
        saveAccounts([...readAccounts(), account]);
        try {
          await connectWhatsApp(account, null);
        } catch (error) {
          saveAccounts(readAccounts().filter(item => item.id !== account.id));
          throw error;
        }
        return res.status(202).json({
          success: true,
          account: accountPublic(account),
          message: "Waiting for WhatsApp to provide a QR code."
        });
      }

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
          `â New account created: ${phone}`
        );

      } else {
        console.log(
          `â»ï¸ Existing account found: ${phone}`
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
          "Enter this code in WhatsApp â Linked Devices â Link a Device â Link with phone number instead."
      });

    } catch (error) {
      console.error(
        "â Connect error:",
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
| QR CONNECTION STATUS
|--------------------------------------------------------------------------
*/
app.get("/api/accounts/:accountId/qr", (req, res) => {
  const account = readAccounts().find(item => item.id === req.params.accountId);
  if (!account) {
    return res.status(404).json({ success: false, error: "Account not found" });
  }
  const connection = connections.get(account.id);
  return res.json({
    success: true,
    connected: !!connection?.connected,
    qr: connection?.qr || null,
    lastDisconnectCode: connection?.lastDisconnectCode || null,
    connectionState: connection?.connected ? "connected" : connection?.qr ? "qr_ready" : "waiting",
    phone: account.phone
  });
});


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


app.delete("/api/accounts/:accountId", async (req,res) => { const id=req.params.accountId; try { const accounts=readAccounts(); const account=accounts.find(a=>a.id===id); if(!account) return res.status(404).json({success:false,error:"Account not found"}); try { await connections.get(id)?.sock?.logout(); } catch {} connections.delete(id); pairingWaiters.delete(id); reconnecting.delete(id); await mongoDb.collection("whatsapp_auth_creds").deleteMany({accountId:id}); await mongoDb.collection("whatsapp_auth_keys").deleteMany({accountId:id}); saveAccounts(accounts.filter(a=>a.id!==id)); res.json({success:true,message:"Account removed successfully"}); } catch(error) { console.error("â Remove account error:",error); res.status(500).json({success:false,error:"Failed to remove account"}); } });

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
        "â Group fetch error:",
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

      const connection = connections.get(accountId);
      if (connection) {
        connection.forwardingStatus = {
          state: "idle",
          current: 0,
          total: 0,
          message: "Waiting"
        };
      }

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
  "/api/accounts/:accountId/post-link-status",
  async (req, res) => {
    const account = readAccounts().find(item => item.id === req.params.accountId);
    if (!account) return res.status(404).json({ success: false, error: "Account not found" });

    const connection = connections.get(account.id);
    if (!connection?.connected || !connection?.sock) {
      return res.status(400).json({ success: false, error: "Connect this WhatsApp account first." });
    }

    const statusText = String(req.body?.text ?? req.body?.url ?? "").trim();
    if (!statusText) return res.status(400).json({ success: false, error: "Write text and/or paste a website link first." });
    if (statusText.length > 700) return res.status(400).json({ success: false, error: "WhatsApp text Status can contain up to 700 characters." });

    const recipients = [...(connection.statusRecipients || [])];
    if (!recipients.length) {
      return res.status(400).json({ success: false, error: "WhatsApp contacts have not synced yet. Reconnect this account, wait for contacts to load, then try again." });
    }

    const latestAccount = readAccounts().find(item => item.id === account.id) || account;
    const requestedGroups = latestAccount.selectedGroups || [];
    const shouldForward = latestAccount.forwardingEnabled !== false && requestedGroups.length > 0;
    const groupCheck = shouldForward ? await getUnlockedGroups(connection.sock, requestedGroups) : { unlocked: [], skipped: [] };
    const selectedGroups = groupCheck.unlocked;
    const initiallySkipped = groupCheck.skipped.length;
    const entry = connection;

    if (shouldForward && entry.forwardingStatus?.state === "forwarding") {
      return res.status(409).json({ success: false, error: "A forwarding job is already running for this account. Wait for it to finish before posting another direct status." });
    }

    // Mark the job before publishing so the WhatsApp status event listener does not
    // start a second, duplicate forwarding run for this same direct post.
    if (shouldForward) {
      entry.stopForwardingRequested = false;
      entry.forwardingStatus = {
        state: "forwarding", current: 0, total: selectedGroups.length,
        attempting: 0, skipped: initiallySkipped, startedAt: Date.now(), etaSeconds: selectedGroups.length * 30,
        message: `Posting status, then forwarding to 0/${selectedGroups.length} unlocked groups`
      };
    }

    try {
      // WhatsApp generates a link preview when the link and metadata are supported.
      // Text-only payload by design: no pictures or videos can be posted here.
      // Publish to the account's own WhatsApp Status first. `statusJidList`
      // is the audience list; do not set `broadcast: true` for a status update.
      const sentStatus = await connection.sock.sendMessage(
        "status@broadcast",
        { text: statusText, linkPreview: true },
        { statusJidList: recipients }
      );
      rememberDirectStatusEcho(account.id, sentStatus, statusText);
    } catch (error) {
      if (shouldForward) entry.forwardingStatus = {
        state: "failed", current: 0, total: selectedGroups.length,
        startedAt: Date.now(), etaSeconds: 0, message: "Could not publish the text Status"
      };
      console.error("Text Status posting failed:", error?.message || error);
      return res.status(500).json({ success: false, error: error?.message || "Failed to post the text Status." });
    }

    console.log(`Text Status posted for ${account.phone} to ${recipients.length} contacts.`);

    if (!shouldForward || selectedGroups.length === 0) {
      const reason = latestAccount.forwardingEnabled === false
        ? "Forwarding is OFF, so it was not sent to group statuses."
        : requestedGroups.length === 0
          ? "No groups are selected, so there were no group statuses to forward to."
          : `All ${initiallySkipped} selected groups are locked/admin-only and were skipped.`;
      if (shouldForward) entry.forwardingStatus = { state: "forwarded", current: 0, total: 0, attempting: 0, skipped: initiallySkipped, startedAt: Date.now(), etaSeconds: 0, message: `Forwarded 0/0 unlocked groups; skipped ${initiallySkipped} locked` };
      return res.json({ success: true, message: `Your text Status was posted. ${reason}` });
    }

    let successCount = 0;
    let failureCount = 0;
    let skippedLocked = initiallySkipped;
    let preparedUrlInfo = null;
    try {
      preparedUrlInfo = await getUrlInfo(statusText, {
        thumbnailWidth: 192, fetchOpts: { timeout: 3000 }, logger,
        uploadImage: connection.sock.waUploadToServer
      });
    } catch (error) {
      console.warn("Direct Status link preview preparation failed; continuing without cached preview:", error?.message || error);
    }
    let wasStopped = false;
    for (let i = 0; i < selectedGroups.length; i++) {
      if (entry.stopForwardingRequested) { wasStopped = true; break; }
      const groupId = selectedGroups[i];
      entry.forwardingStatus = {
        state: "forwarding", current: successCount, total: selectedGroups.length,
        attempting: i + 1, startedAt: entry.forwardingStatus.startedAt,
        etaSeconds: selectedGroups.length * 30,
        message: `Forwarded ${successCount}/${selectedGroups.length}; processing ${i + 1}/${selectedGroups.length}`
      };
      try {
        await sendTextStatusWithPreview(connection.sock, groupId, statusText, preparedUrlInfo);
        successCount++;
        console.log(`Text Status forwarded successfully to selected group ${groupId} (${successCount}/${selectedGroups.length}).`);
      } catch (error) {
        if (isLockedGroupError(error)) {
          skippedLocked++;
          console.log(`Skipping locked/restricted group ${groupId}: ${error?.message || error}`);
        } else {
          failureCount++;
          console.error(`Text Status forwarding failed for ${groupId}:`, error?.message || error);
        }
      }
      entry.forwardingStatus = {
        state: "forwarding", current: successCount, total: selectedGroups.length,
        attempting: i + 1, skipped: skippedLocked, startedAt: entry.forwardingStatus.startedAt,
        etaSeconds: Math.max(0, (selectedGroups.length - i - 1) * 30),
        message: `Forwarded ${successCount}/${selectedGroups.length}; processed ${i + 1}/${selectedGroups.length}; skipped ${skippedLocked} locked`
      };
    }

    const state = wasStopped ? "stopped" : successCount === selectedGroups.length ? "forwarded" : successCount > 0 ? "partial" : "failed";
    entry.forwardingStatus = {
      state, current: successCount, total: selectedGroups.length,
      attempting: selectedGroups.length, skipped: skippedLocked, startedAt: entry.forwardingStatus.startedAt,
      etaSeconds: 0,
      message: wasStopped ? `Stopped after forwarding to ${successCount}/${selectedGroups.length} unlocked groups` : `Forwarded ${successCount}/${selectedGroups.length} unlocked groups; skipped ${skippedLocked} locked; ${failureCount} failed`
    };
    return res.json({
      success: true,
      posted: true,
      forwarding: { enabled: true, successful: successCount, failed: failureCount, skippedLocked, total: selectedGroups.length, selectedTotal: requestedGroups.length },
      message: `Your text Status was posted. Forwarded to ${successCount}/${selectedGroups.length} unlocked group statuses; skipped ${skippedLocked} locked${failureCount ? `; ${failureCount} failed` : ""}.`
    });
  }
);

app.post("/api/accounts/:accountId/stop-forwarding", (req, res) => {
  const accountId = req.params.accountId;
  const account = readAccounts().find(item => item.id === accountId);
  if (!account) return res.status(404).json({ success: false, error: "Account not found" });
  const entry = connections.get(accountId);
  if (!entry || entry.forwardingStatus?.state !== "forwarding") {
    return res.json({ success: true, stopped: false, message: "No forwarding job is currently running." });
  }
  entry.stopForwardingRequested = true;
  updateAccount(accountId, { forwardingEnabled: false });
  entry.forwardingStatus = {
    ...entry.forwardingStatus,
    state: "forwarding",
    stopRequested: true,
    message: "Stopping after the current group finishes..."
  };
  console.log(`Stop requested for forwarding job on ${account.phone}`);
  return res.json({ success: true, stopped: true, message: "Stop requested. The current group send will finish, then no more groups will receive this status." });
});

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
        `ð Forwarding ${
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
          "ð WhatsApp Status Web Backend"
        );
        console.log(
          `ð Port: ${PORT}`
        );
        console.log(
          "======================================"
        );
        console.log("");

        const accounts =
          readAccounts();

        console.log(
          `ð± Saved accounts: ${accounts.length}`
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
              `â Failed restoring ${account.phone}:`,
              error?.message ||
              error
            );
          });
        }
      }
    );
  } catch (error) {
    console.error(
      "â Startup initialization failed:",
      error?.message ||
      error
    );

    process.exit(1);
  }
}

startServer();
