import "dotenv/config";
import express from "express";
import cors from "cors";
import pino from "pino";
import fs from "fs";
import { randomUUID } from "node:crypto";
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

// Add the shared Add WhatsApp Group / Join All Groups panel without replacing existing frontend controls.
app.get("/", (req,res,next) => {
 try {
  let html=fs.readFileSync(path.join(process.cwd(),"frontend","index.html"),"utf8");
  const panel = fs.readFileSync(path.join(process.cwd(), "frontend", "join-groups-panel.html"), "utf8");
  html=html.replace("</body>",panel+"</body>");
  res.type("html").send(html);
 } catch(e) { next(e); }
});
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

function withTimeout(operation, timeoutMs, message) {
  let timer;
  return Promise.race([
    Promise.resolve().then(() => typeof operation === "function" ? operation() : operation),
    new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(message)), timeoutMs); })
  ]).finally(() => clearTimeout(timer));
}

const connections = new Map();
const pairingWaiters = new Map();
const reconnecting = new Set();
// Prevent concurrent requests/reconnect timers from opening multiple sockets for one saved account.
const connectionStarts = new Map();
const SOCKET_LEASE_MS = 30000;
const SOCKET_LEASE_RENEW_MS = 8000;
const instanceOwner = `${process.env.RENDER_INSTANCE_ID || process.env.HOSTNAME || "whatsapp-status-web"}:${process.pid}:${randomUUID()}`;
const socketLeaseTimers = new Map();
const socketRetryTimers = new Map();
let shuttingDown = false;

async function acquireSocketLease(accountId) {
  const collection = mongoDb.collection("whatsapp_socket_locks");
  const now = new Date();
  try {
    const result = await collection.updateOne(
      { _id: accountId, $or: [{ owner: instanceOwner }, { leaseUntil: { $lte: now } }] },
      { $set: { owner: instanceOwner, leaseUntil: new Date(now.getTime() + SOCKET_LEASE_MS), updatedAt: now } },
      { upsert: true }
    );
    return result.matchedCount === 1 || result.upsertedCount === 1;
  } catch (error) {
    if (error?.code === 11000) return false; // Another instance owns the live lease.
    throw error;
  }
}

async function releaseSocketLease(accountId) {
  const timer = socketLeaseTimers.get(accountId);
  if (timer) clearInterval(timer);
  socketLeaseTimers.delete(accountId);
  if (mongoDb) {
    await mongoDb.collection("whatsapp_socket_locks").deleteOne({ _id: accountId, owner: instanceOwner });
  }
}

function startSocketLeaseHeartbeat(accountId) {
  if (socketLeaseTimers.has(accountId)) return;
  const timer = setInterval(async () => {
    try {
      const now = new Date();
      const result = await mongoDb.collection("whatsapp_socket_locks").updateOne(
        { _id: accountId, owner: instanceOwner },
        { $set: { leaseUntil: new Date(now.getTime() + SOCKET_LEASE_MS), updatedAt: now } }
      );
      if (result.matchedCount !== 1) {
        clearInterval(timer);
        socketLeaseTimers.delete(accountId);
        const current = connections.get(accountId);
        if (current) {
          current.leaseLost = true;
          if (connections.get(accountId) === current) connections.delete(accountId);
          try { current.sock?.end?.(new Error("WhatsApp session lease was lost to another server instance")); } catch {}
        }
        scheduleSocketRetry(readAccounts().find(item => item.id === accountId), 10000);
      }
    } catch (error) {
      console.warn("WhatsApp session lease heartbeat failed:", error?.message || error);
    }
  }, SOCKET_LEASE_RENEW_MS);
  timer.unref?.();
  socketLeaseTimers.set(accountId, timer);
}

function scheduleSocketRetry(account, delay = 10000) {
  if (!account || shuttingDown || socketRetryTimers.has(account.id)) return;
  const timer = setTimeout(async () => {
    socketRetryTimers.delete(account.id);
    if (shuttingDown || !readAccounts().some(item => item.id === account.id)) return;
    try { await connectWhatsApp(account, null); }
    catch (error) { console.warn(`WhatsApp reconnect retry failed for ${account.phone}:`, error?.message || error); }
  }, delay);
  timer.unref?.();
  socketRetryTimers.set(account.id, timer);
}

const directStatusEchoes = new Map();


let accountsCache = [];
const joinAllJobs = new Map();
const leaveAllJobs = new Map();
function parseWhatsAppInvite(value) {
  const match = String(value || "").trim().match(/(?:https?:\/\/)?(?:www\.)?chat\.whatsapp\.com\/([A-Za-z0-9_-]+)/i);
  return match ? { code: match[1], link: "https://chat.whatsapp.com/" + match[1] } : null;
}
async function saveSharedJoinGroup(item) {
  const collection = mongoDb.collection("join_groups");
  const parsed = parseWhatsAppInvite(item.link || item.code || item._id);
  const code = parsed?.code || String(item.code || item._id || "").trim();
  if (!code) throw new Error("A valid WhatsApp invite code is required");
  const link = "https://chat.whatsapp.com/" + code;
  // A stable invite code is the canonical ID, so repeated Add Group / Save Groups
  // requests update the same record instead of creating another library entry.
  await collection.updateOne(
    { _id: code },
    { $setOnInsert: { ...item, _id: code, code, link, addedAt: new Date().toISOString() } },
    { upsert: true }
  );
  // Listing performs one legacy-deduplication pass. Avoid a full collection scan
  // after every insert; that made Save Groups increasingly slow for large selections.
  return collection.findOne({ _id: code });
}
async function listSharedJoinGroups() {
  const rows = await mongoDb.collection("join_groups").find({}).sort({ addedAt: -1 }).toArray();
  // Hide and clean any duplicates left by older versions or differently formatted URLs.
  const seen = new Set();
  const unique = [];
  const duplicateIds = [];
  for (const row of rows) {
    const parsed = parseWhatsAppInvite(row.link || row.code || row._id);
    const code = parsed?.code || String(row.code || row._id || "").trim();
    if (!code || seen.has(code)) {
      duplicateIds.push(row._id);
      continue;
    }
    seen.add(code);
    unique.push({ ...row, _id: code, code, link: "https://chat.whatsapp.com/" + code });
  }
  if (duplicateIds.length) {
    await mongoDb.collection("join_groups").deleteMany({ _id: { $in: duplicateIds } });
  }
  return unique;
}


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

function isOwnStatus(message, sock, accountPhone = '') {
  const remoteJid = normalizeJid(message?.key?.remoteJid);
  const remoteJidAlt = normalizeJid(message?.key?.remoteJidAlt);
  const isStatus = remoteJid === 'status@broadcast' || remoteJid?.endsWith('@broadcast') || remoteJidAlt === 'status@broadcast' || remoteJidAlt?.endsWith('@broadcast');
  if (!isStatus) return false;
  if (message?.key?.fromMe === true) return true;
  const ownJids = getOwnJids(sock);
  const possibleSenders = [message?.key?.participant, message?.participant, message?.key?.senderPn, message?.key?.senderLid, message?.key?.remoteJidAlt].filter(Boolean).map(normalizeJid);
  if (possibleSenders.some(sender => ownJids.some(own => sameJid(sender, own)))) return true;
  const ownPhone = Array.from(String(accountPhone || '')).filter(ch => ch >= '0' && ch <= '9').join('');
  if (!ownPhone) return false;
  return possibleSenders.some(sender => sender.endsWith('@s.whatsapp.net') && Array.from(sender.split('@')[0]).filter(ch => ch >= '0' && ch <= '9').join('') === ownPhone);
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

  if (!isOwnStatus(message, sock, account.phone)) {
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
      etaSeconds: Math.ceil(selectedGroups.length / 30) * 30,
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
  // Send up to 30 groups concurrently; the final batch may contain 1–29 groups.
  for (let batchStart = 0; batchStart < selectedGroups.length; batchStart += 30) {
    if (entry?.stopForwardingRequested) { wasStopped = true; break; }
    const batchGroups = selectedGroups.slice(batchStart, batchStart + 30);
    const updateBatchProgress = () => {
      if (!entry) return;
      const startedAt = Number(entry.forwardingStatus?.startedAt || Date.now());
      const elapsedSeconds = Math.floor((Date.now() - startedAt) / 1000);
      const remainingBatches = Math.ceil((selectedGroups.length - batchStart) / 30);
      entry.forwardingStatus = {
        ...entry.forwardingStatus,
        state: "forwarding",
        current: successCount,
        total: selectedGroups.length,
        attempting: batchStart + batchGroups.length,
        skipped: skippedLocked,
        startedAt,
        // The UI subtracts elapsed time, so include elapsed seconds here to avoid double-subtracting.
        etaSeconds: elapsedSeconds + remainingBatches * 30,
        message: `Forwarded ${successCount}/${selectedGroups.length}; processing batch ${Math.floor(batchStart / 30) + 1}/${Math.ceil(selectedGroups.length / 30)}`
      };
    };
    updateBatchProgress();
    await Promise.all(batchGroups.map(async (groupId, batchOffset) => {
    const i = batchStart + batchOffset;

    console.log(`ð¯ ATTEMPTING GROUP: ${groupId}`);

    updateBatchProgress();

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
      updateBatchProgress();

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
      updateBatchProgress();
    }
    }));
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

function connectWhatsApp(account, phoneForPairing = null) {
  if (shuttingDown) return Promise.resolve(null);
  const pending = connectionStarts.get(account.id);
  if (pending) {
    if (phoneForPairing) return pending.then(() => connectWhatsAppInternal(account, phoneForPairing));
    return pending;
  }
  const starting = connectWhatsAppInternal(account, phoneForPairing);
  connectionStarts.set(account.id, starting);
  return starting.finally(() => {
    if (connectionStarts.get(account.id) === starting) connectionStarts.delete(account.id);
  });
}

async function connectWhatsAppInternal(
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

  const waVersion = await getCurrentWhatsAppVersion();
  const ownsLease = await acquireSocketLease(account.id);
  if (!ownsLease) {
    console.log(`WhatsApp session ${account.id} is owned by another live server instance; will retry safely.`);
    scheduleSocketRetry(account, 10000);
    return null;
  }

  connections.set(account.id, entry);
  startSocketLeaseHeartbeat(account.id);
  let sock;
  try {
    sock = makeWASocket({
      version: waVersion,
      logger,
      auth: state,
      browser: Browsers.windows("Chrome"),
      markOnlineOnConnect: false,
      syncFullHistory: false
    });
    entry.sock = sock;
  } catch (error) {
    if (connections.get(account.id) === entry) connections.delete(account.id);
    await releaseSocketLease(account.id).catch(() => {});
    throw error;
  }

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
          statusCode !== DisconnectReason.loggedOut &&
          !shuttingDown &&
          !entry.leaseLost;

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
          // A close event can fire more than once. Schedule at most one reconnect,
          // and never delete or replace a newer socket created by another request.
          if (!reconnecting.has(account.id)) {
            reconnecting.add(account.id);
            setTimeout(async () => {
              try {
                if (!readAccounts().some(item => item.id === account.id)) return;
                const current = connections.get(account.id);
                if (current && current !== entry && current.sock) return;
                if (connections.get(account.id) === entry) connections.delete(account.id);
                await connectWhatsApp(account, null);
              } catch (error) {
                console.error(`Reconnect failed for ${account.phone}:`, error?.message || error);
              } finally {
                reconnecting.delete(account.id);
              }
            }, 3000);
          }
        } else {
          if (connections.get(account.id) === entry) connections.delete(account.id);
          if (!shuttingDown) releaseSocketLease(account.id).catch(error => console.warn("Could not release WhatsApp session lease:", error?.message || error));
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
              sock,
              account.phone
            )
          ) {
            const maskJid = (value) => {
              const normalized = normalizeJid(value);
              if (!normalized) return null;
              const [local, domain] = normalized.split("@");
              const digits = String(local || "").replace(/\D/g, "");
              return `${domain || "unknown"}:${digits ? digits.slice(-4) : String(local).slice(-4)}`;
            };
            console.log("Status ownership mismatch (safe diagnostics):", JSON.stringify({
              fromMe: message?.key?.fromMe ?? null,
              account: maskJid(`${account.phone}@s.whatsapp.net`),
              sockUser: maskJid(sock?.user?.id),
              sockLid: maskJid(sock?.user?.lid),
              participant: maskJid(message?.key?.participant || message?.participant),
              senderPn: maskJid(message?.key?.senderPn),
              senderLid: maskJid(message?.key?.senderLid),
              remoteAlt: maskJid(message?.key?.remoteJidAlt)
            }));
            console.log("Status belongs to another contact");

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


app.delete("/api/accounts/:accountId", async (req,res) => { const id=req.params.accountId; try { const accounts=readAccounts(); const account=accounts.find(a=>a.id===id); if(!account) return res.status(404).json({success:false,error:"Account not found"}); try { await connections.get(id)?.sock?.logout(); } catch {} connections.delete(id); pairingWaiters.delete(id); reconnecting.delete(id); const retry=socketRetryTimers.get(id); if(retry)clearTimeout(retry); socketRetryTimers.delete(id); await releaseSocketLease(id); await mongoDb.collection("whatsapp_auth_creds").deleteMany({accountId:id}); await mongoDb.collection("whatsapp_auth_keys").deleteMany({accountId:id}); saveAccounts(accounts.filter(a=>a.id!==id)); res.json({success:true,message:"Account removed successfully"}); } catch(error) { console.error("â Remove account error:",error); res.status(500).json({success:false,error:"Failed to remove account"}); } });


/* SHARED ADD GROUP / JOIN GROUPS */
app.get("/api/join-groups", async (req,res) => {
 try { const groups=await listSharedJoinGroups(); res.json({success:true,groups:groups.map(g=>({id:g.code||g._id,code:g.code||g._id,link:g.link,name:g.name||"WhatsApp Group"}))}); }
 catch(e){res.status(500).json({success:false,error:e.message||"Could not load shared groups"});}
});
app.post("/api/join-groups/cleanup", async (req,res) => {
 try {
  const configuredPassword=process.env.ADMIN_CLEANUP_PASSWORD;
  const suppliedPassword=String(req.get("X-Admin-Password")||"");
  if(!configuredPassword || suppliedPassword!==configuredPassword) return res.status(403).json({success:false,error:"Admin only. Incorrect password."});
  const accountId=String(req.body?.accountId||"").trim();
  if(!accountId)return res.status(400).json({success:false,error:"Select a connected WhatsApp account to verify the library."});
  const account=readAccounts().find(a=>a.id===accountId);
  const conn=connections.get(accountId);
  if(!account||!conn?.connected||!conn?.sock)return res.status(400).json({success:false,error:"The selected WhatsApp account is not connected. Connect it before cleaning the library."});
  if(typeof conn.sock.groupGetInviteInfo!=="function")return res.status(400).json({success:false,error:"This WhatsApp connection cannot verify invite links."});
  // First remove legacy duplicate entries, then verify every remaining link with WhatsApp.
  await listSharedJoinGroups();
  const collection=mongoDb.collection("join_groups");
  const rows=await collection.find({}).sort({addedAt:-1}).toArray();
  let checked=0,kept=0,removed=0;
  const results=[];
  for(const row of rows) {
   const parsed=parseWhatsAppInvite(row.link||row.code||row._id);
   if(!parsed) {
    await collection.deleteOne({_id:row._id}); removed++;
    results.push({name:row.name||"Unknown link",status:"removed",reason:"Malformed or unrecognized WhatsApp invite URL"});
    continue;
   }
   checked++;
   try {
    const inviteInfo=await conn.sock.groupGetInviteInfo(parsed.code);
    if(inviteInfo?.id) {
     kept++;
     await collection.updateOne({_id:row._id},{$set:{code:parsed.code,link:parsed.link,verifiedAt:new Date(),verifiedBy:accountId,name:row.name||inviteInfo.subject||inviteInfo.name||"WhatsApp Group"}});
     results.push({name:row.name||inviteInfo.subject||inviteInfo.name||"WhatsApp Group",status:"kept",reason:"Verified active by WhatsApp"});
    } else {
     await collection.deleteOne({_id:row._id}); removed++;
     results.push({name:row.name||"WhatsApp Group",status:"removed",reason:"WhatsApp did not confirm this invite"});
    }
   } catch(verifyError) {
    await collection.deleteOne({_id:row._id}); removed++;
    results.push({name:row.name||"WhatsApp Group",status:"removed",reason:"Could not verify this invite with WhatsApp"});
   }
   await new Promise(resolve=>setTimeout(resolve,300));
  }
  res.json({success:true,checked,kept,removed,results,message:`Library cleanup complete: checked ${checked} links, kept ${kept} verified links, removed ${removed} inactive/unverified links.`});
 } catch(error) {
  res.status(500).json({success:false,error:error?.message||"Could not clean the invite library"});
 }
});

app.post("/api/join-groups", async (req,res) => {
 try {
  const rawLinks=Array.isArray(req.body?.links)?req.body.links:[req.body?.link||req.body?.url].filter(Boolean);
  if(!rawLinks.length)return res.status(400).json({success:false,error:"Paste one or more valid WhatsApp invite links."});
  if(rawLinks.length>200)return res.status(400).json({success:false,error:"Please submit no more than 200 invite links at a time."});
  const accountId=String(req.body?.accountId||"").trim();
  if(!accountId)return res.status(400).json({success:false,error:"Select a connected WhatsApp account to verify the invite links."});
  const account=readAccounts().find(a=>a.id===accountId);
  const conn=connections.get(accountId);
  if(!account||!conn?.connected||!conn?.sock)return res.status(400).json({success:false,error:"The selected WhatsApp account is not connected. Connect it and try again."});
  if(typeof conn.sock.groupGetInviteInfo!=="function")return res.status(400).json({success:false,error:"This WhatsApp connection cannot verify invite links. Reconnect or update the WhatsApp library and try again."});
  const customName=String(req.body?.name||"").trim().slice(0,100);
  const seen=new Set(), results=[]; let added=0,duplicates=0,invalid=0,verified=0;
  for(const value of rawLinks) {
   const parsed=parseWhatsAppInvite(value);
   if(!parsed){invalid++;results.push({link:String(value).slice(0,200),status:"invalid",message:"Not a valid WhatsApp invite link"});continue;}
   if(seen.has(parsed.code)){duplicates++;results.push({code:parsed.code,status:"duplicate",message:"Duplicate in this submission; ignored"});continue;}
   seen.add(parsed.code);
   let inviteInfo;
   try { inviteInfo=await conn.sock.groupGetInviteInfo(parsed.code); }
   catch(checkError) { invalid++;results.push({code:parsed.code,link:parsed.link,status:"unverified",message:"WhatsApp could not verify this invite; it was not saved"});continue; }
   if(!inviteInfo?.id){invalid++;results.push({code:parsed.code,link:parsed.link,status:"unverified",message:"Invite is expired, invalid, or WhatsApp did not confirm it; it was not saved"});continue;}
   verified++;
   const existing=await mongoDb.collection("join_groups").findOne({_id:parsed.code});
   const group=await saveSharedJoinGroup({...parsed,name:customName||inviteInfo.subject||inviteInfo.name||"WhatsApp Group",source:"user"});
   if(existing)duplicates++;else added++;
   results.push({code:parsed.code,link:group.link,name:group.name,status:existing?"already_exists":"added"});
   await new Promise(resolve=>setTimeout(resolve,250));
  }
  if(!verified)return res.status(400).json({success:false,error:"No invite links could be verified as active. Nothing was added.",added,duplicates,invalid,results});
  res.json({success:true,added,duplicates,invalid,verified,results,message:"Checked "+rawLinks.length+" links: "+verified+" active/verified, "+added+" added, "+duplicates+" duplicates ignored, "+invalid+" invalid or unverified. Only verified active links were saved."});
 } catch(e){res.status(500).json({success:false,error:e.message||"Could not verify and save invite links"});}
});
async function joinOneSharedGroup(accountId,code) {
 const account=readAccounts().find(a=>a.id===accountId); if(!account)return {code,status:"failed",message:"Account not found"};
 const conn=connections.get(accountId); if(!conn?.connected||!conn?.sock)return {code,status:"failed",message:"Connect this WhatsApp account first"};
 try {
  const group=await mongoDb.collection("join_groups").findOne({_id:code}); if(!group)return {code,status:"failed",message:"Invite link not found"};
  const sock=conn.sock;
  // Fail closed: never attempt to join if membership could not be checked. This avoids
  // duplicate join attempts when WhatsApp's membership/invite lookup is temporarily unavailable.
  if(typeof sock.groupGetInviteInfo!=="function" || typeof sock.groupFetchAllParticipating!=="function") {
   return {code,status:"failed",message:"Membership verification is unavailable; skipped to avoid an unsafe duplicate join. Retry later."};
  }
  let inviteInfo, participating;
  try {
   inviteInfo=await sock.groupGetInviteInfo(code);
   if(!inviteInfo?.id) return {code,status:"failed",message:"WhatsApp did not provide a group ID, so membership could not be verified. Retry later."};
   participating=await sock.groupFetchAllParticipating();
   if(!participating || typeof participating!=="object") {
    return {code,status:"failed",message:"WhatsApp membership lookup returned no usable data; skipped to avoid a duplicate join. Retry later."};
   }
  } catch(checkError) {
   return {code,status:"failed",message:"Could not verify group membership with WhatsApp; skipped to avoid a duplicate join. Retry later."};
  }
  const groupId=inviteInfo.id;
  if(Object.prototype.hasOwnProperty.call(participating,groupId))
   return {code,status:"already_joined",message:"Already a member; skipped without joining again",groupId};
  if(typeof sock.groupAcceptInvite!=="function")return {code,status:"failed",message:"Joining by invite link is not supported by this WhatsApp library version"};
  const result=await sock.groupAcceptInvite(code);
  return result?{code,status:"joined",message:"Joined successfully"}:{code,status:"failed",message:"WhatsApp did not confirm the join"};
 } catch(e) {
  const msg=String(e?.message||e), low=msg.toLowerCase();
  // Only count approval as a successful request when the error explicitly confirms
  // that WhatsApp accepted/sent the request. "Approval required" alone is not proof.
  const requestConfirmed=/(request|requested).{0,30}(sent|submitted|accepted|pending)|(?:sent|submitted|accepted).{0,30}(join request|approval request)|pending.{0,20}approval/.test(low);
  if(requestConfirmed) return {code,status:"pending_approval",message:"Request Sent — Awaiting Admin Approval"};
  const approvalRequired=/approval required|requires? admin approval|admin approval required|join request required|must request to join/.test(low);
  if(approvalRequired) return {code,status:"approval_required",message:"This group requires admin approval, but WhatsApp did not confirm that a join request was sent."};
  const status=/already.*(participant|member)|already.*in group|already.?joined/.test(low)?"already_joined":/expired|invalid.*invite|not.?found|invite.*revoked|revoked.*invite/.test(low)?"skipped":"failed";
  return {code,status,message:status==="skipped"?"Invite is invalid, expired, or revoked":msg.slice(0,200)};
 }
}
app.post("/api/accounts/:accountId/join-groups/:code/join",async(req,res)=>{
 const result=await joinOneSharedGroup(req.params.accountId,req.params.code); const ok=["joined","already_joined","pending_approval"].includes(result.status);
 res.status(ok?200:400).json({success:ok,result});
});
function startJoinAllJob(id) {
 const account=readAccounts().find(a=>a.id===id), conn=connections.get(id);
 const old=joinAllJobs.get(id);
 if(old?.state==="running") return old;
 if(!account) {
  const failed={state:"failed",current:0,total:0,joined:0,alreadyJoined:0,skipped:0,failed:1,message:"Account not found"};
  joinAllJobs.set(id,failed); return failed;
 }
 if(!conn?.connected||!conn?.sock) {
  const failed={state:"failed",current:0,total:0,joined:0,alreadyJoined:0,skipped:0,failed:0,message:"Groups saved, but connect this WhatsApp account before joining shared groups."};
  joinAllJobs.set(id,failed); return failed;
 }
 const job={state:"running",current:0,total:0,joined:0,alreadyJoined:0,requestsSent:0,skipped:0,failed:0,cancelRequested:false,message:"Preparing shared group invites..."};
 joinAllJobs.set(id,job);
 (async()=>{
  let groups;
  try {
   groups=await listSharedJoinGroups();
  } catch(loadError) {
   // Keep a readable, retryable failed state instead of leaving progress at “Preparing”.
   job.state="failed";
   job.retryable=true;
   job.message="Could not load the shared invite library. No groups were joined. Check the database connection and try Join All Groups again.";
   job.error=String(loadError?.message||loadError).slice(0,200);
   return;
  }
  job.total=groups.length;
  if(job.cancelRequested) { job.state="stopped"; job.message="Joining stopped before the first group."; return; }
  if(!groups.length) { job.state="completed"; job.message="No shared group invite links are available to join."; return; }
  for(const g of groups) {
   if(job.cancelRequested) break;
   job.current++;
   job.message="Joining Groups "+job.current+"/"+job.total+": "+(g.name||"WhatsApp Group");
   const result=await joinOneSharedGroup(id,g.code||g._id);
   if(result.status==="joined")job.joined++;
   else if(result.status==="already_joined")job.alreadyJoined++;
   else if(result.status==="pending_approval")job.requestsSent++;
   else if(result.status==="skipped")job.skipped++;
   else job.failed++;
   job.lastResult={...result,name:g.name||"WhatsApp Group"};
   if(result.status==="pending_approval") job.message="Request Sent — Awaiting Admin Approval: "+(g.name||"WhatsApp Group")+" ("+job.current+"/"+job.total+")";
   if(job.cancelRequested) break;
   await new Promise(resolve=>setTimeout(resolve,1800));
  }
  if(job.cancelRequested) {
   job.state="stopped";
   job.message="Stopped at "+job.current+"/"+job.total+" groups. "+job.joined+" joined, "+job.alreadyJoined+" already joined, "+job.requestsSent+" requests sent awaiting approval, "+job.skipped+" skipped, "+job.failed+" failed.";
  } else {
   job.state="completed";
   job.message="Finished: "+job.joined+" joined, "+job.alreadyJoined+" already joined, "+job.requestsSent+" requests sent awaiting approval, "+job.skipped+" skipped, "+job.failed+" failed.";
  }
 })().catch(e=>{
  job.state="failed";
  job.retryable=true;
  job.message="Join All Groups stopped because of an unexpected error. You can retry after checking the connection.";
  job.error=String(e?.message||e).slice(0,200);
 });
 return job;
}
app.post("/api/accounts/:accountId/join-groups/join-all",async(req,res)=>{
 const id=req.params.accountId, account=readAccounts().find(a=>a.id===id);
 if(!account)return res.status(404).json({success:false,error:"Account not found"});
 const job=startJoinAllJob(id);
 if(job.state==="failed")return res.status(400).json({success:false,error:job.message,progress:job});
 res.json({success:true,message:"Join All Groups started",progress:job});
});
app.post("/api/accounts/:accountId/join-groups/stop",(req,res)=>{
 const job=joinAllJobs.get(req.params.accountId);
 if(!job||job.state!=="running")return res.json({success:true,stopped:false,message:"No group-joining task is running."});
 job.cancelRequested=true;
 job.message="Stop requested — the current group operation will finish, then no more groups will be joined.";
 res.json({success:true,stopped:true,message:job.message});
});
app.get("/api/accounts/:accountId/join-groups/progress",(req,res)=>res.json({success:true,progress:joinAllJobs.get(req.params.accountId)||{state:"idle",message:"No Join All Groups task is running"}}));

function startLeaveAllJob(id) {
 const account=readAccounts().find(a=>a.id===id), conn=connections.get(id);
 const old=leaveAllJobs.get(id);
 if(old?.state==="running") return old;
 const fail=(message)=>{const job={state:"failed",current:0,total:0,left:0,failed:0,cancelRequested:false,message};leaveAllJobs.set(id,job);return job;};
 if(!account) return fail("Account not found.");
 if(!conn?.connected||!conn?.sock) return fail("Connect this WhatsApp account before leaving groups.");
 if(joinAllJobs.get(id)?.state==="running") return fail("Stop Join All Groups and wait for it to finish before leaving groups.");
 if(conn.forwardingStatus?.state==="forwarding") return fail("Forwarding is currently running. Stop forwarding and try again so the group list does not change mid-task.");
 if(typeof conn.sock.groupFetchAllParticipating!=="function"||typeof conn.sock.groupLeave!=="function") return fail("This WhatsApp connection does not support listing or leaving groups.");
 const job={state:"running",current:0,total:0,left:0,failed:0,cancelRequested:false,message:"Loading groups from this WhatsApp account...",errors:[]};
 leaveAllJobs.set(id,job);
 (async()=>{
  const leftIds=new Set();
  try {
   const groups=await withTimeout(()=>conn.sock.groupFetchAllParticipating(),15000,"Loading groups timed out. No groups were left.");
   const list=Object.values(groups||{}).filter(g=>g?.id);
   job.total=list.length;
   if(!list.length){job.state="completed";job.message="This account is not currently a member of any groups.";return;}
   for(const group of list){
    if(job.cancelRequested) break;
    job.current++;
    const name=String(group.subject||group.name||group.id);
    job.message=`Leaving group ${job.current}/${job.total}: ${name}`;
    try {
     await withTimeout(()=>conn.sock.groupLeave(group.id),15000,"Leaving this group timed out.");
     leftIds.add(group.id); job.left++;
    } catch(error) {
     job.failed++;
     if(job.errors.length<8) job.errors.push({group:name,message:String(error?.message||error).slice(0,140)});
    }
    // Pace requests to reduce the chance of triggering WhatsApp rate limits.
    if(!job.cancelRequested && job.current<job.total) await new Promise(resolve=>setTimeout(resolve,650));
   }
   const latest=readAccounts().find(a=>a.id===id);
   if(latest) updateAccount(id,{selectedGroups:(latest.selectedGroups||[]).filter(groupId=>!leftIds.has(groupId))});
   if(job.cancelRequested){job.state="stopped";job.message=`Stopped. Left ${job.left} of ${job.total} groups; ${job.failed} failed. Remaining group selections were preserved.`;}
   else if(job.failed){job.state=job.left?"partial":"failed";job.message=`Finished with ${job.left} groups left and ${job.failed} failures out of ${job.total}. Failed groups remain selected if they were previously selected.`;}
   else {job.state="completed";job.message=`Successfully left all ${job.left} groups for this WhatsApp account. Forwarding selections for those groups were cleared.`;}
  } catch(error){
   job.state="failed"; job.message=String(error?.message||error||"Could not leave groups.").slice(0,220);
  }
 })();
 return job;
}
app.post("/api/accounts/:accountId/leave-all-groups",(req,res)=>{
 const id=req.params.accountId;
 if(!readAccounts().some(a=>a.id===id)) return res.status(404).json({success:false,error:"Account not found"});
 const job=startLeaveAllJob(id);
 if(job.state==="failed") return res.status(400).json({success:false,error:job.message,progress:job});
 res.json({success:true,message:"Leave All Groups started",progress:job});
});
app.post("/api/accounts/:accountId/leave-all-groups/stop",(req,res)=>{
 const job=leaveAllJobs.get(req.params.accountId);
 if(!job||job.state!=="running") return res.json({success:true,stopped:false,message:"No Leave All Groups task is running."});
 job.cancelRequested=true; job.message="Stop requested. The current group operation will finish, then no more groups will be left.";
 res.json({success:true,stopped:true,message:job.message});
});
app.get("/api/accounts/:accountId/leave-all-groups/progress",(req,res)=>res.json({success:true,progress:leaveAllJobs.get(req.params.accountId)||{state:"idle",message:"No Leave All Groups task is running"}}));

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

      const groups = await withTimeout(
        () => connection.sock.groupFetchAllParticipating(),
        15000,
        "Loading WhatsApp groups timed out. Check the WhatsApp connection and tap Refresh Groups to retry."
      );

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
  async (req, res) => {
    try {
      const accountId = req.params.accountId;
      const groupIds = Array.isArray(req.body?.groups) ? [...new Set(req.body.groups.filter(Boolean))] : [];
      const accounts = readAccounts();
      const account = accounts.find(a => a.id === accountId);
      if (!account) return res.status(404).json({ success: false, error: "Account not found" });
      account.selectedGroups = groupIds;
      const connection = connections.get(accountId);
      if (connection) connection.forwardingStatus = { state: "idle", current: 0, total: 0, message: "Waiting" };
      saveAccounts(accounts);

      // Save Groups never joins groups. It saves forwarding selections and only publishes
      // invite links that WhatsApp confirms are valid. Existing invites are reported as duplicates.
      let verified = 0, added = 0, duplicates = 0, unverified = 0;
      const results = [];
      if (connection?.sock && typeof connection.sock.groupInviteCode === "function" &&
          typeof connection.sock.groupGetInviteInfo === "function") {
        let metadata = {};
        try { metadata = await withTimeout(() => connection.sock.groupFetchAllParticipating(), 15000, "Fetching group details timed out."); }
        catch (e) { console.warn("Could not fetch participating groups:", e?.message || e); }
        let nextGroupIndex = 0;
        const seenInviteCodes = new Set();
        const processSelectedGroup = async groupId => {
          try {
            const code = await withTimeout(
              () => connection.sock.groupInviteCode(groupId),
              12000,
              "WhatsApp did not return an invite code in time."
            );
            if (!code) {
              unverified++;
              results.push({ groupId, status: "unverified", message: "WhatsApp did not provide an invite code; not added to the library." });
              return;
            }
            if (seenInviteCodes.has(code)) {
              duplicates++;
              results.push({ groupId, code, status: "duplicate", message: "This invite was already processed in this save; duplicate ignored." });
              return;
            }
            seenInviteCodes.add(code);
            let inviteInfo;
            try {
              inviteInfo = await withTimeout(
                () => connection.sock.groupGetInviteInfo(code),
                12000,
                "WhatsApp invite verification timed out."
              );
            } catch { inviteInfo = null; }
            if (!inviteInfo?.id) {
              unverified++;
              results.push({ groupId, code, status: "unverified", message: "WhatsApp could not verify this invite; not added to the library." });
              return;
            }
            verified++;
            const existing = await mongoDb.collection("join_groups").findOne({ _id: code });
            const group = metadata?.[groupId];
            await saveSharedJoinGroup({
              code,
              link: "https://chat.whatsapp.com/" + code,
              name: group?.subject || group?.name || inviteInfo.subject || inviteInfo.name || "WhatsApp Group",
              source: "saved-group"
            });
            if (existing) {
              duplicates++;
              results.push({ groupId, code, status: "duplicate", message: "Invite already exists in the shared library; duplicate ignored." });
            } else {
              added++;
              results.push({ groupId, code, status: "added", message: "Invite verified by WhatsApp and added to the shared library." });
            }
          } catch (e) {
            unverified++;
            results.push({ groupId, status: "unverified", message: "Could not verify/publish invite: " + String(e?.message || e).slice(0, 160) });
          }
        };
        const worker = async () => {
          while (true) {
            const index = nextGroupIndex++;
            if (index >= groupIds.length) return;
            await processSelectedGroup(groupIds[index]);
          }
        };
        await Promise.all(Array.from({ length: Math.min(3, groupIds.length) }, () => worker()));
      } else if (groupIds.length) { unverified = groupIds.length; }
      const message = !connection?.sock
        ? "Forwarding selection saved. Connect this WhatsApp account to verify and publish its invite links."
        : `Saved ${groupIds.length} forwarding group${groupIds.length === 1 ? "" : "s"}. Shared invites: ${added} added, ${duplicates} duplicates ignored, ${unverified} unverified/not added. No groups were joined.`;
      res.json({ success: true, account: accountPublic(account), verified, added, duplicates, unverified, results, message });
    } catch (error) {
      res.status(500).json({ success: false, error: error?.message || "Failed to save groups" });
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
        attempting: 0, skipped: initiallySkipped, startedAt: Date.now(), etaSeconds: Math.ceil(selectedGroups.length / 30) * 30,
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
    const startedAt = Number(entry.forwardingStatus?.startedAt || Date.now());
    for (let batchStart = 0; batchStart < selectedGroups.length; batchStart += 30) {
      if (entry.stopForwardingRequested) { wasStopped = true; break; }
      const batchGroups = selectedGroups.slice(batchStart, batchStart + 30);
      const updateBatchProgress = () => {
        const elapsedSeconds = Math.floor((Date.now() - startedAt) / 1000);
        const remainingBatches = Math.ceil((selectedGroups.length - batchStart) / 30);
        entry.forwardingStatus = {
          state: "forwarding", current: successCount, total: selectedGroups.length,
          attempting: batchStart + batchGroups.length, skipped: skippedLocked,
          startedAt, etaSeconds: elapsedSeconds + remainingBatches * 30,
          message: `Forwarded ${successCount}/${selectedGroups.length}; processing batch ${Math.floor(batchStart / 30) + 1}/${Math.ceil(selectedGroups.length / 30)}; skipped ${skippedLocked} locked`
        };
      };
      updateBatchProgress();
      await Promise.all(batchGroups.map(async (groupId) => {
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
        updateBatchProgress();
      }));
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

async function shutdownServer() {
  if (shuttingDown) return;
  shuttingDown = true;
  for (const timer of socketRetryTimers.values()) clearTimeout(timer);
  socketRetryTimers.clear();
  for (const timer of socketLeaseTimers.values()) clearInterval(timer);
  socketLeaseTimers.clear();
  for (const entry of connections.values()) {
    try { entry.sock?.end?.(new Error("Server shutting down for deployment")); } catch {}
  }
  try {
    if (mongoDb) await mongoDb.collection("whatsapp_socket_locks").deleteMany({ owner: instanceOwner });
  } catch (error) {
    console.warn("Could not release WhatsApp session leases during shutdown:", error?.message || error);
  }
  try { await mongoClient.close(); } catch {}
}

for (const signal of ["SIGTERM", "SIGINT"]) {
  process.once(signal, () => {
    shutdownServer().finally(() => process.exit(0));
    setTimeout(() => process.exit(0), 5000).unref();
  });
}

startServer();
