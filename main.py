import os
import asyncio
import gc
import time
import re
import random
import sys
from urllib.parse import urlparse, parse_qs

from pyrogram import Client, enums, errors
from aiohttp import web
import uvloop

# Install uvloop for better performance (if available)
try:
    uvloop.install()
except Exception:
    pass

# ===== ENVIRONMENT VARIABLES =====
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
CMD_CHANNEL_ID = int(os.getenv("CMD_CHANNEL_ID"))

# ===== GLOBAL STATE =====
COPY_JOB = None
LOG_MESSAGE_ID = None  # ID pesan log di CMD_CHANNEL_ID
app = Client("userbot", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)

# ===== UTILS =====
def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

def parse_telegram_link(url):
    url = url.strip()
    if url.startswith("https://t.me/"):
        parsed = urlparse(url)
        path = parsed.path.lstrip("/")
        query = parse_qs(parsed.query)

        if "/c/" in url:
            # Private channel: t.me/c/chat_id/message_id
            parts = path.split("/")[1:]  # ['123456789', '100']
            if len(parts) >= 2:
                chat_id = int(f"-100{parts[0]}")
                msg_id = int(parts[1])
                return chat_id, msg_id
        else:
            # Public channel: t.me/username/123
            parts = path.split("/")
            if len(parts) >= 2:
                username = parts[0]
                msg_id = int(parts[1])
                return username, msg_id
    raise ValueError("Invalid Telegram link")

def extract_thread_id(url):
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    thread = query.get("thread") or query.get("topic")
    if thread:
        return int(thread[0])
    return None

async def safe_copy_message(from_chat, msg_id, to_chat, thread_id=None, filters=None):
    if filters is None:
        filters = {}

    try:
        msg = await app.get_messages(from_chat, msg_id)
        if not msg:
            return False

        # Filter: skip based on media type
        if msg.sticker and filters.get("skip_sticker", True):
            log(f"⏭️ Skip stiker: {msg_id}")
            return True

        if msg.photo and filters.get("skip_photo", False):
            log(f"⏭️ Skip foto: {msg_id}")
            return True

        if msg.video and filters.get("skip_video", False):
            log(f"⏭️ Skip video: {msg_id}")
            return True

        if msg.document and filters.get("skip_document", False):
            log(f"⏭️ Skip dokumen: {msg_id}")
            return True

        if filters.get("only_media", False):
            if not (msg.photo or msg.video or msg.document or msg.audio or msg.voice or msg.animation):
                log(f"⏭️ Skip non-media: {msg_id}")
                return True

        # Copy message
        await asyncio.wait_for(
            app.copy_message(
                chat_id=to_chat,
                from_chat_id=from_chat,
                message_id=msg_id,
                message_thread_id=thread_id,
                disable_notification=True,
                remove_caption_mentions=True
            ),
            timeout=25  # < Render NAT timeout
        )
        return True

    except asyncio.TimeoutError:
        log(f"⚠️ Timeout pada pesan {msg_id}, skip")
        return False
    except errors.FloodWait as e:
        log(f"⏳ FloodWait {e.value} detik")
        await asyncio.sleep(e.value + 5)
        return False
    except (ConnectionError, OSError, errors.InterdcError, errors.RpcCallFail) as e:
        log(f"🔌 Error jaringan pada {msg_id}: {e}. Akan reconnect...")
        raise
    except Exception as e:
        log(f"❌ Error tak dikenal pada {msg_id}: {e}")
        return False
    finally:
        gc.collect()

async def update_progress(current, total, to_chat, thread_id, min_delay, max_delay, batch_size):
    global LOG_MESSAGE_ID
    try:
        progress = f"{current} / {total}"
        avg_delay = (min_delay + max_delay) / 2
        remaining = total - current
        est_seconds = remaining * avg_delay
        est_minutes = max(1, int(est_seconds // 60))

        chat_title = "Unknown"
        try:
            chat = await app.get_chat(to_chat)
            chat_title = getattr(chat, "username", str(to_chat))
        except:
            pass

        thread_info = f" (Topik: {thread_id})" if thread_id else ""
        text = (
            f"📥 Copy progres: {progress}\n"
            f"⏱️ Jeda: {min_delay}–{max_delay} detik | Batch: {batch_size}\n"
            f"📤 Tujuan: @{chat_title}{thread_info}\n"
            f"⏳ Estimasi: ~{est_minutes} menit\n"
            f"🔄 Terakhir: Pesan ID {current}"
        )

        if LOG_MESSAGE_ID is None:
            sent = await app.send_message(CMD_CHANNEL_ID, text)
            LOG_MESSAGE_ID = sent.id
        else:
            try:
                await app.edit_message_text(CMD_CHANNEL_ID, LOG_MESSAGE_ID, text)
            except errors.MessageIdInvalid:
                # Pesan dihapus manual — buat baru
                sent = await app.send_message(CMD_CHANNEL_ID, text)
                LOG_MESSAGE_ID = sent.id

    except Exception as e:
        log(f"⚠️ Gagal update progres: {e}")

# ===== BACKGROUND TASKS =====
async def keep_alive():
    """Jaga koneksi MTProto tetap hidup."""
    while True:
        try:
            await app.send_chat_action("me", enums.ChatAction.TYPING)
        except:
            pass
        await asyncio.sleep(20)

async def copy_worker():
    global COPY_JOB, LOG_MESSAGE_ID
    while True:
        if COPY_JOB:
            job = COPY_JOB
            current = job["current"]
            if current > job["end"]:
                await app.send_message(CMD_CHANNEL_ID, "✅ ✅ Selesai! Semua pesan berhasil disalin.")
                COPY_JOB = None
                LOG_MESSAGE_ID = None
                continue

            try:
                success = await safe_copy_message(
                    job["from_chat"],
                    current,
                    job["to_chat"],
                    job.get("thread_id"),
                    job.get("filters", {})
                )

                if success:
                    job["current"] += 1

                    # Update progres tiap 20 pesan
                    if (job["current"] - job["start"]) % 20 == 1:
                        await update_progress(
                            job["current"] - 1,
                            job["end"],
                            job["to_chat"],
                            job.get("thread_id"),
                            job["min_delay"],
                            job["max_delay"],
                            job["batch_size"]
                        )

                # Jeda antar pesan
                delay = random.uniform(job["min_delay"], job["max_delay"])
                await asyncio.sleep(delay)

                # Jeda batch
                if (job["current"] - job["start"]) % job["batch_size"] == 0:
                    batch_delay = random.uniform(job["min_batch_delay"], job["max_batch_delay"])
                    await asyncio.sleep(batch_delay)

            except (ConnectionError, OSError, errors.InterdcError, errors.RpcCallFail):
                # Reconnect
                log("🔌 Reconnecting...")
                try:
                    await app.stop()
                except:
                    pass
                await asyncio.sleep(5)
                await app.start()
                log("✅ Reconnected.")

            except Exception as e:
                log(f"💀 Error kritis di worker: {e}")
                await asyncio.sleep(5)

        else:
            await asyncio.sleep(5)

# ===== COMMAND HANDLER =====
@app.on_message(filters.chat(CMD_CHANNEL_ID) & filters.regex(r"^/copy"))
async def copy_cmd(client, message):
    global COPY_JOB
    if COPY_JOB:
        await message.reply("⏳ Proses sedang berjalan. Tunggu selesai atau restart service.")
        return

    text = message.text
    lines = text.strip().split("\n")[1:]  # Skip "/copy"

    config = {}
    for line in lines:
        if ":" in line:
            key, val = line.split(":", 1)
            config[key.strip().lower()] = val.strip()

    try:
        # Parsing links
        from_chat, start_id = parse_telegram_link(config["sumber_awal"])
        _, end_id = parse_telegram_link(config["sumber_akhir"])
        to_chat, _ = parse_telegram_link(config["tujuan"])
        thread_id = extract_thread_id(config["tujuan"])

        # Parsing delays
        jeda = config.get("jeda", "3-7")
        jeda_parts = list(map(int, jeda.split("-")))
        min_delay, max_delay = jeda_parts[0], jeda_parts[1]

        batch = int(config.get("batch", "10"))
        jeda_batch = config.get("jeda_batch", "20-40")
        jeda_batch_parts = list(map(int, jeda_batch.split("-")))
        min_batch_delay, max_batch_delay = jeda_batch_parts[0], jeda_batch_parts[1]

        # Parsing filters
        filters = {
            "skip_sticker": config.get("skip_sticker", "true").lower() == "true",
            "skip_photo": config.get("skip_photo", "false").lower() == "true",
            "skip_video": config.get("skip_video", "false").lower() == "true",
            "skip_document": config.get("skip_document", "false").lower() == "true",
            "only_media": config.get("only_media", "false").lower() == "true",
        }

        COPY_JOB = {
            "from_chat": from_chat,
            "to_chat": to_chat,
            "thread_id": thread_id,
            "start": start_id,
            "end": end_id,
            "current": start_id,
            "min_delay": min_delay,
            "max_delay": max_delay,
            "batch_size": batch,
            "min_batch_delay": min_batch_delay,
            "max_batch_delay": max_batch_delay,
            "filters": filters
        }

        await message.reply("🟢 Mulai proses copy...\nSaya akan kirim progres di sini.")
        LOG_MESSAGE_ID = None  # Reset log message

    except Exception as e:
        await message.reply(f"❌ Error parsing: {e}")
        log(f"Parsing error: {e}")

# ===== DUMMY WEB SERVER (for Render) =====
async def healthcheck(request):
    return web.Response(text="OK", content_type="text/plain")

# ===== MAIN =====
async def main():
    # Start Pyrogram
    await app.start()
    log("✅ Userbot started")

    # Preload peer cache
    try:
        await app.get_dialogs(limit=20)
        log("📥 Dialogs loaded (peer cache ready)")
    except Exception as e:
        log(f"⚠️ Gagal preload dialogs: {e}")

    # Start background tasks
    asyncio.create_task(copy_worker())
    asyncio.create_task(keep_alive())

    # Start web server
    web_app = web.Application()
    web_app.router.add_get("/", healthcheck)
    runner = web.AppRunner(web_app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log(f"🌐 Web server running on port {port}")

    # Keep alive forever
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("🛑 Stopped by user")
    except Exception as e:
        log(f"💥 Fatal error: {e}")
        sys.exit(1)
