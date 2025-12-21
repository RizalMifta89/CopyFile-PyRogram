import os
import asyncio
import gc
import time
import re
import random
import sys
from urllib.parse import urlparse, parse_qs

from pyrogram import Client, enums, errors, filters
from pyrogram.handlers import MessageHandler
from aiohttp import web

# ===== ENVIRONMENT VARIABLES =====
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
CMD_CHANNEL_ID = int(os.getenv("CMD_CHANNEL_ID"))

# ===== GLOBAL STATE =====
COPY_JOB = None
LOG_MESSAGE_ID = None
app = None

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
            parts = path.split("/")[1:]
            if len(parts) >= 2:
                chat_id = int(f"-100{parts[0]}")
                msg_id = int(parts[1])
                return chat_id, msg_id
        else:
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

def parse_range(range_str, default_min=20, default_max=40):
    """Parse 'a-b' menjadi (min, max), aman terhadap spasi dan format salah."""
    try:
        clean = range_str.replace(" ", "")
        parts = clean.split("-")
        if len(parts) == 2:
            a, b = int(parts[0]), int(parts[1])
            return min(a, b), max(a, b)
        else:
            return default_min, default_max
    except:
        return default_min, default_max

# ===== HANDLER FUNCTIONS =====
async def copy_cmd(client, message):
    global COPY_JOB
    if COPY_JOB:
        await message.reply("⏳ Proses sedang berjalan. Tunggu selesai atau restart service.")
        return

    text = message.text
    lines = text.strip().split("\n")[1:]

    config = {}
    for line in lines:
        if ":" in line:
            key, val = line.split(":", 1)
            config[key.strip().lower()] = val.strip()

    try:
        from_chat, start_id = parse_telegram_link(config["sumber_awal"])
        _, end_id = parse_telegram_link(config["sumber_akhir"])
        to_chat, _ = parse_telegram_link(config["tujuan"])
        thread_id = extract_thread_id(config["tujuan"])

        # Parsing jeda dan jeda_batch dengan fungsi aman
        min_delay, max_delay = parse_range(config.get("jeda", "3-7"), 3, 7)
        batch = int(config.get("batch", "10"))
        min_batch_delay, max_batch_delay = parse_range(config.get("jeda_batch", "20-40"), 20, 40)

        filters_dict = {
            "skip_sticker": config.get("skip_sticker", "true").lower() == "true",
            "skip_photo": config.get("skip_photo", "false").lower() == "true",
            "skip_video": config.get("skip_video", "false").lower() == "true",
            "skip_document": config.get("skip_document", "false").lower() == "true",
            "only_media": config.get("only_media", "false").lower() == "true",
        }

        global COPY_JOB
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
            "filters": filters_dict
        }

        await message.reply("🟢 Mulai proses copy...\nSaya akan kirim progres di sini.")
        global LOG_MESSAGE_ID
        LOG_MESSAGE_ID = None

    except Exception as e:
        await message.reply(f"❌ Error parsing: {e}")
        log(f"Parsing error: {e}")

async def ping_cmd(client, message):
    await message.reply("🏓 Pong! Bot aktif dan terhubung ke Telegram.\n✅ Siap menerima perintah /copy.")

# ===== SAFE COPY FUNCTION =====
async def safe_copy_message(from_chat, msg_id, to_chat, thread_id=None, filters=None):
    if filters is None:
        filters = {}

    try:
        msg = await app.get_messages(from_chat, msg_id)
        if not msg:
            return False

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

        await asyncio.wait_for(
            app.copy_message(
                chat_id=to_chat,
                from_chat_id=from_chat,
                message_id=msg_id,
                message_thread_id=thread_id,
                disable_notification=True,
                remove_caption_mentions=True
            ),
            timeout=25
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

# ===== BACKGROUND TASKS =====
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
                sent = await app.send_message(CMD_CHANNEL_ID, text)
                LOG_MESSAGE_ID = sent.id
    except Exception as e:
        log(f"⚠️ Gagal update progres: {e}")

async def keep_alive():
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

                delay = random.uniform(job["min_delay"], job["max_delay"])
                await asyncio.sleep(delay)

                if (job["current"] - job["start"]) % job["batch_size"] == 0:
                    batch_delay = random.uniform(job["min_batch_delay"], job["max_batch_delay"])
                    await asyncio.sleep(batch_delay)

            except (ConnectionError, OSError, errors.InterdcError, errors.RpcCallFail):
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

# ===== DUMMY SERVER =====
async def healthcheck(request):
    return web.Response(text="OK", content_type="text/plain")

# ===== MAIN =====
async def main():
    global app
    app = Client(
        "userbot",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=SESSION_STRING
    )

    await app.start()
    log("✅ Userbot started")

    try:
        await app.get_dialogs(limit=20)
        log("📥 Dialogs loaded (peer cache ready)")
    except Exception as e:
        log(f"⚠️ Gagal preload dialogs: {e}")

    # Daftarkan handler
    copy_handler = MessageHandler(
        copy_cmd,
        filters.chat(CMD_CHANNEL_ID) & filters.regex(r"^/copy")
    )
    ping_handler = MessageHandler(
        ping_cmd,
        filters.chat(CMD_CHANNEL_ID) & filters.regex(r"^/ping")
    )
    app.add_handler(copy_handler)
    app.add_handler(ping_handler)

    # Start tasks
    asyncio.create_task(copy_worker())
    asyncio.create_task(keep_alive())

    # Web server
    web_app = web.Application()
    web_app.router.add_get("/", healthcheck)
    runner = web.AppRunner(web_app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log(f"🌐 Web server running on port {port}")

    while True:
        await asyncio.sleep(3600)

# ===== ENTRY =====
if __name__ == "__main__":
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        log("🛑 Stopped by user")
    except Exception as e:
        log(f"💥 Fatal error: {e}")
        sys.exit(1)
