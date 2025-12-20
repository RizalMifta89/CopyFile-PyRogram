import os
import asyncio
import random
import re
import logging
from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait
from aiohttp import web # Tambahan untuk Web Service

# --- KONFIGURASI DARI RENDER ---
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
CMD_CHANNEL_ID = int(os.environ.get("CMD_CHANNEL_ID", 0)) 
OWNER_ID = int(os.environ.get("OWNER_ID", 0))
# Render otomatis memberi PORT, kalau lokal pakai 8080
PORT = int(os.environ.get("PORT", 8080)) 

# Logger setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RenderBot")

# Inisialisasi Client
if not API_ID or not API_HASH or not SESSION_STRING:
    logger.error("❌ ERROR: Variable Environment belum diisi lengkap!")
    exit(1)

app = Client(
    "render_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    in_memory=True
)

# Global Flags
IS_WORKING = False
STOP_EVENT = asyncio.Event()

# --- FUNGSI PARSING & HELPER (Sama seperti sebelumnya) ---

def parse_link(link):
    if not link: return None, None
    private_match = re.search(r"t\.me/c/(\d+)/(\d+)", link)
    if private_match:
        return int("-100" + private_match.group(1)), int(private_match.group(2))
    public_match = re.search(r"t\.me/([^/]+)/(\d+)", link)
    if public_match:
        return public_match.group(1), int(public_match.group(2))
    return None, None

async def resolve_peer(chat_id):
    try:
        await app.get_chat(chat_id)
        return True
    except:
        return False

# --- WORKER UTAMA ---

async def copy_worker(config, status_msg):
    global IS_WORKING
    IS_WORKING = True
    STOP_EVENT.clear()

    src_start = config['start_id']
    src_end = config['end_id']
    src_chat = config['src_chat']
    dst_chat = config['dst_chat']
    dst_topic = config['dst_topic']
    delay_min, delay_max = config['delay_range']
    batch_size = config['batch_size']
    batch_wait = config['batch_wait']

    processed_count = 0
    current_id = src_start

    await status_msg.edit(f"🔄 **Menyiapkan Akses...**")
    if not await resolve_peer(src_chat) or not await resolve_peer(dst_chat):
        await status_msg.edit(f"❌ **Gagal!** Bot tidak bisa akses Sumber/Tujuan.")
        IS_WORKING = False
        return

    await status_msg.edit(f"🚀 **Mulai Copy...**\n`{src_start}` ➔ `{src_end}`")

    try:
        while current_id <= src_end:
            if STOP_EVENT.is_set():
                await status_msg.edit("⏹ **Dihentikan.**")
                break

            try:
                msg = await app.get_messages(src_chat, current_id)
                if msg and not msg.empty and not msg.service:
                    if msg.sticker:
                        pass
                    else:
                        try:
                            await msg.copy(
                                chat_id=dst_chat,
                                reply_to_message_id=dst_topic if dst_topic else None
                            )
                            processed_count += 1
                            await asyncio.sleep(random.randint(delay_min, delay_max))
                        except FloodWait as e:
                            await status_msg.edit(f"⚠️ **FloodWait** {e.value}s...")
                            await asyncio.sleep(e.value + 5)
                            continue

                if processed_count > 0 and processed_count % batch_size == 0:
                    await status_msg.edit(f"☕ **Istirahat Batch**\nID: {current_id}\nTidur {batch_wait}s...")
                    await asyncio.sleep(batch_wait)
                    await status_msg.edit(f"🚀 **Lanjut...** ID: {current_id}")

            except Exception:
                pass

            if current_id % 20 == 0:
                 try: await status_msg.edit(f"🏃 **Proses:** {current_id}/{src_end}\nSukses: {processed_count}")
                 except: pass

            current_id += 1

        if not STOP_EVENT.is_set():
            await status_msg.edit(f"✅ **Selesai!** Total: {processed_count}")

    except Exception as e:
        await status_msg.edit(f"❌ Error: {e}")
    finally:
        IS_WORKING = False

# --- HANDLER COMMAND ---

@app.on_message(filters.chat(CMD_CHANNEL_ID) & filters.user(OWNER_ID) & filters.command("copy"))
async def start_handler(client, message):
    global IS_WORKING
    if IS_WORKING: return await message.reply("⚠️ Sedang sibuk.")
    
    text = message.text
    config = {}
    try:
        for line in text.split('\n'):
            if "sumber_awal:" in line: config['src_start_link'] = line.split(":", 1)[1].strip()
            if "sumber_akhir:" in line: config['src_end_link'] = line.split(":", 1)[1].strip()
            if "tujuan:" in line: config['dst_link'] = line.split(":", 1)[1].strip()
            if "jeda:" in line: 
                parts = line.split(":", 1)[1].strip().split("-")
                config['delay_range'] = (int(parts[0]), int(parts[1]))
            if "batch:" in line: config['batch_size'] = int(line.split(":", 1)[1].strip())
            if "jeda_batch:" in line: config['batch_wait'] = int(line.split(":", 1)[1].strip())

        src_chat, start_id = parse_link(config.get('src_start_link'))
        _, end_id = parse_link(config.get('src_end_link'))
        dst_chat, dst_topic = parse_link(config.get('dst_link'))

        if not src_chat or not dst_chat: return await message.reply("❌ Link salah.")

        job_config = {
            'src_chat': src_chat, 'start_id': start_id, 'end_id': end_id,
            'dst_chat': dst_chat, 'dst_topic': dst_topic,
            'delay_range': config.get('delay_range', (5, 10)),
            'batch_size': config.get('batch_size', 50),
            'batch_wait': config.get('batch_wait', 300)
        }
        msg = await message.reply("⚙️ Memulai worker...")
        asyncio.create_task(copy_worker(job_config, msg))
    except Exception as e: await message.reply(f"❌ Error: {e}")

@app.on_message(filters.chat(CMD_CHANNEL_ID) & filters.user(OWNER_ID) & filters.command("stop"))
async def stop_handler(client, message):
    if IS_WORKING:
        STOP_EVENT.set()
        await message.reply("🛑 Mengerem...")
    else:
        await message.reply("💤 Bot idle.")

@app.on_message(filters.chat(CMD_CHANNEL_ID) & filters.user(OWNER_ID) & filters.command("ping"))
async def ping_handler(client, message):
    await message.reply("🏓 Pong! Web Service Aktif.")

# --- WEB SERVER (AGAR RENDER TIDAK MEMATIKAN BOT) ---

async def web_health_check(request):
    return web.Response(text="Bot Sedang Berjalan! Jangan dimatikan.")

async def start_web_server():
    server = web.Application()
    server.add_routes([web.get('/', web_health_check)])
    runner = web.AppRunner(server)
    await runner.setup()
    # Bind ke 0.0.0.0 dan PORT dari Render
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"🌍 Web Server berjalan di Port {PORT}")

async def main():
    # Jalanin Web Server dulu
    await start_web_server()
    # Jalanin Bot Telegram
    logger.info("🤖 Menjalankan Userbot...")
    await app.start()
    await idle()
    await app.stop()

if __name__ == "__main__":
    # Loop Asyncio Utama
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())