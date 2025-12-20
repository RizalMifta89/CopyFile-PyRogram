import os
import asyncio
import random
import re
import logging
import sys
from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait
from aiohttp import web

# --- LOGGING ---
# Memaksa log keluar ke konsol Render
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("RenderBot")

def debug_log(text):
    print(f"[DEBUG] {text}", flush=True)

debug_log("--- SYSTEM BOOT ---")

# --- KONFIGURASI ---
try:
    API_ID = int(os.environ.get("API_ID", 0))
    API_HASH = os.environ.get("API_HASH", "")
    SESSION_STRING = os.environ.get("SESSION_STRING", "")
    CMD_CHANNEL_ID = int(os.environ.get("CMD_CHANNEL_ID", 0)) 
    PORT = int(os.environ.get("PORT", 8080)) # Default port Render
except Exception as e:
    debug_log(f"❌ Config Error: {e}")

# Inisialisasi Client
app = Client(
    "render_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    in_memory=True
)

IS_WORKING = False
STOP_EVENT = asyncio.Event()

# --- FUNGSI HELPER ---
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

# --- WORKER ---
async def copy_worker(config, status_msg):
    global IS_WORKING
    IS_WORKING = True
    STOP_EVENT.clear()
    
    src_chat = config['src_chat']
    dst_chat = config['dst_chat']
    current_id = config['start_id']
    end_id = config['end_id']
    
    await status_msg.edit(f"🚀 **Mulai Copy...**\n`{current_id}` ➔ `{end_id}`")

    try:
        while current_id <= end_id:
            if STOP_EVENT.is_set():
                await status_msg.edit("⏹ **Stop.**")
                break
            try:
                msg = await app.get_messages(src_chat, current_id)
                if msg and not msg.empty and not msg.service and not msg.sticker:
                    await msg.copy(
                        chat_id=dst_chat,
                        reply_to_message_id=config['dst_topic'] if config['dst_topic'] else None
                    )
                    await asyncio.sleep(random.randint(*config['delay_range']))
            except FloodWait as e:
                await asyncio.sleep(e.value + 5)
            except Exception:
                pass
            
            if current_id % 20 == 0:
                try: await status_msg.edit(f"🏃 **Proses:** {current_id}")
                except: pass
            
            current_id += 1
        
        if not STOP_EVENT.is_set():
            await status_msg.edit("✅ **Selesai.**")
    except Exception as e:
        debug_log(f"Worker Error: {e}")
    finally:
        IS_WORKING = False

# --- HANDLER ---
# Handler untuk melihat pesan masuk (DEBUG)
@app.on_message(filters.chat(CMD_CHANNEL_ID), group=-1)
async def spy(client, message):
    print(f"📩 PESAN MASUK: {message.text}", flush=True)

@app.on_message(filters.chat(CMD_CHANNEL_ID) & filters.command("copy"))
async def start_cmd(client, message):
    global IS_WORKING
    if IS_WORKING: return await message.reply("⚠️ Sibuk.")
    
    # Simple Parser
    try:
        txt = message.text
        conf = {}
        for l in txt.split('\n'):
            if "sumber_awal:" in l: conf['src_start'] = l.split(":", 1)[1].strip()
            if "sumber_akhir:" in l: conf['src_end'] = l.split(":", 1)[1].strip()
            if "tujuan:" in l: conf['dst'] = l.split(":", 1)[1].strip()
            if "jeda:" in l: conf['d'] = [int(x) for x in l.split(":", 1)[1].strip().split("-")]
            if "batch:" in l: conf['b'] = int(l.split(":", 1)[1].strip())
            if "jeda_batch:" in l: conf['bw'] = int(l.split(":", 1)[1].strip())

        src_chat, start_id = parse_link(conf.get('src_start'))
        _, end_id = parse_link(conf.get('src_end'))
        dst_chat, dst_topic = parse_link(conf.get('dst'))

        if not src_chat or not dst_chat: return await message.reply("❌ Link Salah")

        job = {
            'src_chat': src_chat, 'start_id': start_id, 'end_id': end_id,
            'dst_chat': dst_chat, 'dst_topic': dst_topic,
            'delay_range': conf.get('d', [5, 10]),
            'batch_size': conf.get('b', 50), 'batch_wait': conf.get('bw', 300)
        }
        m = await message.reply("⚙️ Mulai...")
        asyncio.create_task(copy_worker(job, m))
    except Exception as e:
        await message.reply(f"❌ Error: {e}")

@app.on_message(filters.chat(CMD_CHANNEL_ID) & filters.command("stop"))
async def stop_cmd(client, message):
    if IS_WORKING:
        STOP_EVENT.set()
        await message.reply("🛑 Stop.")

@app.on_message(filters.chat(CMD_CHANNEL_ID) & filters.command("ping"))
async def ping_cmd(client, message):
    await message.reply("🏓 Pong!")

# --- WEB SERVER (PENTING AGAR TIDAK DISCONNECTED) ---
async def web_handler(request):
    return web.Response(text="Bot is Running correctly.")

async def start_web():
    debug_log(f"🌍 Starting Web Server on Port {PORT}")
    app_web = web.Application()
    app_web.add_routes([web.get('/', web_handler)])
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    debug_log("✅ Web Server Running!")

# --- MAIN LOOP ---
async def main():
    # 1. Start Web Server DULUAN (Supaya Render senang)
    await start_web()
    
    # 2. Start Bot Telegram (Pakai try-except agar web server tidak ikut mati jika bot gagal)
    debug_log("🤖 Starting Telegram Client...")
    try:
        await app.start()
        
        # Pancing Cache Dialogs
        debug_log("📚 Refreshing Cache...")
        async for d in app.get_dialogs(limit=20): pass
        
        debug_log("✅ TELEGRAM LOGIN SUCCESS!")
        
        # Coba kirim pesan ke channel
        try:
            await app.send_message(CMD_CHANNEL_ID, "✅ **Bot Hidup Kembali!**")
        except Exception as e:
            debug_log(f"⚠️ Gagal kirim pesan awal: {e}")

        # Masuk mode Idle (Tunggu perintah)
        await idle()
        
    except Exception as e:
        debug_log(f"❌ TELEGRAM ERROR: {e}")
        debug_log("⚠️ Bot gagal login, tapi Web Server tetap jalan agar log bisa dibaca.")
        
        # Loop abadi supaya Render TIDAK mematikan service
        # Ini kunci agar Anda bisa baca log errornya!
        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
