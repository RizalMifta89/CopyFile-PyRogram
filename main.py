import os
import asyncio
import random
import logging
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError
from aiohttp import web

# ==========================================
# KONFIGURASI (WAJIB LENGKAP)
# ==========================================
SESSION_STRING = os.environ.get("SESSION_STRING", "")
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
CMD_CHANNEL_ID = os.environ.get("CMD_CHANNEL_ID", None)

# --- BAGIAN YANG BARU ANDA TAMBAHKAN ---
API_ID = os.environ.get("API_ID", None)
API_HASH = os.environ.get("API_HASH", None)
# ---------------------------------------

if CMD_CHANNEL_ID:
    CMD_CHANNEL_ID = int(CMD_CHANNEL_ID)
    
PORT = int(os.environ.get("PORT", "8080"))

CONFIG = {
    "delay_min": 4, "delay_max": 7,
    "sleep_every": 50, "sleep_duration": 60,
    "log_channel": None, "target_chat": None, "target_topic": None
}

STATUS = {
    "is_running": False, "is_paused": False,
    "current_id": 0, "total_success": 0, "total_failed": 0, "task": None
}

# Inisialisasi Client dengan API ID & HASH (WAJIB)
if not API_ID or not API_HASH:
    print("❌ FATAL ERROR: API_ID atau API_HASH belum diisi di Render!")
else:
    # Konversi API_ID ke integer
    API_ID = int(API_ID)

app = Client(
    "my_render_bot",
    api_id=API_ID,       # <--- PENTING
    api_hash=API_HASH,   # <--- PENTING
    session_string=SESSION_STRING
)

# ==========================================
# FILTER & WEB SERVER
# ==========================================
def is_authorized(_, __, message):
    is_owner = message.from_user and message.from_user.id == OWNER_ID
    is_cmd_channel = CMD_CHANNEL_ID and message.chat and message.chat.id == CMD_CHANNEL_ID
    return is_owner or is_cmd_channel

auth_filter = filters.create(is_authorized)

async def web_server():
    async def handle(request): return web.Response(text="Bot Userbot Live...")
    server = web.Application()
    server.router.add_get("/", handle)
    runner = web.AppRunner(server)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"🌍 Web Server berjalan di Port {PORT}")

def parse_link(link):
    try:
        if 't.me/c/' in link:
            parts = link.split('/')
            chat = int('-100' + parts[4])
            msg_id = int(parts[-1])
            return chat, msg_id
        elif 't.me/' in link:
            parts = link.split('/')
            chat = parts[3] # Username
            msg_id = int(parts[-1])
            return chat, msg_id
    except: return None, None

async def send_log(text):
    print(text)
    if CMD_CHANNEL_ID:
        try: await app.send_message(CMD_CHANNEL_ID, f"💬 {text}")
        except: pass

async def background_worker(src_chat, start_id, end_id, message_cmd):
    STATUS["is_running"] = True
    STATUS["total_success"] = 0
    STATUS["total_failed"] = 0
    
    await message_cmd.reply(f"🚀 **Gas!**\nSumber: `{src_chat}`\nID: {start_id} - {end_id}")
    
    try:
        # Resolve Username
        chat_info = await app.get_chat(src_chat)
        real_chat_id = chat_info.id
        print(f"✅ Resolved: {src_chat} -> {real_chat_id}")
    except Exception as e:
        await message_cmd.reply(f"❌ **GAGAL AKSES SUMBER!**\nBot tidak bisa membuka `{src_chat}`.\nError: `{e}`")
        STATUS["is_running"] = False
        return

    for current_id in range(start_id, end_id + 1):
        while STATUS["is_paused"]: await asyncio.sleep(1)
        if not STATUS["is_running"]: break

        STATUS["current_id"] = current_id
        try:
            msg = await app.get_messages(real_chat_id, current_id)
            
            should_send = False
            if msg.empty: pass
            elif msg.media: should_send = True
            elif msg.text: should_send = False
            
            if should_send:
                target = CONFIG["target_chat"]
                if target == "me":
                    await app.copy_message(chat_id="me", from_chat_id=msg.chat.id, message_id=msg.id, caption=msg.caption)
                else:
                    await app.copy_message(chat_id=target, from_chat_id=msg.chat.id, message_id=msg.id, caption=msg.caption, reply_to_message_id=CONFIG["target_topic"])
                
                STATUS["total_success"] += 1
                
                if STATUS["total_success"] % CONFIG["sleep_every"] == 0:
                    await send_log(f"☕ Istirahat {CONFIG['sleep_duration']}s dulu...")
                    await asyncio.sleep(CONFIG['sleep_duration'])
                else:
                    await asyncio.sleep(random.uniform(CONFIG["delay_min"], CONFIG["delay_max"]))
            else:
                await asyncio.sleep(0.5)

        except FloodWait as e:
            await send_log(f"⚠️ **Kena Limit!** Tidur {e.value}s.")
            await asyncio.sleep(e.value + 5)
        except RPCError as e:
            await send_log(f"❌ **Telegram Error ID {current_id}:** `{e}`")
            STATUS["total_failed"] += 1
        except Exception as e:
            print(f"Err {current_id}: {e}")
            STATUS["total_failed"] += 1
            await asyncio.sleep(1)

    await send_log(f"🏁 **SELESAI!** Sukses: {STATUS['total_success']}")
    STATUS["is_running"] = False

# ==========================================
# COMMAND HANDLERS
# ==========================================
@app.on_message(filters.command("start") & auth_filter)
async def start_cmd(_, message): await message.reply("👋 **Bot Ready!**\nPastikan API_ID & HASH sudah diset di Render.")

@app.on_message(filters.command("set_target") & auth_filter)
async def set_target(_, message):
    try:
        val = message.command[1]
        if val.lower() == "me":
            CONFIG["target_chat"] = "me"
            CONFIG["target_topic"] = None
            await message.reply("✅ Target: **Saved Messages**")
        else:
            CONFIG["target_chat"] = int(val)
            CONFIG["target_topic"] = int(message.command[2]) if len(message.command) > 2 else None
            await message.reply(f"✅ Target: `{CONFIG['target_chat']}`")
    except: await message.reply("❌ Fail. `/set_target -100xxx 0` atau `/set_target me 0`")

@app.on_message(filters.command("copy") & auth_filter)
async def copy_start(_, message):
    if STATUS["is_running"]: return await message.reply("⚠️ Masih jalan!")
    if not CONFIG["target_chat"]: return await message.reply("⚠️ Target belum diset!")
    
    try:
        src, s_id = parse_link(message.command[1])
        _, e_id = parse_link(message.command[2])
        if not src: return await message.reply("❌ Link Salah.")
        STATUS["task"] = asyncio.create_task(background_worker(src, s_id, e_id, message))
    except Exception as e: await message.reply(f"❌ Error Init: {e}")

@app.on_message(filters.command("config") & auth_filter)
async def config_set(_, message):
    try:
        CONFIG["delay_min"] = float(message.command[1])
        CONFIG["delay_max"] = float(message.command[2])
        CONFIG["sleep_every"] = int(message.command[3])
        CONFIG["sleep_duration"] = int(message.command[4])
        await message.reply("✅ Config Updated")
    except: await message.reply("❌ Fail")

@app.on_message(filters.command("status") & auth_filter)
async def status_cmd(_, message):
    await message.reply(f"📊 Run: {STATUS['is_running']} | ID: {STATUS['current_id']} | OK: {STATUS['total_success']}")

@app.on_message(filters.command("stop") & auth_filter)
async def stop_cmd(_, message):
    STATUS["is_running"] = False
    if STATUS["task"]: STATUS["task"].cancel()
    await message.reply("🛑 Stopped.")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(web_server())
    app.run()
