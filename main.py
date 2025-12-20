import os
import asyncio
import random
import logging
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from aiohttp import web

# ==========================================
# KONFIGURASI
# ==========================================
SESSION_STRING = os.environ.get("SESSION_STRING", "")
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
CMD_CHANNEL_ID = os.environ.get("CMD_CHANNEL_ID", None)
if CMD_CHANNEL_ID:
    CMD_CHANNEL_ID = int(CMD_CHANNEL_ID)

PORT = int(os.environ.get("PORT", "8080"))

CONFIG = {
    "delay_min": 4,
    "delay_max": 7,
    "sleep_every": 50,
    "sleep_duration": 60,
    "log_channel": None,
    "target_chat": None,
    "target_topic": None
}

STATUS = {
    "is_running": False,
    "is_paused": False,
    "current_id": 0,
    "total_success": 0,
    "total_failed": 0,
    "task": None
}

app = Client("my_render_bot", session_string=SESSION_STRING)

# ==========================================
# FILTER IZIN
# ==========================================
def is_authorized(_, __, message):
    is_owner = message.from_user and message.from_user.id == OWNER_ID
    is_cmd_channel = False
    if CMD_CHANNEL_ID and message.chat:
        if message.chat.id == CMD_CHANNEL_ID:
            is_cmd_channel = True
    return is_owner or is_cmd_channel

auth_filter = filters.create(is_authorized)

# ==========================================
# HELPER: REFRESH DIALOGS
# ==========================================
async def force_refresh_dialogs():
    """Memancing cache agar kenal ID grup/channel"""
    # print("🔄 Auto-Refresh Cache...") # Debug di console
    count = 0
    try:
        # Baca 100 chat terakhir tempat bot bergabung
        async for dialog in app.get_dialogs(limit=100):
            count += 1
            _ = dialog.chat.id 
        return True, count
    except Exception as e:
        print(f"❌ Refresh Fail: {e}")
        return False, 0

# ==========================================
# WEB SERVER
# ==========================================
async def web_server():
    async def handle(request):
        return web.Response(text="Bot Userbot Berjalan 24 Jam...")
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
            chat = parts[3]
            msg_id = int(parts[-1])
            return chat, msg_id
    except:
        return None, None

async def send_log(text):
    if CONFIG["log_channel"]:
        try:
            await app.send_message(CONFIG["log_channel"], f"🤖 **BOT LOG:**\n{text}")
        except: pass
    if CMD_CHANNEL_ID:
        try:
            await app.send_message(CMD_CHANNEL_ID, f"💬 **Info:**\n{text}")
        except: pass

async def background_worker(src_chat, start_id, end_id, message_cmd):
    STATUS["is_running"] = True
    STATUS["total_success"] = 0
    STATUS["total_failed"] = 0
    
    start_msg = f"🚀 **Memulai Tugas!**\nSumber: `{src_chat}`\nRange: {start_id} - {end_id}"
    await message_cmd.reply(start_msg) 
    
    for current_id in range(start_id, end_id + 1):
        while STATUS["is_paused"]:
            await asyncio.sleep(1)
        if not STATUS["is_running"]:
            break

        STATUS["current_id"] = current_id
        try:
            msg = await app.get_messages(src_chat, current_id)
            should_send = False
            
            if msg.empty: pass
            elif msg.media and not msg.sticker: should_send = True
            
            if should_send:
                await app.copy_message(
                    chat_id=CONFIG["target_chat"],
                    from_chat_id=msg.chat.id,
                    message_id=msg.id,
                    caption=msg.caption,
                    reply_to_message_id=CONFIG["target_topic"]
                )
                STATUS["total_success"] += 1
                
                if STATUS["total_success"] % CONFIG["sleep_every"] == 0:
                    lapor = f"☕ **Istirahat**\nSukses: {STATUS['total_success']}\nTidur {CONFIG['sleep_duration']}s."
                    await send_log(lapor)
                    await asyncio.sleep(CONFIG['sleep_duration'])
                else:
                    delay = random.uniform(CONFIG["delay_min"], CONFIG["delay_max"])
                    await asyncio.sleep(delay)
            else:
                await asyncio.sleep(0.5)

        except FloodWait as e:
            await send_log(f"⚠️ **FloodWait** {e.value} detik.")
            await asyncio.sleep(e.value + 5)
        except Exception as e:
            # Jika masih error peer invalid saat loop, coba refresh lagi
            if "PEER_ID_INVALID" in str(e) or "CHANNEL_INVALID" in str(e):
                await force_refresh_dialogs()
            
            print(f"Err {current_id}: {e}")
            STATUS["total_failed"] += 1
            await asyncio.sleep(2)

    await send_log(f"🏁 **SELESAI!**\nSukses: {STATUS['total_success']}")
    CONFIG["target_chat"] = None
    STATUS["is_running"] = False

# ==========================================
# COMMAND HANDLERS
# ==========================================

@app.on_message(filters.command("start") & auth_filter)
async def start_cmd(_, message):
    await message.reply("👋 **Bot Ready!**\nFitur Auto-Refresh sudah aktif di `/copy`.")

@app.on_message(filters.command("refresh") & auth_filter)
async def refresh_cmd(_, message):
    msg = await message.reply("🔄 **Manual Refresh...**")
    success, count = await force_refresh_dialogs()
    if success:
        await msg.edit(f"✅ **Sukses!** Memuat {count} chat.")
    else:
        await msg.edit("❌ Gagal.")

@app.on_message(filters.command("help") & auth_filter)
async def help_cmd(_, message):
    await message.reply(
        "🛠 **MENU:**\n"
        "`/set_target [id] [topic]`\n"
        "`/copy [link1] [link2]` (Auto-Refresh)\n"
        "`/config [min] [max] [limit] [sleep]`\n"
        "`/status` `/stop` `/pause` `/resume`"
    )

@app.on_message(filters.command("set_target") & auth_filter)
async def set_target(_, message):
    try:
        # Auto refresh juga saat set target biar aman
        await force_refresh_dialogs()
        CONFIG["target_chat"] = int(message.command[1])
        CONFIG["target_topic"] = int(message.command[2]) if len(message.command) > 2 else None
        await message.reply("✅ Target OK (Cache Refreshed)")
    except:
        await message.reply("❌ Fail. Format: `/set_target -100xxxx 0`")

@app.on_message(filters.command("set_log") & auth_filter)
async def set_log(_, message):
    try:
        CONFIG["log_channel"] = int(message.command[1])
        await message.reply("✅ Log Channel OK")
    except:
        await message.reply("❌ Fail")

@app.on_message(filters.command("config") & auth_filter)
async def config_set(_, message):
    try:
        CONFIG["delay_min"] = float(message.command[1])
        CONFIG["delay_max"] = float(message.command[2])
        CONFIG["sleep_every"] = int(message.command[3])
        CONFIG["sleep_duration"] = int(message.command[4])
        await message.reply("✅ Config Updated")
    except:
        await message.reply("❌ Fail")

@app.on_message(filters.command("copy") & auth_filter)
async def copy_start(_, message):
    if STATUS["is_running"]:
        return await message.reply("⚠️ Sedang sibuk! `/stop` dulu.")
    if not CONFIG["target_chat"]:
        return await message.reply("⚠️ Set Target dulu!")
    
    try:
        # === FITUR BARU: AUTO REFRESH SEBELUM MULAI ===
        status_msg = await message.reply("🔄 **Menyiapkan cache & link...**")
        await force_refresh_dialogs()
        # ==============================================

        src, s_id = parse_link(message.command[1])
        _, e_id = parse_link(message.command[2])
        
        if not src: 
            return await status_msg.edit("❌ Link Invalid / Tidak Terbaca.")
        
        await status_msg.delete() # Hapus pesan loading
        STATUS["task"] = asyncio.create_task(background_worker(src, s_id, e_id, message))
    except IndexError:
        await message.reply("❌ Format Salah. Contoh:\n`/copy link1 link2`")
    except Exception as e:
        await message.reply(f"❌ Error: {e}")

@app.on_message(filters.command("status") & auth_filter)
async def status_cmd(_, message):
    await message.reply(f"📊 Running: {STATUS['is_running']}\nID: {STATUS['current_id']}\nSukses: {STATUS['total_success']}")

@app.on_message(filters.command("stop") & auth_filter)
async def stop_cmd(_, message):
    STATUS["is_running"] = False
    if STATUS["task"]: STATUS["task"].cancel()
    CONFIG["target_chat"] = None
    CONFIG["target_topic"] = None
    await message.reply("🛑 Stopped & Reset.")

@app.on_message(filters.command("pause") & auth_filter)
async def pause_cmd(_, message):
    STATUS["is_paused"] = True
    await message.reply("⏸️ Paused")

@app.on_message(filters.command("resume") & auth_filter)
async def resume_cmd(_, message):
    STATUS["is_paused"] = False
    await message.reply("▶️ Resume")

# ==========================================
# MAIN LOOP
# ==========================================
if __name__ == "__main__":
    print("🚀 Bot Starting...")
    loop = asyncio.get_event_loop()
    loop.create_task(web_server())
    loop.create_task(force_refresh_dialogs()) # Refresh awal saat boot
    app.run()
