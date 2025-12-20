import os
import asyncio
import random
import logging
from pyrogram import Client, filters
from pyrogram.errors import FloodWait

# ==========================================
# KONFIGURASI DARI ENVIRONMENT VARIABLE
# (Jangan ubah disini, ubah di Setting Render)
# ==========================================
SESSION_STRING = os.environ.get("SESSION_STRING", "")
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))

# Konfigurasi Default (Bisa diubah lewat Chat nanti)
CONFIG = {
    "delay_min": 4,         # Detik
    "delay_max": 7,         # Detik
    "sleep_every": 50,      # Jumlah pesan sebelum istirahat panjang
    "sleep_duration": 60,   # Lama istirahat panjang (detik)
    "log_channel": None,    # ID Channel untuk laporan otomatis
    "target_chat": None,    # ID Grup Tujuan
    "target_topic": None    # ID Topik Tujuan
}

# Status Global Bot
STATUS = {
    "is_running": False,
    "is_paused": False,
    "current_id": 0,
    "total_success": 0,
    "total_failed": 0,
    "task": None  # Menyimpan object background task
}

# Inisialisasi Client
app = Client("my_render_bot", session_string=SESSION_STRING)

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def is_owner(_, __, message):
    # Satpam: Hanya merespon jika pengirim adalah OWNER_ID
    return message.from_user and message.from_user.id == OWNER_ID

owner_filter = filters.create(is_owner)

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
    """Kirim log ke Channel yang sudah diset, atau reply ke user jika belum diset"""
    log_text = f"🤖 **BOT LOG:**\n{text}"
    if CONFIG["log_channel"]:
        try:
            await app.send_message(CONFIG["log_channel"], log_text)
        except Exception as e:
            print(f"Gagal kirim log ke channel: {e}")
    else:
        # Jika tidak ada channel log, print saja di console server
        print(text)

# ==========================================
# CORE WORKER (TUKANG COPY)
# ==========================================
async def background_worker(src_chat, start_id, end_id, message_cmd):
    STATUS["is_running"] = True
    STATUS["total_success"] = 0
    STATUS["total_failed"] = 0
    
    total_range = end_id - start_id + 1
    await message_cmd.reply(f"🚀 **Memulai Tugas!**\nSumber: `{src_chat}`\nRange: {start_id} - {end_id}\nTotal: {total_range} pesan.")
    
    for current_id in range(start_id, end_id + 1):
        # 1. Cek Pause
        while STATUS["is_paused"]:
            await asyncio.sleep(1) # Tidur 1 detik sambil nunggu resume
            
        # 2. Cek Stop Paksa (Task dibatalkan)
        if not STATUS["is_running"]:
            break

        STATUS["current_id"] = current_id
        
        try:
            # Ambil Pesan
            msg = await app.get_messages(src_chat, current_id)
            
            should_send = False
            tipe = "Unknown"

            if msg.empty:
                print(f"ID {current_id} Kosong/Terhapus")
            elif msg.media and not msg.sticker:
                should_send = True
                tipe = "Media"
            elif msg.text:
                # Disini kita skip teks biasa sesuai request lama, ubah jika perlu
                should_send = False 
                tipe = "Teks (Skip)"
            
            if should_send:
                await app.copy_message(
                    chat_id=CONFIG["target_chat"],
                    from_chat_id=msg.chat.id,
                    message_id=msg.id,
                    caption=msg.caption,
                    reply_to_message_id=CONFIG["target_topic"]
                )
                STATUS["total_success"] += 1
                print(f"✅ ID {current_id} Sukses")

                # --- ISTIRAHAT PANJANG ---
                if STATUS["total_success"] % CONFIG["sleep_every"] == 0:
                    lapor = f"☕ **Istirahat Panjang**\nSudah kirim: {STATUS['total_success']}\nBobo dulu {CONFIG['sleep_duration']} detik."
                    await send_log(lapor)
                    await asyncio.sleep(CONFIG['sleep_duration'])
                else:
                    # --- DELAY ACAK ---
                    delay = random.uniform(CONFIG["delay_min"], CONFIG["delay_max"])
                    await asyncio.sleep(delay)

            else:
                # Jika skip, beri jeda dikit biar gak ngebut banget
                await asyncio.sleep(0.5)

        except FloodWait as e:
            err_msg = f"⚠️ **TERKENA LIMIT (FLOODWAIT)**\nHarus tidur: {e.value} detik."
            await send_log(err_msg)
            await asyncio.sleep(e.value + 5)
        except Exception as e:
            print(f"Error ID {current_id}: {e}")
            STATUS["total_failed"] += 1
            await asyncio.sleep(2)

    # Selesai Loop
    finish_msg = (
        f"🏁 **TUGAS SELESAI!**\n"
        f"✅ Sukses: {STATUS['total_success']}\n"
        f"❌ Gagal/Skip: {STATUS['total_failed']}\n"
        f"📍 Sampai ID: {end_id}\n\n"
        f"🧹 *Memori Sumber & Target telah dibersihkan (Auto-Clear).*"
    )
    await send_log(finish_msg)
    
    # Auto Clear Data
    CONFIG["target_chat"] = None
    CONFIG["target_topic"] = None
    STATUS["is_running"] = False

# ==========================================
# HANDLERS (REMOTE CONTROL)
# ==========================================

@app.on_message(filters.command("start") & owner_filter)
async def start_cmd(_, message):
    await message.reply(
        "👋 **Halo Bos! Bot Siap Diperintah.**\n\n"
        "Gunakan `/help` untuk lihat daftar perintah."
    )

@app.on_message(filters.command("help") & owner_filter)
async def help_cmd(_, message):
    teks = (
        "🛠 **MENU REMOTE CONTROL**\n\n"
        "1️⃣ **Persiapan:**\n"
        "`/set_log [id_channel]` - Set channel laporan\n"
        "`/set_target [id_grup] [id_topik]` - Set tujuan\n"
        "`/config [min] [max] [setiap] [lama]` - Atur waktu\n\n"
        "2️⃣ **Eksekusi:**\n"
        "`/copy [link_awal] [link_akhir]` - Mulai jalan\n\n"
        "3️⃣ **Kontrol:**\n"
        "`/pause` - Jeda sementara\n"
        "`/resume` - Lanjut jalan\n"
        "`/status` - Cek progres live\n"
        "`/stop` - Berhenti & Reset data\n"
        "`/reset` - Hapus settingan target manual"
    )
    await message.reply(teks)

@app.on_message(filters.command("set_log") & owner_filter)
async def set_log(_, message):
    try:
        cid = int(message.command[1])
        CONFIG["log_channel"] = cid
        await message.reply(f"✅ **Log Channel Diset:** `{cid}`")
    except:
        await message.reply("❌ Format salah. Contoh: `/set_log -1001234567`")

@app.on_message(filters.command("set_target") & owner_filter)
async def set_target(_, message):
    try:
        chat_id = int(message.command[1])
        topic_id = int(message.command[2]) if len(message.command) > 2 else None
        CONFIG["target_chat"] = chat_id
        CONFIG["target_topic"] = topic_id
        await message.reply(f"🎯 **Target Terkunci!**\nGrup: `{chat_id}`\nTopik: `{topic_id}`")
    except:
        await message.reply("❌ Format salah. Contoh: `/set_target -10012345 55`\n(Isi 0 atau None pada topik jika tidak ada)")

@app.on_message(filters.command("config") & owner_filter)
async def config_set(_, message):
    try:
        # Urutan: min, max, setiap_brp, lama_tidur
        d_min = float(message.command[1])
        d_max = float(message.command[2])
        every = int(message.command[3])
        dur = int(message.command[4])
        
        CONFIG["delay_min"] = d_min
        CONFIG["delay_max"] = d_max
        CONFIG["sleep_every"] = every
        CONFIG["sleep_duration"] = dur
        
        await message.reply(
            f"⚙️ **Konfigurasi Diupdate!**\n"
            f"⏱ Delay Acak: {d_min} - {d_max} detik\n"
            f"☕ Istirahat: Tiap {every} pesan, tidur {dur} detik."
        )
    except:
        await message.reply("❌ Format salah. Contoh: `/config 4 7 50 60`")

@app.on_message(filters.command("copy") & owner_filter)
async def copy_start(_, message):
    if STATUS["is_running"]:
        return await message.reply("⚠️ **Sedang sibuk!** Stop dulu atau tunggu selesai.")
    
    if not CONFIG["target_chat"]:
        return await message.reply("⚠️ **Target belum diset!** Gunakan `/set_target` dulu.")

    try:
        l_start = message.command[1]
        l_end = message.command[2]
        src, s_id = parse_link(l_start)
        _, e_id = parse_link(l_end)
        
        if not src or not s_id or not e_id:
            return await message.reply("❌ Link tidak valid.")
        
        # Jalankan Worker di Background
        STATUS["task"] = asyncio.create_task(background_worker(src, s_id, e_id, message))
        
    except IndexError:
        await message.reply("❌ Format salah. Contoh:\n`/copy https://t.me/A/10 https://t.me/A/20`")

@app.on_message(filters.command("pause") & owner_filter)
async def pause_cmd(_, message):
    if STATUS["is_running"]:
        STATUS["is_paused"] = True
        await message.reply("⏸️ **Sistem DIPAUSE.**\nKetik `/resume` untuk lanjut.")
    else:
        await message.reply("Bot sedang tidak bekerja.")

@app.on_message(filters.command("resume") & owner_filter)
async def resume_cmd(_, message):
    if STATUS["is_paused"]:
        STATUS["is_paused"] = False
        await message.reply("▶️ **Sistem DILANJUTKAN.**")
    else:
        await message.reply("Bot tidak sedang dipause.")

@app.on_message(filters.command("status") & owner_filter)
async def status_cmd(_, message):
    state = "🟢 Berjalan" if STATUS["is_running"] and not STATUS["is_paused"] else ("⏸️ Paused" if STATUS["is_paused"] else "⚪ Idle (Diam)")
    
    info = (
        f"📊 **STATUS BOT**\n"
        f"Status: {state}\n"
        f"Target: `{CONFIG['target_chat']}` (Topik: {CONFIG['target_topic']})\n"
        f"Proses ID: `{STATUS['current_id']}`\n"
        f"Sukses: {STATUS['total_success']} | Gagal: {STATUS['total_failed']}\n\n"
        f"⚙️ **Setting Saat Ini:**\n"
        f"Delay: {CONFIG['delay_min']}-{CONFIG['delay_max']}s\n"
        f"Istirahat: {CONFIG['sleep_duration']}s setiap {CONFIG['sleep_every']} pesan."
    )
    await message.reply(info)

@app.on_message(filters.command("stop") & owner_filter)
async def stop_cmd(_, message):
    if STATUS["is_running"]:
        STATUS["is_running"] = False # Ini akan memutus loop di worker
        STATUS["is_paused"] = False
        STATUS["task"].cancel()
        
        # Reset Data
        CONFIG["target_chat"] = None
        CONFIG["target_topic"] = None
        
        await message.reply("🛑 **STOP PAKSA BERHASIL.**\nData target & progres direset.")
    else:
        await message.reply("Bot memang sedang tidak jalan.")

@app.on_message(filters.command("reset") & owner_filter)
async def reset_cmd(_, message):
    CONFIG["target_chat"] = None
    CONFIG["target_topic"] = None
    CONFIG["log_channel"] = None
    await message.reply("🧹 **Semua Data Config (Target/Log) Direset.**")

if __name__ == "__main__":
    print("Bot Render sedang berjalan...")
    app.run()