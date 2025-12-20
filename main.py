import os
import asyncio
import random
import re
import logging
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait

# --- KONFIGURASI DARI RENDER (ENVIRONMENT VARIABLES) ---
# Pastikan nama variabel di Render SAMA PERSIS dengan yang di dalam kurung ("...")
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
CMD_CHANNEL_ID = int(os.environ.get("CMD_CHANNEL_ID", 0)) 
OWNER_ID = int(os.environ.get("OWNER_ID", 0))

# Logger setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RenderBot")

# Inisialisasi Client
if not API_ID or not API_HASH or not SESSION_STRING:
    logger.error("❌ ERROR: Variable Environment belum diisi lengkap di Render!")
    exit(1)

app = Client(
    "render_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    in_memory=True
)

# Global Flags & Control
IS_WORKING = False
STOP_EVENT = asyncio.Event()

# --- FUNGSI PARSING & HELPER ---

def parse_link(link):
    """Mengubah link t.me menjadi chat_id dan message_id/thread_id"""
    if not link: return None, None
    
    # Format Private: t.me/c/123456789/100
    private_match = re.search(r"t\.me/c/(\d+)/(\d+)", link)
    if private_match:
        chat_id = int("-100" + private_match.group(1))
        msg_id = int(private_match.group(2))
        return chat_id, msg_id

    # Format Public: t.me/namachannel/100
    public_match = re.search(r"t\.me/([^/]+)/(\d+)", link)
    if public_match:
        username = public_match.group(1)
        msg_id = int(public_match.group(2))
        return username, msg_id
        
    return None, None

async def resolve_peer(chat_id):
    """Pemanasan agar bot mengenali chat ID"""
    try:
        await app.get_chat(chat_id)
        return True
    except Exception as e:
        logger.error(f"Gagal resolve peer {chat_id}: {e}")
        return False

# --- WORKER UTAMA (PEMROSES FILE) ---

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

    # Tahap 1: Pemanasan
    await status_msg.edit(f"🔄 **Menyiapkan Akses...**\nMencoba mengenali Chat Sumber & Tujuan...")
    
    if not await resolve_peer(src_chat):
        await status_msg.edit(f"❌ **Gagal!** Bot tidak bisa akses Sumber.\nPastikan akun sudah join.")
        IS_WORKING = False
        return
        
    if not await resolve_peer(dst_chat):
        await status_msg.edit(f"❌ **Gagal!** Bot tidak bisa akses Tujuan.\nPastikan akun sudah join.")
        IS_WORKING = False
        return

    await status_msg.edit(f"🚀 **Gas! Mulai Copy...**\n`{src_start}` ➔ `{src_end}`")

    try:
        while current_id <= src_end:
            # Cek Sinyal Stop
            if STOP_EVENT.is_set():
                await status_msg.edit("⏹ **Dihentikan oleh Admin.**")
                break

            try:
                # Ambil Pesan
                msg = await app.get_messages(src_chat, current_id)

                if msg and not msg.empty and not msg.service:
                    # Filter Sticker
                    if msg.sticker:
                        logger.info(f"Skip Sticker: {current_id}")
                    else:
                        # Eksekusi Copy
                        try:
                            await msg.copy(
                                chat_id=dst_chat,
                                reply_to_message_id=dst_topic if dst_topic else None
                            )
                            processed_count += 1
                            
                            # Jeda Random (Agar aman)
                            sleep_time = random.randint(delay_min, delay_max)
                            await asyncio.sleep(sleep_time)

                        except FloodWait as e:
                            wait_sec = e.value + 5
                            await status_msg.edit(f"⚠️ **Telegram Marah (FloodWait)**\nDisuruh tidur {wait_sec} detik...")
                            await asyncio.sleep(wait_sec)
                            continue # Ulangi pesan yang sama

                # Cek Batch (Istirahat Panjang)
                if processed_count > 0 and processed_count % batch_size == 0:
                    await status_msg.edit(
                        f"☕ **Istirahat Batch**\n"
                        f"Sudah copy: {processed_count} file\n"
                        f"Posisi ID: {current_id}\n"
                        f"Tidur {batch_wait} detik..."
                    )
                    await asyncio.sleep(batch_wait)
                    await status_msg.edit(f"🚀 **Lanjut Kerja...**\nPosisi ID: {current_id}")

            except Exception as e:
                logger.error(f"Skip ID {current_id} karena error: {e}")

            # Update Laporan di Channel (Tiap 20 ID biar gak spam)
            if current_id % 20 == 0:
                 try:
                     await status_msg.edit(
                        f"🏃 **Status Jalan**\n"
                        f"Target: {current_id} / {src_end}\n"
                        f"Sukses: {processed_count}"
                    )
                 except: pass

            current_id += 1 # Lanjut ke pesan berikutnya

        if not STOP_EVENT.is_set():
            await status_msg.edit(f"✅ **Selesai Bos!**\nTotal sukses: {processed_count} file.")

    except Exception as e:
        await status_msg.edit(f"❌ **Error Fatal:** {str(e)}")
    
    finally:
        IS_WORKING = False

# --- FITUR CHAT & PERINTAH ---

@app.on_message(filters.chat(CMD_CHANNEL_ID) & filters.user(OWNER_ID) & filters.command("copy"))
async def start_handler(client, message):
    global IS_WORKING
    
    if IS_WORKING:
        return await message.reply("⚠️ **Antrian Penuh!**\nSelesaikan tugas sekarang atau kirim `/stop` dulu.")

    # Parsing Perintah
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

        # Validasi Link
        src_chat, start_id = parse_link(config.get('src_start_link'))
        _, end_id = parse_link(config.get('src_end_link'))
        dst_chat, dst_topic = parse_link(config.get('dst_link'))

        if not src_chat or not start_id or not end_id or not dst_chat:
            return await message.reply("❌ **Format Salah!**\nLink sumber/tujuan tidak valid. Cek lagi.")

        job_config = {
            'src_chat': src_chat,
            'start_id': start_id,
            'end_id': end_id,
            'dst_chat': dst_chat,
            'dst_topic': dst_topic,
            'delay_range': config.get('delay_range', (5, 10)),
            'batch_size': config.get('batch_size', 50),
            'batch_wait': config.get('batch_wait', 300)
        }

        status_msg = await message.reply("⚙️ **Perintah Diterima.**\nMemulai worker di server...")
        asyncio.create_task(copy_worker(job_config, status_msg))

    except Exception as e:
        await message.reply(f"❌ **Error Parsing:**\n{e}")

@app.on_message(filters.chat(CMD_CHANNEL_ID) & filters.user(OWNER_ID) & filters.command("stop"))
async def stop_handler(client, message):
    if IS_WORKING:
        STOP_EVENT.set()
        await message.reply("🛑 **Mengerem...**\nMenunggu proses berhenti dengan aman.")
    else:
        await message.reply("💤 Bot lagi gak ngapa-ngapain.")

@app.on_message(filters.chat(CMD_CHANNEL_ID) & filters.user(OWNER_ID) & filters.command("ping"))
async def ping_handler(client, message):
    await message.reply("🏓 **Pong!**\nServer Render Aktif & Sehat.")

print("Bot Userbot Siap di Render...")
app.run()