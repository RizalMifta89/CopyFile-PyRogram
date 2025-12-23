import os
import asyncio
import random
import re
import logging
import sys
import gc
import time
import psutil
from enum import Enum
from typing import List, Tuple, Optional, Dict
from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait, RPCError
from aiohttp import web

# --- LOGGING SYSTEM ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logging.getLogger("pyrogram").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

logger.info("--- SYSTEM BOOT: V9.3 OPTIMIZED SMART CHUNKING EDITION (BOT TOKEN VERSION) ---")

# --- KONFIGURASI ---
try:
    API_ID = int(os.environ.get("API_ID", 0))
    API_HASH = os.environ.get("API_HASH", "")
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
    PORT = int(os.environ.get("PORT", 8080))
except ValueError as e:
    logger.error(f"❌ Config Error: {e}")
    sys.exit(1)

app = Client(
    "render_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    sleep_threshold=3600  # Allow auto-sleep for floodwait up to 1 hour
)

IS_WORKING = False
STOP_EVENT = asyncio.Event()

DEFAULT_BATCH_SIZE = 10000  # Nilai tinggi untuk efisiensi pada bot API
DEFAULT_BATCH_TIME = 60
DEFAULT_CHUNK_SIZE = 50  # Tingkatkan default untuk fetch lebih efisien
DEFAULT_SPEED = 0.1

class FilterType(Enum):
    ALL = 'all'
    VIDEO = 'video'
    FOTO = 'foto'
    DOKUMEN = 'dokumen'
    AUDIO = 'audio'

# --- 1. HELPER: FORMAT WAKTU (ETA) ---
def format_time(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)} detik"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes} menit {secs} detik"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours} jam {minutes} menit"

# --- 2. LOGIKA TRAFFIC LIGHT ---
def get_system_status(delay_avg: float = 0) -> Tuple[float, str, float, str]:
    try:
        proc = psutil.Process(os.getpid())
        cpu = proc.cpu_percent(interval=0.1)
        if cpu <= 10:
            cpu_stat = "🟢 Santai"
        elif cpu <= 50:
            cpu_stat = "🟡 Sibuk"
        else:
            cpu_stat = "🔴 Berat"
        
        ram_bytes = proc.memory_info().rss
        ram_mb = ram_bytes / (1024 * 1024)
        
        speed_stat = "💤 Idle"
        if delay_avg > 0:
            if delay_avg <= 0.2:
                speed_stat = "🚀 MODE NGEBUT"
            elif delay_avg <= 1:
                speed_stat = "⚠️ Agak Cepat"
            else:
                speed_stat = "✅ Sangat Aman"
            
        return cpu, cpu_stat, ram_mb, speed_stat
    except Exception as e:
        logger.warning(f"Failed to get system status: {e}")
        return 0.0, "?", 0.0, "?"

def make_bar(current: int, total: int, length: int = 10) -> str:
    try:
        pct = current / total
    except ZeroDivisionError:
        pct = 0
    filled = int(length * pct)
    bar = "🟧" * filled + "⬜" * (length - filled)
    return f"{bar} **{int(pct * 100)}%**"

# --- 3. PARSE LINK ---
def parse_link(link: Optional[str]) -> Tuple[Optional[any], Optional[int]]:  # src_chat bisa str atau int
    if not link:
        return None, None
    private_match = re.search(r"t\.me/c/(\d+)/(\d+)", link)
    if private_match:
        return int("-100" + private_match.group(1)), int(private_match.group(2))
    public_match = re.search(r"t\.me/([^/]+)/(\d+)", link)
    if public_match:
        return public_match.group(1), int(public_match.group(2))
    return None, None

# --- 4. PARSE CONFIG FROM COMMAND (OPTIMIZED WITH REGEX, TAMBAH BATCH & EMBER) ---
def parse_config(text: str) -> Dict:
    config = {}
    patterns = {
        'src_start': r"sumber_awal:\s*(.+)",
        'src_end': r"sumber_akhir:\s*(.+)",
        'dst': r"tujuan:\s*(.+)",
        'speed': r"speed:\s*(\d+\.?\d*)",
        'filter_type': r"filter:\s*(\w+)",
        'batch_size': r"batch_size:\s*(\d+)",  # Baru: untuk set batch size
        'batch_time': r"batch_time:\s*(\d+)",  # Baru: untuk set batch time
        'ember': r"ember:\s*(\d+)"  # Baru: untuk set chunk_size (ember)
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            if key in ['speed']:
                config[key] = float(match.group(1))
            elif key in ['batch_size', 'batch_time', 'ember']:
                config[key] = int(match.group(1))
            else:
                config[key] = match.group(1).strip().lower() if key == 'filter_type' else match.group(1).strip()
    
    return config

# --- 5. VALIDATE CONFIG ---
def validate_config(config: Dict) -> Tuple[bool, str]:
    required = ['src_start', 'src_end', 'dst']
    for req in required:
        if req not in config:
            return False, f"Missing required field: {req}"
    
    try:
        config['delay_min'] = config.get('speed', DEFAULT_SPEED)
        if config['delay_min'] <= 0:
            return False, "Speed must be positive"
        
        filter_str = config.get('filter_type', 'all')
        config['filter_type'] = FilterType(filter_str)
        
        config['batch_size'] = config.get('batch_size', DEFAULT_BATCH_SIZE)
        config['batch_time'] = config.get('batch_time', DEFAULT_BATCH_TIME)
        config['chunk_size'] = config.get('ember', DEFAULT_CHUNK_SIZE)
        
        if config['batch_size'] <= 0 or config['batch_time'] < 0 or config['chunk_size'] <= 0:
            return False, "Batch/Ember values must be positive"
        
    except ValueError:
        return False, f"Invalid filter type: {filter_str}. Pilihan: all, video, foto, dokumen, audio"
    
    return True, ""

# --- 6. WORKER UTAMA (SMART CHUNKING / EMBER) ---
async def copy_worker(job: Dict, status_msg):
    global IS_WORKING
    IS_WORKING = True
    STOP_EVENT.clear()
    
    start_id: int = job['start_id']
    end_id: int = job['end_id']
    src_chat: any = job['src_chat']  # Bisa str atau int
    dst_chat: any = job['dst_chat']
    
    batch_size: int = job['batch_size']
    batch_time: int = job['batch_time']
    delay_min: float = job['delay_min']
    chunk_size: int = job['chunk_size']
    filter_type: FilterType = job['filter_type']
    
    delay_avg: float = delay_min + 0.25
    
    stats = {'success': 0, 'failed': 0, 'total': end_id - start_id + 1}
    processed_count = 0  # Success count for batch sleep
    last_update_time = time.time()
    last_error_log = "-"

    try:
        for chunk_start in range(start_id, end_id + 1, chunk_size):
            if STOP_EVENT.is_set():
                break

            chunk_end = min(chunk_start + chunk_size - 1, end_id)
            ids_to_fetch = list(range(chunk_start, chunk_end + 1))
            
            messages_batch = []
            fetch_retries = 5  # Tingkatkan retry untuk stabilitas
            for retry in range(fetch_retries):
                try:
                    messages_batch = await app.get_messages(src_chat, ids_to_fetch)
                    break
                except Exception as e:
                    last_error_log = str(e)
                    logger.warning(f"⚠️ Fetch chunk {chunk_start}-{chunk_end} failed (retry {retry+1}/{fetch_retries}): {e}")
                    if retry == fetch_retries - 1:
                        stats['failed'] += len(ids_to_fetch)
                        continue
                    await asyncio.sleep(5)  # Backoff lebih panjang

            if not messages_batch:
                continue

            for msg in messages_batch:
                if STOP_EVENT.is_set():
                    break
                
                # --- LOGIKA BATCH SLEEP (ISTIRAHAT PANJANG) ---
                if processed_count > 0 and processed_count % batch_size == 0:
                    await status_msg.edit(f"😴 **SEDANG ISTIRAHAT BATCH ({batch_time}s)...**\n\n❄️ Mendinginkan Mesin...")
                    await asyncio.sleep(batch_time)
                    last_update_time = time.time()

                # Cek Validitas Pesan
                if not msg or msg.empty or msg.service:
                    stats['failed'] += 1
                    continue

                # --- FILTERING ---
                should_copy = False
                if filter_type == FilterType.VIDEO and msg.video:
                    should_copy = True
                elif filter_type == FilterType.FOTO and msg.photo:
                    should_copy = True
                elif filter_type == FilterType.DOKUMEN and msg.document:
                    should_copy = True
                elif filter_type == FilterType.AUDIO and (msg.audio or msg.voice):
                    should_copy = True
                elif filter_type == FilterType.ALL and not msg.sticker:
                    should_copy = True
                
                if not should_copy:
                    stats['failed'] += 1
                    continue

                # --- EKSEKUSI COPY (DENGAN RETRY LOOP) ---
                max_retries = 10  # Tingkatkan retry untuk hindari macet
                msg_success = False
                for retry_idx in range(max_retries):
                    if STOP_EVENT.is_set():
                        break
                    try:
                        copy_params = {'chat_id': dst_chat}
                        if job['dst_topic']:
                            copy_params['reply_to_message_id'] = job['dst_topic']

                        await msg.copy(**copy_params)
                        
                        stats['success'] += 1
                        processed_count += 1
                        msg_success = True
                        
                        # Jeda aman per pesan (float)
                        await asyncio.sleep(random.uniform(delay_min, delay_min + 0.5))
                        break

                    except FloodWait as e:
                        logger.info(f"FloodWait: Sleeping for {e.value} seconds")
                        await status_msg.edit(f"🌊 **Kena Limit Telegram!**\nTunggu {e.value} detik...")
                        await asyncio.sleep(e.value + 10)  # Tambah buffer
                        # Continue tanpa increment retry_idx
                    
                    except RPCError as e:
                        last_error_log = str(e)
                        logger.warning(f"RPCError in copy: {e}")
                        if "500" in str(e) or "INTERDC" in str(e):
                            await asyncio.sleep(10)
                        else:
                            await asyncio.sleep(5)
                    except Exception as e:
                        last_error_log = str(e)
                        logger.error(f"Unexpected error in copy: {e}")
                        await asyncio.sleep(5)

                if not msg_success:
                    stats['failed'] += 1

                # --- UPDATE TAMPILAN STATUS (PERBAIKI UNTUK LEBIH RINGKAS) ---
                if time.time() - last_update_time > 10:
                    current_proc = stats['success'] + stats['failed']
                    remaining_files = stats['total'] - current_proc
                    
                    eta_val = (remaining_files * delay_avg) + ((remaining_files // batch_size) * batch_time)
                    eta_text = format_time(eta_val)

                    bar_str = make_bar(current_proc, stats['total'])
                    cpu_val, cpu_txt, ram_val, speed_txt = get_system_status(delay_avg)
                    
                    text = (
                        f"🐎 **WORKHORSE V9.3: OPTIMIZED SMART CHUNKING (BOT VERSION)**\n"
                        f"{bar_str}\n\n"
                        f"📊 **Stats:** Total `{stats['total']}` | Sukses `{stats['success']}` | Gagal `{stats['failed']}` | Sisa `{remaining_files}`\n"
                        f"🏁 **ETA:** ± {eta_text} | Filter: `{filter_type.value.upper()}`\n\n"
                        f"🌡️ **Resources:** CPU {cpu_val}% [{cpu_txt}] | RAM {ram_val:.2f} MB\n\n"
                        f"⚡ **Config:** Ember {chunk_size} | Jeda {delay_avg:.2f}s | {speed_txt}\n"
                        f"Batch: {batch_time}s tiap {batch_size} file\n\n"
                        f"🔄 Update tiap 10s | ⚠️ Last Error: {last_error_log}"
                    )
                    try:
                        await status_msg.edit(text)
                        last_update_time = time.time()
                    except Exception as e:
                        logger.warning(f"Failed to update status: {e}")
            
            # Bersihkan memori setiap chunk selesai
            del messages_batch
            gc.collect()

        # --- LAPORAN AKHIR ---
        final_msg = "✅ **SELESAI!**" if not STOP_EVENT.is_set() else "🛑 **DIBATALKAN!**"
        await status_msg.edit(
            f"{final_msg}\n\n"
            f"📊 **Laporan Akhir:** Total `{stats['total']}` | Sukses `{stats['success']}` | Gagal/Skip `{stats['failed']}`\n"
            f"📝 **Last Error:** {last_error_log}"
        )

    except Exception as e:
        logger.error(f"❌ CRASH IN WORKER: {e}")
        await status_msg.edit(f"❌ **CRASH SYSTEM:** {e}")
    finally:
        IS_WORKING = False

# --- COMMANDS (PERBAIKI UNTUK LEBIH ROBUST) ---
@app.on_message(filters.command("start") & filters.group)
async def start_cmd(client, message):
    global IS_WORKING
    if IS_WORKING:
        return await message.reply("⚠️ **Sedang Sibuk!** Gunakan `/stop` dulu.")
    
    try:
        config = parse_config(message.text)
        valid, error = validate_config(config)
        if not valid:
            return await message.reply(f"❌ **Config Gagal:** {error}\nCoba cek format perintah.")

        src_chat, start_id = parse_link(config['src_start'])
        _, end_id = parse_link(config['src_end'])
        dst_chat, dst_topic = parse_link(config['dst'])

        if not src_chat or not dst_chat or not start_id or not end_id:
            return await message.reply("❌ **Link Salah Format!** Pastikan link seperti https://t.me/c/1234/100.")

        status_msg = await message.reply("🐎 **Menyiapkan Proses Copy...**")

        job = {
            'src_chat': src_chat, 
            'start_id': start_id, 
            'end_id': end_id,
            'dst_chat': dst_chat, 
            'dst_topic': dst_topic,
            'delay_min': config['delay_min'],
            'filter_type': config['filter_type'],
            'batch_size': config['batch_size'],
            'batch_time': config['batch_time'],
            'chunk_size': config['chunk_size']
        }
        
        asyncio.create_task(copy_worker(job, status_msg))
        
    except Exception as e:
        logger.error(f"❌ Error in start_cmd: {e}")
        await message.reply(f"❌ **Error Config:** {e}\nCoba cek env vars atau akses bot.")

@app.on_message(filters.command("stop") & filters.group)
async def stop_cmd(client, message):
    if IS_WORKING:
        STOP_EVENT.set()
        await message.reply("🛑 **Proses Dihentikan!** Menunggu selesai...")
    else:
        await message.reply("💤 **Tidak Ada Proses Berjalan.**")

@app.on_message(filters.command("stats") & filters.group)
async def stats_cmd(client, message):
    cpu_val, cpu_txt, ram_val, _ = get_system_status(0)
    status_bot = "🔥 Aktif" if IS_WORKING else "💤 Istirahat"
    text = (
        f"🐴 **Status Server V9.3 (BOT VERSION)**\n"
        f"──────────────────\n"
        f"🤖 **Status:** {status_bot}\n"
        f"🧠 **CPU:** {cpu_val}% [{cpu_txt}]\n"
        f"💾 **RAM:** {ram_val:.2f} MB\n"
        f"──────────────────"
    )
    await message.reply(text)

@app.on_message(filters.command("ping") & filters.group)
async def ping_cmd(client, message):
    start = time.time()
    msg = await message.reply("🏓 **Pong!**")
    end = time.time()
    await msg.edit(f"🏓 **Pong!** Latency: `{(end - start) * 1000:.2f}ms`")

# --- WEB SERVER ---
async def web_handler(request):
    return web.Response(text="Bot Running V9.3 (Bot Version).")

async def start_web():
    app_web = web.Application()
    app_web.add_routes([web.get('/', web_handler)])
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

async def main():
    await start_web()
    logger.info("🤖 Start Telegram...")
    await app.start()
    await idle()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
