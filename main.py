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

logger.info("--- SYSTEM BOOT: V9.3 OPTIMIZED SMART CHUNKING EDITION (MULTI-BOT TOKEN VERSION) ---")

# --- KONFIGURASI MULTI-BOT ---
NUM_BOTS = 5
clients = []
bot_data = []  # List of dicts for each bot: {'client': Client, 'is_working': False, 'stop_event': asyncio.Event(), 'logger': logger}

try:
    PORT = int(os.environ.get("PORT", 8080))
except ValueError as e:
    logger.error(f"❌ Config Error: {e}")
    sys.exit(1)

DEFAULT_BATCH_SIZE = 10000
DEFAULT_BATCH_TIME = 60
DEFAULT_CHUNK_SIZE = 50
DEFAULT_SPEED = 0.1

class FilterType(Enum):
    ALL = 'all'
    VIDEO = 'video'
    FOTO = 'foto'
    DOKUMEN = 'dokumen'
    AUDIO = 'audio'
    ALLOUT = 'allout'

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
def parse_link(link: Optional[str]) -> Tuple[Optional[any], Optional[int]]:
    if not link:
        return None, None
    private_match = re.search(r"t\.me/c/(\d+)/(\d+)", link)
    if private_match:
        return int("-100" + private_match.group(1)), int(private_match.group(2))
    public_match = re.search(r"t\.me/([^/]+)/(\d+)", link)
    if public_match:
        return public_match.group(1), int(public_match.group(2))
    return None, None

# --- 4. PARSE CONFIG FROM COMMAND ---
def parse_config(text: str) -> Dict:
    config = {}
    patterns = {
        'src_start': r"sumber_awal:\s*(.+)",
        'src_end': r"sumber_akhir:\s*(.+)",
        'dst': r"tujuan:\s*(.+)",
        'speed': r"speed:\s*(\d+\.?\d*)",
        'filter_type': r"filter:\s*(\w+)",
        'batch_size': r"batch_size:\s*(\d+)",
        'batch_time': r"batch_time:\s*(\d+)",
        'ember': r"ember:\s*(\d+)"
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
        return False, f"Invalid filter type: {filter_str}. Pilihan: all, video, foto, dokumen, audio, allout"
    
    return True, ""

# --- 6. WORKER UTAMA (SMART CHUNKING / EMBER) ---
async def copy_worker(job: Dict, status_msg, bot_id: int, app: Client, bot_logger):
    bot_data[bot_id]['is_working'] = True
    bot_data[bot_id]['stop_event'].clear()
    
    start_id: int = job['start_id']
    end_id: int = job['end_id']
    src_chat: any = job['src_chat']
    dst_chat: any = job['dst_chat']
    
    batch_size: int = job['batch_size']
    batch_time: int = job['batch_time']
    delay_min: float = job['delay_min']
    chunk_size: int = job['chunk_size']
    filter_type: FilterType = job['filter_type']
    
    delay_avg: float = delay_min + 0.25
    
    stats = {'success': 0, 'failed': 0, 'total': end_id - start_id + 1}
    processed_count = 0
    last_update_time = time.time()
    last_error_log = "-"

    try:
        for chunk_start in range(start_id, end_id + 1, chunk_size):
            if bot_data[bot_id]['stop_event'].is_set():
                break

            chunk_end = min(chunk_start + chunk_size - 1, end_id)
            ids_to_fetch = list(range(chunk_start, chunk_end + 1))
            
            messages_batch = []
            fetch_retries = 5
            for retry in range(fetch_retries):
                try:
                    messages_batch = await app.get_messages(src_chat, ids_to_fetch)
                    break
                except Exception as e:
                    last_error_log = str(e)
                    bot_logger.warning(f"⚠️ Fetch chunk {chunk_start}-{chunk_end} failed (retry {retry+1}/{fetch_retries}): {e}")
                    if retry == fetch_retries - 1:
                        stats['failed'] += len(ids_to_fetch)
                        continue
                    await asyncio.sleep(5)

            if not messages_batch:
                continue

            for msg in messages_batch:
                if bot_data[bot_id]['stop_event'].is_set():
                    break
                
                if processed_count > 0 and processed_count % batch_size == 0:
                    await status_msg.edit(f"😴 **SEDANG ISTIRAHAT BATCH ({batch_time}s)...**\n\n❄️ Mendinginkan Mesin...")
                    await asyncio.sleep(batch_time)
                    last_update_time = time.time()

                if not msg or msg.empty or msg.service:
                    stats['failed'] += 1
                    continue

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
                elif filter_type == FilterType.ALLOUT:
                    should_copy = True
                
                if not should_copy:
                    stats['failed'] += 1
                    continue

                max_retries = 10
                msg_success = False
                for retry_idx in range(max_retries):
                    if bot_data[bot_id]['stop_event'].is_set():
                        break
                    try:
                        copy_params = {'chat_id': dst_chat}
                        if job['dst_topic']:
                            copy_params['reply_to_message_id'] = job['dst_topic']

                        await msg.copy(**copy_params)
                        
                        stats['success'] += 1
                        processed_count += 1
                        msg_success = True
                        
                        await asyncio.sleep(random.uniform(delay_min, delay_min + 0.5))
                        break

                    except FloodWait as e:
                        bot_logger.info(f"FloodWait: Sleeping for {e.value} seconds")
                        await status_msg.edit(f"🌊 **Kena Limit Telegram!**\nTunggu {e.value} detik...")
                        await asyncio.sleep(e.value + 10)
                    
                    except RPCError as e:
                        last_error_log = str(e)
                        bot_logger.warning(f"RPCError in copy: {e}")
                        if "500" in str(e) or "INTERDC" in str(e):
                            await asyncio.sleep(10)
                        else:
                            await asyncio.sleep(5)
                    except Exception as e:
                        last_error_log = str(e)
                        bot_logger.error(f"Unexpected error in copy: {e}")
                        await asyncio.sleep(5)

                if not msg_success:
                    stats['failed'] += 1

                if time.time() - last_update_time > 10:
                    current_proc = stats['success'] + stats['failed']
                    remaining_files = stats['total'] - current_proc
                    
                    eta_val = (remaining_files * delay_avg) + ((remaining_files // batch_size) * batch_time)
                    eta_text = format_time(eta_val)

                    bar_str = make_bar(current_proc, stats['total'])
                    cpu_val, cpu_txt, ram_val, speed_txt = get_system_status(delay_avg)
                    
                    text = (
                        f"🐎 **WORKHORSE V9.3: OPTIMIZED SMART CHUNKING (BOT {bot_id})**\n"
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
                        bot_logger.warning(f"Failed to update status: {e}")
            
            del messages_batch
            gc.collect()

        final_msg = "✅ **SELESAI!**" if not bot_data[bot_id]['stop_event'].is_set() else "🛑 **DIBATALKAN!**"
        await status_msg.edit(
            f"{final_msg}\n\n"
            f"📊 **Laporan Akhir (BOT {bot_id}):** Total `{stats['total']}` | Sukses `{stats['success']}` | Gagal/Skip `{stats['failed']}`\n"
            f"📝 **Last Error:** {last_error_log}"
        )

    except Exception as e:
        bot_logger.error(f"❌ CRASH IN WORKER: {e}")
        await status_msg.edit(f"❌ **CRASH SYSTEM:** {e}")
    finally:
        bot_data[bot_id]['is_working'] = False

# --- COMMANDS (PERBAIKI UNTUK LEBIH ROBUST & DINAMIS) ---
def register_handlers(app: Client, bot_id: int):
    bot_logger = logging.getLogger(f"{__name__}.bot{bot_id}")
    bot_logger.handlers = logger.handlers
    bot_logger.setLevel(logger.level)

    # === LOGIKA NAMA PERINTAH DINAMIS ===
    # Bot 1: Handle polosan (start, stop, stats) DAN yang bernomor (start1, stop1, stats1)
    # Bot Lain: Hanya handle yang bernomor (start2, stop2, stats2, dst)
    
    if bot_id == 1:
        start_commands = ["start", "start1"]
        stop_commands = ["stop", "stop1"]
        stats_commands = ["stats", "stats1"]
    else:
        start_commands = [f"start{bot_id}"]
        stop_commands = [f"stop{bot_id}"]
        stats_commands = [f"stats{bot_id}"]

    # --- HANDLER START ---
    @app.on_message(filters.command(start_commands) & filters.group)
    async def start_cmd(client, message):
        if bot_data[bot_id]['is_working']:
            return await message.reply(f"⚠️ **Bot {bot_id} Sedang Sibuk!** Gunakan `/{stop_commands[-1]}` dulu.")
        
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

            status_msg = await message.reply(f"🐎 **Bot {bot_id} Menyiapkan Proses Copy...**")

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
            
            asyncio.create_task(copy_worker(job, status_msg, bot_id, client, bot_logger))
            
        except Exception as e:
            bot_logger.error(f"❌ Error in start_cmd: {e}")
            await message.reply(f"❌ **Error Config:** {e}\nCoba cek env vars atau akses bot.")

    # --- HANDLER STOP (DINAMIS) ---
    @app.on_message(filters.command(stop_commands) & filters.group)
    async def stop_cmd(client, message):
        if bot_data[bot_id]['is_working']:
            bot_data[bot_id]['stop_event'].set()
            await message.reply(f"🛑 **Proses Bot {bot_id} Dihentikan!** Menunggu selesai...")
        else:
            await message.reply(f"💤 **Bot {bot_id} Tidak Ada Proses Berjalan.**")

    # --- HANDLER STATS (DINAMIS) ---
    @app.on_message(filters.command(stats_commands) & filters.group)
    async def stats_cmd(client, message):
        cpu_val, cpu_txt, ram_val, _ = get_system_status(0)
        status_bot = "🔥 Aktif" if bot_data[bot_id]['is_working'] else "💤 Istirahat"
        text = (
            f"🐴 **Status Server V9.3 (BOT {bot_id})**\n"
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
        msg = await message.reply(f"🏓 **Pong Bot {bot_id}!**")
        end = time.time()
        await msg.edit(f"🏓 **Pong Bot {bot_id}!** Latency: `{(end - start) * 1000:.2f}ms`")

# --- INIT BOTS ---
bot_data = [None] * (NUM_BOTS + 1)
for i in range(1, NUM_BOTS + 1):
    try:
        api_id = int(os.environ.get(f"API_ID_{i}", 0))
        api_hash = os.environ.get(f"API_HASH_{i}", "")
        bot_token = os.environ.get(f"BOT_TOKEN_{i}", "")
        
        if api_id == 0 or not api_hash or not bot_token:
            logger.info(f"Skipping Bot {i}: Missing config")
            continue
        
        client = Client(
            f"render_bot_{i}",
            api_id=api_id,
            api_hash=api_hash,
            bot_token=bot_token,
            sleep_threshold=3600
        )
        clients.append(client)
        bot_data[i] = {
            'client': client,
            'is_working': False,
            'stop_event': asyncio.Event()
        }
        register_handlers(client, i)
        logger.info(f"Bot {i} initialized successfully")
    except ValueError as e:
        logger.error(f"❌ Config Error for Bot {i}: {e}")

if not clients:
    logger.error("No bots initialized. Exiting.")
    sys.exit(1)

# --- WEB SERVER ---
async def web_handler(request):
    return web.Response(text="Multi-Bot Running V9.3 (Bot Version).")

async def start_web():
    app_web = web.Application()
    app_web.add_routes([web.get('/', web_handler)])
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

async def main():
    await start_web()
    logger.info("🤖 Starting Telegram Bots...")
    for client in clients:
        await client.start()
    await idle()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
