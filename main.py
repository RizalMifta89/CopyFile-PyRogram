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

logger.info("--- SYSTEM BOOT: V9.1 OPTIMIZED SMART CHUNKING EDITION (MULTI-BOT VERSION) ---")

# --- LOAD BOTS FROM ENV ---
def load_bots_config():
    bots = []
    for i in range(1, 6):  # Bot1 to Bot5
        api_id = os.environ.get(f"BOT{i}_API_ID")
        api_hash = os.environ.get(f"BOT{i}_API_HASH")
        bot_token = os.environ.get(f"BOT{i}_BOT_TOKEN")
        
        if api_id and api_hash and bot_token:
            try:
                api_id = int(api_id)
                bots.append({
                    "id": i,
                    "name": f"Bot{i}",
                    "api_id": api_id,
                    "api_hash": api_hash,
                    "bot_token": bot_token,
                    "start_cmd": f"start{i}",
                    "stop_cmd": f"stop{i}",
                    "is_working": False,
                    "stop_event": asyncio.Event(),
                })
            except ValueError:
                logger.error(f"❌ BOT{i}_API_ID must be integer!")
        else:
            logger.info(f"ℹ️ BOT{i} not configured (missing env).")
    return bots

# Initialize bots
BOTS = load_bots_config()

if not BOTS:
    logger.error("❌ No valid bots found in environment! Please set BOT1_* ... BOT5_*")
    sys.exit(1)

# Create Pyrogram clients
for bot in BOTS:
    bot["client"] = Client(
        bot["name"].lower(),
        api_id=bot["api_id"],
        api_hash=bot["api_hash"],
        bot_token=bot["bot_token"]
    )

# --- CONFIGURATION ---
try:
    PORT = int(os.environ.get("PORT", 8080))
except ValueError as e:
    logger.error(f"❌ PORT config error: {e}")
    sys.exit(1)

# --- ENUM & DEFAULTS ---
class FilterType(Enum):
    ALL = 'all'
    VIDEO = 'video'
    FOTO = 'foto'
    DOKUMEN = 'dokumen'
    AUDIO = 'audio'

DEFAULT_BATCH_SIZE = 50
DEFAULT_BATCH_TIME = 60
DEFAULT_CHUNK_SIZE = 20  # Default Ember Size
DEFAULT_SPEED = 0.1

# --- HELPER FUNCTIONS (UNCHANGED) ---
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

def parse_link(link: Optional[str]) -> Tuple[Optional[str], Optional[int]]:
    if not link:
        return None, None
    private_match = re.search(r"t\.me/c/(\d+)/(\d+)", link)
    if private_match:
        return int("-100" + private_match.group(1)), int(private_match.group(2))
    public_match = re.search(r"t\.me/([^/]+)/(\d+)", link)
    if public_match:
        return public_match.group(1), int(public_match.group(2))
    return None, None

def parse_config(text: str) -> Dict:
    config = {}
    patterns = {
        'src_start': r"sumber_awal:\s*(.+)",
        'src_end': r"sumber_akhir:\s*(.+)",
        'dst': r"tujuan:\s*(.+)",
        'speed': r"speed:\s*(\d+\.?\d*)",
        'filter_type': r"filter:\s*(\w+)",
        'chunk_size': r"ember:\s*(\d+)",
        'batch_size': r"batch_size:\s*(\d+)",
        'batch_time': r"batch_time:\s*(\d+)",
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            if key in ('speed',):
                config[key] = float(match.group(1))
            elif key in ('chunk_size', 'batch_size', 'batch_time'):
                config[key] = int(match.group(1))
            else:
                config[key] = match.group(1).strip().lower() if key == 'filter_type' else match.group(1).strip()
    
    return config

def validate_config(config: Dict) -> Tuple[bool, str]:
    required = ['src_start', 'src_end', 'dst']
    for req in required:
        if req not in config:
            return False, f"Missing required field: {req}"
    
    try:
        config['delay_min'] = config.get('speed', DEFAULT_SPEED)
        if config['delay_min'] <= 0:
            return False, "Speed must be positive"
        
        config['filter_type'] = FilterType(config.get('filter_type', 'all'))
        config['batch_size'] = config.get('batch_size', DEFAULT_BATCH_SIZE)
        config['batch_time'] = config.get('batch_time', DEFAULT_BATCH_TIME)
        config['chunk_size'] = config.get('chunk_size', DEFAULT_CHUNK_SIZE)
        
    except ValueError:
        return False, "Invalid filter type or number"
    
    return True, ""

# --- WORKER UTAMA (DIPERTAHANKAN, HANYA TAMBAH PREFIX BOT) ---
async def copy_worker(job: Dict, status_msg, bot_info):
    bot_name = bot_info["name"]
    stop_event = bot_info["stop_event"]
    bot_info["is_working"] = True
    stop_event.clear()
    
    start_id: int = job['start_id']
    end_id: int = job['end_id']
    src_chat: str = job['src_chat']
    dst_chat: str = job['dst_chat']
    
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
            if stop_event.is_set():
                break

            chunk_end = min(chunk_start + chunk_size - 1, end_id)
            ids_to_fetch = list(range(chunk_start, chunk_end + 1))
            
            messages_batch = []
            fetch_retries = 3
            for retry in range(fetch_retries):
                try:
                    messages_batch = await bot_info["client"].get_messages(src_chat, ids_to_fetch)
                    break
                except Exception as e:
                    logger.warning(f"[{bot_name}] ⚠️ Fetch chunk {chunk_start}-{chunk_end} failed (retry {retry+1}/{fetch_retries}): {e}")
                    if retry == fetch_retries - 1:
                        stats['failed'] += len(ids_to_fetch)
                        last_error_log = str(e)
                        continue
                    await asyncio.sleep(2)

            if not messages_batch:
                continue

            for msg in messages_batch:
                if stop_event.is_set():
                    break
                
                if processed_count > 0 and processed_count % batch_size == 0:
                    await status_msg.edit(f"😴 **[{bot_name}] SEDANG ISTIRAHAT BATCH ({batch_time}s)...**\n\n❄️ Mendinginkan Mesin...")
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
                
                if not should_copy:
                    stats['failed'] += 1
                    continue

                max_retries = 5
                for retry_idx in range(max_retries):
                    if stop_event.is_set():
                        break
                    try:
                        copy_params = {'chat_id': dst_chat}
                        if job['dst_topic']:
                            copy_params['reply_to_message_id'] = job['dst_topic']

                        await msg.copy(**copy_params)
                        
                        stats['success'] += 1
                        processed_count += 1
                        await asyncio.sleep(random.uniform(delay_min, delay_min + 0.5))
                        break

                    except FloodWait as e:
                        await status_msg.edit(f"🌊 **[{bot_name}] Kena Limit Telegram!**\nTunggu {e.value} detik...")
                        await asyncio.sleep(e.value + 5)
                    except RPCError as e:
                        last_error_log = str(e)
                        if "500" in str(e) or "INTERDC" in str(e):
                            await asyncio.sleep(5)
                        else:
                            await asyncio.sleep(2)
                    except Exception as e:
                        last_error_log = str(e)
                        logger.error(f"[{bot_name}] Unexpected error in copy: {e}")
                        break
                else:
                    stats['failed'] += 1

                if time.time() - last_update_time > 10:
                    current_proc = stats['success'] + stats['failed']
                    remaining_files = stats['total'] - current_proc
                    eta_val = (remaining_files * delay_avg) + ((remaining_files // batch_size) * batch_time)
                    eta_text = format_time(eta_val)
                    bar_str = make_bar(current_proc, stats['total'])
                    cpu_val, cpu_txt, ram_val, speed_txt = get_system_status(delay_avg)
                    
                    text = (
                        f"🐎 **[{bot_name}] WORKHORSE V9.1: OPTIMIZED SMART CHUNKING**\n"
                        f"{bar_str}\n\n"
                        f"📊 **Logistik Kargo:**\n"
                        f"• 📥 Total: `{stats['total']}`  |  ✅ Sukses: `{stats['success']}`\n"
                        f"• 🗑 Gagal: `{stats['failed']}`  |  ⏳ Sisa: `{remaining_files}`\n"
                        f"• 🏁 Estimasi: `± {eta_text}`\n"
                        f"• 🔍 Filter: `{filter_type.value.upper()}`\n\n"
                        f"🌡️ **Resource Bot:**\n"
                        f"• 🧠 CPU: {cpu_val}% [{cpu_txt}]\n"
                        f"• 💾 RAM: {ram_val:.2f} MB\n\n"
                        f"⚡ **Konfigurasi:**\n"
                        f"• 🪣 Ember: {chunk_size} pesan/tarik\n"
                        f"• ⏱ Jeda: {delay_avg:.2f} detik\n"
                        f"• {speed_txt}\n"
                        f"• 🛡️ Batch: Istirahat {batch_time}s tiap {batch_size} file\n\n"
                        f"🔄 *Update tiap 10 detik...*\n"
                        f"⚠️ *Last Error: {last_error_log}*"
                    )
                    try:
                        await status_msg.edit(text)
                        last_update_time = time.time()
                    except Exception as e:
                        logger.warning(f"[{bot_name}] Failed to update status: {e}")
            
            del messages_batch
            gc.collect()

        final_msg = "✅ **SELESAI!**" if not stop_event.is_set() else "🛑 **DIBATALKAN!**"
        await status_msg.edit(
            f"[{bot_name}] {final_msg}\n\n"
            f"📊 **Laporan Akhir:**\n"
            f"• 📥 Total: `{stats['total']}`\n"
            f"• ✅ Sukses: `{stats['success']}`\n"
            f"• 🗑 Gagal/Skip: `{stats['failed']}`\n\n"
            f"📝 **Log Error:** `{last_error_log}`"
        )

    except Exception as e:
        logger.error(f"[{bot_name}] ❌ CRASH IN WORKER: {e}")
        await status_msg.edit(f"[{bot_name}] ❌ **CRASH SYSTEM:** {e}")
    finally:
        bot_info["is_working"] = False

# --- REGISTER HANDLERS PER BOT ---
def register_handlers(bot):
    @bot["client"].on_message(filters.command(bot["start_cmd"]) & filters.group)
    async def start_cmd(client, message):
        if bot["is_working"]:
            return await message.reply(f"⚠️ **[{bot['name']}] Sedang Sibuk!** `/{bot['stop_cmd']}` dulu.")
        
        try:
            config = parse_config(message.text)
            valid, error = validate_config(config)
            if not valid:
                return await message.reply(f"❌ **Config Invalid:** {error}")

            src_chat, start_id = parse_link(config['src_start'])
            _, end_id = parse_link(config['src_end'])
            dst_chat, dst_topic = parse_link(config['dst'])

            if not src_chat or not dst_chat or not start_id or not end_id:
                return await message.reply("❌ **Link Salah Format!**")

            status_msg = await message.reply(f"🚑 **[{bot['name']}] Sedang Menyembuhkan Sesi...**")
            try:
                count = 0
                async for dialog in bot["client"].get_dialogs(limit=50):
                    count += 1
                await status_msg.edit(f"✅ **[{bot['name']}] Sesi Pulih! (Memuat {count} chat)**\n🐎 **Menyiapkan Kuda...**")
            except Exception as e:
                logger.warning(f"[{bot['name']}] Gagal refresh dialog: {e}")
                await status_msg.edit(f"⚠️ **[{bot['name']}] Gagal Refresh Sesi:** {e}\nTetap mencoba lanjut...")

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
            
            asyncio.create_task(copy_worker(job, status_msg, bot))
            
        except Exception as e:
            logger.error(f"[{bot['name']}] ❌ Error in start_cmd: {e}")
            await message.reply(f"❌ Error Config: {e}")

    @bot["client"].on_message(filters.command(bot["stop_cmd"]) & filters.group)
    async def stop_cmd(client, message):
        if bot["is_working"]:
            bot["stop_event"].set()
            await message.reply(f"🛑 **[{bot['name']}] REM DARURAT DITARIK!**\nMenunggu proses terakhir selesai...")
        else:
            await message.reply(f"💤 **[{bot['name']}] sedang tidur.**")

    @bot["client"].on_message(filters.command(f"stats{bot['id']}") & filters.group)
    async def stats_cmd(client, message):
        cpu_val, cpu_txt, ram_val, _ = get_system_status(0)
        status_bot = "🔥 SEDANG LEMBUR" if bot["is_working"] else "💤 SEDANG ISTIRAHAT"
        text = (
            f"🐴 **[{bot['name']}] STATUS SERVER (V9.1)**\n"
            f"──────────────────\n"
            f"🤖 **Status:** {status_bot}\n"
            f"🌡️ **Resource Bot:**\n"
            f"• 🧠 CPU: {cpu_val}% [{cpu_txt}]\n"
            f"• 💾 RAM: {ram_val:.2f} MB\n"
            f"──────────────────"
        )
        await message.reply(text)

    @bot["client"].on_message(filters.command(f"ping{bot['id']}") & filters.group)
    async def ping_cmd(client, message):
        start = time.time()
        msg = await message.reply(f"🏓 **[{bot['name']}] Pong!**")
        end = time.time()
        await msg.edit(f"🏓 **[{bot['name']}] Pong!** `{(end - start) * 1000:.2f}ms`")

# Register all
for bot in BOTS:
    register_handlers(bot)

# --- WEB SERVER (UNCHANGED) ---
async def web_handler(request):
    return web.Response(text="Multi-Bot Running V9.1 (Multi-Bot Edition).")

async def start_web():
    app_web = web.Application()
    app_web.add_routes([web.get('/', web_handler)])
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

# --- MAIN ---
async def main():
    await start_web()
    logger.info(f"🤖 Starting {len(BOTS)} bot(s)...")
    tasks = [bot["client"].start() for bot in BOTS]
    await asyncio.gather(*tasks)
    logger.info("✅ All bots ready!")
    await idle()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
