import os
import asyncio
import random
import re
import logging
import sys
import gc
import time
import psutil
from datetime import datetime
from enum import Enum
from typing import List, Tuple, Optional, Dict
from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait, RPCError, PeerIdInvalid, ChannelInvalid, ChannelPrivate, MessageNotModified
from aiohttp import web

# --- LOGGING SYSTEM ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logging.getLogger("pyrogram").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

logger.info("--- SYSTEM BOOT: V9.9 STABLE (BASE V9.4 + DUAL MSG + ANTI-PIKUN) ---")

# --- KONFIGURASI MULTI-BOT ---
NUM_BOTS = 5
clients = []
bot_data = []  # List of dicts for each bot

try:
    PORT = int(os.environ.get("PORT", 8080))
except ValueError as e:
    logger.error(f"❌ Config Error: {e}")
    sys.exit(1)

DEFAULT_BATCH_SIZE = 10000
DEFAULT_BATCH_TIME = 60
DEFAULT_CHUNK_SIZE = 50
DEFAULT_SPEED = 0.1
CHECKPOINT_INTERVAL = 50  # Interval untuk pesan kedua

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
    # Match private link: https://t.me/c/1234567890/100 -> ID: -1001234567890
    private_match = re.search(r"t\.me/c/(\d+)/(\d+)", link)
    if private_match:
        chat_id_str = private_match.group(1)
        msg_id = int(private_match.group(2))
        return int("-100" + chat_id_str), msg_id
        
    # Match public link: https://t.me/username/100
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

# --- 6. WORKER UTAMA (BASIS V9.4 + FITUR BARU) ---
async def copy_worker(job: Dict, status_msg, checkpoint_msg, bot_id: int, app: Client, bot_logger):
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
    
    last_update_time = time.time()       # Untuk Dashboard
    last_checkpoint_time = time.time()   # Untuk Checkpoint (Fitur Baru)
    
    last_error_log = "-"
    last_success_id = start_id - 1

    try:
        for chunk_start in range(start_id, end_id + 1, chunk_size):
            if bot_data[bot_id]['stop_event'].is_set():
                break

            chunk_end = min(chunk_start + chunk_size - 1, end_id)
            ids_to_fetch = list(range(chunk_start, chunk_end + 1))
            
            messages_batch = []
            fetch_retries = 5
            
            # --- FETCHING DENGAN ANTI-PIKUN ---
            for retry in range(fetch_retries):
                try:
                    messages_batch = await app.get_messages(src_chat, ids_to_fetch)
                    break
                except (PeerIdInvalid, ChannelInvalid):
                    # FITUR ANTI-PIKUN: Refresh Source
                    bot_logger.warning(f"⚠️ Peer Invalid saat Fetch (Bot {bot_id}). Refreshing Source...")
                    try:
                        await app.get_chat(src_chat)
                        await asyncio.sleep(2)
                        continue # Retry loop
                    except: break
                except FloodWait as e:
                    await asyncio.sleep(e.value + 5)
                except Exception as e:
                    last_error_log = str(e)
                    bot_logger.warning(f"⚠️ Fetch chunk {chunk_start}-{chunk_end} failed (retry {retry+1}): {e}")
                    if retry == fetch_retries - 1:
                        stats['failed'] += len(ids_to_fetch)
                        continue
                    await asyncio.sleep(5)

            if not messages_batch:
                continue

            for msg in messages_batch:
                if bot_data[bot_id]['stop_event'].is_set():
                    break
                
                # BATCH SLEEP
                if processed_count > 0 and processed_count % batch_size == 0:
                    try:
                        await status_msg.edit(f"😴 **SEDANG ISTIRAHAT BATCH ({batch_time}s)...**\n\n❄️ Mendinginkan Mesin...")
                    except MessageNotModified:
                        pass
                    await asyncio.sleep(batch_time)
                    last_update_time = time.time()

                # Cek Validitas
                if not msg or msg.empty or msg.service:
                    stats['failed'] += 1
                    continue

                # Filtering
                should_copy = False
                if filter_type == FilterType.VIDEO and msg.video: should_copy = True
                elif filter_type == FilterType.FOTO and msg.photo: should_copy = True
                elif filter_type == FilterType.DOKUMEN and msg.document: should_copy = True
                elif filter_type == FilterType.AUDIO and (msg.audio or msg.voice): should_copy = True
                elif filter_type == FilterType.ALL and not msg.sticker: should_copy = True
                elif filter_type == FilterType.ALLOUT: should_copy = True
                
                if not should_copy:
                    stats['failed'] += 1
                    continue

                # Copy Logic
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
                        last_success_id = msg.id
                        msg_success = True
                        
                        await asyncio.sleep(random.uniform(delay_min, delay_min + 0.5))
                        break

                    except FloodWait as e:
                        bot_logger.info(f"FloodWait: Sleeping for {e.value} seconds")
                        try:
                            await status_msg.edit(f"🌊 **Kena Limit Telegram!**\nTunggu {e.value} detik...")
                        except: pass
                        await asyncio.sleep(e.value + 10)
                    
                    except (PeerIdInvalid, ChannelInvalid, ChannelPrivate):
                        # FITUR ANTI-PIKUN: Refresh Destination
                        bot_logger.error(f"Peer Invalid saat copy: {dst_chat}. Refreshing...")
                        try:
                            await app.get_chat(dst_chat)
                            await asyncio.sleep(2)
                            # Retry loop will continue
                        except Exception as e:
                            last_error_log = f"Refresh Dest Failed: {e}"
                            break 
                    
                    except RPCError as e:
                        last_error_log = str(e)
                        if "500" in str(e) or "INTERDC" in str(e):
                            await asyncio.sleep(10)
                        else:
                            await asyncio.sleep(5)
                    except Exception as e:
                        last_error_log = str(e)
                        await asyncio.sleep(5)

                if not msg_success:
                    stats['failed'] += 1

                # --- UPDATE PESAN 1 (DASHBOARD) - 10 Detik ---
                if time.time() - last_update_time > 10:
                    current_proc = stats['success'] + stats['failed']
                    remaining_files = stats['total'] - current_proc
                    
                    eta_val = (remaining_files * delay_avg) + ((remaining_files // batch_size) * batch_time)
                    eta_text = format_time(eta_val)

                    bar_str = make_bar(current_proc, stats['total'])
                    cpu_val, cpu_txt, ram_val, speed_txt = get_system_status(delay_avg)
                    
                    text_dashboard = (
                        f"🐎 **WORKHORSE V9.9: BASE 9.4 + DUAL MSG (BOT {bot_id})**\n"
                        f"{bar_str}\n\n"
                        f"📊 **Stats:** Total `{stats['total']}` | Sukses `{stats['success']}` | Gagal `{stats['failed']}` | Sisa `{remaining_files}`\n"
                        f"🏁 **ETA:** ± {eta_text} | Filter: `{filter_type.value.upper()}`\n\n"
                        f"🌡️ **Resources:** CPU {cpu_val}% [{cpu_txt}] | RAM {ram_val:.2f} MB\n\n"
                        f"⚡ **Config:** Ember {chunk_size} | Jeda {delay_avg:.2f}s | {speed_txt}\n"
                        f"Batch: {batch_time}s tiap {batch_size} file\n\n"
                        f"🔄 Update tiap 10s | ⚠️ Last Error: {last_error_log}"
                    )
                    
                    # FITUR ANTI-BAPER
                    try:
                        await status_msg.edit(text_dashboard)
                    except MessageNotModified:
                        pass # Abaikan jika pesan sama
                    except Exception as e:
                        bot_logger.warning(f"Failed to update status: {e}")
                    
                    last_update_time = time.time()

                # --- UPDATE PESAN 2 (CHECKPOINT) - 50 Detik ---
                if time.time() - last_checkpoint_time > CHECKPOINT_INTERVAL:
                    time_now = datetime.now().strftime("%H:%M:%S")
                    # Format Sesuai Request Opsi 1
                    text_checkpoint = (
                        f"💾 **AUTOSAVE: CHECKPOINT (BOT {bot_id})**\n"
                        f"➖➖➖➖➖➖➖➖➖➖\n\n"
                        f"📌 **Last ID:** `{last_success_id}`\n"
                        f"🕒 **Saved:** {time_now}"
                    )
                    
                    # FITUR ANTI-BAPER
                    try:
                        await checkpoint_msg.edit(text_checkpoint)
                    except MessageNotModified:
                        pass
                    except Exception as e:
                        bot_logger.warning(f"Failed to update checkpoint: {e}")
                    
                    last_checkpoint_time = time.time()
            
            del messages_batch
            gc.collect()

        final_msg = "✅ **SELESAI!**" if not bot_data[bot_id]['stop_event'].is_set() else "🛑 **DIBATALKAN!**"
        
        # Update Dashboard Final
        try:
            await status_msg.edit(
                f"{final_msg}\n\n"
                f"📊 **Laporan Akhir (BOT {bot_id}):** Total `{stats['total']}` | Sukses `{stats['success']}` | Gagal/Skip `{stats['failed']}`\n"
                f"📝 **Last Error:** {last_error_log}"
            )
        except: pass

        # Update Checkpoint Final
        try:
            await checkpoint_msg.edit(
                f"💾 **FINAL CHECKPOINT (BOT {bot_id})**\n"
                f"➖➖➖➖➖➖➖➖➖➖\n"
                f"📌 **Finish ID:** `{last_success_id}`\n"
                f"🏁 **Status:** {final_msg}"
            )
        except: pass

    except Exception as e:
        bot_logger.error(f"❌ CRASH IN WORKER: {e}")
        try:
            await status_msg.edit(f"❌ **CRASH SYSTEM:** {e}")
        except: pass
    finally:
        bot_data[bot_id]['is_working'] = False

# --- COMMANDS (DINAMIS & ROBUST) ---
def register_handlers(app: Client, bot_id: int):
    bot_logger = logging.getLogger(f"{__name__}.bot{bot_id}")
    bot_logger.handlers = logger.handlers
    bot_logger.setLevel(logger.level)

    if bot_id == 1:
        start_commands = ["start", "start1"]
        stop_commands = ["stop", "stop1"]
        stats_commands = ["stats", "stats1"]
    else:
        start_commands = [f"start{bot_id}"]
        stop_commands = [f"stop{bot_id}"]
        stats_commands = [f"stats{bot_id}"]

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
                return await message.reply("❌ **Link Salah Format!** Pastikan link valid (contoh: `https://t.me/c/1234/10`).")

            # Kirim DUA Pesan Awal
            status_msg = await message.reply(f"🐎 **Bot {bot_id} Menyiapkan Dashboard...**")
            checkpoint_msg = await message.reply(f"💾 **Bot {bot_id} Menyiapkan Checkpoint...**")
            
            await status_msg.edit(f"🔍 **Verifikasi Akses Channel (Bot {bot_id})...**")

            # --- FIX PEER ID INVALID (ANTI-PIKUN CHECK AWAL) ---
            try:
                # Cek Sumber
                try:
                    chat_src = await client.get_chat(src_chat)
                    bot_logger.info(f"Source verified: {chat_src.title}")
                except Exception as e:
                    return await status_msg.edit(f"❌ **Gagal Akses SUMBER:**\nBot belum join ke channel/grup sumber atau ID salah.\n\nError: `{e}`")

                # Cek Tujuan
                try:
                    chat_dst = await client.get_chat(dst_chat)
                    bot_logger.info(f"Dest verified: {chat_dst.title}")
                except Exception as e:
                    return await status_msg.edit(f"❌ **Gagal Akses TUJUAN:**\nBot belum admin di channel/grup tujuan.\n\nError: `{e}`")

            except Exception as e:
                return await status_msg.edit(f"❌ **Verifikasi Gagal:** {e}")

            # Jika lolos verifikasi, lanjut
            await status_msg.edit(f"🐎 **Bot {bot_id} Memulai Proses Copy...**")

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
            
            # Panggil Worker dengan 2 Pesan
            asyncio.create_task(copy_worker(job, status_msg, checkpoint_msg, bot_id, client, bot_logger))
            
        except Exception as e:
            bot_logger.error(f"❌ Error in start_cmd: {e}")
            await message.reply(f"❌ **Error Config:** {e}")

    @app.on_message(filters.command(stop_commands) & filters.group)
    async def stop_cmd(client, message):
        if bot_data[bot_id]['is_working']:
            bot_data[bot_id]['stop_event'].set()
            await message.reply(f"🛑 **Proses Bot {bot_id} Dihentikan!** Menunggu selesai...")
        else:
            await message.reply(f"💤 **Bot {bot_id} Tidak Ada Proses Berjalan.**")

    @app.on_message(filters.command(stats_commands) & filters.group)
    async def stats_cmd(client, message):
        cp
