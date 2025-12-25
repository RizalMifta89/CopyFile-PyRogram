import os
import asyncio
import random
import re
import logging
import sys
import gc
import time
import psutil
import io
import json
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

logger.info("--- SYSTEM BOOT: V9.6 WITH NEW FEATURES (ON/OFF CONFIGURABLE) ---")

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
        chat_id_str = private_match.group(1)
        msg_id = int(private_match.group(2))
        return int("-100" + chat_id_str), msg_id
        
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
        'ember': r"ember:\s*(\d+)",
        'dynamic_delay': r"dynamic_delay:\s*(\w+)",
        'error_notify': r"error_notify:\s*(\w+)",
        'admin_chat': r"admin_chat:\s*(.+)",
        'date_from': r"date_from:\s*(.+)",
        'date_to': r"date_to:\s*(.+)",
        'keyword': r"keyword:\s*(.+)",
        'mode': r"mode:\s*(\w+)",
        'auto_batch': r"auto_batch:\s*(\w+)",
        'export_stats': r"export_stats:\s*(\w+)",
        'anti_modify': r"anti_modify:\s*(\w+)"
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            if key == 'dst':
                dst_links = match.group(1).strip().split()
                config['dst_links'] = dst_links
            elif key in ['speed']:
                config[key] = float(match.group(1))
            elif key in ['batch_size', 'batch_time', 'ember']:
                config[key] = int(match.group(1))
            elif key in ['dynamic_delay', 'error_notify', 'mode', 'auto_batch', 'export_stats', 'anti_modify']:
                config[key] = match.group(1).strip().lower() == 'on'
            else:
                config[key] = match.group(1).strip().lower() if key == 'filter_type' else match.group(1).strip()
    
    # Parse dynamic filter_tujuanN
    filter_tujuan_matches = re.findall(r"filter_tujuan(\d+):\s*(\w+)", text, re.IGNORECASE)
    config['filter_tujuan'] = {int(num): val.lower() for num, val in filter_tujuan_matches}
    
    return config

# --- 5. VALIDATE CONFIG ---
def validate_config(config: Dict) -> Tuple[bool, str]:
    required = ['src_start', 'src_end']
    for req in required:
        if req not in config:
            return False, f"Missing required field: {req}"
    
    if 'dst_links' not in config or not config['dst_links']:
        return False, "Missing or empty 'tujuan:' field"
    
    try:
        config['delay_min'] = config.get('speed', DEFAULT_SPEED)
        if config['delay_min'] <= 0:
            return False, "Speed must be positive"
        
        default_filter_str = config.get('filter_type', 'all')
        default_filter = FilterType(default_filter_str)
        
        # Validate filter_tujuan
        config['dst_filters'] = []
        for i in range(len(config['dst_links'])):
            filter_str = config['filter_tujuan'].get(i+1, default_filter_str)
            config['dst_filters'].append(FilterType(filter_str))
        
        config['batch_size'] = config.get('batch_size', DEFAULT_BATCH_SIZE)
        config['batch_time'] = config.get('batch_time', DEFAULT_BATCH_TIME)
        config['chunk_size'] = config.get('ember', DEFAULT_CHUNK_SIZE)
        
        if config['batch_size'] <= 0 or config['batch_time'] < 0 or config['chunk_size'] <= 0:
            return False, "Batch/Ember values must be positive"
        
        # Selective copy validation
        if 'date_from' in config or 'date_to' in config or 'keyword' in config:
            config['selective_copy'] = True
        else:
            config['selective_copy'] = False
        
        # Defaults for new features
        config['dynamic_delay'] = config.get('dynamic_delay', False)  # Default off
        config['error_notify'] = config.get('error_notify', False)  # Default off
        config['mode_aggressive'] = config.get('mode', False)  # Default off (safe)
        config['auto_batch'] = config.get('auto_batch', False)  # Default off
        config['export_stats'] = config.get('export_stats', True)  # Default on
        config['anti_modify'] = config.get('anti_modify', True)  # Default on
        
    except ValueError as e:
        return False, f"Invalid filter type: {e}. Pilihan: all, video, foto, dokumen, audio, allout"
    
    return True, ""

# --- 6. WORKER UTAMA (SMART CHUNKING / EMBER) ---
async def copy_worker(job: Dict, status_msg, checkpoint_msg, bot_id: int, app: Client, bot_logger, group_chat_id):
    bot_data[bot_id]['is_working'] = True
    bot_data[bot_id]['stop_event'].clear()
    
    start_id: int = job['start_id']
    end_id: int = job['end_id']
    src_chat: any = job['src_chat']
    
    batch_size: int = job['batch_size']
    batch_time: int = job['batch_time']
    delay_min: float = job['delay_min']
    chunk_size: int = job['chunk_size']
    
    dst_list: List[Dict] = job['dst_list']
    
    num_dst = len(dst_list)
    delay_avg: float = delay_min + 0.25
    dynamic_delay = job['dynamic_delay']
    error_notify = job['error_notify']
    admin_chat = job['admin_chat']
    selective_copy = job['selective_copy']
    date_from = job.get('date_from')
    date_to = job.get('date_to')
    keyword = job.get('keyword')
    mode_aggressive = job['mode_aggressive']
    auto_batch = job['auto_batch']
    export_stats_flag = job['export_stats']
    anti_modify = job['anti_modify']
    
    flood_count = 0
    last_progress_time = time.time()
    
    fetch_retries = 2 if mode_aggressive else 5
    max_retries = 3 if mode_aggressive else 10
    
    stats = {'success': 0, 'failed': 0, 'total': (end_id - start_id + 1) * num_dst}
    per_dst_stats = {i: {'success': 0, 'failed': 0} for i in range(num_dst)}
    
    processed_count = 0
    last_update_time = time.time()
    last_checkpoint_time = time.time()
    last_error_log = "-"
    update_counter = 0  # For anti_modify

    try:
        for chunk_start in range(start_id, end_id + 1, chunk_size):
            if bot_data[bot_id]['stop_event'].is_set():
                break

            chunk_end = min(chunk_start + chunk_size - 1, end_id)
            ids_to_fetch = list(range(chunk_start, chunk_end + 1))
            
            messages_batch = []
            for retry in range(fetch_retries):
                try:
                    messages_batch = await app.get_messages(src_chat, ids_to_fetch)
                    break
                except FloodWait as e:
                    flood_count += 1
                    await asyncio.sleep(e.value + 5)
                except Exception as e:
                    last_error_log = str(e)
                    bot_logger.warning(f"⚠️ Fetch chunk {chunk_start}-{chunk_end} failed (retry {retry+1}): {e}")
                    if retry == fetch_retries - 1:
                        for _ in ids_to_fetch:
                            stats['failed'] += num_dst
                            for i in range(num_dst):
                                per_dst_stats[i]['failed'] += 1
                        continue
                    await asyncio.sleep(5)

            if not messages_batch:
                continue

            for msg in messages_batch:
                if bot_data[bot_id]['stop_event'].is_set():
                    break
                
                # Selective Copy Check
                if selective_copy:
                    skip = False
                    if date_from and msg.date < time.mktime(time.strptime(date_from, "%Y-%m-%d")):
                        skip = True
                    if date_to and msg.date > time.mktime(time.strptime(date_to, "%Y-%m-%d")):
                        skip = True
                    if keyword and keyword.lower() not in (msg.text or "").lower():
                        skip = True
                    if skip:
                        stats['failed'] += num_dst
                        for i in range(num_dst):
                            per_dst_stats[i]['failed'] += 1
                        continue
                
                # Auto Batch Scaling
                if auto_batch:
                    cpu, _, _, _ = get_system_status()
                    if cpu > 50:
                        batch_time += 30  # Extra sleep if high load
                    if num_dst > 3:
                        batch_size = max(1, batch_size // 2)
                
                # BATCH SLEEP
                if processed_count > 0 and processed_count % batch_size == 0:
                    await status_msg.edit(f"😴 **SEDANG ISTIRAHAT BATCH ({batch_time}s)...**\n\n❄️ Mendinginkan Mesin...")
                    await asyncio.sleep(batch_time)
                    last_update_time = time.time()

                # Cek Validitas
                if not msg or msg.empty or msg.service:
                    stats['failed'] += num_dst
                    for i in range(num_dst):
                        per_dst_stats[i]['failed'] += 1
                    continue

                # Parallel Copy Tasks
                copy_tasks = []
                for idx, dst in enumerate(dst_list):
                    if not dst['active']:
                        per_dst_stats[idx]['failed'] += 1
                        continue
                    
                    # Filtering per dst
                    should_copy = False
                    filter_type = dst['filter']
                    if filter_type == FilterType.VIDEO and msg.video: should_copy = True
                    elif filter_type == FilterType.FOTO and msg.photo: should_copy = True
                    elif filter_type == FilterType.DOKUMEN and msg.document: should_copy = True
                    elif filter_type == FilterType.AUDIO and (msg.audio or msg.voice): should_copy = True
                    elif filter_type == FilterType.ALL and not msg.sticker: should_copy = True
                    elif filter_type == FilterType.ALLOUT: should_copy = True
                    
                    if not should_copy:
                        per_dst_stats[idx]['failed'] += 1
                        continue

                    # Create copy task
                    async def copy_to_dst(dst_info, msg_id):
                        for retry_idx in range(max_retries):
                            try:
                                copy_params = {'chat_id': dst_info['chat']}
                                if dst_info['topic']:
                                    copy_params['reply_to_message_id'] = dst_info['topic']

                                await msg.copy(**copy_params)
                                
                                per_dst_stats[idx]['success'] += 1
                                dst_info['last_success_id'] = msg_id
                                
                                return True
                            except FloodWait as e:
                                nonlocal flood_count, delay_min
                                flood_count += 1
                                bot_logger.info(f"FloodWait for dst {idx}: Sleeping for {e.value} seconds")
                                await asyncio.sleep(e.value + 10)
                                if dynamic_delay and flood_count > 3:
                                    delay_min *= 1.2  # Increase delay 20%
                                    flood_count = 0
                            except (PeerIdInvalid, ChannelInvalid, ChannelPrivate) as e:
                                last_error_log = f"Peer Invalid for dst {idx}: {str(e)}"
                                bot_logger.error(last_error_log)
                                if time.time() - dst_info['refresh_cooldown'] > 300:
                                    try:
                                        await app.get_chat(dst_info['chat'])
                                        dst_info['refresh_cooldown'] = time.time()
                                        bot_logger.info(f"Refreshed peer for dst {idx}")
                                    except Exception as refresh_e:
                                        bot_logger.error(f"Refresh failed for dst {idx}: {refresh_e}")
                                        dst_info['active'] = False
                                        return False
                            except RPCError as e:
                                last_error_log = f"RPCError for dst {idx}: {str(e)}"
                                if "500" in str(e) or "INTERDC" in str(e):
                                    await asyncio.sleep(10)
                                else:
                                    await asyncio.sleep(5)
                            except Exception as e:
                                last_error_log = f"Error for dst {idx}: {str(e)}"
                                await asyncio.sleep(5)
                        
                        per_dst_stats[idx]['failed'] += 1
                        return False

                    copy_tasks.append(copy_to_dst(dst, msg.id))

                # Run parallel if multiple dst
                if copy_tasks:
                    results = await asyncio.gather(*copy_tasks, return_exceptions=True)
                    for res in results:
                        if isinstance(res, Exception):
                            bot_logger.warning(f"Parallel copy exception: {res}")
                        elif res:
                            stats['success'] += 1
                            last_progress_time = time.time()
                        else:
                            stats['failed'] += 1
                    processed_count += len(copy_tasks)
                    await asyncio.sleep(random.uniform(delay_min, delay_min + 0.5))

                # Idle Detection (built-in, always on)
                if time.time() - last_progress_time > 300:  # 5 min no success
                    bot_data[bot_id]['stop_event'].set()
                    last_error_log = "Idle Detected: No progress for 5 min"
                    if error_notify and admin_chat:
                        await app.send_message(admin_chat, f"⚠️ Idle Detected in Bot {bot_id}: {last_error_log}")

                # Update Status (Pesan 1 - Dashboard, tiap 10s)
                if time.time() - last_update_time > 10:
                    current_proc = stats['success'] + stats['failed']
                    remaining_files = stats['total'] - current_proc
                    
                    eta_val = (remaining_files * delay_avg) + ((remaining_files // batch_size) * batch_time)
                    eta_text = format_time(eta_val)

                    bar_str = make_bar(current_proc, stats['total'])
                    cpu_val, cpu_txt, ram_val, speed_txt = get_system_status(delay_avg)
                    
                    active_dst = sum(1 for d in dst_list if d['active'])
                    text = (
                        f"🐎 **WORKHORSE V9.6: MULTI-DEST (BOT {bot_id})**\n"
                        f"{bar_str}\n\n"
                        f"📊 **Stats:** Total `{stats['total']}` | Sukses `{stats['success']}` | Gagal `{stats['failed']}` | Sisa `{remaining_files}`\n"
                        f"🏁 **ETA:** ± {eta_text} | Tujuan Aktif: `{active_dst}/{num_dst}`\n\n"
                    )
                    if num_dst > 1:  # UI Enhancement: Breakdown only for multi-dst
                        text += "📈 **Per Tujuan:**\n"
                        for idx, dst in enumerate(dst_list):
                            status_emoji = "✅" if dst['active'] else "❌"
                            text += f"Tujuan {idx+1}: Sukses {per_dst_stats[idx]['success']}/Gagal {per_dst_stats[idx]['failed']} {status_emoji}\n"
                    text += (
                        f"🌡️ **Resources:** CPU {cpu_val}% [{cpu_txt}] | RAM {ram_val:.2f} MB\n\n"
                        f"⚡ **Config:** Ember {chunk_size} | Jeda {delay_avg:.2f}s | {speed_txt}\n"
                        f"Batch: {batch_time}s tiap {batch_size} file\n\n"
                        f"🔄 Update tiap 10s | ⚠️ Last Error: {last_error_log}"
                    )
                    if anti_modify:
                        text += f" | #{update_counter}"  # Force change
                        update_counter += 1
                    
                    try:
                        await status_msg.edit(text)
                        last_update_time = time.time()
                    except MessageNotModified:
                        pass  # Ignore if enabled
                    except Exception as e:
                        if error_notify and admin_chat:
                            await app.send_message(admin_chat, f"⚠️ Error in Bot {bot_id}: {e}")

                 # Update Checkpoint (Pesan 2, tiap 60s)
                if time.time() - last_checkpoint_time > 60:
                    saved_time = time.strftime("%H:%M:%S")
                    checkpoint_text = f"💾 AUTOSAVE: CHECKPOINT (BOT {bot_id}) ➖➖➖➖➖➖➖➖➖➖\n\n"
                    for idx, dst in enumerate(dst_list):
                        status = "Aktif ✅" if dst['active'] else "Non-Aktif ❌ (Error)"
                        remaining_per_dst = (end_id - dst['last_success_id']) if dst['active'] else 0
                        eta_per_dst = format_time(remaining_per_dst * delay_avg)
                        checkpoint_text += f"📌 Tujuan {idx+1} ({dst['chat']}): Last ID {dst['last_success_id']} | {status} | ETA: {eta_per_dst}\n"
                    checkpoint_text += f"🕒 Saved: {saved_time}"
                    if anti_modify:
                        checkpoint_text += f" | #{update_counter}"
                        update_counter += 1
                    
                    try:
                        await checkpoint_msg.edit(checkpoint_text)
                        last_checkpoint_time = time.time()
                    except MessageNotModified:
                        pass
                    except Exception as e:
                        if error_notify and admin_chat:
                            await app.send_message(admin_chat, f"⚠️ Error in Bot {bot_id}: {e}")
            
            del messages_batch
            gc.collect()

        final_msg = "✅ **SELESAI!**" if not bot_data[bot_id]['stop_event'].is_set() else "🛑 **DIBATALKAN!**"
        await status_msg.edit(
            f"{final_msg}\n\n"
            f"📊 **Laporan Akhir (BOT {bot_id}):** Total `{stats['total']}` | Sukses `{stats['success']}` | Gagal/Skip `{stats['failed']}`\n"
            f"📝 **Last Error:** {last_error_log}"
        )
        
        # Update Checkpoint akhir
        saved_time = time.strftime("%H:%M:%S")
        checkpoint_text = f"💾 AUTOSAVE: CHECKPOINT (BOT {bot_id}) ➖➖➖➖➖➖➖➖➖➖\n\n"
        for idx, dst in enumerate(dst_list):
            status = "Aktif ✅" if dst['active'] else "Non-Aktif ❌ (Error)"
            remaining_per_dst = (end_id - dst['last_success_id']) if dst['active'] else 0
            eta_per_dst = format_time(remaining_per_dst * delay_avg)
            checkpoint_text += f"📌 Tujuan {idx+1} ({dst['chat']}): Last ID {dst['last_success_id']} | {status} | ETA: {eta_per_dst}\n"
        checkpoint_text += f"🕒 Saved: {saved_time}"
        await checkpoint_msg.edit(checkpoint_text)

        # Export Stats to File if enabled
        if export_stats_flag:
            stats_data = {
                'total': stats['total'],
                'success': stats['success'],
                'failed': stats['failed'],
                'per_dst': per_dst_stats,
                'last_error': last_error_log
            }
            stats_json = json.dumps(stats_data, indent=4)
            stats_file = io.BytesIO(stats_json.encode())
            stats_file.name = f"stats_bot_{bot_id}.json"
            await app.send_document(group_chat_id, stats_file, caption=f"📊 Stats Akhir Bot {bot_id}")

    except Exception as e:
        bot_logger.error(f"❌ CRASH IN WORKER: {e}")
        await status_msg.edit(f"❌ **CRASH SYSTEM:** {e}")
        if error_notify and admin_chat:
            await app.send_message(admin_chat, f"❌ CRASH in Bot {bot_id}: {e}")
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
        group_chat_id = message.chat.id
        if bot_data[bot_id]['is_working']:
            return await message.reply(f"⚠️ **Bot {bot_id} Sedang Sibuk!** Gunakan `/{stop_commands[-1]}` dulu.")
        
        try:
            config = parse_config(message.text)
            valid, error = validate_config(config)
            if not valid:
                return await message.reply(f"❌ **Config Gagal:** {error}\nCoba cek format perintah.")

            src_chat, start_id = parse_link(config['src_start'])
            _, end_id = parse_link(config['src_end'])

            if not src_chat or not start_id or not end_id:
                return await message.reply("❌ **Link Sumber Salah Format!** Pastikan link valid.")

            # Parse multiple dst
            dst_list = []
            for link in config['dst_links']:
                dst_chat, dst_topic = parse_link(link)
                if not dst_chat:
                    return await message.reply(f"❌ **Link Tujuan Salah: {link}**")
                dst_list.append({
                    'chat': dst_chat,
                    'topic': dst_topic,
                    'filter': config['dst_filters'][len(dst_list) - 1],
                    'last_success_id': start_id - 1,
                    'active': True,
                    'refresh_cooldown': 0
                })

            status_msg = await message.reply(f"🔍 **Verifikasi Akses Channel (Bot {bot_id})...**")

            try:
                chat_src = await client.get_chat(src_chat)
                bot_logger.info(f"Source verified: {chat_src.title}")

                for idx, dst in enumerate(dst_list):
                    try:
                        chat_dst = await client.get_chat(dst['chat'])
                        bot_logger.info(f"Dest {idx+1} verified: {chat_dst.title}")
                    except Exception as e:
                        dst['active'] = False
                        bot_logger.warning(f"Dest {idx+1} verification failed: {e}")

            except Exception as e:
                return await status_msg.edit(f"❌ **Verifikasi Gagal:** {e}")

            await status_msg.edit(f"🐎 **Bot {bot_id} Memulai Proses Copy ke {len(dst_list)} Tujuan...**")

            initial_saved_time = time.strftime("%H:%M:%S")
            checkpoint_text = f"💾 AUTOSAVE: CHECKPOINT (BOT {bot_id}) ➖➖➖➖➖➖➖➖➖➖\n\n"
            for idx, dst in enumerate(dst_list):
                status = "Aktif ✅" if dst['active'] else "Non-Aktif ❌ (Error)"
                eta_per_dst = format_time((end_id - start_id + 1) * config['delay_min'])
                checkpoint_text += f"📌 Tujuan {idx+1} ({dst['chat']}): Last ID {dst['last_success_id']} | {status} | ETA: {eta_per_dst}\n"
            checkpoint_text += f"🕒 Saved: {initial_saved_time}"
            checkpoint_msg = await message.reply(checkpoint_text)

            job = {
                'src_chat': src_chat, 
                'start_id': start_id, 
                'end_id': end_id,
                'dst_list': dst_list,
                'delay_min': config['delay_min'],
                'batch_size': config['batch_size'],
                'batch_time': config['batch_time'],
                'chunk_size': config['chunk_size'],
                'dynamic_delay': config['dynamic_delay'],
                'error_notify': config['error_notify'],
                'admin_chat': config.get('admin_chat'),
                'selective_copy': config['selective_copy'],
                'date_from': config.get('date_from'),
                'date_to': config.get('date_to'),
                'keyword': config.get('keyword'),
                'mode_aggressive': config['mode_aggressive'],
                'auto_batch': config['auto_batch'],
                'export_stats': config['export_stats'],
                'anti_modify': config['anti_modify']
            }
            
            asyncio.create_task(copy_worker(job, status_msg, checkpoint_msg, bot_id, client, bot_logger, group_chat_id))
            
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
        cpu_val, cpu_txt, ram_val, _ = get_system_status(0)
        status_bot = "🔥 Aktif" if bot_data[bot_id]['is_working'] else "💤 Istirahat"
        text = (
            f"🐴 **Status Server V9.6 (BOT {bot_id})**\n"
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

    @app.on_message(filters.command("panduan") & filters.group)
    async def panduan_cmd(client, message):
        panduan_text = """
/start2
sumber_awal: https://t.me/jj/25000
sumber_akhir: https://t.me/bb/25050
tujuan: https://t.me/c/nnn/1311 https://t.me/c/mmm/1414 https://t.me/ooo/1515  # Multiple tujuan, split by space
speed: 2
filter: allout  # Default filter untuk semua tujuan jika tidak ada filter_tujuanN
filter_tujuan1: video  # Filter khusus untuk tujuan pertama (index 1)
filter_tujuan2: foto   # Untuk tujuan kedua
# filter_tujuan3: default ke 'allout' karena tidak dispecify
batch_size: 500
batch_time: 60
ember: 100
dynamic_delay: on  # Aktifkan dynamic delay adjustment (default: off)
error_notify: on  # Aktifkan notif error ke admin (default: off)
admin_chat: @username_admin  # Chat untuk notif error
date_from: 2023-01-01  # Filter msg dari tanggal ini (selective copy)
date_to: 2023-12-31  # Filter msg sampai tanggal ini
keyword: kata_kunci  # Filter msg yang mengandung keyword
mode: aggressive  # Mode aggressive (retry rendah, default: off/safe)
auto_batch: on  # Auto scaling batch size (default: off)
export_stats: on  # Export stats ke file JSON di akhir (default: on)
anti_modify: on  # Handle MESSAGE_NOT_MODIFIED (default: on)
"""
        await message.reply(panduan_text)

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
    return web.Response(text="Multi-Bot Running V9.6 (Enhanced Features).")

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