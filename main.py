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
from pyrogram.errors import FloodWait, RPCError, MessageIdInvalid, MessageEmpty
from aiohttp import web

# --- LOGGING SYSTEM ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logging.getLogger("pyrogram").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

logger.info("--- SYSTEM BOOT: V10.0 MASTERPIECE EDITION Dibuat Oleh RizalMifta---")

# --- KONFIGURASI MULTI-BOT ---
# Ubah angka ini jika ingin menambah bot (pastikan Env Vars sudah diisi)
NUM_BOTS = 5 
clients = []
bot_data = [] 

try:
    PORT = int(os.environ.get("PORT", 8080))
except ValueError as e:
    logger.error(f"❌ Config Error: {e}")
    sys.exit(1)

# --- DEFAULT SETTINGS ---
DEFAULT_BATCH_SIZE = 1000
DEFAULT_BATCH_TIME = 60
DEFAULT_CHUNK_SIZE = 50 
DEFAULT_SPEED = 0.1
DEFAULT_FETCH_RETRIES = 5  # Default Mengambil Pesan
DEFAULT_COPY_RETRIES = 10  # Default Mengirim Pesan

class FilterType(Enum):
    ALL = 'all'
    VIDEO = 'video'
    FOTO = 'foto'
    DOKUMEN = 'dokumen'
    AUDIO = 'audio'
    ALLOUT = 'allout'

# --- 1. HELPER: FORMAT WAKTU & SYSTEM STATUS ---
def format_time(seconds: float) -> str:
    if seconds < 60: return f"{int(seconds)} detik"
    elif seconds < 3600: return f"{int(seconds // 60)} menit {int(seconds % 60)} detik"
    else: return f"{int(seconds // 3600)} jam {int((seconds % 3600) // 60)} menit"

def make_bar(current: int, total: int, length: int = 10) -> str:
    try: pct = current / total
    except ZeroDivisionError: pct = 0
    filled = int(length * pct)
    return f"{'🟧' * filled}{'⬜' * (length - filled)} **{int(pct * 100)}%**"

def get_system_status(delay_avg: float = 0) -> Tuple[float, str, float, str]:
    try:
        proc = psutil.Process(os.getpid())
        cpu = proc.cpu_percent(interval=0.1)
        ram_mb = proc.memory_info().rss / (1024 * 1024)
        
        cpu_stat = "🟢 Santai" if cpu <= 10 else "🟡 Sibuk" if cpu <= 50 else "🔴 Berat"
        
        speed_stat = "💤 Idle"
        if delay_avg > 0:
            speed_stat = "🚀 MODE NGEBUT" if delay_avg <= 0.2 else "✅ Aman"
            
        return cpu, cpu_stat, ram_mb, speed_stat
    except:
        return 0.0, "?", 0.0, "?"

# --- 2. PARSE LINK ---
def parse_link(link: Optional[str]) -> Tuple[Optional[any], Optional[int]]:
    if not link: return None, None
    private_match = re.search(r"t\.me/c/(\d+)/(\d+)", link)
    if private_match: return int("-100" + private_match.group(1)), int(private_match.group(2))
    public_match = re.search(r"t\.me/([^/]+)/(\d+)", link)
    if public_match: return public_match.group(1), int(public_match.group(2))
    return None, None

# --- 3. PARSE CONFIG (DENGAN FITUR BARU) ---
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
        # --- FITUR BARU ---
        'fetch_retries': r"mengambil_pesan:\s*(\d+)",
        'copy_retries': r"mengirim_pesan:\s*(\d+)",
        'skip_kata': r"skip_kata:\s*(.+)",
        'ganti_kata': r"ganti_kata:\s*(.+)"
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            val = match.group(1).strip()
            if key in ['speed']: config[key] = float(val)
            elif key in ['batch_size', 'batch_time', 'ember', 'fetch_retries', 'copy_retries']: config[key] = int(val)
            elif key == 'skip_kata':
                # Pisah koma jadi list: "judi, slot" -> ['judi', 'slot']
                config[key] = [x.strip().lower() for x in val.split(',') if x.strip()]
            elif key == 'ganti_kata':
                # Parse format: "admin=mimin, sumber=" -> {'admin': 'mimin', 'sumber': ''}
                replacements = {}
                items = val.split(',')
                for item in items:
                    if '=' in item:
                        k, v = item.split('=', 1)
                        replacements[k.strip()] = v.strip()
                config[key] = replacements
            else:
                config[key] = val.lower() if key == 'filter_type' else val
    return config

# --- 4. VALIDATE CONFIG ---
def validate_config(config: Dict) -> Tuple[bool, str]:
    required = ['src_start', 'src_end', 'dst']
    for req in required:
        if req not in config: return False, f"Missing: {req}"
    
    try:
        config['delay_min'] = config.get('speed', DEFAULT_SPEED)
        filter_str = config.get('filter_type', 'all')
        config['filter_type'] = FilterType(filter_str)
        
        config['batch_size'] = config.get('batch_size', DEFAULT_BATCH_SIZE)
        config['batch_time'] = config.get('batch_time', DEFAULT_BATCH_TIME)
        config['chunk_size'] = config.get('ember', DEFAULT_CHUNK_SIZE)
        
        # Default Retries
        config['fetch_retries'] = config.get('fetch_retries', DEFAULT_FETCH_RETRIES)
        config['copy_retries'] = config.get('copy_retries', DEFAULT_COPY_RETRIES)
        
        # Default Filters
        config['skip_kata'] = config.get('skip_kata', [])
        config['ganti_kata'] = config.get('ganti_kata', {})
        
    except ValueError:
        return False, f"Filter salah: {filter_str}"
    
    return True, ""

# --- 5. WORKER UTAMA (FINAL) ---
async def copy_worker(job: Dict, status_msg, bot_id: int, app: Client, bot_logger):
    bot_data[bot_id]['is_working'] = True
    bot_data[bot_id]['stop_event'].clear()
    
    # Unpack job
    start_id, end_id = job['start_id'], job['end_id']
    src_chat, dst_chat = job['src_chat'], job['dst_chat']
    
    stats = {'success': 0, 'failed': 0, 'total': end_id - start_id + 1}
    processed_count = 0
    last_update_time = time.time()
    last_error_log = "-"

    try:
        for chunk_start in range(start_id, end_id + 1, job['chunk_size']):
            if bot_data[bot_id]['stop_event'].is_set(): break

            # ==========================================
            # 🛑 MEMORY BRAKE (REM DARURAT RENDER)
            # ==========================================
            proc = psutil.Process(os.getpid())
            mem_usage = proc.memory_info().rss / (1024 * 1024)
            if mem_usage > 420: # Batas Aman 420MB
                bot_logger.warning(f"⚠️ MEMORY CRITICAL: {mem_usage:.2f} MB. Braking!")
                if 'messages_batch' in locals(): del messages_batch
                gc.collect()
                await status_msg.edit(f"🥵 **RAM KRITIS ({mem_usage:.2f} MB)**\n🧊 Pendinginan 30 detik...")
                await asyncio.sleep(30)
            # ==========================================

            chunk_end = min(chunk_start + job['chunk_size'] - 1, end_id)
            ids_to_fetch = list(range(chunk_start, chunk_end + 1))
            
            # --- FETCH MESSAGES ---
            messages_batch = []
            for retry in range(job['fetch_retries']):
                try:
                    messages_batch = await app.get_messages(src_chat, ids_to_fetch)
                    break
                except Exception as e:
                    last_error_log = f"Fetch Error: {e}"
                    if retry == job['fetch_retries'] - 1:
                        stats['failed'] += len(ids_to_fetch)
                    await asyncio.sleep(3)
            
            if not messages_batch: continue

            for msg in messages_batch:
                if bot_data[bot_id]['stop_event'].is_set(): break
                
                # --- BATCH SLEEP ---
                if processed_count > 0 and processed_count % job['batch_size'] == 0:
                    await status_msg.edit(f"😴 **Batch Sleep ({job['batch_time']}s)...**")
                    await asyncio.sleep(job['batch_time'])
                    last_update_time = time.time()

                # Cek Pesan Kosong/Service
                if not msg or msg.empty or msg.service:
                    stats['failed'] += 1
                    continue

                # --- FILTER TIPE FILE ---
                ft = job['filter_type']
                should_copy = False
                if ft == FilterType.VIDEO and msg.video: should_copy = True
                elif ft == FilterType.FOTO and msg.photo: should_copy = True
                elif ft == FilterType.DOKUMEN and msg.document: should_copy = True
                elif ft == FilterType.AUDIO and (msg.audio or msg.voice): should_copy = True
                elif ft == FilterType.ALL and not msg.sticker: should_copy = True
                elif ft == FilterType.ALLOUT: should_copy = True
                
                if not should_copy:
                    stats['failed'] += 1
                    continue

                # --- PRE-PROCESSING: CAPTION & KEYWORDS ---
                original_caption = msg.caption or msg.text or ""
                final_caption = original_caption

                # 1. Skip Kata (Blacklist)
                if job['skip_kata']:
                    # Cek apakah ada kata terlarang di caption
                    if any(bad_word in original_caption.lower() for bad_word in job['skip_kata']):
                        bot_logger.info(f"⏩ Skipped msg {msg.id} due to keyword filter.")
                        stats['failed'] += 1 # Hitung sebagai skip
                        continue
                
                # 2. Ganti Kata (Replacer)
                if job['ganti_kata'] and final_caption:
                    for old_word, new_word in job['ganti_kata'].items():
                        final_caption = final_caption.replace(old_word, new_word)

                # --- EKSEKUSI COPY ---
                msg_success = False
                for retry_idx in range(job['copy_retries']):
                    try:
                        copy_params = {
                            'chat_id': dst_chat,
                            'caption': final_caption # Gunakan caption yang sudah diedit
                        }
                        if job['dst_topic']: copy_params['reply_to_message_id'] = job['dst_topic']

                        await msg.copy(**copy_params)
                        
                        stats['success'] += 1
                        processed_count += 1
                        msg_success = True
                        await asyncio.sleep(random.uniform(job['delay_min'], job['delay_min'] + 0.5))
                        break

                    except FloodWait as e:
                        bot_logger.warning(f"FloodWait: {e.value}s")
                        await asyncio.sleep(e.value + 5)
                    except (MessageIdInvalid, MessageEmpty):
                        # Jangan retry kalau pesan invalid/dihapus
                        break 
                    except Exception as e:
                        last_error_log = str(e)
                        await asyncio.sleep(3)

                if not msg_success: stats['failed'] += 1

                # --- UPDATE STATUS (HEMAT CPU) ---
                if time.time() - last_update_time > 10:
                    current = stats['success'] + stats['failed']
                    rem = stats['total'] - current
                    # Hitung ETA
                    delay_total = job['delay_min'] + 0.25
                    eta = (rem * delay_total) + ((rem // job['batch_size']) * job['batch_time'])
                    
                    cpu, cpu_txt, ram, speed = get_system_status(delay_total)
                    
                    text = (
                        f"🐎 **WORKHORSE V10: BOT {bot_id}**\n"
                        f"{make_bar(current, stats['total'])}\n\n"
                        f"📊 **Progres:** `{stats['success']}` Sukses | `{stats['failed']}` Gagal/Skip\n"
                        f"⏱️ **ETA:** ± {format_time(eta)}\n"
                        f"🌡️ **Server:** RAM {ram:.1f}MB | {cpu_txt}\n\n"
                        f"🔧 **Config:** Ember {job['chunk_size']} | Retry {job['copy_retries']}x\n"
                        f"🔎 Filter: `{ft.value}` | Skip: `{len(job['skip_kata'])} kata`"
                    )
                    try:
                        await status_msg.edit(text)
                        last_update_time = time.time()
                    except: pass
            
            del messages_batch
            gc.collect()

        # --- LAPORAN AKHIR ---
        final_status = "✅ SELESAI" if not bot_data[bot_id]['stop_event'].is_set() else "🛑 DIBATALKAN"
        await status_msg.edit(
            f"{final_status}\n\n"
            f"📊 Total Diproses: `{stats['total']}`\n"
            f"✅ Sukses: `{stats['success']}`\n"
            f"🗑️ Gagal/Skip: `{stats['failed']}`\n"
            f"📝 Last Error: {last_error_log}"
        )

    except Exception as e:
        bot_logger.error(f"CRASH: {e}")
        await status_msg.edit(f"❌ **SYSTEM CRASH:** {e}")
    finally:
        bot_data[bot_id]['is_working'] = False

# --- 6. HANDLERS (DYNAMIC) ---
def register_handlers(app: Client, bot_id: int):
    bot_logger = logging.getLogger(f"bot{bot_id}")
    bot_logger.setLevel(logging.INFO)

    # Generate Commands: Bot 1 bisa /start & /start1. Bot lain hanya /startN
    cmds = lambda cmd: [cmd, f"{cmd}1"] if bot_id == 1 else [f"{cmd}{bot_id}"]
    
    @app.on_message(filters.command(cmds("start")) & filters.group)
    async def start_handler(c, m):
        if bot_data[bot_id]['is_working']:
            return await m.reply(f"⚠️ Bot {bot_id} sibuk! `/stop{bot_id}` dulu.")
        
        try:
            cfg = parse_config(m.text)
            ok, err = validate_config(cfg)
            if not ok: return await m.reply(f"❌ Config Error: {err}")

            src, sid = parse_link(cfg['src_start'])
            _, eid = parse_link(cfg['src_end'])
            dst, did = parse_link(cfg['dst'])

            if not all([src, dst, sid, eid]): return await m.reply("❌ Link Invalid!")
            
            status = await m.reply(f"🐎 **Bot {bot_id} Siap!** Memulai proses...")
            
            job = {
                'src_chat': src, 'start_id': sid, 'end_id': eid,
                'dst_chat': dst, 'dst_topic': did,
                'delay_min': cfg['delay_min'], 'filter_type': cfg['filter_type'],
                'batch_size': cfg['batch_size'], 'batch_time': cfg['batch_time'],
                'chunk_size': cfg['chunk_size'],
                # New Params
                'fetch_retries': cfg['fetch_retries'],
                'copy_retries': cfg['copy_retries'],
                'skip_kata': cfg['skip_kata'],
                'ganti_kata': cfg['ganti_kata']
            }
            asyncio.create_task(copy_worker(job, status, bot_id, c, bot_logger))
            
        except Exception as e:
            await m.reply(f"❌ Error: {e}")

    @app.on_message(filters.command(cmds("stop")) & filters.group)
    async def stop_handler(c, m):
        if bot_data[bot_id]['is_working']:
            bot_data[bot_id]['stop_event'].set()
            await m.reply(f"🛑 Bot {bot_id} berhenti...")
        else: await m.reply(f"💤 Bot {bot_id} sedang tidur.")

    @app.on_message(filters.command(cmds("stats")) & filters.group)
    async def stats_handler(c, m):
        _, cpu_txt, ram, _ = get_system_status()
        st = "🔥 Kerja" if bot_data[bot_id]['is_working'] else "💤 Tidur"
        await m.reply(f"**Bot {bot_id} Stats:**\nStatus: {st}\nRAM: {ram:.1f}MB\nCPU: {cpu_txt}")

    @app.on_message(filters.command(cmds("ping")) & filters.group)
    async def ping_handler(c, m):
        s = time.time()
        msg = await m.reply(f"🏓 Pong {bot_id}!")
        await msg.edit(f"🏓 Pong {bot_id}! `{(time.time()-s)*1000:.0f}ms`")

# --- MAIN SETUP ---
async def web_handler(req): return web.Response(text="Workhorse V10 Active")

async def main():
    # Setup Web Server (Keep Alive)
    app_web = web.Application()
    app_web.add_routes([web.get('/', web_handler)])
    runner = web.AppRunner(app_web)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    
    # Setup Bots
    bot_data.extend([None] * (NUM_BOTS + 1))
    for i in range(1, NUM_BOTS + 1):
        try:
            if not os.environ.get(f"BOT_TOKEN_{i}"): continue
            c = Client(f"bot_{i}", api_id=int(os.environ[f"API_ID_{i}"]),
                       api_hash=os.environ[f"API_HASH_{i}"],
                       bot_token=os.environ[f"BOT_TOKEN_{i}"])
            clients.append(c)
            bot_data[i] = {'client': c, 'is_working': False, 'stop_event': asyncio.Event()}
            register_handlers(c, i)
            logger.info(f"✅ Bot {i} Loaded")
        except Exception as e: logger.error(f"❌ Bot {i} Fail: {e}")

    if not clients: sys.exit("No bots.")
    await asyncio.gather(*[c.start() for c in clients])
    await idle()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
