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

logger.info("--- SYSTEM BOOT: V9.8 FINAL CHECKPOINT EDITION (OPTION 1 STYLE) ---")

# --- KONFIGURASI MULTI-BOT ---
NUM_BOTS = 5
clients = []
bot_data = []

try:
    PORT = int(os.environ.get("PORT", 8080))
except ValueError as e:
    logger.error(f"❌ Config Error: {e}")
    sys.exit(1)

DEFAULT_BATCH_SIZE = 10000
DEFAULT_BATCH_TIME = 60
DEFAULT_CHUNK_SIZE = 50
DEFAULT_SPEED = 0.1
CHECKPOINT_INTERVAL = 50  # Sesuai permintaan (Aman)

class FilterType(Enum):
    ALL = 'all'
    VIDEO = 'video'
    FOTO = 'foto'
    DOKUMEN = 'dokumen'
    AUDIO = 'audio'
    ALLOUT = 'allout'

# --- 1. HELPER: FORMAT WAKTU ---
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
        if cpu <= 10: cpu_stat = "🟢 Santai"
        elif cpu <= 50: cpu_stat = "🟡 Sibuk"
        else: cpu_stat = "🔴 Berat"
        
        ram_bytes = proc.memory_info().rss
        ram_mb = ram_bytes / (1024 * 1024)
        
        speed_stat = "💤 Idle"
        if delay_avg > 0:
            if delay_avg <= 0.2: speed_stat = "🚀 MODE NGEBUT"
            elif delay_avg <= 1: speed_stat = "⚠️ Agak Cepat"
            else: speed_stat = "✅ Sangat Aman"
            
        return cpu, cpu_stat, ram_mb, speed_stat
    except Exception:
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

# --- 4. PARSE CONFIG ---
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
            if key in ['speed']: config[key] = float(match.group(1))
            elif key in ['batch_size', 'batch_time', 'ember']: config[key] = int(match.group(1))
            else: config[key] = match.group(1).strip().lower() if key == 'filter_type' else match.group(1).strip()
    return config

# --- 5. VALIDATE CONFIG ---
def validate_config(config: Dict) -> Tuple[bool, str]:
    required = ['src_start', 'src_end', 'dst']
    for req in required:
        if req not in config: return False, f"Missing: {req}"
    try:
        config['delay_min'] = config.get('speed', DEFAULT_SPEED)
        if config['delay_min'] <= 0: return False, "Speed must be positive"
        config['filter_type'] = FilterType(config.get('filter_type', 'all'))
        config['batch_size'] = config.get('batch_size', DEFAULT_BATCH_SIZE)
        config['batch_time'] = config.get('batch_time', DEFAULT_BATCH_TIME)
        config['chunk_size'] = config.get('ember', DEFAULT_CHUNK_SIZE)
    except ValueError:
        return False, "Invalid format."
    return True, ""

# --- 6. WORKER UTAMA (V9.8) ---
async def copy_worker(job: Dict, status_msg, checkpoint_msg, bot_id: int, app: Client, bot_logger):
    bot_data[bot_id]['is_working'] = True
    bot_data[bot_id]['stop_event'].clear()
    
    start_id = job['start_id']
    end_id = job['end_id']
    src_chat = job['src_chat']
    dst_chat = job['dst_chat']
    chunk_size = job['chunk_size']
    delay_avg = job['delay_min'] + 0.25
    
    stats = {'success': 0, 'failed': 0, 'total': end_id - start_id + 1}
    processed_count = 0
    
    # Timer independen
    last_update_status = time.time()
    last_update_checkpoint = time.time()
    
    last_error = "-"
    last_success_id = start_id - 1 

    try:
        for chunk_start in range(start_id, end_id + 1, chunk_size):
            if bot_data[bot_id]['stop_event'].is_set(): break

            chunk_end = min(chunk_start + chunk_size - 1, end_id)
            ids = list(range(chunk_start, chunk_end + 1))
            
            # --- FETCHING ---
            msgs = []
            fetch_tries = 0
            while fetch_tries < 3:
                try:
                    msgs = await app.get_messages(src_chat, ids)
                    break
                except (PeerIdInvalid, ChannelInvalid):
                    bot_logger.warning(f"⚠️ Fetch Peer Invalid (Bot {bot_id}). Refreshing Source...")
                    try:
                        await app.get_chat(src_chat) 
                        await asyncio.sleep(2)
                        fetch_tries += 1
                    except: break
                except FloodWait as e:
                    await asyncio.sleep(e.value + 5)
                except Exception as e:
                    bot_logger.warning(f"Fetch Error: {e}")
                    await asyncio.sleep(5)
                    break
            
            if not msgs: 
                stats['failed'] += len(ids)
                continue

            for msg in msgs:
                if bot_data[bot_id]['stop_event'].is_set(): break
                
                # BATCH SLEEP
                if processed_count > 0 and processed_count % job['batch_size'] == 0:
                    try:
                        await status_msg.edit(f"😴 **Batch Sleep ({job['batch_time']}s)...**")
                    except MessageNotModified: pass
                    await asyncio.sleep(job['batch_time'])
                    last_update_status = time.time()

                if not msg or msg.empty or msg.service:
                    stats['failed'] += 1
                    continue

                # FILTERING
                ft = job['filter_type']
                ok = False
                if ft == FilterType.ALLOUT: ok = True
                elif ft == FilterType.ALL and not msg.sticker: ok = True
                elif ft == FilterType.VIDEO and msg.video: ok = True
                elif ft == FilterType.FOTO and msg.photo: ok = True
                elif ft == FilterType.DOKUMEN and msg.document: ok = True
                elif ft == FilterType.AUDIO and (msg.audio or msg.voice): ok = True
                
                if not ok:
                    stats['failed'] += 1
                    continue

                # --- COPY ---
                success = False
                for _ in range(10): 
                    try:
                        kwargs = {'chat_id': dst_chat}
                        if job['dst_topic']: kwargs['reply_to_message_id'] = job['dst_topic']
                        
                        await msg.copy(**kwargs)
                        
                        stats['success'] += 1
                        processed_count += 1
                        last_success_id = msg.id
                        success = True
                        await asyncio.sleep(random.uniform(job['delay_min'], job['delay_min']+0.5))
                        break

                    except FloodWait as e:
                        await asyncio.sleep(e.value + 5)
                    except (PeerIdInvalid, ChannelInvalid):
                        bot_logger.warning(f"⚠️ Copy Peer Invalid (Bot {bot_id}). Refreshing Dest...")
                        try:
                            await app.get_chat(dst_chat) 
                            await asyncio.sleep(2)
                        except Exception as e:
                            last_error = f"Refresh Failed: {e}"
                            break 
                    except RPCError as e:
                        last_error = str(e)
                        await asyncio.sleep(3)
                    except Exception as e:
                        last_error = str(e)
                        await asyncio.sleep(3)
                
                if not success: stats['failed'] += 1

                # --- UPDATE STATUS (PESAN 1) - 10 Detik ---
                if time.time() - last_update_status > 10:
                    rem = stats['total'] - (stats['success'] + stats['failed'])
                    eta = format_time((rem * delay_avg) + ((rem // job['batch_size']) * job['batch_time']))
                    bar = make_bar(stats['success'] + stats['failed'], stats['total'])
                    cpu, cpu_t, ram, _ = get_system_status()
                    
                    text_dashboard = (
                        f"🐎 **WORKHORSE V9.8: DUAL MSG (BOT {bot_id})**\n{bar}\n"
                        f"✅ `{stats['success']}` | ❌ `{stats['failed']}` | ⏳ `{rem}`\n"
                        f"🏁 ETA: {eta}\n"
                        f"⚠️ Last Err: {last_error}"
                    )
                    try:
                        await status_msg.edit(text_dashboard)
                    except MessageNotModified: pass
                    except Exception: pass
                    last_update_status = time.time()

                # --- UPDATE CHECKPOINT (PESAN 2) - 50 Detik ---
                if time.time() - last_update_checkpoint > CHECKPOINT_INTERVAL:
                    time_now = datetime.now().strftime("%H:%M:%S")
                    # Tampilan Opsi 1 (Kembar Identik)
                    text_checkpoint = (
                        f"💾 **AUTOSAVE: CHECKPOINT (BOT {bot_id})**\n"
                        f"➖➖➖➖➖➖➖➖➖➖\n\n"
                        f"📌 **Last ID:** `{last_success_id}`\n"
                        f"🕒 **Saved:** {time_now}\n\n"
                        f"⚠️ *Gunakan ID ini jika bot restart.*"
                    )
                    try:
                        await checkpoint_msg.edit(text_checkpoint)
                    except MessageNotModified: pass
                    except Exception: pass
                    last_update_checkpoint = time.time()
            
            del msgs
            gc.collect()

        # FINISH
        final = "🛑 STOPPED" if bot_data[bot_id]['stop_event'].is_set() else "✅ DONE"
        
        try:
            await status_msg.edit(f"{final}\nBot {bot_id} Finished.\nTotal: {stats['total']} | Ok: {stats['success']}")
        except: pass
        
        try:
            await checkpoint_msg.edit(
                f"💾 **FINAL CHECKPOINT (BOT {bot_id})**\n"
                f"➖➖➖➖➖➖➖➖➖➖\n"
                f"📌 **Finish ID:** `{last_success_id}`\n"
                f"🏁 **Status:** {final}"
            )
        except: pass

    except Exception as e:
        try:
            await status_msg.edit(f"❌ **CRASH:** {e}")
        except: pass
        bot_logger.error(f"FATAL CRASH: {e}")
    finally:
        bot_data[bot_id]['is_working'] = False

# --- COMMANDS ---
def register_handlers(app: Client, bot_id: int):
    bot_logger = logging.getLogger(f"bot_{bot_id}")
    bot_logger.setLevel(logging.INFO)

    if bot_id == 1:
        cmds_start = ["start", "start1"]
        cmds_stop = ["stop", "stop1"]
        cmds_stats = ["stats", "stats1"]
    else:
        cmds_start = [f"start{bot_id}"]
        cmds_stop = [f"stop{bot_id}"]
        cmds_stats = [f"stats{bot_id}"]

    @app.on_message(filters.command(cmds_start) & filters.group)
    async def start_handler(client, message):
        if bot_data[bot_id]['is_working']:
            return await message.reply(f"⚠️ **Bot {bot_id} Busy!**")

        try:
            cfg = parse_config(message.text)
            valid, err = validate_config(cfg)
            if not valid: return await message.reply(f"❌ Error: {err}")

            src, s_id = parse_link(cfg['src_start'])
            _, e_id = parse_link(cfg['src_end'])
            dst, dst_top = parse_link(cfg['dst'])

            if not src or not dst: return await message.reply("❌ Link Invalid")

            # Kirim 2 Pesan Awal
            status_msg = await message.reply(f"🐎 **Bot {bot_id} Menyiapkan Dashboard...**")
            checkpoint_msg = await message.reply(f"💾 **Bot {bot_id} Menyiapkan Checkpoint...**")

            # Verifikasi Akses
            try:
                try: await client.get_chat(src)
                except Exception: return await status_msg.edit(f"❌ **Bot {bot_id} Gagal Akses SUMBER**")
                
                try: await client.get_chat(dst)
                except Exception: return await status_msg.edit(f"❌ **Bot {bot_id} Gagal Akses TUJUAN**")
                    
            except Exception as e:
                return await status_msg.edit(f"❌ Error Verifikasi: {e}")

            job = {
                'src_chat': src, 'start_id': s_id, 'end_id': e_id,
                'dst_chat': dst, 'dst_topic': dst_top,
                'delay_min': cfg['delay_min'], 'filter_type': cfg['filter_type'],
                'batch_size': cfg['batch_size'], 'batch_time': cfg['batch_time'],
                'chunk_size': cfg['chunk_size']
            }
            
            asyncio.create_task(copy_worker(job, status_msg, checkpoint_msg, bot_id, client, bot_logger))

        except Exception as e:
            await message.reply(f"❌ Config Error: {e}")

    @app.on_message(filters.command(cmds_stop) & filters.group)
    async def stop_handler(client, message):
        if bot_data[bot_id]['is_working']:
            bot_data[bot_id]['stop_event'].set()
            await message.reply(f"🛑 **Bot {bot_id} Stopping...**")
        else:
            await message.reply(f"💤 **Bot {bot_id} Idle.**")

    @app.on_message(filters.command(cmds_stats) & filters.group)
    async def stats_handler(client, message):
        st = "🔥 WORKING" if bot_data[bot_id]['is_working'] else "💤 IDLE"
        cpu, _, ram, _ = get_system_status()
        await message.reply(f"🤖 **Bot {bot_id}:** {st}\nCPU: {cpu}% | RAM: {ram:.1f}MB")

    @app.on_message(filters.command("ping") & filters.group)
    async def ping_cmd(client, message):
        s = time.time()
        m = await message.reply(f"🏓 **Bot {bot_id}**")
        await m.edit(f"🏓 **Bot {bot_id}** `{(time.time()-s)*1000:.1f}ms`")

# --- INITIALIZATION ---
for i in range(1, NUM_BOTS + 1):
    api_id = int(os.environ.get(f"API_ID_{i}", 0))
    api_hash = os.environ.get(f"API_HASH_{i}", "")
    bot_token = os.environ.get(f"BOT_TOKEN_{i}", "")

    if api_id and bot_token:
        cli = Client(f"bot_session_{i}", api_id, api_hash, bot_token=bot_token)
        clients.append(cli)
        bot_data.append(None) 
        bot_data.append({'is_working': False, 'stop_event': asyncio.Event()})
        register_handlers(cli, i)
        logger.info(f"✅ Bot {i} Ready")
    else:
        logger.warning(f"⚠️ Bot {i} Config Missing")
        bot_data.append(None)

# --- MAIN ---
async def web_handler(req): return web.Response(text="Bot Running V9.8")

async def main():
    if not clients: 
        logger.error("No bots configured!")
        return
        
    app = web.Application()
    app.add_routes([web.route('*', '/', web_handler)])
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    
    logger.info("🚀 Starting Bots...")
    await asyncio.gather(*[c.start() for c in clients])
    await idle()

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
