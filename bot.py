import os
import asyncio
import tempfile
import logging
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
import uuid
import chardet
import subprocess

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, InputMediaPhoto
from pyrogram.errors import MessageNotModified, QueryIdInvalid

from database import db
from utils import (
    AsyncVideoProcessor, StreamingFileManager as FileManager,
    EnhancedMessageUtils as MessageUtils
)

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = list(map(int, os.getenv("ADMIN_IDS", "").split()))
MAX_CONCURRENT_DOWNLOADS = 8
MAX_CONCURRENT_PROCESSING = 3
CALLBACK_TIMEOUT = 600

app = Client("subtitle_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

@dataclass
class Task:
    task_id: str
    user_id: int
    task_type: str
    status: str
    progress: float
    message_id: int
    chat_id: int
    data: dict
    created_at: datetime
    updated_at: datetime
    file_name: str = ""
    file_size: int = 0
    speed: float = 0.0
    eta: int = 0

class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, Task] = {}
        self.completed: Dict[str, Task] = {}
        self.download_sem = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        self.process_sem = asyncio.Semaphore(MAX_CONCURRENT_PROCESSING)
        self.download_q = asyncio.Queue()
        self.process_q = asyncio.Queue()
        self.active_dl = set()
        self.active_proc = set()

    async def add(self, task: Task):
        self.tasks[task.task_id] = task

    async def update(self, tid, **kw):
        t = self.tasks.get(tid)
        if not t: return
        for k, v in kw.items():
            if k == "status" and v in ["completed", "failed", "cancelled"]:
                self.completed[tid] = self.tasks.pop(tid, None)
            elif k == "progress": setattr(t, k, min(100, max(0, v)))
            else: setattr(t, k, v)
        t.updated_at = datetime.now()

    async def get(self, tid): return self.tasks.get(tid) or self.completed.get(tid)

task_manager = TaskManager()
user_states: Dict[int, dict] = {}

async def detect_subtitle_lang(file_path: str) -> str:
    try:
        with open(file_path, 'rb') as f:
            raw = f.read(1024)
            enc = chardet.detect(raw)['encoding'] or 'utf-8'
        with open(file_path, 'r', encoding=enc) as f:
            sample = f.read(512).lower()
        lang_map = {
            'english': 'eng', 'spanish': 'spa', 'french': 'fre', 'german': 'ger',
            'chinese': 'chi', 'japanese': 'jpn', 'korean': 'kor', 'arabic': 'ara',
            'hindi': 'hin', 'russian': 'rus', 'portuguese': 'por'
        }
        for word, code in lang_map.items():
            if word in sample:
                return code
        return 'und'
    except:
        return 'und'

async def update_progress(msg: Message, task: Task, action: str):
    try:
        cur = int(task.file_size * task.progress / 100) if task.file_size else 0
        speed = task.speed / (1024*1024)
        eta = f"{task.eta//60}m {task.eta%60}s" if task.eta else "N/A"
        bar = MessageUtils.get_advanced_progress_bar(int(task.progress), 100, 12)
        text = f"**{action}**\n\n{bar}\n**{task.progress:.1f}%** | {FileManager.format_size(cur)}/{FileManager.format_size(task.file_size)}\n**Speed:** {speed:.2f} MB/s | **ETA:** {eta}"
        await msg.edit_text(text)
    except: pass

class StreamingDownloader:
    @staticmethod
    async def download(message: Message, file_path: str, task_id: str, status_msg: Message) -> bool:
        try:
            file_obj = message.document or message.video
            if not file_obj: return False
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            last_time = datetime.now()
            last_current = 0

            async def progress(current, total):
                nonlocal last_time, last_current
                now = datetime.now()
                if (now - last_time).total_seconds() >= 1.5 or current == total:
                    diff = current - last_current
                    speed = diff / max((now - last_time).total_seconds(), 0.1)
                    eta = int((total - current) / speed) if speed > 0 else 0
                    await task_manager.update(task_id, progress=current/total*100, speed=speed, eta=eta)
                    task = await task_manager.get(task_id)
                    if task:
                        await update_progress(status_msg, task, "Downloading")
                    last_time, last_current = now, current

            await message.download(file_name=file_path, progress=progress)
            await task_manager.update(task_id, progress=100)
            return True
        except Exception as e:
            logger.error(f"Download error: {e}")
            return False

class StreamingUploader:
    @staticmethod
    async def upload(client: Client, chat_id: int, file_path: str, task_id: str, upload_mode: str, video_info: dict, caption: str) -> bool:
        try:
            await task_manager.update(task_id, status="uploading", progress=0)
            task = await task_manager.get(task_id)
            task.file_size = os.path.getsize(file_path)
            last_time = datetime.now()
            last_current = 0

            async def progress(current, total):
                nonlocal last_time, last_current
                now = datetime.now()
                if (now - last_time).total_seconds() >= 2.0:
                    diff = current - last_current
                    speed = diff / (now - last_time).total_seconds()
                    eta = int((total - current) / speed) if speed > 0 else 0
                    await task_manager.update(task_id, progress=current/total*100, speed=speed, eta=eta)
                    await update_progress(task.data['status_msg'], task, "Uploading")
                    last_time, last_current = now, current

            if upload_mode == "video":
                await client.send_video(chat_id, file_path, caption=caption, thumb=None, progress=progress)
            else:
                await client.send_document(chat_id, file_path, caption=caption, progress=progress)
            await task_manager.update(task_id, status="completed", progress=100)
            return True
        except Exception as e:
            logger.error(f"Upload error: {e}")
            return False

@app.on_callback_query()
async def callback(client, cq: CallbackQuery):
    data = cq.data
    uid = cq.from_user.id

    try:
        if data == "continue_download":
            if uid not in user_states or user_states[uid].get('step') != 'confirm_download':
                return await cq.message.edit_text("Session expired.")
            state = user_states[uid]
            await cq.message.edit_text("Starting download...")
            await task_manager.download_q.put(state['pending_task_id'])
            user_states[uid]['step'] = 'downloading'

        elif data == "cancel":
            await cleanup_user(uid)
            await cq.message.edit_text("Operation cancelled.")

        elif data == "soft_sub_start":
            await start_soft_sub_collection(uid, cq)

        elif data == "add_more_sub":
            user_states[uid]['step'] = 'collecting_soft_subs'
            await cq.message.edit_text("Send more subtitle files or click **Done** when ready.", reply_markup=done_button())

        elif data == "done_soft_subs":
            await process_soft_subs(uid, cq)

        elif data == "hard_sub_menu":
            await cq.message.edit_text(
                "How do you wish to select the subtitles file:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Send another File", callback_data="hard_sub_file")],
                    [InlineKeyboardButton("Select from downloaded video", callback_data="hard_sub_extract")]
                ])
            )
            user_states[uid]['step'] = 'hard_sub_menu'

        elif data == "hard_sub_file":
            user_states[uid]['step'] = 'waiting_hard_sub_file'
            await cq.message.edit_text("Send the subtitle file for hard subtitles.")

        elif data == "hard_sub_extract":
            await show_extractable_subs(uid, cq)

        elif data.startswith("extract_sub_"):
            track_idx = int(data.split("_")[-1])
            await extract_and_hard_sub(uid, cq, track_idx)

        await cq.answer()
    except Exception as e:
        logger.error(f"CB Error: {e}")
        await safe_answer(cq, "Error occurred.")

async def safe_answer(cq, text=None, alert=False):
    try: await cq.answer(text, show_alert=alert)
    except QueryIdInvalid: pass

def done_button():
    return InlineKeyboardMarkup([[InlineKeyboardButton("Done", callback_data="done_soft_subs")]])

@app.on_message((filters.document | filters.video) & filters.private)
async def handle_file(client, message: Message):
    await register_user(message)
    uid = message.from_user.id

    if await db.is_user_banned(uid):
        return await message.reply_text("You are banned.")

    if uid in user_states:
        await cleanup_user(uid)

    file_obj = message.document or message.video
    if not file_obj: return

    is_video = AsyncVideoProcessor.is_video_file(file_obj.file_name or "") or bool(message.video)
    is_sub = AsyncVideoProcessor.is_subtitle_file(file_obj.file_name or "")

    if is_video:
        temp_dir = await FileManager.create_temp_dir()
        path = os.path.join(temp_dir, file_obj.file_name or f"video_{message.id}.mp4")
        task_id = str(uuid.uuid4())
        task = Task(task_id=task_id, user_id=uid, task_type="video", status="pending", progress=0,
                    message_id=message.id, chat_id=message.chat.id, data={'path': path, 'temp_dir': temp_dir, 'message': message},
                    created_at=datetime.now(), updated_at=datetime.now(), file_name=file_obj.file_name or "video.mp4",
                    file_size=file_obj.file_size or 0)
        await task_manager.add(task)

        user_states[uid] = {
            'step': 'confirm_download', 'pending_task_id': task_id, 'video_path': path,
            'video_temp_dir': temp_dir, 'status_msg': None
        }

        status_msg = await message.reply_text(
            f"**File received for Subtitle Muxer!**\n\n"
            f"**Name:** `{file_obj.file_name or 'video.mp4'}`\n"
            f"**Size:** {FileManager.format_size(file_obj.file_size)}\n\n"
            f"Do you want to process it?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Continue", callback_data="continue_download")],
                [InlineKeyboardButton("Cancel", callback_data="cancel")]
            ])
        )
        user_states[uid]['status_msg'] = status_msg

    elif is_sub and uid in user_states and user_states[uid].get('step') in ['collecting_soft_subs', 'waiting_hard_sub_file']:
        await handle_subtitle_upload(message)

    else:
        await message.reply_text("Send a video first.")

async def start_soft_sub_collection(uid, cq):
    state = user_states[uid]
    state['soft_subs'] = []
    state['step'] = 'collecting_soft_subs'
    await cq.message.edit_text(
        "Send subtitle files to add (multiple allowed).\nClick **Done** when finished.",
        reply_markup=done_button()
    )

async def handle_subtitle_upload(message: Message):
    uid = message.from_user.id
    state = user_states[uid]
    file_obj = message.document
    temp_dir = await FileManager.create_temp_dir()
    path = os.path.join(temp_dir, file_obj.file_name)

    await message.download(file_name=path)
    lang = await detect_subtitle_lang(path)
    title = f"{os.path.basename(file_obj.file_name)} ({lang})"

    if state['step'] == 'collecting_soft_subs':
        state['soft_subs'].append({'path': path, 'temp_dir': temp_dir, 'lang': lang, 'title': title})
        subs_list = "\n".join([f"{i+1}. {s['title']}" for i, s in enumerate(state['soft_subs'])])
        await state['status_msg'].edit_text(
            f"**Received subtitles:**\n{subs_list}\n\nSend more or click **Done**.",
            reply_markup=done_button()
        )
    elif state['step'] == 'waiting_hard_sub_file':
        state['hard_sub_path'] = path
        state['hard_sub_temp_dir'] = temp_dir
        state['step'] = 'ready_hard_sub'
        await process_hard_sub_file(uid, state)

async def process_soft_subs(uid, cq):
    state = user_states[uid]
    if not state.get('soft_subs'):
        return await cq.message.edit_text("No subtitles added.")

    can_start, reason = await db.can_start_task(uid)
    if not can_start:
        return await cq.message.edit_text(reason)

    await db.increment_user_task(uid)
    output_dir = await FileManager.create_temp_dir()
    base = os.path.splitext(os.path.basename(state['video_path']))[0]
    output_path = os.path.join(output_dir, f"{base}_soft.mkv")

    task_id = str(uuid.uuid4())
    task = Task(task_id, uid, "soft_sub", "queued", 0, cq.message.id, cq.message.chat.id,
                data={'output_path': output_path, 'output_dir': output_dir, 'subs': state['soft_subs'],
                      'video_path': state['video_path'], 'status_msg': state['status_msg']},
                created_at=datetime.now(), updated_at=datetime.now(), file_name=os.path.basename(output_path))
    await task_manager.add(task)
    state['processing_task_id'] = task_id
    state['step'] = 'processing'

    await state['status_msg'].edit_text("Adding multiple soft subtitles...")
    await task_manager.process_q.put(task_id)

async def show_extractable_subs(uid, cq):
    state = user_states[uid]
    info = await AsyncVideoProcessor.get_video_info_async(state['video_path'])
    if not info or 'subtitle_streams' not in info or not info['subtitle_streams']:
        return await cq.message.edit_text("No subtitles found in video.")

    buttons = []
    for i, sub in enumerate(info['subtitle_streams']):
        lang = sub.get('tags', {}).get('language', 'und')
        title = sub.get('tags', {}).get('title', f"Track {i+1}")
        buttons.append([InlineKeyboardButton(f"{title} ({lang})", callback_data=f"extract_sub_{i}")])
    buttons.append([InlineKeyboardButton("Back", callback_data="hard_sub_menu")])

    await cq.message.edit_text("**Select subtitle to extract:**", reply_markup=InlineKeyboardMarkup(buttons))

async def extract_and_hard_sub(uid, cq, track_idx):
    state = user_states[uid]
    info = await AsyncVideoProcessor.get_video_info_async(state['video_path'])
    if track_idx >= len(info['subtitle_streams']):
        return await cq.message.edit_text("Invalid track.")

    temp_dir = await FileManager.create_temp_dir()
    sub_path = os.path.join(temp_dir, f"extracted_sub_{track_idx}.srt")

    success = await AsyncVideoProcessor.extract_subtitle_async(
        state['video_path'], track_idx, sub_path
    )
    if not success:
        return await cq.message.edit_text("Failed to extract subtitle.")

    state['hard_sub_path'] = sub_path
    state['hard_sub_temp_dir'] = temp_dir
    state['step'] = 'ready_hard_sub'
    await process_hard_sub_file(uid, state)

async def process_hard_sub_file(uid, state):
    can_start, reason = await db.can_start_task(uid)
    if not can_start:
        return await state['status_msg'].edit_text(reason)

    await db.increment_user_task(uid)
    output_dir = await FileManager.create_temp_dir()
    base = os.path.splitext(os.path.basename(state['video_path']))[0]
    output_path = os.path.join(output_dir, f"{base}_hard.mp4")

    task_id = str(uuid.uuid4())
    task = Task(task_id, uid, "hard_sub", "queued", 0, state['status_msg'].id, state['status_msg'].chat.id,
                data={'output_path': output_path, 'output_dir': output_dir,
                      'video_path': state['video_path'], 'sub_path': state['hard_sub_path'],
                      'status_msg': state['status_msg']},
                created_at=datetime.now(), updated_at=datetime.now(), file_name=os.path.basename(output_path))
    await task_manager.add(task)
    state['processing_task_id'] = task_id
    state['step'] = 'processing'

    await state['status_msg'].edit_text("Burning hard subtitle...")
    await task_manager.process_q.put(task_id)

async def download_worker():
    while True:
        tid = await task_manager.download_q.get()
        task = await task_manager.get(tid)
        if not task or task.status == "cancelled": 
            task_manager.download_q.task_done()
            continue

        async with task_manager.download_sem:
            task_manager.active_dl.add(tid)
            try:
                await task_manager.update(tid, status="downloading")
                success = await StreamingDownloader.download(task.data['message'], task.data['path'], tid, task.data['status_msg'])
                if success:
                    await task_manager.update(tid, status="downloaded", progress=100)
                    uid = task.user_id
                    if uid in user_states and user_states[uid].get('pending_task_id') == tid:
                        info = await AsyncVideoProcessor.get_video_info_async(task.data['path'])
                        dur = FileManager.format_duration(info.get('duration', 0)) if info else "N/A"
                        await user_states[uid]['status_msg'].edit_text(
                            f"**Video Downloaded**\n**Duration:** {dur}\n\nChoose subtitle type:",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("Soft Sub (Multiple)", callback_data="soft_sub_start")],
                                [InlineKeyboardButton("Hard Sub", callback_data="hard_sub_menu")]
                            ])
                        )
                        user_states[uid]['step'] = 'choose_type'
            finally:
                task_manager.active_dl.discard(tid)
                task_manager.download_q.task_done()

async def process_worker():
    while True:
        tid = await task_manager.process_q.get()
        task = await task_manager.get(tid)
        if not task or task.status == "cancelled": 
            task_manager.process_q.task_done()
            continue

        async with task_manager.process_sem:
            task_manager.active_proc.add(tid)
            try:
                await task_manager.update(tid, status="processing")
                if task.task_type == "soft_sub":
                    success = await AsyncVideoProcessor.add_multiple_soft_subs(
                        task.data['video_path'], [s['path'] for s in task.data['subs']],
                        task.data['output_path'], [s['lang'] for s in task.data['subs']]
                    )
                else:
                    success = await AsyncVideoProcessor.add_subtitle_hard_async(
                        task.data['video_path'], task.data['sub_path'], task.data['output_path']
                    )
                if success:
                    await task_manager.update(tid, status="processed")
                    asyncio.create_task(upload_result(tid))
                else:
                    await task_manager.update(tid, status="failed")
                    await task.data['status_msg'].edit_text("Processing failed.")
                    await cleanup_task(tid)
            finally:
                task_manager.active_proc.discard(tid)
                task_manager.process_q.task_done()

async def upload_result(tid):
    task = await task_manager.get(tid)
    if not task: return
    try:
        await task.data['status_msg'].edit_text("Uploading...")
        video_info = await AsyncVideoProcessor.get_video_info_async(task.data['output_path'])
        success = await StreamingUploader.upload(
            app, task.chat_id, task.data['output_path'], tid,
            "video", video_info, "Processed by Subtitle Muxer Bot!"
        )
        if success:
            await task.data['status_msg'].delete()
    except Exception as e:
        logger.error(f"Upload error: {e}")
    finally:
        await cleanup_task(tid)

async def cleanup_user(uid):
    if uid not in user_states: return
    state = user_states[uid]
    dirs = [state.get(k) for k in ['video_temp_dir', 'hard_sub_temp_dir', 'output_dir']]
    for d in dirs:
        if d: await FileManager.cleanup_temp_dir(d)
    for sub in state.get('soft_subs', []):
        if sub.get('temp_dir'): await FileManager.cleanup_temp_dir(sub['temp_dir'])
    for tid in [state.get(f"{t}_task_id") for t in ['pending', 'processing']]:
        if tid: await task_manager.update(tid, status="cancelled")
    user_states.pop(uid, None)

async def cleanup_task(tid):
    task = await task_manager.get(tid)
    if not task: return
    dirs = [task.data.get(k) for k in ['video_temp_dir', 'output_dir', 'hard_sub_temp_dir']]
    for d in dirs:
        if d: await FileManager.cleanup_temp_dir(d)
    if task.user_id in user_states:
        user_states.pop(task.user_id, None)

async def start_workers():
    asyncio.create_task(download_worker())
    for _ in range(MAX_CONCURRENT_PROCESSING):
        asyncio.create_task(process_worker())

@app.on_message(filters.command("start"))
async def start(c, m):
    await register_user(m)
    await m.reply_text("Send a video to begin.")

async def register_user(m):
    if not await db.is_user_exist(m.from_user.id):
        await db.add_user(m.from_user.id, m.from_user.first_name, m.from_user.username)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_workers())
    app.run()
