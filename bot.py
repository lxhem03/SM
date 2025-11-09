import os
import asyncio
import tempfile
import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import queue
import threading
from pathlib import Path
import uuid

from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, InputMediaPhoto
from pyrogram.errors import FloodWait, UserIsBlocked, InputUserDeactivated, QueryIdInvalid, MessageNotModified

# Import Hachoir for metadata
from hachoir.metadata import extractMetadata
from hachoir.parser import createParser

from database import db
from utils import VideoProcessor, FileManager, MessageUtils

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Bot configuration
API_ID = int(os.getenv("API_ID", "23340285"))
API_HASH = os.getenv("API_HASH", "ab18f905cb5f4a75d41bb48d20acfa50")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8482406149:AAHGP7MdJn4OXHt7WYHztIOY9YcBkVdOjVc")
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://RahulPrince720:Q7qg69E1oH30LT6d@cluster0.fb0ldjk.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
ADMIN_IDS = list(map(int, os.getenv("ADMIN_IDS", "7465574522").split()))

# Validate environment variables
if not all([API_ID, API_HASH, BOT_TOKEN, MONGO_URI]):
    raise ValueError("Missing required environment variables")

# Configuration
MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "100"))
MAX_CONCURRENT_PROCESSING = int(os.getenv("MAX_CONCURRENT_PROCESSING", "100"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "1048576"))  # 1MB chunks
CALLBACK_QUERY_TIMEOUT = int(os.getenv("CALLBACK_TIMEOUT", "300"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "300"))
FREE_USER_DAILY_LIMIT = 10  # Daily task limit for free users

# Initialize bot
app = Client(
    "subtitle_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# Task management
@dataclass
class Task:
    task_id: str
    user_id: int
    user_name: str
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
        self.completed_tasks: Dict[str, Task] = {}  # Store completed tasks
        self.download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        self.processing_semaphore = asyncio.Semaphore(MAX_CONCURRENT_PROCESSING)
        self.download_queue = asyncio.Queue()
        self.processing_queue = asyncio.Queue()
        self.active_downloads: Set[str] = set()
        self.active_processing: Set[str] = set()
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    async def add_task(self, task: Task) -> str:
        self.tasks[task.task_id] = task
        return task.task_id

    async def update_task(self, task_id: str, status: str = None, progress: float = None,
                         data: dict = None, speed: float = None, eta: int = None):
        if task_id in self.tasks:
            task = self.tasks[task_id]
            if status:
                task.status = status
                # Move to completed tasks if finished
                if status in ["completed", "failed", "cancelled", "upload_failed"]:
                    if task_id in self.tasks:
                        self.completed_tasks[task_id] = self.tasks.pop(task_id)

            if progress is not None:
                task.progress = min(100.0, max(0.0, float(progress)))
            if data:
                task.data.update(data)
            if speed is not None:
                task.speed = float(speed)
            if eta is not None:
                task.eta = int(eta)
            task.updated_at = datetime.now()

    async def get_task(self, task_id: str) -> Optional[Task]:
        # Check active tasks first, then completed
        return self.tasks.get(task_id) or self.completed_tasks.get(task_id)

    async def remove_task(self, task_id: str):
        if task_id in self.tasks:
            del self.tasks[task_id]
        if task_id in self.completed_tasks:
            del self.completed_tasks[task_id]

    async def get_user_active_tasks(self, user_id: int) -> List[Task]:
        return [task for task in self.tasks.values() if task.user_id == user_id]

    async def get_all_tasks(self) -> List[Task]:
        # Return both active and recently completed tasks
        all_tasks = list(self.tasks.values()) + list(self.completed_tasks.values())
        return sorted(all_tasks, key=lambda x: x.updated_at, reverse=True)

    async def cleanup_expired_tasks(self):
        """Remove tasks older than 1 hour from completed tasks"""
        cutoff = datetime.now().timestamp() - 30
        expired = [
            task_id for task_id, task in self.completed_tasks.items()
            if task.updated_at.timestamp() < cutoff
        ]
        for task_id in expired:
            if task_id in self.completed_tasks:
                del self.completed_tasks[task_id]

# Global task manager
task_manager = TaskManager()

# User states for multi-step operations
user_states: Dict[int, Dict] = {}

class StreamingUploader:
    @staticmethod
    async def upload_with_progress(client: Client, chat_id: int, file_path: str,
                                 task_id: str, upload_mode: str, video_info: dict,
                                 caption: str, status_message: Message, thumbnail_path: str = None) -> bool:
        """Non-blocking streaming upload with fixed progress handling"""
        try:
            await task_manager.update_task(task_id, status="uploading", progress=0.0)
            
            task = await task_manager.get_task(task_id)
            task.file_size = os.path.getsize(file_path)

            last_time = datetime.now()
            last_current = 0

            async def progress_handler(current, total):
                nonlocal last_time, last_current
                try:
                    now = datetime.now()
                    time_diff = (now - last_time).total_seconds()

                    if time_diff >= 2.0:  # Update every 2 seconds
                        progress = (current / total * 100) if total > 0 else 0
                        bytes_diff = current - last_current
                        speed = bytes_diff / time_diff if time_diff > 0 else 0
                        eta = int((total - current) / speed) if speed > 0 else 0

                        await task_manager.update_task(
                            task_id,
                            progress=progress,
                            speed=speed,
                            eta=eta
                        )
                        
                        current_task = await task_manager.get_task(task_id)
                        if current_task:
                           await update_progress_message(status_message, current_task, "📤 Uploading")

                        last_time = now
                        last_current = current

                except Exception as e:
                    logger.error(f"Progress handler error: {e}")

            if upload_mode == "video":
                await client.send_video(
                    chat_id,
                    file_path,
                    caption=caption,
                    thumb=thumbnail_path,
                    duration=int(video_info.get('duration', 0)),
                    width=int(video_info.get('width', 1280)),
                    height=int(video_info.get('height', 720)),
                    supports_streaming=True,
                    progress=progress_handler
                )
            else:
                await client.send_document(
                    chat_id,
                    file_path,
                    caption=caption,
                    progress=progress_handler
                )

            await task_manager.update_task(task_id, status="completed", progress=100.0)
            return True

        except Exception as e:
            logger.error(f"Upload failed for task {task_id}: {e}")
            await task_manager.update_task(task_id, status="upload_failed")
            return False

def admin_required(func):
    async def wrapper(client, message):
        if not message.from_user:
            return
        user_id = message.from_user.id
        if user_id not in ADMIN_IDS and not await db.is_admin(user_id):
            await message.reply_text("❌ You don't have permission to use this command.")
            return
        return await func(client, message)
    return wrapper


class StreamingDownloader:
    @staticmethod
    async def download_with_progress(client: Client, message: Message, file_path: str,
                                     task_id: str, status_msg: Message) -> bool:
        """Robust streaming download with proper checks for .download attribute."""
        try:
            # Check valid media presence
            file_obj = message.document or message.video or message.audio or message.photo
            if not file_obj:
                raise ValueError("No valid media found in message.")

            if not hasattr(message, "download") or not callable(message.download):
                raise ValueError("The message object has no valid .download method.")

            total_size = file_obj.file_size or 0
            if total_size <= 0:
                raise ValueError("Invalid or missing file size.")

            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            last_time = datetime.now()
            last_current = 0

            async def progress_handler(current, total):
                nonlocal last_time, last_current
                try:
                    now = datetime.now()
                    time_diff = (now - last_time).total_seconds()

                    if time_diff >= 1.5 or current == total:
                        progress = (current / total * 100) if total else 0
                        bytes_diff = current - last_current
                        speed = bytes_diff / time_diff if time_diff > 0 else 0
                        eta = int((total - current) / speed) if speed > 0 else 0

                        await task_manager.update_task(
                            task_id,
                            progress=round(progress, 2),
                            speed=speed,
                            eta=eta
                        )

                        task = await task_manager.get_task(task_id)
                        if task:
                            action = "📥 Downloading Video" if task.task_type == "download_video" else "📥 Downloading Subtitle"
                            await update_progress_message(status_msg, task, action)

                        last_time = now
                        last_current = current

                except Exception as e:
                    logger.warning(f"Progress handler warning: {e}")

            await message.download(
                file_name=file_path,
                progress=progress_handler
            )

            await task_manager.update_task(task_id, status="downloaded", progress=100.0)
            return True

        except Exception as e:
            logger.error(f"Streaming download failed for task {task_id}: {e}")
            await task_manager.update_task(task_id, status="failed")
            return False

@app.on_callback_query()
async def handle_callback(client, callback_query: CallbackQuery):
    data = callback_query.data
    user_id = callback_query.from_user.id

    try:
        if data == "help":
            help_text = get_help_text()
            await callback_query.message.edit_text(
                help_text,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]
                ])
            )
            await safe_answer_callback(callback_query)

        elif data == "settings":
            await show_settings_menu(user_id, callback_query)
            await safe_answer_callback(callback_query)

        elif data == "stats":
            stats_text = await get_stats_text(user_id)
            if stats_text:
                await callback_query.message.edit_text(
                    stats_text,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]])
                )
                await safe_answer_callback(callback_query)
            else:
                await safe_answer_callback(callback_query, "❌ User data not found.", show_alert=True)

        elif data == "back_to_main":
            welcome_text, keyboard = get_start_menu()
            await callback_query.message.edit_text(welcome_text, reply_markup=keyboard)
            await safe_answer_callback(callback_query)

        elif data.startswith("toggle_"):
            setting = data.replace("toggle_", "")
            await toggle_setting(user_id, setting, callback_query)
            await safe_answer_callback(callback_query, "✅ Setting updated")

        elif data in ["subtitle_soft", "subtitle_hard"]:
            if user_id not in user_states:
                await callback_query.message.edit_text("❌ Session expired. Please start over by sending a video.")
                await safe_answer_callback(callback_query)
                return
            
            session_timestamp = user_states[user_id].get('timestamp')
            if not session_timestamp or (datetime.now() - session_timestamp).total_seconds() > CALLBACK_QUERY_TIMEOUT:
                await callback_query.message.edit_text("❌ Session expired. Please start over.")
                await cleanup_task_files_by_user_state(user_id)
                await safe_answer_callback(callback_query)
                return

            await safe_answer_callback(callback_query, "🔄 Starting processing...")
            asyncio.create_task(process_subtitle_request_async(client, callback_query, data))

        elif data == "cancel_operation":
            if user_id in user_states:
                await callback_query.message.edit_text("✅ Operation cancelled.")
                await cleanup_task_files_by_user_state(user_id)
            else:
                 await callback_query.message.edit_text("✅ Operation has already been cancelled or completed.")

            await safe_answer_callback(callback_query)

    except Exception as e:
        logger.error(f"Error in callback handler: {e}", exc_info=True)
        await safe_answer_callback(callback_query, "❌ An unexpected error occurred.", show_alert=True)

@app.on_message((filters.document | filters.video) & filters.private)
async def handle_file(client, message):
    await register_user(client, message)

    user_id = message.from_user.id
    user_name = message.from_user.username or message.from_user.first_name

    can_use, status = await db.can_use_bot(user_id)
    if not can_use:
        await message.reply_text(f"❌ {status}")
        return

    user_tasks = await task_manager.get_user_active_tasks(user_id)
    is_paid = await db.is_user_paid(user_id)

    if not is_paid and len(user_tasks) >= 4:
        await message.reply_text("❌ Free users can have maximum 4 concurrent tasks. Please wait for current tasks to complete or upgrade to paid plan.")
        return

    file_obj = message.document or message.video
    file_name = file_obj.file_name or f"video_{message.id}.mp4"
    
    is_video = VideoProcessor.is_video_file(file_name) or bool(message.video)
    is_subtitle = VideoProcessor.is_subtitle_file(file_name)

    if is_subtitle and user_id in user_states and user_states[user_id].get('step') == 'waiting_subtitle':
        status_msg = user_states[user_id]['status_message']
        await message.delete() # Don't clutter chat
        await handle_subtitle_file_async(client, message, status_msg)
        return

    if is_video:
        if user_id in user_states:
            try:
                await user_states[user_id]['status_message'].edit_text("⚠️ Previous operation cancelled, new video received. Starting over.")
            except Exception as e:
                logger.warning(f"Could not edit previous status message on cancel: {e}")
            await cleanup_task_files_by_user_state(user_id)
        
        status_msg = await message.reply_text(f"📥 **File Received**\n\n📁 Name: `{file_name}`\n\nStarting download...")
        await handle_video_file_async(client, message, status_msg)
        return

    if is_subtitle:
        await message.reply_text("❌ Please send a video file first.")
    else:
        await message.reply_text("❌ Unsupported file type. Please send a video or subtitle file.")

async def update_progress_message(status_msg: Message, task: Task, action: str):
    try:
        progress = task.progress
        current_size = int(task.file_size * progress / 100) if task.file_size else 0
        total_size = task.file_size
        speed_mbps = task.speed / (1024 * 1024) if task.speed > 0 else 0
        eta_str = f"{int(task.eta // 60)}m {int(task.eta % 60)}s" if task.eta > 0 else "N/A"

        filled = int(progress // 10)
        progress_bar = "⬢" * filled + "⬡" * (10 - filled)

        text = (f"🎬 **File:** `{task.file_name}`\n\n"
                f"**Status:** {action}\n"
                f"📊 {progress_bar} {progress:.1f}%\n"
                f"📦 **Size:** {FileManager.format_size(current_size)} / {FileManager.format_size(total_size)}\n"
                f"💨 **Speed:** {speed_mbps:.2f} MB/s\n"
                f"⏳ **ETA:** {eta_str}")

        await status_msg.edit_text(text)
    except MessageNotModified:
        pass
    except Exception as e:
        logger.error(f"Progress update error: {e}", exc_info=False)


def get_start_menu() -> Tuple[str, InlineKeyboardMarkup]:
    welcome_text = """
🎬 **Welcome to Subtitle Muxer Bot!**

I can help you add subtitles to your videos in two ways:
• **Soft Subtitles** - Embedded subtitles that can be turned on/off.
• **Hard Subtitles** - Burned-in subtitles (permanent).

**How to use:**
1. Send me a video file.
2. Send the subtitle file (.srt, .vtt, .ass, etc.).
3. Choose your desired subtitle type.
4. I'll process and upload your new video!

Use the buttons below for help and settings.
    """
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📚 Help", callback_data="help"),
         InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
        [InlineKeyboardButton("📊 My Stats", callback_data="stats")]
    ])
    return welcome_text, keyboard

def get_help_text() -> str:
    return """
📚 **How to Use Subtitle Muxer Bot**

**Step-by-step guide:**

1️⃣ **Send Video File**
   • Simply upload or forward your video file to me.
   • I'll confirm when the download is complete.

2️⃣ **Send Subtitle File**
   • After the video is ready, send the matching subtitle file (e.g., `.srt`, `.vtt`, `.ass`).

3️⃣ **Choose Subtitle Type**
   • **Soft Subtitle:** This embeds the subtitle track in the video. You can turn it on or off in your video player. This is fast and preserves quality. Recommended for most cases.
   • **Hard Subtitle:** This permanently burns the text into the video frames. It cannot be turned off. This process is slower as it requires re-encoding the video.

4️⃣ **Processing and Upload**
   • I'll show you the progress as I work.
   • Once finished, I'll upload the final video back to you.

**Commands:**
/start - Start the bot.
/cancel - Cancel your current operation at any time.
/help - Show this help message.
/settings - Change your preferences (e.g., upload as video or file).
/stats - See your usage statistics.

**Supported Formats:**
📹 **Video:** Almost any format (MP4, MKV, AVI, MOV...).
📝 **Subtitles:** SRT, VTT, ASS, SSA.
    """

async def safe_answer_callback(callback_query: CallbackQuery, text: str = None, show_alert: bool = False):
    try:
        await callback_query.answer(text=text, show_alert=show_alert)
    except QueryIdInvalid:
        logger.warning(f"Callback query expired for user {callback_query.from_user.id}")
    except Exception as e:
        logger.error(f"Error answering callback query: {e}")

async def register_user(client, message):
    if not message.from_user: return
    user_id = message.from_user.id
    if not await db.is_user_exist(user_id):
        await db.add_user(
            user_id=user_id,
            first_name=message.from_user.first_name,
            username=message.from_user.username
        )
@app.on_message(filters.command("start") & filters.private)
async def start_command(client, message):
    await register_user(client, message)
    
    user_id = message.from_user.id
    if await db.is_user_banned(user_id):
        await message.reply_text("❌ You are banned from using this bot.")
        return
    
    welcome_text, keyboard = get_start_menu()
    await message.reply_text(welcome_text, reply_markup=keyboard)



@app.on_message(filters.command("help") & filters.private)
async def help_command(client, message):
    await register_user(client, message)
    await message.reply_text(get_help_text())

@app.on_message(filters.command("settings") & filters.private)
async def settings_command(client, message):
    await register_user(client, message)
    await show_settings_menu(message.from_user.id, message)

async def show_settings_menu(user_id: int, message_or_query):
    settings = await db.get_user_settings(user_id)
    upload_mode = settings.get("upload_mode", "video")
    screenshot_mode = settings.get("screenshot_mode", True)
    
    settings_text = f"""
⚙️ **Your Settings**

Configure how the bot behaves for you.

**Upload Mode:** {'📹 Video (Supports Streaming)' if upload_mode == 'video' else '📁 File (Document)'}
**Generate Screenshots:** {'✅ On' if screenshot_mode else '❌ Off'}
    """
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"Toggle Upload: {'📹 Video' if upload_mode == 'video' else '📁 File'}", 
            callback_data="toggle_upload_mode"
        )],
        [InlineKeyboardButton(
            f"Toggle Screenshots: {'✅ On' if screenshot_mode else '❌ Off'}", 
            callback_data="toggle_screenshot_mode"
        )],
        [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
    ])
    
    target_message = message_or_query.message if isinstance(message_or_query, CallbackQuery) else message_or_query
    
    try:
        await target_message.edit_text(settings_text, reply_markup=keyboard)
    except MessageNotModified: pass
    except Exception:
        await target_message.reply_text(settings_text, reply_markup=keyboard)

async def get_stats_text(user_id: int) -> Optional[str]:
    user = await db.get_user(user_id)
    if not user: return None
    
    is_paid = await db.is_user_paid(user_id)
    can_use, status = await db.can_use_bot(user_id)
    
    last_task = user.get('last_task_date')
    last_task_str = last_task.strftime('%Y-%m-%d %H:%M') if last_task else 'Never'
    
    settings = await db.get_user_settings(user_id)
    
    task_limit = "Unlimited" if is_paid else FREE_USER_DAILY_LIMIT
    daily_tasks = user.get('daily_tasks', 0)
    if last_task and last_task.date() < date.today():
        daily_tasks = 0

    return f"""
📊 **Your Statistics**

👤 **User Info:**
• **Name:** {user['first_name']}
• **Username:** @{user.get('username', 'N/A')}
• **User ID:** `{user_id}`
• **Joined:** {user['joined_date'].strftime('%Y-%m-%d')}

💎 **Subscription:**
• **Type:** {'💎 Paid' if is_paid else '🆓 Free'}
• **Status:** {status}

📈 **Usage:**
• **Today's Tasks:** {daily_tasks} / {task_limit}
• **Last Task:** {last_task_str}

⚙️ **Current Settings:**
• **Upload Mode:** {settings.get('upload_mode', 'video').title()}
• **Screenshots:** {'On' if settings.get('screenshot_mode', True) else 'Off'}
    """

@app.on_message(filters.command("stats") & filters.private)
async def stats_command(client, message):
    await register_user(client, message)
    stats_text = await get_stats_text(message.from_user.id)
    if stats_text:
        await message.reply_text(stats_text)
    else:
        await message.reply_text("❌ User data not found.")

@app.on_message(filters.command("cancel") & filters.private)
async def cancel_command(client, message):
    user_id = message.from_user.id
    if user_id in user_states:
        await cleanup_task_files_by_user_state(user_id)
        await message.reply_text("✅ Your current operation has been cancelled.")
    else:
        await message.reply_text("ℹ️ You have no active operation to cancel.")

async def handle_video_file_async(client, message, status_msg):
    user_id = message.from_user.id
    file_obj = message.document or message.video
    file_name = file_obj.file_name or f"video_{message.id}.mp4"
    
    temp_dir = await FileManager.create_temp_dir()
    file_path = os.path.join(temp_dir, file_name)
    
    task_id = str(uuid.uuid4())
    task = Task(
        task_id=task_id, user_id=user_id, user_name=message.from_user.username,
        task_type="download_video", status="queued", progress=0.0,
        message_id=status_msg.id, chat_id=message.chat.id,
        data={'message': message, 'file_path': file_path, 'temp_dir': temp_dir, 'status_message': status_msg},
        created_at=datetime.now(), updated_at=datetime.now(),
        file_name=file_name, file_size=file_obj.file_size
    )
    await task_manager.add_task(task)

    user_states[user_id] = {
        'step': 'downloading_video', 'video_path': file_path,
        'temp_dir': temp_dir, 'video_task_id': task_id,
        'status_message': status_msg, 'timestamp': datetime.now()
    }
    
    await task_manager.download_queue.put(task_id)

async def handle_subtitle_file_async(client, message, status_msg):
    user_id = message.from_user.id
    file_obj = message.document or message.video
    file_name = file_obj.file_name
    
    await status_msg.edit_text(f"📥 **File Received**\n\n📁 Name: `{file_name}`\n\nStarting download...")

    temp_dir = await FileManager.create_temp_dir()
    file_path = os.path.join(temp_dir, file_name)
    
    task_id = str(uuid.uuid4())
    task = Task(
        task_id=task_id, user_id=user_id, user_name=message.from_user.username,
        task_type="download_subtitle", status="queued", progress=0.0,
        message_id=status_msg.id, chat_id=message.chat.id,
        data={'message': message, 'file_path': file_path, 'temp_dir': temp_dir, 'status_message': status_msg},
        created_at=datetime.now(), updated_at=datetime.now(),
        file_name=file_name, file_size=file_obj.file_size
    )
    await task_manager.add_task(task)

    user_states[user_id].update({
        'step': 'downloading_subtitle', 'subtitle_path': file_path,
        'subtitle_temp_dir': temp_dir, 'subtitle_task_id': task_id,
        'timestamp': datetime.now()
    })
    
    await task_manager.download_queue.put(task_id)

async def get_video_info_async(file_path: str) -> Optional[dict]:
    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            task_manager.executor, VideoProcessor.get_video_info_sync, file_path
        )
    except Exception as e:
        logger.error(f"Video info extraction failed: {e}")
        return None

async def toggle_setting(user_id: int, setting: str, callback_query: CallbackQuery):
    current_settings = await db.get_user_settings(user_id)
    if setting == "upload_mode":
        new_value = "file" if current_settings.get("upload_mode", "video") == "video" else "video"
        await db.update_user_setting(user_id, "upload_mode", new_value)
    elif setting == "screenshot_mode":
        new_value = not current_settings.get("screenshot_mode", True)
        await db.update_user_setting(user_id, "screenshot_mode", new_value)
    await show_settings_menu(user_id, callback_query)

async def process_subtitle_request_async(client, callback_query, subtitle_type):
    user_id = callback_query.from_user.id
    
    if user_id not in user_states or user_states[user_id].get('step') != 'choose_type':
        await callback_query.message.edit_text("❌ Session expired or invalid. Please start over.")
        return
    
    can_start, reason = await db.can_start_task(user_id)
    if not can_start:
        await callback_query.message.edit_text(f"❌ {reason}")
        await cleanup_task_files_by_user_state(user_id)
        return

    state = user_states[user_id]
    await db.increment_user_task(user_id)
    
    output_dir = await FileManager.create_temp_dir()
    is_soft = subtitle_type == "subtitle_soft"
    base_name = os.path.splitext(os.path.basename(state['video_path']))[0]
    output_path = os.path.join(output_dir, f"{base_name}_[soft-sub].mkv" if is_soft else f"{base_name}_[hard-sub].mp4")
    
    video_info = await get_video_info_async(state['video_path']) or {}
    settings = await db.get_user_settings(user_id)
    
    caption = (f"✅ **Processed By @SubtitlesMuxerChBot**\n\n"
               f"**Subtitle Type:** {'Soft (Toggleable)' if is_soft else 'Hard (Burned-in)'}")
    
    task_id = str(uuid.uuid4())
    task = Task(
        task_id=task_id, user_id=user_id, user_name=callback_query.from_user.username,
        task_type="processing", status="queued", progress=0.0,
        message_id=callback_query.message.id, chat_id=callback_query.message.chat.id,
        data={
            'video_path': state['video_path'], 'subtitle_path': state['subtitle_path'],
            'output_path': output_path, 'output_dir': output_dir,
            'is_soft': is_soft, 'video_info': video_info,
            'settings': settings, 'caption': caption,
            'status_message': state['status_message'],
            'video_temp_dir': state.get('temp_dir'),
            'subtitle_temp_dir': state.get('subtitle_temp_dir')
        },
        created_at=datetime.now(), updated_at=datetime.now(),
        file_name=os.path.basename(output_path),
        file_size=video_info.get('size', os.path.getsize(state['video_path']))
    )
    
    await task_manager.add_task(task)
    user_states[user_id].update({'step': 'processing', 'processing_task_id': task_id})
    
    await state['status_message'].edit_text(f"⏳ **Queued for Processing**\n\nYour task to add `{'soft' if is_soft else 'hard'}` subtitles is in the queue.")
    
    await task_manager.processing_queue.put(task_id)

async def start_download_worker():
    while True:
        try:
            task_id = await task_manager.download_queue.get()
            
            # FIX: A robust check is performed *before* starting the work.
            # This prevents the worker from processing a task that has been cancelled
            # or is otherwise invalid, which is the root cause of the "stucking" issue.
            task = await task_manager.get_task(task_id)
            if not task or task.status == "cancelled" or task_id in task_manager.active_downloads:
                # Discard invalid, cancelled, or duplicate tasks and move to the next.
                task_manager.download_queue.task_done()
                continue
            
            async with task_manager.download_semaphore:
                task_manager.active_downloads.add(task_id)
                try:
                    await task_manager.update_task(task_id, status="downloading")
                    success = await StreamingDownloader.download_with_progress(
                        app, task.data['message'], task.data['file_path'], task_id, task.data['status_message']
                    )
                    
                    if success:
                        user_id = task.user_id
                        if user_id in user_states:
                            # Verify state consistency before proceeding
                            state = user_states[user_id]
                            if state.get('video_task_id') != task_id and state.get('subtitle_task_id') != task_id:
                                # State mismatch, probably from a newer task. Abort this one.
                                logger.warning(f"State mismatch for user {user_id} on task {task_id}. Aborting post-download step.")
                                return

                            status_msg = state['status_message']
                            if task.task_type == 'download_video':
                                video_info = await get_video_info_async(task.data['file_path'])
                                duration_str = FileManager.format_duration(video_info.get('duration', 0)) if video_info else "N/A"
                                info_text = (f"✅ **Video Downloaded**\n\n"
                                             f"📹 **File:** `{task.file_name}`\n"
                                             f"**Duration:** {duration_str}\n\n"
                                             f"**Next Step:** Now, please send me the subtitle file (`.srt`, `.ass`, etc.).")
                                await status_msg.edit_text(info_text)
                                state['step'] = 'waiting_subtitle'
                            elif task.task_type == 'download_subtitle':
                                await status_msg.edit_text(
                                    "✅ **Subtitle Downloaded**\n\nWhat kind of subtitle would you like to add?",
                                    reply_markup=InlineKeyboardMarkup([
                                        [InlineKeyboardButton("⚡ Soft Subtitle (Fast, Recommended)", callback_data="subtitle_soft")],
                                        [InlineKeyboardButton("🔥 Hard Subtitle (Slower, Burned-in)", callback_data="subtitle_hard")],
                                        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_operation")]
                                    ])
                                )
                                state['step'] = 'choose_type'
                    else:
                        # Handle download failure
                        await task_manager.update_task(task_id, status="failed")
                        await task.data['status_message'].edit_text(f"❌ Download of `{task.file_name}` failed. Please try again.")
                        await cleanup_task_files(task_id) # Use the master cleanup function

                finally:
                    # This block ensures resources are released correctly
                    task_manager.active_downloads.discard(task_id)
                    task_manager.download_queue.task_done()
        except Exception as e:
            logger.error(f"Download worker error: {e}", exc_info=True)
            await asyncio.sleep(1)

class AsyncVideoProcessor:
    @staticmethod
    async def process_in_background(video_path: str, subtitle_path: str, output_path: str, is_soft: bool) -> bool:
        loop = asyncio.get_event_loop()
        try:
            if is_soft:
                return await loop.run_in_executor(
                    task_manager.executor, VideoProcessor.add_subtitle_soft_sync, video_path, subtitle_path, output_path
                )
            else:
                return await loop.run_in_executor(
                    task_manager.executor, VideoProcessor.add_subtitle_hard_sync, video_path, subtitle_path, output_path
                )
        except Exception as e:
            logger.error(f"Sync video processing failed: {e}")
            return False

async def start_processing_worker():
    while True:
        try:
            task_id = await task_manager.processing_queue.get()
            
            # FIX: Same robust check as the download worker.
            # This prevents processing a task that was cancelled by the user
            # while they were choosing the subtitle type.
            task = await task_manager.get_task(task_id)
            if not task or task.status == "cancelled" or task_id in task_manager.active_processing:
                # Discard invalid, cancelled, or duplicate tasks
                task_manager.processing_queue.task_done()
                continue
            
            async with task_manager.processing_semaphore:
                task_manager.active_processing.add(task_id)
                try:
                    await task_manager.update_task(task_id, status="processing")
                    await task.data['status_message'].edit_text(f"🔄 **Processing:** `{task.file_name}`\n\nThis may take a moment, especially for hard subs...")
                    
                    success = await AsyncVideoProcessor.process_in_background(
                        task.data['video_path'], task.data['subtitle_path'],
                        task.data['output_path'], task.data['is_soft']
                    )
                    
                    if success:
                        await task_manager.update_task(task_id, status="processed")
                        asyncio.create_task(handle_upload_task(task_id))
                    else:
                        await task_manager.update_task(task_id, status="failed")
                        await task.data['status_message'].edit_text(f"❌ **Processing Failed:** `{task.file_name}`. The operation has been cancelled.")
                        await cleanup_task_files(task_id)
                finally:
                    # Ensure resources are released correctly
                    task_manager.active_processing.discard(task_id)
                    task_manager.processing_queue.task_done()
        except Exception as e:
            logger.error(f"Processing worker error: {e}", exc_info=True)
            await asyncio.sleep(1)

async def handle_upload_task(task_id: str):
    task = await task_manager.get_task(task_id)
    if not task:
        logger.warning(f"handle_upload_task: Task {task_id} not found for upload.")
        return
    
    try:
        await task.data['status_message'].edit_text(f"📤 **Preparing to Upload:** `{task.file_name}`")
        
        data = task.data
        thumbnail_path = None
        if data['settings'].get('upload_mode') == 'video':
            thumbnail_path = os.path.join(data['output_dir'], "thumbnail.jpg")
            await create_thumbnail_async(data['output_path'], thumbnail_path)
        
        upload_tasks = [
            StreamingUploader.upload_with_progress(
                app, task.chat_id, data['output_path'], task_id,
                data['settings'].get('upload_mode', 'video'),
                data['video_info'], data['caption'],
                data['status_message'], thumbnail_path
            )
        ]
        
        if data['settings'].get('screenshot_mode', True):
            upload_tasks.append(generate_and_send_screenshots(task_id))
        
        results = await asyncio.gather(*upload_tasks, return_exceptions=True)
        
        video_upload_success = isinstance(results[0], bool) and results[0]
        if video_upload_success:
            await task.data['status_message'].delete()
        else:
            await task_manager.update_task(task_id, status="upload_failed")
            await task.data['status_message'].edit_text(f"❌ **Upload Failed:** `{task.file_name}`")
            for res in results:
                if isinstance(res, Exception):
                    logger.error(f"Error during upload/screenshot process for task {task_id}: {res}")

    except Exception as e:
        logger.error(f"Outer upload handler error for task {task_id}: {e}", exc_info=True)
        try:
            await task_manager.update_task(task_id, status="upload_failed")
            await task.data['status_message'].edit_text(f"❌ **An unexpected error occurred during upload:** `{task.file_name}`")
        except Exception as e_inner:
            logger.error(f"Failed to even notify user of upload error: {e_inner}")
    finally:
        await cleanup_task_files(task_id)

async def create_thumbnail_async(video_path: str, thumbnail_path: str) -> bool:
    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            task_manager.executor, VideoProcessor.create_thumbnail_sync, video_path, thumbnail_path
        )
    except Exception as e:
        logger.error(f"Thumbnail creation failed: {e}")
        return False

async def generate_and_send_screenshots(task_id: str):
    """Generate and send screenshots. Errors are logged but do not stop the main flow."""
    try:
        task = await task_manager.get_task(task_id)
        if not task: return

        data = task.data
        screenshot_dir = os.path.join(data['output_dir'], "screenshots")
        
        loop = asyncio.get_event_loop()
        screenshots = await loop.run_in_executor(
            task_manager.executor, VideoProcessor.generate_screenshots_sync, data['output_path'], screenshot_dir, 10
        )
        
        valid_screenshots = []
        for s in screenshots:
            if os.path.exists(s) and os.path.getsize(s) > 0:
                valid_screenshots.append(InputMediaPhoto(s))
        
        if valid_screenshots:
            await app.send_media_group(task.chat_id, valid_screenshots)
    except Exception as e:
        logger.error(f"Screenshot generation/sending failed for task {task_id}: {e}")
        raise

async def cleanup_task_files_by_user_state(user_id: int):
    """Cleanup based on user_states entry. Typically used for explicit cancellation."""
    if user_id in user_states:
        state = user_states[user_id]
        
        for task_key in ['video_task_id', 'subtitle_task_id', 'processing_task_id']:
            task_id = state.get(task_key)
            if task_id:
                await task_manager.update_task(task_id, status="cancelled")

        for dir_key in ['temp_dir', 'subtitle_temp_dir', 'output_dir']:
             temp_dir = state.get(dir_key)
             if temp_dir:
                 await FileManager.cleanup_temp_dir(temp_dir)
        
        del user_states[user_id]

async def cleanup_task_files(task_id: str):
    """Master cleanup function based on a task_id. Called after any terminal state."""
    try:
        task = await task_manager.get_task(task_id)
        if not task:
            logger.warning(f"Cleanup called for non-existent or already cleaned task_id: {task_id}")
            return
        
        temp_dirs_to_clean = [
            task.data.get('video_temp_dir'),
            task.data.get('subtitle_temp_dir'),
            task.data.get('output_dir'),
            task.data.get('temp_dir')
        ]

        for temp_dir in set(filter(None, temp_dirs_to_clean)):
            await FileManager.cleanup_temp_dir(temp_dir)
        
        user_id = task.user_id
        if user_id in user_states:
            state = user_states[user_id]
            if state.get('processing_task_id') == task_id or \
               state.get('video_task_id') == task_id or \
               state.get('subtitle_task_id') == task_id:
                del user_states[user_id]
        
    except Exception as e:
        logger.error(f"Error during cleanup for task {task_id}: {e}", exc_info=True)


@app.on_message(filters.command("adminstats") & filters.private)
@admin_required
async def admin_stats(client, message):
    stats = await db.get_stats()
    stats_text = f"""
📊 **Bot Statistics**

👥 **Users:**
• **Total:** {stats['total_users']:,}
• **Free:** {stats['free_users']:,}
• **Paid:** {stats['paid_users']:,}
• **Banned:** {stats['banned_users']:,}

⚙️ **System:**
• **Active User Sessions:** {len(user_states)}
• **Active Tasks:** {len(task_manager.tasks)}
• **Downloads Running:** {len(task_manager.active_downloads)}
• **Processing Running:** {len(task_manager.active_processing)}
    """
    await message.reply_text(stats_text)

@app.on_message(filters.command("ban") & filters.private)
@admin_required
async def ban_user(client, message):
    try:
        user_id = int(message.command[1])
        if await db.ban_user(user_id):
            await message.reply_text(f"✅ User `{user_id}` has been banned.")
            try:
                await client.send_message(user_id, "❌ You have been banned from using this bot by an administrator.")
            except (UserIsBlocked, InputUserDeactivated): pass
        else:
            await message.reply_text("❌ Failed to ban user (maybe already banned or not found).")
    except (IndexError, ValueError):
        await message.reply_text("❌ Usage: `/ban <user_id>`")

@app.on_message(filters.command("unban") & filters.private)
@admin_required
async def unban_user(client, message):
    try:
        user_id = int(message.command[1])
        if await db.unban_user(user_id):
            await message.reply_text(f"✅ User `{user_id}` has been unbanned.")
            try:
                await client.send_message(user_id, "✅ You have been unbanned by an administrator. You can now use the bot again.")
            except (UserIsBlocked, InputUserDeactivated): pass
        else:
            await message.reply_text("❌ Failed to unban user (maybe not banned or not found).")
    except (IndexError, ValueError):
        await message.reply_text("❌ Usage: `/unban <user_id>`")

@app.on_message(filters.command("addpaid") & filters.private)
@admin_required
async def add_paid_user(client, message):
    try:
        user_id = int(message.command[1])
        duration_days = int(message.command[2]) if len(message.command) > 2 else 30 # Default to 30 days
        if await db.add_paid_user(user_id, duration_days):
            await message.reply_text(f"✅ User `{user_id}` has been granted paid access for `{duration_days}` days.")
            try:
                await client.send_message(user_id, f"🎉 Congratulations! You now have paid access with unlimited usage for {duration_days} days.")
            except (UserIsBlocked, InputUserDeactivated): pass
        else:
            await message.reply_text("❌ Failed to grant paid access.")
    except (IndexError, ValueError):
        await message.reply_text("❌ Usage: `/addpaid <user_id> [duration_days]`")

@app.on_message(filters.command("removepaid") & filters.private)
@admin_required
async def remove_paid_user(client, message):
    try:
        user_id = int(message.command[1])
        if await db.remove_paid_user(user_id):
            await message.reply_text(f"✅ Paid access has been removed from user `{user_id}`.")
            try:
                await client.send_message(user_id, "ℹ️ Your paid access has been revoked by an administrator.")
            except (UserIsBlocked, InputUserDeactivated): pass
        else:
            await message.reply_text("❌ Failed to remove paid access (user may not be paid or found).")
    except (IndexError, ValueError):
        await message.reply_text("❌ Usage: `/removepaid <user_id>`")

@app.on_message(filters.command("broadcast") & filters.private)
@admin_required
async def broadcast_message(client, message):
    if not message.reply_to_message:
        await message.reply_text("❌ Please reply to the message you want to broadcast with the `/broadcast` command.")
        return
    
    users = await db.get_all_users()
    total_users = len(users)
    success, failed = 0, 0
    status_msg = await message.reply_text(f"📢 Starting broadcast to {total_users} users...")
    
    start_time = datetime.now()
    for i, user in enumerate(users):
        try:
            await message.reply_to_message.copy(user['user_id'])
            success += 1
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
            await message.reply_to_message.copy(user['user_id']) # Retry
            success += 1
        except (UserIsBlocked, InputUserDeactivated):
            failed += 1
        except Exception:
            failed += 1
        
        if (i + 1) % 50 == 0:
            elapsed = (datetime.now() - start_time).seconds
            await status_msg.edit_text(
                f"📢 Broadcasting...\n\n"
                f"✅ Sent: {success}\n"
                f"❌ Failed: {failed}\n"
                f"📊 Progress: {i + 1} / {total_users}\n"
                f"⏳ Elapsed: {elapsed}s"
            )
    
    await status_msg.edit_text(f"📢 **Broadcast Complete**\n\n✅ **Sent:** {success}\n❌ **Failed:** {failed}")


async def start_background_workers():
    logger.info("🚀 Starting background workers...")
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_cleanup())
    for _ in range(MAX_CONCURRENT_DOWNLOADS):
        loop.create_task(start_download_worker())
    for _ in range(MAX_CONCURRENT_PROCESSING):
        loop.create_task(start_processing_worker())
    logger.info("✅ All background workers have been scheduled.")

async def periodic_cleanup():
    while True:
        await asyncio.sleep(30)  # Run every hour
        try:
            logger.info("Running periodic cleanup of expired tasks and subscriptions...")
            await task_manager.cleanup_expired_tasks()
            await db.cleanup_expired_subscriptions()
            logger.info("Periodic cleanup finished.")
        except Exception as e:
            logger.error(f"Periodic cleanup error: {e}", exc_info=True)


if __name__ == "__main__":
    logger.info("🤖 Starting Subtitle Muxer Bot with Auto-Cleanup...")
    
    asyncio.get_event_loop().create_task(start_background_workers())
    
    logger.info("🚀 Bot started successfully!")
    app.run()
