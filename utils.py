import os
import math
import magic
import asyncio
import aiofiles
import ffmpeg
from PIL import Image
from typing import List, Tuple, Optional, Callable, Any
from pyrogram import Client
from pyrogram.types import Message
import subprocess
import tempfile
import shutil
import logging
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import time
from dataclasses import dataclass
import psutil
import signal
from contextlib import asynccontextmanager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class VideoInfo:
    duration: float
    width: int
    height: int
    size: int
    format: str
    video_codec: str
    audio_streams: int
    bit_rate: int
    fps: float
    
@dataclass
class ProcessingProgress:
    current: int
    total: int
    percentage: float
    speed: str
    eta: str
    action: str

class AsyncVideoProcessor:
    """Non-blocking video processor with unlimited file size support"""
    
    _executor = None
    _max_workers = int(os.getenv("VIDEO_WORKERS", "4"))
    
    @classmethod
    def get_executor(cls):
        if cls._executor is None:
            cls._executor = ThreadPoolExecutor(max_workers=cls._max_workers, thread_name_prefix="video_worker")
        return cls._executor

    @staticmethod
    async def add_multiple_soft_subs(video_path: str, sub_paths: List[str], output_path: str, langs: List[str]) -> bool:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            AsyncVideoProcessor._add_multiple_soft_subs_sync,
            video_path, sub_paths, output_path, langs
        )

    @staticmethod
    def _add_multiple_soft_subs_sync(video_path: str, sub_paths: List[str], output_path: str, langs: List[str]) -> bool:
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            cmd = ['ffmpeg', '-y', '-i', video_path]
            for i, sub in enumerate(sub_paths):
                cmd += ['-i', sub]
            cmd += ['-c', 'copy', '-map', '0:v', '-map', '0:a']
            for i in range(len(sub_paths)):
                cmd += ['-map', f'{i+1}:0', f'-metadata:s:s:{i}', f'language={langs[i]}', f'-disposition:s:{i}', 'default' if i == 0 else '0']
            cmd.append(output_path)
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
            return result.returncode == 0 and os.path.exists(output_path)
        except: return False

    @staticmethod
    async def extract_subtitle_async(video_path: str, track_idx: int, output_path: str) -> bool:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            AsyncVideoProcessor._extract_subtitle_sync,
            video_path, track_idx, output_path
        )

    @staticmethod
    def _extract_subtitle_sync(video_path: str, track_idx: int, output_path: str) -> bool:
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            cmd = ['ffmpeg', '-y', '-i', video_path, '-map', f'0:s:{track_idx}', '-c:s', 'srt', output_path]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            return result.returncode == 0 and os.path.getsize(output_path) > 0
        except: return False
    
    @staticmethod
    def get_file_type(file_path: str) -> str:
        """Detect file type using python-magic"""
        try:
            if not os.path.exists(file_path):
                return "unknown"
            mime = magic.Magic(mime=True)
            file_type = mime.from_file(file_path)
            return file_type
        except Exception as e:
            logger.error(f"Error detecting file type: {e}")
            return "unknown"

    @staticmethod
    def is_video_file(file_path: str) -> bool:
        """Check if file is a video"""
        video_types = [
            'video/mp4', 'video/avi', 'video/mkv', 'video/mov', 
            'video/wmv', 'video/flv', 'video/webm', 'video/m4v',
            'video/3gp', 'video/quicktime', 'video/x-msvideo'
        ]
        
        # Check by extension as fallback
        video_extensions = ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.3gp']
        
        try:
            file_type = AsyncVideoProcessor.get_file_type(file_path)
            extension_check = any(file_path.lower().endswith(ext) for ext in video_extensions)
            mime_check = any(vtype in file_type.lower() for vtype in video_types)
            
            return mime_check or extension_check
        except Exception:
            # Fallback to extension check
            return any(file_path.lower().endswith(ext) for ext in video_extensions)

    @staticmethod
    def is_subtitle_file(file_path: str) -> bool:
        """Check if file is a subtitle file"""
        subtitle_extensions = ['.srt', '.vtt', '.ass', '.ssa', '.sub', '.idx', '.sbv', '.ttml', '.dfxp']
        return any(file_path.lower().endswith(ext) for ext in subtitle_extensions)

    @staticmethod
    async def validate_subtitle_file_async(file_path: str) -> bool:
        """Async validate subtitle file content"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            AsyncVideoProcessor.get_executor(),
            AsyncVideoProcessor._validate_subtitle_file_sync,
            file_path
        )
    
    @staticmethod
    def _validate_subtitle_file_sync(file_path: str) -> bool:
        """Sync validate subtitle file content"""
        try:
            if not os.path.exists(file_path):
                logger.error(f"Subtitle file does not exist: {file_path}")
                return False
            
            if os.path.getsize(file_path) == 0:
                logger.error(f"Subtitle file is empty: {file_path}")
                return False
            
            # Try to read the file to check encoding
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read(100)  # Read first 100 chars
                    if not content.strip():
                        logger.error(f"Subtitle file appears to be empty: {file_path}")
                        return False
            except UnicodeDecodeError:
                # Try with other encodings
                try:
                    with open(file_path, 'r', encoding='latin-1') as f:
                        content = f.read(100)
                        # Convert to UTF-8
                        with open(file_path + '.utf8', 'w', encoding='utf-8') as out_f:
                            with open(file_path, 'r', encoding='latin-1') as in_f:
                                out_f.write(in_f.read())
                        # Replace original file
                        shutil.move(file_path + '.utf8', file_path)
                except Exception as e:
                    logger.error(f"Could not read subtitle file with any encoding: {e}")
                    return False
            
            return True
        except Exception as e:
            logger.error(f"Error validating subtitle file: {e}")
            return False

    @staticmethod
    async def get_video_info_async(file_path: str) -> Optional[VideoInfo]:
        """Get video information asynchronously"""
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            AsyncVideoProcessor.get_executor(),
            AsyncVideoProcessor._get_video_info_sync,
            file_path
        )
        return result

    @staticmethod
    def _get_video_info_sync(file_path: str) -> Optional[VideoInfo]:
        """Sync get video information using ffprobe"""
        try:
            if not os.path.exists(file_path):
                logger.error(f"Video file does not exist: {file_path}")
                return None
            
            # Use subprocess for better error handling
            cmd = [
                'ffprobe', '-v', 'quiet', '-print_format', 'json', 
                '-show_format', '-show_streams', file_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                logger.error(f"FFprobe error: {result.stderr}")
                return None
            
            probe_data = json.loads(result.stdout)
            
            # Find video stream
            video_stream = None
            audio_streams = 0
            
            for stream in probe_data.get('streams', []):
                if stream['codec_type'] == 'video' and video_stream is None:
                    video_stream = stream
                elif stream['codec_type'] == 'audio':
                    audio_streams += 1
            
            if not video_stream:
                logger.error("No video stream found")
                return None
            
            format_info = probe_data.get('format', {})
            duration = float(format_info.get('duration', 0))
            width = int(video_stream.get('width', 0))
            height = int(video_stream.get('height', 0))
            size = os.path.getsize(file_path)
            
            # Calculate FPS safely
            fps_str = video_stream.get('r_frame_rate', '0/1')
            try:
                fps = eval(fps_str) if '/' in fps_str else float(fps_str)
            except:
                fps = 0.0
            
            return VideoInfo(
                duration=duration,
                width=width,
                height=height,
                size=size,
                format=format_info.get('format_name', 'unknown'),
                video_codec=video_stream.get('codec_name', 'unknown'),
                audio_streams=audio_streams,
                bit_rate=int(format_info.get('bit_rate', 0)),
                fps=fps
            )
        except Exception as e:
            logger.error(f"Error getting video info: {e}")
            return None

    @staticmethod
    async def generate_screenshots_async(video_path: str, output_dir: str, count: int = 10, 
                                       progress_callback: Optional[Callable] = None) -> List[str]:
        """Generate screenshots asynchronously with progress tracking"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            AsyncVideoProcessor.get_executor(),
            AsyncVideoProcessor._generate_screenshots_sync,
            video_path, output_dir, count, progress_callback
        )
                                           
    @staticmethod
    def _generate_screenshots_sync(video_path: str, output_dir: str, count: int = 10, 
                                 progress_callback: Optional[Callable] = None) -> List[str]:
        """Generate screenshots from video using subprocess (sync)"""
        try:
            if not os.path.exists(video_path):
                logger.error(f"Video file does not exist: {video_path}")
                return []
            
            os.makedirs(output_dir, exist_ok=True)
            video_info = AsyncVideoProcessor._get_video_info_sync(video_path)
            
            if not video_info or video_info.duration <= 0:
                logger.error("Invalid video duration")
                return []
            
            screenshots = []
            interval = video_info.duration / (count + 1)
            
            for i in range(1, count + 1):
                timestamp = interval * i
                output_path = os.path.join(output_dir, f"screenshot_{i:02d}.jpg")
                
                if progress_callback:
                    try:
                        progress_callback(i, count, f"Generating screenshot {i}/{count}")
                    except:
                        pass
                
                try:
                    # Use subprocess for better error handling
                    cmd = [
                        'ffmpeg', '-y',  # Overwrite output files
                        '-ss', str(timestamp),  # Seek to timestamp
                        '-i', video_path,  # Input file
                        '-vframes', '1',  # Extract only 1 frame
                        '-vf', 'scale=min(1920\\,iw):min(1080\\,ih):force_original_aspect_ratio=decrease',
                        '-q:v', '2',  # High quality
                        '-f', 'image2',  # Output format
                        output_path
                    ]
                    
                    result = subprocess.run(
                        cmd, 
                        capture_output=True, 
                        text=True, 
                        timeout=60,
                        cwd=os.path.dirname(video_path) if os.path.dirname(video_path) else None
                    )
                    
                    # Ensure the screenshot exists and is a non-zero size file
                    if (
                        result.returncode == 0
                        and os.path.exists(output_path)
                        and os.path.getsize(output_path) > 0
                    ):
                        # Try opening the file with PIL to ensure it's a valid image
                        try:
                            from PIL import Image
                            with Image.open(output_path) as img:
                                img.verify()
                            screenshots.append(output_path)
                            logger.info(f"Generated screenshot {i} at {timestamp:.2f}s")
                        except Exception as img_exc:
                            logger.error(f"Screenshot {i} is not a valid image: {img_exc}")
                            if os.path.exists(output_path):
                                os.remove(output_path)
                    else:
                        logger.error(f"Failed to generate screenshot {i}: {result.stderr}")
                        if os.path.exists(output_path):
                            os.remove(output_path)
                except subprocess.TimeoutExpired:
                    logger.error(f"Timeout generating screenshot {i}")
                    if os.path.exists(output_path):
                        os.remove(output_path)
                except Exception as e:
                    logger.error(f"Error generating screenshot {i}: {e}")
                    if os.path.exists(output_path):
                        os.remove(output_path)
                    continue
            
            logger.info(f"Generated {len(screenshots)} screenshots out of {count}")
            return screenshots
            
        except Exception as e:
            logger.error(f"Error in generate_screenshots: {e}")
            return []

    @staticmethod
    async def create_thumbnail_async(video_path: str, output_path: str, timestamp: float = 10) -> bool:
        """Create thumbnail asynchronously"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            AsyncVideoProcessor.get_executor(),
            AsyncVideoProcessor._create_thumbnail_sync,
            video_path, output_path, timestamp
        )

    @staticmethod
    def _create_thumbnail_sync(video_path: str, output_path: str, timestamp: float = 10) -> bool:
        """Create thumbnail from video using subprocess (sync)"""
        try:
            if not os.path.exists(video_path):
                logger.error(f"Video file does not exist: {video_path}")
                return False
            
            # Create output directory if needed
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Get video info to validate timestamp
            video_info = AsyncVideoProcessor._get_video_info_sync(video_path)
            
            if not video_info or video_info.duration <= 0:
                logger.error("Invalid video duration for thumbnail")
                return False
            
            # Adjust timestamp if it's beyond video duration
            if timestamp >= video_info.duration:
                timestamp = min(video_info.duration * 0.1, 5)
            
            # Use subprocess for better error handling
            cmd = [
                'ffmpeg', '-y',  # Overwrite output files
                '-ss', str(timestamp),  # Seek to timestamp
                '-i', video_path,  # Input file
                '-vframes', '1',  # Extract only 1 frame
                '-vf', 'scale=320:240:force_original_aspect_ratio=decrease,pad=320:240:(ow-iw)/2:(oh-ih)/2',
                '-q:v', '2',  # High quality
                '-f', 'image2',  # Output format
                output_path
            ]
            
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=60,
                cwd=os.path.dirname(video_path) if os.path.dirname(video_path) else None
            )
            
            if result.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                logger.info(f"Created thumbnail at {timestamp:.2f}s")
                return True
            else:
                logger.error(f"Failed to create thumbnail: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("Timeout creating thumbnail")
            return False
        except Exception as e:
            logger.error(f"Error creating thumbnail: {e}")
            return False

    @staticmethod
    async def create_thumbnail_at_time_async(video_path: str, output_path: str, timestamp: int) -> bool:
        """Create thumbnail at specific time asynchronously"""
        return await AsyncVideoProcessor.create_thumbnail_async(video_path, output_path, float(timestamp))

    @staticmethod
    async def add_subtitle_soft_async(video_path: str, subtitle_path: str, output_path: str,
                                    progress_callback: Optional[Callable] = None) -> bool:
        """Add soft subtitle asynchronously"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            AsyncVideoProcessor.get_executor(),
            AsyncVideoProcessor._add_subtitle_soft_sync,
            video_path, subtitle_path, output_path, progress_callback
        )

    @staticmethod
    def _add_subtitle_soft_sync(video_path: str, subtitle_path: str, output_path: str,
                              progress_callback: Optional[Callable] = None) -> bool:
        """Add subtitle as soft subtitle (embedded) using subprocess (sync)"""
        try:
            if not os.path.exists(video_path):
                logger.error(f"Video file does not exist: {video_path}")
                return False
            
            if not AsyncVideoProcessor._validate_subtitle_file_sync(subtitle_path):
                logger.error("Invalid subtitle file")
                return False
            
            # Get subtitle format
            sub_ext = os.path.splitext(subtitle_path)[1].lower()
            
            # Map subtitle extensions to codecs
            codec_map = {
                '.srt': 'srt',
                '.vtt': 'webvtt',
                '.ass': 'ass',
                '.ssa': 'ass',
                '.sub': 'subrip',
                '.sbv': 'srt',
                '.ttml': 'ttml',
                '.dfxp': 'ttml'
            }
            
            codec = codec_map.get(sub_ext, 'srt')  # Default to srt
            
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            if progress_callback:
                try:
                    progress_callback(0, 100, "Starting soft subtitle processing")
                except:
                    pass
            
            # Use subprocess for better control with progress monitoring
            cmd = [
                'ffmpeg', '-y',  # -y for overwrite
                '-progress', 'pipe:1',  # Enable progress output
                '-i', video_path,
                '-i', subtitle_path,
                '-c:v', 'copy',
                '-c:a', 'copy',
                '-c:s', codec,
                '-map', '0:v',
                '-map', '0:a',
                '-map', '1:0',
                '-disposition:s:0', 'default',
                output_path
            ]
            
            # Get video duration for progress calculation
            video_info = AsyncVideoProcessor._get_video_info_sync(video_path)
            total_duration = video_info.duration if video_info else 0
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1
            )
            
            # Monitor progress
            if progress_callback and total_duration > 0:
                try:
                    for line in iter(process.stdout.readline, ''):
                        if line.startswith('out_time_ms='):
                            time_ms = int(line.split('=')[1])
                            current_time = time_ms / 1_000_000  # Convert to seconds
                            progress = min((current_time / total_duration) * 100, 100)
                            progress_callback(int(progress), 100, f"Processing: {progress:.1f}%")
                        elif line.startswith('progress=end'):
                            progress_callback(100, 100, "Soft subtitle processing complete")
                            break
                except:
                    pass
            
            # Wait for completion
            stdout, stderr = process.communicate(timeout=1800)  # 30 minutes timeout
            
            if process.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                logger.info("Soft subtitle added successfully")
                return True
            else:
                logger.error(f"FFmpeg error adding soft subtitle: {stderr}")
                return False
            
        except subprocess.TimeoutExpired:
            logger.error("Timeout adding soft subtitle")
            if 'process' in locals():
                process.kill()
            return False
        except Exception as e:
            logger.error(f"Error adding soft subtitle: {e}")
            return False

    @staticmethod
    async def add_subtitle_hard_async(video_path: str, subtitle_path: str, output_path: str,
                                    progress_callback: Optional[Callable] = None) -> bool:
        """Add hard subtitle asynchronously"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            AsyncVideoProcessor.get_executor(),
            AsyncVideoProcessor._add_subtitle_hard_sync,
            video_path, subtitle_path, output_path, progress_callback
        )

    @staticmethod
    def _add_subtitle_hard_sync(video_path: str, subtitle_path: str, output_path: str,
                              progress_callback: Optional[Callable] = None) -> bool:
        """Add subtitle as hard subtitle (burned-in) using subprocess (sync)"""
        try:
            if not os.path.exists(video_path):
                logger.error(f"Video file does not exist: {video_path}")
                return False
            
            if not AsyncVideoProcessor._validate_subtitle_file_sync(subtitle_path):
                logger.error("Invalid subtitle file")
                return False
            
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            if progress_callback:
                try:
                    progress_callback(0, 100, "Starting hard subtitle processing")
                except:
                    pass
            
            # Escape subtitle path for cross-platform compatibility
            subtitle_path_escaped = subtitle_path.replace('\\', '/').replace(':', '\\:')
            
            # Use subprocess for better control with progress monitoring
            cmd = [
                'ffmpeg', '-y',  # -y for overwrite
                '-progress', 'pipe:1',  # Enable progress output
                '-i', video_path,
                '-vf', f"subtitles='{subtitle_path_escaped}'",
                '-c:a', 'copy',
                '-preset', 'medium',
                '-crf', '23',
                output_path
            ]
            
            # Get video duration for progress calculation
            video_info = AsyncVideoProcessor._get_video_info_sync(video_path)
            total_duration = video_info.duration if video_info else 0
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1
            )
            
            # Monitor progress
            if progress_callback and total_duration > 0:
                try:
                    for line in iter(process.stdout.readline, ''):
                        if line.startswith('out_time_ms='):
                            time_ms = int(line.split('=')[1])
                            current_time = time_ms / 1_000_000  # Convert to seconds
                            progress = min((current_time / total_duration) * 100, 100)
                            progress_callback(int(progress), 100, f"Processing: {progress:.1f}%")
                        elif line.startswith('progress=end'):
                            progress_callback(100, 100, "Hard subtitle processing complete")
                            break
                except:
                    pass
            
            # Wait for completion
            stdout, stderr = process.communicate(timeout=3600)  # 1 hour timeout for encoding
            
            if process.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                logger.info("Hard subtitle added successfully")
                return True
            else:
                logger.error(f"FFmpeg error adding hard subtitle: {stderr}")
                return False
            
        except subprocess.TimeoutExpired:
            logger.error("Timeout adding hard subtitle")
            if 'process' in locals():
                process.kill()
            return False
        except Exception as e:
            logger.error(f"Error adding hard subtitle: {e}")
            return False

    @staticmethod
    async def split_video_async(video_path: str, output_dir: str, max_size_mb: int = 1900,
                              progress_callback: Optional[Callable] = None) -> List[str]:
        """Split video asynchronously"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            AsyncVideoProcessor.get_executor(),
            AsyncVideoProcessor._split_video_sync,
            video_path, output_dir, max_size_mb, progress_callback
        )

    @staticmethod
    def _split_video_sync(video_path: str, output_dir: str, max_size_mb: int = 1900,
                        progress_callback: Optional[Callable] = None) -> List[str]:
        """Split video into parts if size > max_size_mb using subprocess (sync)"""
        try:
            if not os.path.exists(video_path):
                logger.error(f"Video file does not exist: {video_path}")
                return []
            
            file_size_mb = os.path.getsize(video_path) / (1024 * 1024)
            
            if file_size_mb <= max_size_mb:
                return [video_path]
            
            os.makedirs(output_dir, exist_ok=True)
            video_info = AsyncVideoProcessor._get_video_info_sync(video_path)
            
            if not video_info or video_info.duration <= 0:
                logger.error("Invalid video duration for splitting")
                return [video_path]
            
            # Calculate number of parts needed
            parts_needed = math.ceil(file_size_mb / max_size_mb)
            segment_duration = video_info.duration / parts_needed
            
            output_files = []
            base_name = os.path.splitext(os.path.basename(video_path))[0]
            
            for i in range(parts_needed):
                start_time = i * segment_duration
                output_file = os.path.join(output_dir, f"{base_name}_part{i+1:02d}.mp4")
                
                if progress_callback:
                    try:
                        progress_callback(i, parts_needed, f"Creating part {i+1}/{parts_needed}")
                    except:
                        pass
                
                try:
                    cmd = [
                        'ffmpeg', '-y',
                        '-ss', str(start_time),
                        '-i', video_path,
                        '-t', str(segment_duration),
                        '-c', 'copy',
                        '-avoid_negative_ts', 'make_zero',
                        output_file
                    ]
                    
                    result = subprocess.run(
                        cmd, 
                        capture_output=True, 
                        text=True, 
                        timeout=600  # 10 minutes per part
                    )
                    
                    if result.returncode == 0 and os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                        output_files.append(output_file)
                        logger.info(f"Created part {i+1}/{parts_needed}")
                    else:
                        logger.error(f"Error creating part {i+1}: {result.stderr}")
                        
                except subprocess.TimeoutExpired:
                    logger.error(f"Timeout creating part {i+1}")
                except Exception as e:
                    logger.error(f"Error creating part {i+1}: {e}")
                    continue
            
            if progress_callback and output_files:
                try:
                    progress_callback(parts_needed, parts_needed, f"Split complete: {len(output_files)} parts")
                except:
                    pass
            
            return output_files if output_files else [video_path]
            
        except Exception as e:
            logger.error(f"Error splitting video: {e}")
            return [video_path]

# Legacy compatibility layer
class VideoProcessor:
    """Legacy VideoProcessor class for backward compatibility"""
    
    @staticmethod
    def is_video_file(file_path: str) -> bool:
        return AsyncVideoProcessor.is_video_file(file_path)
    
    @staticmethod
    def is_subtitle_file(file_path: str) -> bool:
        return AsyncVideoProcessor.is_subtitle_file(file_path)
    
    @staticmethod
    async def get_video_info(file_path: str) -> dict:
        video_info = await AsyncVideoProcessor.get_video_info_async(file_path)
        if video_info:
            return {
                'duration': video_info.duration,
                'width': video_info.width,
                'height': video_info.height,
                'size': video_info.size,
                'format': video_info.format,
                'video_codec': video_info.video_codec,
                'audio_streams': video_info.audio_streams,
                'bit_rate': video_info.bit_rate,
                'fps': video_info.fps
            }
        return {}
    
    @staticmethod
    def get_video_info_sync(file_path: str) -> dict:
        video_info = AsyncVideoProcessor._get_video_info_sync(file_path)
        if video_info:
            return {
                'duration': video_info.duration,
                'width': video_info.width,
                'height': video_info.height,
                'size': video_info.size,
                'format': video_info.format,
                'video_codec': video_info.video_codec,
                'audio_streams': video_info.audio_streams,
                'bit_rate': video_info.bit_rate,
                'fps': video_info.fps
            }
        return {}
    
    @staticmethod
    async def generate_screenshots(video_path: str, output_dir: str, count: int = 10) -> List[str]:
        return await AsyncVideoProcessor.generate_screenshots_async(video_path, output_dir, count)
    
    @staticmethod
    def generate_screenshots_sync(video_path: str, output_dir: str, count: int = 10) -> List[str]:
        return AsyncVideoProcessor._generate_screenshots_sync(video_path, output_dir, count)
    
    @staticmethod
    async def create_thumbnail(video_path: str, output_path: str, timestamp: float = 10) -> bool:
        return await AsyncVideoProcessor.create_thumbnail_async(video_path, output_path, timestamp)
    
    @staticmethod
    def create_thumbnail_sync(video_path: str, output_path: str, timestamp: float = 10) -> bool:
        return AsyncVideoProcessor._create_thumbnail_sync(video_path, output_path, timestamp)
    
    @staticmethod
    async def create_thumbnail_at_time(video_path: str, output_path: str, timestamp: int) -> bool:
        return await AsyncVideoProcessor.create_thumbnail_at_time_async(video_path, output_path, timestamp)
    
    @staticmethod
    async def add_subtitle_soft(video_path: str, subtitle_path: str, output_path: str) -> bool:
        return await AsyncVideoProcessor.add_subtitle_soft_async(video_path, subtitle_path, output_path)
    
    @staticmethod
    def add_subtitle_soft_sync(video_path: str, subtitle_path: str, output_path: str) -> bool:
        return AsyncVideoProcessor._add_subtitle_soft_sync(video_path, subtitle_path, output_path)
    
    @staticmethod
    async def add_subtitle_hard(video_path: str, subtitle_path: str, output_path: str) -> bool:
        return await AsyncVideoProcessor.add_subtitle_hard_async(video_path, subtitle_path, output_path)
    
    @staticmethod
    def add_subtitle_hard_sync(video_path: str, subtitle_path: str, output_path: str) -> bool:
        return AsyncVideoProcessor._add_subtitle_hard_sync(video_path, subtitle_path, output_path)
    
    @staticmethod
    async def split_video(video_path: str, output_dir: str, max_size_mb: int = 1900) -> List[str]:
        return await AsyncVideoProcessor.split_video_async(video_path, output_dir, max_size_mb)
    
    @staticmethod
    def split_video_sync(video_path: str, output_dir: str, max_size_mb: int = 1900) -> List[str]:
        return AsyncVideoProcessor._split_video_sync(video_path, output_dir, max_size_mb)

class StreamingFileManager:
    """File manager with streaming support for unlimited file sizes"""
    
    @staticmethod
    async def create_temp_dir() -> str:
        """Create temporary directory asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            temp_dir = await loop.run_in_executor(None, tempfile.mkdtemp)
            logger.info(f"Created temp directory: {temp_dir}")
            return temp_dir
        except Exception as e:
            logger.error(f"Error creating temp directory: {e}")
            return ""

    @staticmethod
    async def cleanup_temp_dir(temp_dir: str):
        """Clean up temporary directory asynchronously"""
        try:
            if temp_dir and os.path.exists(temp_dir):
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, shutil.rmtree, temp_dir)
                logger.info(f"Cleaned up temp directory: {temp_dir}")
        except Exception as e:
            logger.error(f"Error cleaning up temp dir: {e}")

    @staticmethod
    async def get_available_space(path: str) -> int:
        """Get available disk space in bytes"""
        try:
            loop = asyncio.get_event_loop()
            stat = await loop.run_in_executor(None, os.statvfs, path)
            return stat.f_bavail * stat.f_frsize
        except Exception:
            return 0

    @staticmethod
    async def move_file_async(src: str, dst: str) -> bool:
        """Move file asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, shutil.move, src, dst)
            return True
        except Exception as e:
            logger.error(f"Error moving file {src} to {dst}: {e}")
            return False

    @staticmethod
    async def copy_file_async(src: str, dst: str, chunk_size: int = 1024*1024) -> bool:
        """Copy file asynchronously with chunked reading for large files"""
        try:
            async with aiofiles.open(src, 'rb') as src_file:
                async with aiofiles.open(dst, 'wb') as dst_file:
                    while True:
                        chunk = await src_file.read(chunk_size)
                        if not chunk:
                            break
                        await dst_file.write(chunk)
            return True
        except Exception as e:
            logger.error(f"Error copying file {src} to {dst}: {e}")
            return False

    @staticmethod
    def format_size(size_bytes: int) -> str:
        """Format file size in human readable format"""
        if size_bytes == 0:
            return "0B"
        
        size_names = ["B", "KB", "MB", "GB", "TB", "PB"]
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{s} {size_names[i]}"

    @staticmethod
    def format_duration(seconds: float) -> str:
        """Format duration in human readable format"""
        if seconds <= 0:
            return "00:00"
        
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        else:
            return f"{minutes:02d}:{secs:02d}"

    @staticmethod
    def format_speed(bytes_per_second: float) -> str:
        """Format transfer speed"""
        return f"{StreamingFileManager.format_size(int(bytes_per_second))}/s"

    @staticmethod
    def format_eta(seconds: float) -> str:
        """Format ETA"""
        if seconds <= 0 or seconds == float('inf'):
            return "Unknown"
        
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            return f"{int(seconds//60)}m {int(seconds%60)}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"

# Legacy compatibility
class FileManager(StreamingFileManager):
    """Legacy FileManager class for backward compatibility"""
    pass

class EnhancedMessageUtils:
    """Enhanced message utilities with better progress tracking"""
    
    @staticmethod
    def get_progress_bar(current: int, total: int, length: int = 20) -> str:
        """Generate enhanced progress bar"""
        if total == 0:
            return "█" * length
        
        filled = int(length * current / total)
        bar = "█" * filled + "░" * (length - filled)
        percentage = (current / total) * 100
        return f"{bar} {percentage:.1f}%"

    @staticmethod
    def get_advanced_progress_bar(current: int, total: int, length: int = 30) -> str:
        """Generate advanced progress bar with animations"""
        if total == 0:
            return "█" * length
        
        percentage = (current / total) * 100
        filled = int(length * percentage / 100)
        
        # Add animation for active downloads
        if filled < length:
            bar = "█" * filled + "▓" + "░" * (length - filled - 1)
        else:
            bar = "█" * length
        
        return f"{bar} {percentage:.1f}%"

    @staticmethod
    async def progress_callback_enhanced(current: int, total: int, message: Message, 
                                       action: str = "Processing", start_time: float = None):
        """Enhanced progress callback with speed and ETA calculation"""
        try:
            if current == total:
                return
            
            now = time.time()
            percentage = (current * 100) / total
            
            # Calculate speed and ETA
            if start_time:
                elapsed = now - start_time
                if elapsed > 0:
                    speed = current / elapsed
                    remaining = (total - current) / speed if speed > 0 else 0
                    speed_str = StreamingFileManager.format_speed(speed)
                    eta_str = StreamingFileManager.format_eta(remaining)
                else:
                    speed_str = "Calculating..."
                    eta_str = "Calculating..."
            else:
                speed_str = "Unknown"
                eta_str = "Unknown"
            
            progress_bar = EnhancedMessageUtils.get_advanced_progress_bar(current, total)
            
            current_mb = current / (1024 * 1024)
            total_mb = total / (1024 * 1024)
            
            text = (
                f"**{action}**\n\n"
                f"{progress_bar}\n"
                f"**Progress:** {percentage:.1f}%\n"
                f"**Size:** {current_mb:.1f} MB / {total_mb:.1f} MB\n"
                f"**Speed:** {speed_str}\n"
                f"**ETA:** {eta_str}"
            )
            
            # Update message every 2% to avoid too many edits but provide smooth feedback
            if int(percentage) % 2 == 0:
                try:
                    await message.edit_text(text)
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Enhanced progress callback error: {e}")

    @staticmethod
    async def progress_callback(current: int, total: int, message: Message, action: str = "Processing"):
        """Legacy progress callback for backward compatibility"""
        await EnhancedMessageUtils.progress_callback_enhanced(current, total, message, action)

# Legacy compatibility
class MessageUtils(EnhancedMessageUtils):
    """Legacy MessageUtils class for backward compatibility"""
    pass

class SystemMonitor:
    """System resource monitor for optimization"""
    
    @staticmethod
    def get_memory_usage() -> dict:
        """Get current memory usage"""
        try:
            memory = psutil.virtual_memory()
            return {
                'total': memory.total,
                'available': memory.available,
                'percent': memory.percent,
                'used': memory.used,
                'free': memory.free
            }
        except Exception as e:
            logger.error(f"Error getting memory usage: {e}")
            return {}

    @staticmethod
    def get_disk_usage(path: str = '/') -> dict:
        """Get disk usage for path"""
        try:
            usage = psutil.disk_usage(path)
            return {
                'total': usage.total,
                'used': usage.used,
                'free': usage.free,
                'percent': (usage.used / usage.total) * 100
            }
        except Exception as e:
            logger.error(f"Error getting disk usage: {e}")
            return {}

    @staticmethod
    def get_cpu_usage() -> float:
        """Get CPU usage percentage"""
        try:
            return psutil.cpu_percent(interval=1)
        except Exception as e:
            logger.error(f"Error getting CPU usage: {e}")
            return 0.0

    @staticmethod
    def should_optimize_for_resources() -> bool:
        """Check if system should optimize for low resources"""
        try:
            memory = SystemMonitor.get_memory_usage()
            disk = SystemMonitor.get_disk_usage()
            
            # Optimize if memory usage > 80% or disk usage > 90%
            return (memory.get('percent', 0) > 80 or 
                   disk.get('percent', 0) > 90)
        except Exception:
            return False

class AdvancedValidator:
    """Advanced validation utilities"""
    
    @staticmethod
    def validate_user_id(user_id_str: str) -> Optional[int]:
        """Validate and convert user ID string to integer"""
        try:
            user_id = int(user_id_str)
            if user_id > 0:
                return user_id
            return None
        except ValueError:
            return None

    @staticmethod
    def validate_duration(duration_str: str) -> Optional[int]:
        """Validate and convert duration string to integer"""
        try:
            duration = int(duration_str)
            if 1 <= duration <= 365:  # Max 1 year
                return duration
            return None
        except ValueError:
            return None

    @staticmethod
    def validate_file_path(file_path: str) -> bool:
        """Validate file path exists and is accessible"""
        try:
            return (os.path.exists(file_path) and 
                   os.path.isfile(file_path) and 
                   os.access(file_path, os.R_OK))
        except Exception:
            return False

    @staticmethod
    async def validate_video_file_async(file_path: str) -> bool:
        """Async validate video file"""
        try:
            if not AdvancedValidator.validate_file_path(file_path):
                return False
            
            video_info = await AsyncVideoProcessor.get_video_info_async(file_path)
            return video_info is not None and video_info.duration > 0
        except Exception:
            return False

    @staticmethod
    async def validate_subtitle_file_async(file_path: str) -> bool:
        """Async validate subtitle file"""
        try:
            if not AdvancedValidator.validate_file_path(file_path):
                return False
            
            return await AsyncVideoProcessor.validate_subtitle_file_async(file_path)
        except Exception:
            return False

# Legacy compatibility
class Validator(AdvancedValidator):
    """Legacy Validator class for backward compatibility"""
    pass

class FFmpegUtils:
    """Enhanced FFmpeg utilities"""
    
    @staticmethod
    def check_ffmpeg_availability() -> bool:
        """Check if ffmpeg and ffprobe are available"""
        try:
            # Check ffmpeg
            result = subprocess.run(['ffmpeg', '-version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                logger.error("FFmpeg not found or not working")
                return False
            
            # Check ffprobe
            result = subprocess.run(['ffprobe', '-version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                logger.error("FFprobe not found or not working")
                return False
            
            return True
        except Exception as e:
            logger.error(f"Error checking FFmpeg availability: {e}")
            return False

    @staticmethod
    def get_ffmpeg_version() -> str:
        """Get FFmpeg version"""
        try:
            result = subprocess.run(['ffmpeg', '-version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                lines = result.stdout.split('\n')
                if lines:
                    return lines[0].replace('ffmpeg version ', '')
            return "Unknown"
        except Exception:
            return "Unknown"

    @staticmethod
    def get_safe_filename(filename: str) -> str:
        """Get safe filename for cross-platform compatibility"""
        import re
        # Remove or replace unsafe characters
        safe_chars = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Remove multiple underscores
        safe_chars = re.sub(r'_+', '_', safe_chars)
        # Remove leading/trailing underscores and spaces
        return safe_chars.strip('_ ')

    @staticmethod
    def estimate_processing_time(video_duration: float, file_size: int, operation: str) -> float:
        """Estimate processing time based on video properties"""
        try:
            # Base estimation (very rough)
            if operation == "soft_subtitle":
                # Soft subtitles are just muxing, very fast
                return min(video_duration * 0.1, 30)  # Max 30 seconds
            elif operation == "hard_subtitle":
                # Hard subtitles require encoding
                return video_duration * 0.5  # Roughly 50% of video duration
            elif operation == "split":
                # Splitting is fast (stream copying)
                return min(video_duration * 0.05, 60)  # Max 1 minute
            else:
                return video_duration * 0.3  # Default estimation
        except Exception:
            return 300  # 5 minutes default

class ResourceOptimizer:
    """Resource optimization utilities"""
    
    @staticmethod
    def optimize_ffmpeg_params(video_info: VideoInfo, operation: str) -> dict:
        """Optimize FFmpeg parameters based on video properties and system resources"""
        params = {}
        
        try:
            # Check system resources
            memory = SystemMonitor.get_memory_usage()
            should_optimize = SystemMonitor.should_optimize_for_resources()
            
            if should_optimize:
                # Use more conservative settings for low-resource systems
                if operation == "hard_subtitle":
                    params.update({
                        'preset': 'ultrafast',
                        'crf': '28',  # Lower quality for speed
                        'threads': '1'
                    })
                elif operation == "split":
                    params.update({
                        'threads': '1'
                    })
            else:
                # Use optimized settings for better performance
                if operation == "hard_subtitle":
                    # Optimize based on video resolution
                    if video_info.width * video_info.height > 1920 * 1080:
                        params.update({
                            'preset': 'medium',
                            'crf': '23',
                            'threads': '4'
                        })
                    else:
                        params.update({
                            'preset': 'fast',
                            'crf': '20',
                            'threads': '2'
                        })
            
            return params
        except Exception as e:
            logger.error(f"Error optimizing FFmpeg params: {e}")
            return {}

    @staticmethod
    async def cleanup_old_temp_files(max_age_hours: int = 2):
        """Clean up old temporary files"""
        try:
            temp_base = tempfile.gettempdir()
            current_time = time.time()
            cutoff_time = current_time - (max_age_hours * 3600)
            
            for root, dirs, files in os.walk(temp_base):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        if os.path.getmtime(file_path) < cutoff_time:
                            os.remove(file_path)
                            logger.info(f"Cleaned up old temp file: {file_path}")
                    except Exception as e:
                        logger.debug(f"Could not clean up {file_path}: {e}")
                        
                # Clean up empty directories
                for dir in dirs:
                    dir_path = os.path.join(root, dir)
                    try:
                        if not os.listdir(dir_path):
                            os.rmdir(dir_path)
                    except Exception:
                        pass
                        
        except Exception as e:
            logger.error(f"Error cleaning up old temp files: {e}")

# Initialize and check FFmpeg availability on import
if not FFmpegUtils.check_ffmpeg_availability():
    logger.warning("FFmpeg/FFprobe not available. Video processing will not work.")
else:
    logger.info(f"FFmpeg available: {FFmpegUtils.get_ffmpeg_version()}")

# Start periodic cleanup task
async def start_periodic_cleanup():
    """Start periodic cleanup of temporary files"""
    while True:
        try:
            await ResourceOptimizer.cleanup_old_temp_files()
            await asyncio.sleep(3600)  # Run every hour
        except Exception as e:
            logger.error(f"Periodic cleanup error: {e}")
            await asyncio.sleep(3600)

# Auto-start cleanup when module is imported
def _start_cleanup_task():
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(start_periodic_cleanup())
    except RuntimeError:
        # No event loop running, will be started later
        pass

_start_cleanup_task()
