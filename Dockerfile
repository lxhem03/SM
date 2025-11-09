FROM python:3.11-slim

# Install FFmpeg and libmagic1 without cache bloat
RUN apt-get update && \
    apt-get install -y ffmpeg libmagic1 && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy project files (including requirements.txt and bot folder)
COPY . /app/
RUN pip install backoff
# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Default command to run the bot
CMD gunicorn app:app & python3 bot.py
