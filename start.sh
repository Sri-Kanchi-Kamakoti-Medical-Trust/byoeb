# ffmpeg comes from the imageio-ffmpeg pip package (see src/utils.py ffmpeg_exe), so nothing
# is downloaded or installed as root at startup (Quadra P1-10).
# Start the Python application
# The Google key is read from Key Vault into memory when needed (src/utils.py); remove any
# copy an older version wrote to disk (Quadra P0-4 / S-02).
rm -f cron_jobs/credentials.json "$APP_PATH/cron_jobs/credentials.json"
# Exactly one worker: the queue consumer thread and scheduler_state.json assume a single process.
python -m gunicorn --bind=0.0.0.0 --workers 1 --timeout 600 app:app