apt-get update -y
apt-get install -y ffmpeg
# Start the Python application
python processing/get_secret.py
# Exactly one worker: the queue consumer thread and scheduler_state.json assume a single process.
python -m gunicorn --bind=0.0.0.0 --workers 1 --timeout 600 app:app