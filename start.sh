apt-get update -y
apt-get install -y ffmpeg
# Start the Python application
python processing/get_secret.py
python -m gunicorn --bind=0.0.0.0 --timeout 600 app:app