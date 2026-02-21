from flask import Flask
from threading import Thread
import logging

logger = logging.getLogger(__name__)

app = Flask('')

@app.route('/')
def home():
    return "Bot is running!"

def run():
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    try:
        t = Thread(target=run, daemon=True)
        t.start()
    except Exception:
        logger.exception("Failed to start keep_aliove web server")
