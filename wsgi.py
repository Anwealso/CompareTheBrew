"""
WSGI entry point for Gunicorn.
Production deployment uses Gunicorn with multiple workers.
"""

from app import app
from observability.logging import setup_server_logging
import logging

# Setup logging for production
setup_server_logging()
logging.info("Starting CompareTheBrew via Gunicorn...")

# This is what Gunicorn looks for
app = app

# For binding to the port that Gunicorn expects
if __name__ == "__gunicorn__":
    # Gunicorn will call this
    pass