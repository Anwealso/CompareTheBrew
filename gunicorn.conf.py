# Gunicorn configuration for production
import multiprocessing

# Bind to 0.0.0.0:8000 (Render expects this)
bind = "0.0.0.0:8000"

# Workers based on CPU count
workers = multiprocessing.cpu_count() * 2 + 1

# Use threads for I/O bound operations
threads = 2

# Worker class
worker_class = "sync"

# Timeout
timeout = 120

# Keep-alive
keepalive = 5

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"

# SSL (handled by Render's proxy)
# keyfile = "key.pem"
# certfile = "cert.pem"

# Preload app for memory efficiency with multiple workers
preload_app = True

# Process naming
proc_name = "comparethebrew"

# Max requests before worker restart (helps with memory leaks)
max_requests = 1000
max_requests_jitter = 50