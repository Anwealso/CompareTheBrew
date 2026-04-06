import os
from urllib.parse import quote
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))


def _build_supabase_url():
    """Build Supabase connection URL, constructing it from SUPABASE_PASSWORD if needed."""
    url = os.environ.get('SUPABASE_DB_URL', '')
    if url:
        return url
    password = os.environ.get('SUPABASE_PASSWORD', '')
    if password:
        return f"postgresql://postgres:{quote(password, safe='')}@db.mjhxcgnkxreakcqubbms.supabase.co:5432/postgres"
    return ''


class Config(object):
    DEBUG = False
    TESTING = False
    CSRF_ENABLED = True
    SCRAPINGBEE_API_KEY = os.environ.get('SCRAPING_API_KEY')
    IPINFO_TOKEN = os.environ.get('IPINFO_TOKEN')
    BRIGHTDATA_CUSTOMER_ID = os.environ.get('BRIGHTDATA_CUSTOMER_ID')
    BRIGHTDATA_ZONE = os.environ.get('BRIGHTDATA_ZONE')
    BRIGHTDATA_PASSWORD = os.environ.get('BRIGHTDATA_PASSWORD')
    FLAG_SHOW_STALENESS = True
    USE_LOCAL_DB = os.environ.get('USE_LOCAL_DB', '').lower() == 'true'
    SUPABASE_DB_URL = _build_supabase_url()

class ProductionConfig(Config):
    DEBUG = False


class StagingConfig(Config):
    DEVELOPMENT = True
    DEBUG = True


class DevelopmentConfig(Config):
    DEVELOPMENT = True
    DEBUG = True


class TestingConfig(Config):
    TESTING = True
