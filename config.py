import os
try:
    from dotenv import load_dotenv
    basedir = os.path.abspath(os.path.dirname(__file__))
    load_dotenv(os.path.join(basedir, '.env'))
except ImportError:
    print("python-dotenv not installed, skipping load_dotenv")



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
