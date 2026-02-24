import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    JSON_AS_ASCII = False
    JSONIFY_PRETTYPRINT_REGULAR = True

    APP_NAME = "Qualtrics Data Processor"
    APP_VERSION = "1.0.0"

    QUALTRICS_API_TOKEN = os.getenv("QUALTRICS_API_TOKEN")
    QUALTRICS_DATA_CENTER = os.getenv("QUALTRICS_DATA_CENTER")

    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = int(os.getenv("DB_PORT"))
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    DB_POOL_MIN_CONN = int(os.getenv("DB_POOL_MIN_CONN", "1"))
    DB_POOL_MAX_CONN = int(os.getenv("DB_POOL_MAX_CONN", "10"))

    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    DATA_DIR = BASE_DIR / "data"

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    API_TIMEOUT = int(os.getenv("API_TIMEOUT", "30"))
    EXPORT_POLL_MAX_SECONDS = int(os.getenv("EXPORT_POLL_MAX_SECONDS", "300"))
    EXPORT_POLL_INTERVAL = float(os.getenv("EXPORT_POLL_INTERVAL", "2.0"))

    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    @property
    def database_url(self):
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    @classmethod
    def debug_paths(cls):
        print(f"settings.py file: {__file__}")
        print(f"BASE_DIR: {cls.BASE_DIR}")
        print(f"DATA_DIR: {cls.DATA_DIR}")
        print(f"BASE_DIR exists: {cls.BASE_DIR.exists()}")
        print(f"DATA_DIR exists: {cls.DATA_DIR.exists()}")

    @classmethod
    def validate_config(cls):
        required_vars = [
            ('QUALTRICS_API_TOKEN', cls.QUALTRICS_API_TOKEN),
            ('QUALTRICS_DATA_CENTER', cls.QUALTRICS_DATA_CENTER),
            ('DB_HOST', cls.DB_HOST),
            ('DB_NAME', cls.DB_NAME),
            ('DB_USER', cls.DB_USER),
            ('DB_PASSWORD', cls.DB_PASSWORD),
        ]

        missing_vars = []
        for var_name, var_value in required_vars:
            if not var_value:
                missing_vars.append(var_name)

        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

        try:
            port = int(cls.DB_PORT)
            if not (1 <= port <= 65535):
                raise ValueError(f"Invalid DB_PORT: {port}. Must be between 1 and 65535")
        except (ValueError, TypeError):
            raise ValueError(f"Invalid DB_PORT: {cls.DB_PORT}. Must be a valid integer")

        try:
            min_conn = int(cls.DB_POOL_MIN_CONN)
            max_conn = int(cls.DB_POOL_MAX_CONN)
            if min_conn < 1:
                raise ValueError(f"DB_POOL_MIN_CONN must be at least 1, got {min_conn}")
            if max_conn < min_conn:
                raise ValueError(f"DB_POOL_MAX_CONN ({max_conn}) must be >= DB_POOL_MIN_CONN ({min_conn})")
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid connection pool configuration: {e}")

        return True

    @classmethod
    def print_config_summary(cls):
        print("=== Configuration Summary ===")
        print(f"App Name: {cls.APP_NAME}")
        print(f"App Version: {cls.APP_VERSION}")
        print(f"Database Host: {cls.DB_HOST}")
        print(f"Database Port: {cls.DB_PORT}")
        print(f"Database Name: {cls.DB_NAME}")
        print(f"Database User: {cls.DB_USER}")
        print(f"Qualtrics Data Center: {cls.QUALTRICS_DATA_CENTER}")
        print(f"Data Directory: {cls.DATA_DIR}")
        print(f"Log Level: {cls.LOG_LEVEL}")
        print(f"Connection Pool: {cls.DB_POOL_MIN_CONN}-{cls.DB_POOL_MAX_CONN}")
        print("===========================")


class DevelopmentConfig(Config):
    DEBUG = True
    FLASK_ENV = 'development'


class ProductionConfig(Config):
    DEBUG = False
    FLASK_ENV = 'production'


class TestingConfig(Config):
    TESTING = True
    DEBUG = True
    DATABASE_URL = 'sqlite:///:memory:'


config_map = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}


def get_config():
    env = os.getenv('FLASK_ENV', 'default')
    config_class = config_map.get(env, DevelopmentConfig)
    return config_class()