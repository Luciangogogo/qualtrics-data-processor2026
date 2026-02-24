import logging
import os
import sys
from flask import Flask

from .config.settings import get_config
from .config.database import db_manager
from .api.routes import api_bp, health_bp


def setup_logging(config):
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format=config.LOG_FORMAT,
        handlers=[
            logging.StreamHandler(),
        ]
    )


def create_app():
    app = Flask(__name__)

    try:
        config = get_config()
        app.config.from_object(config)

        setup_logging(config)

        config.validate_config()

        if config.DEBUG:
            config.print_config_summary()

        app.logger.info(f"Starting {config.APP_NAME} v{config.APP_VERSION}")
        app.logger.info(f"Environment: {app.config.get('FLASK_ENV', 'unknown')}")
        app.logger.info(f"Debug mode: {app.config.get('DEBUG', False)}")

        try:
            db_manager.initialize_with_config(config)
            app.logger.info("Database connection pool initialized successfully")

            if db_manager.test_connection():
                app.logger.info("Database connection test passed")
            else:
                app.logger.error("Database connection test failed")

        except Exception as e:
            app.logger.error(f"Failed to initialize database: {e}")
            app.logger.error("Please check your database configuration and ensure the database server is running")
            raise

        app.register_blueprint(health_bp)
        app.register_blueprint(api_bp)

        @app.errorhandler(404)
        def not_found(error):
            return {"success": False, "error": "Endpoint not found"}, 404

        @app.errorhandler(500)
        def internal_error(error):
            app.logger.error(f"Internal server error: {error}")
            return {"success": False, "error": "Internal server error"}, 500

        @app.teardown_appcontext
        def close_db_connections(error):
            if error:
                app.logger.error(f"Application context error: {error}")

        def cleanup_resources():
            try:
                db_manager.close_all_connections()
                app.logger.info("Database connections closed")
            except Exception as e:
                app.logger.error(f"Error closing database connections: {e}")

        import atexit
        atexit.register(cleanup_resources)

        return app

    except ValueError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Failed to create application: {e}", file=sys.stderr)
        sys.exit(1)


try:
    app = create_app()
except Exception as e:
    print(f"Failed to initialize application: {e}", file=sys.stderr)
    sys.exit(1)

if __name__ == '__main__':
    try:
        port = int(os.getenv('PORT', 5000))
        debug = app.config.get('DEBUG', False)

        app.logger.info(f"Starting server on port {port}")
        app.run(
            host='0.0.0.0',
            port=port,
            debug=debug
        )
    except KeyboardInterrupt:
        app.logger.info("Shutting down gracefully...")
    except Exception as e:
        app.logger.error(f"Failed to start server: {e}")
        raise