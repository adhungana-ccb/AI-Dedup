from flask import Flask
from .config import Config
from .web.routes import web_bp


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # Register the web blueprint
    app.register_blueprint(web_bp)

    return app