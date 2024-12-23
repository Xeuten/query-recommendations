from flask import Flask

from app.routes import main
from config import cache


def create_app():
    app = Flask(__name__)
    app.register_blueprint(main)
    cache.init_app(app)
    return app
