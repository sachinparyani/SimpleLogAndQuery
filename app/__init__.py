from flask import Flask

from .routes.log_routes import log_routes
from .routes.count_routes import count_routes

def create_app():
    app = Flask(__name__)

    app.register_blueprint(log_routes)
    app.register_blueprint(count_routes)

    return app