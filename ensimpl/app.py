# -*- coding: utf-8 -*-
import os

from flask import Flask
from flask import render_template

from ensimpl.extensions import debug_toolbar
from ensimpl.modules.api.views import api
from ensimpl.modules.navigator.views import navigator
from ensimpl.modules.page.views import page
from ensimpl.utils import ReverseProxied


def create_app(settings_override=None):
    """
    Create a Flask application using the app factory pattern.

    :param settings_override: Override settings
    :return: Flask app
    """
    app = Flask(__name__)

    app.config.from_object('config.settings')
    
    if app.config.from_envvar('ENSIMPL_SETTINGS', silent=True):
        env_settings = os.environ['ENSIMPL_SETTINGS']
        app.logger.info('Using ENSIMPL_SETTINGS: {}'.format(env_settings))

    if settings_override:
        app.logger.info('Overriding settings with parameters')
        app.config.update(settings_override)

    app.logger.setLevel(app.config['LOG_LEVEL'])

    middleware(app)

    app.register_blueprint(api)
    app.register_blueprint(navigator)
    app.register_blueprint(page)

    extensions(app)
    error_templates(app)

    return app


def extensions(app):
    """
    Register 0 or more extensions (mutates the app passed in).

    :param app: Flask application instance
    :return: None
    """
    debug_toolbar.init_app(app)

    return None


def middleware(app):
    """
    Register 0 or more middleware (mutates app that is passed in).

    :param app: Flask application instance
    :return: None
    """
    app.wsgi_app = ReverseProxied(app.wsgi_app)

    return None


def error_templates(app):
    """
    Register 0 or more custom error pages (mutates the app passed in).

    :param app: Flask application instance
    :return: None
    """

    def render_status(status):
        """
         Render a custom template for a specific status.
           Source: http://stackoverflow.com/a/30108946

         :param status: Status as a written name
         :type status: str
         :return: None
         """
        # Get the status code from the status, default to a 500 so that we
        # catch all types of errors and treat them as a 500.
        code = getattr(status, 'code', 500)
        return render_template('errors/{0}.html'.format(code)), code

    for error in [404, 500]:
        app.errorhandler(error)(render_status)

    return None


