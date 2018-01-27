# -*- coding: utf-8 -*-
from flask import Blueprint, render_template

page = Blueprint('page', __name__, template_folder='templates')


@page.route('/')
def index():
    """Default page.

    Returns:
        :class:`flask.Response`: The response object.

    """
    return render_template('page/index.html')


@page.route('/ping')
def ping():
    """A page to test if any pages are up and running.

    Returns:
        :class:`flask.Response`: The response object with just 'OK!!!!'.
    """
    return 'OK!!!!'


