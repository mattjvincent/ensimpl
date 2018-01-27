# -*- coding: utf-8 -*-
from flask import Blueprint
from flask import render_template

navigator = Blueprint('nav', __name__, template_folder='templates', url_prefix='/navigator')


@navigator.route("/js/karyotype.js")
def karyotype_js():
    """Get the Karyotype Javascript file.

    Returns:
        :class:`flask.Response`: The response which is the karyotype.js.

    """
    headers = {'Content-Type': 'application/javascript'}
    return render_template('navigator/karyotype.js'), 200, headers


@navigator.route("/")
def index():
    """Show the navigator page.

    Returns:
        :class:`flask.Response`: The response object.
    """
    return render_template('navigator/navigator.html')
