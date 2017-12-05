# -*- coding: utf-8 -*-

from flask import Blueprint
from flask import render_template

navigator = Blueprint('nav', __name__, template_folder='templates', url_prefix='/navigator')


@navigator.route("/js/karyotype.js")
def karyotype_js():
    """Get the Ensimpl Javascript file.

    Returns:
        the Javascript response object
    """
    headers = {'Content-Type': 'application/javascript'}
    return render_template('karyotype.js'), 200, headers


@navigator.route("/")
def index():
    """Show the navigator page.
    """
    return render_template('navigator.html')
