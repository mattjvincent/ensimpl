# -*- coding: utf-8 -*-
from flask import Blueprint
from flask import render_template

lookup = Blueprint('lookup', __name__, template_folder='templates', url_prefix='/lookup')


@lookup.route("/")
def index():
    """Show the navigator page.

    Returns:
        :class:`flask.Response`: The response object.
    """
    return render_template('lookup/lookup.html')



