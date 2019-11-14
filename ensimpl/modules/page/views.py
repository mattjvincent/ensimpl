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


@page.route('/search')
def search():
    """A page to test if any pages are up and running.

    Returns:
        :class:`flask.Response`: The response object.
    """
    return render_template('page/search.html')


@page.route("/js/karyotype.js")
def karyotype_js():
    """Get the Karyotype Javascript file.

    Returns:
        :class:`flask.Response`: The response which is the karyotype.js.

    """
    headers = {'Content-Type': 'application/javascript'}
    return render_template('page/karyotype.js'), 200, headers


@page.route("/navigator")
def navigator():
    """Show the navigator page.

    Returns:
        :class:`flask.Response`: The response object.
    """
    return render_template('page/navigator.html')


@page.route("/external_ids")
def external_ids():
    """Show the converter page.

    Returns:
        :class:`flask.Response`: The response object.
    """
    return render_template('page/external_ids.html')


@page.route("/lookup")
def lookup():
    """Show the batch ID lookup page.

    Returns:
        :class:`flask.Response`: The response object.
    """
    return render_template('page/lookup.html')


@page.route("/history")
def history():
    """Show the history page.

    Returns:
        :class:`flask.Response`: The response object.
    """
    return render_template('page/history.html')


@page.route("/help")
def help():
    """Show the navigator page.

    Returns:
        :class:`flask.Response`: The response object.
    """
    return render_template('page/help.html')


@page.route("/js/ensimpl.js")
def ensimpl_js():
    """Get the Ensimpl Javascript file.

    Returns:
        :class:`flask.Response`: The response which is the Javascript file.
    """
    headers = {'Content-Type': 'application/javascript'}
    return render_template('page/ensimpl.js'), 200, headers








