# -*- coding: utf-8 -*-
from flask import Blueprint, render_template

page = Blueprint('page', __name__, template_folder='templates')


@page.route('/')
def home():
    return render_template('page/index.html')


@page.route('/ping')
def ping():
    return 'OK!!!!'


