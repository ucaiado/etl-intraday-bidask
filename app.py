#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Main class of the Flask website


@author: ucaiado

Created on 06/15/2020
"""
# import libraries
import os
import dash
import logging
from datetime import date, datetime, timedelta
import dash_bootstrap_components as dbc
from flask.helpers import get_root_path
from flask import (Flask, render_template, request, make_response, redirect,
                   url_for, jsonify, Response, flash)


'''
Begin help functions
'''


def create_app():
    '''
    Return a Flask app
    '''
    server = Flask(__name__)

    # other configurations
    server.config['SESSION_TYPE'] = 'filesystem'
    server.config['LOG'] = 'logs/'
    file_handler = logging.FileHandler(os.path.join(server.config['LOG'],
                                       'app_{}.log'.format(date.today())))
    server.logger.addHandler(file_handler)
    server.logger.setLevel(logging.INFO)

    from dashapps.app1.layout import layout as layout1
    from dashapps.app1.callbacks import register_callbacks as regcallbacks1
    from dashapps.app2.layout import layout as layout2
    from dashapps.app2.callbacks import register_callbacks as regcallbacks2

    register_dashapp(server, 'Dashapp 1', 'app_dash1', layout1, regcallbacks1)
    register_dashapp(server, 'Dashapp 2', 'app_dash2', layout2, regcallbacks2)

    return server


def register_dashapp(app, s_title, s_url, layout, regcallbacks_fun):
    '''
    Map a dashapp to flask url passed
    source: https://bit.ly/2CmvywN

    :param app: Falsk object.
    :param s_title: string.
    :param s_url: string.
    :param layout: function object.
    :param regcallbacks_fun: function object.

    '''
    # Meta tags for viewport responsiveness
    meta_viewport = {
        "name": "viewport",
        "content": "width=device-width, initial-scale=1, shrink-to-fit=no"}

    base_pathname = 'static/HTML'

    my_dashapp = dash.Dash(
        __name__,
        server=app,
        url_base_pathname=f'/{s_url}/',
        assets_folder=(get_root_path(__name__) + f'/{base_pathname}/assets/'),
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        meta_tags=[meta_viewport])

    with app.app_context():
        my_dashapp.title = s_title
        my_dashapp.layout = layout
        regcallbacks_fun(my_dashapp)


# set up global variables
app = create_app()

# define list of templates to be used
TEMPLATES = {
    'hello': 'welcome.html',
    'app1': 'app1.html',
    'app2': 'app2.html'
}


'''
End help functions
'''


@app.route('/', methods=['POST', 'GET'])
def process_mainpage():
    '''
    handles main page. Redirect to the App2 page
    '''
    response = make_response(redirect('/app2'))

    return response


@app.route('/hello/')
@app.route('/hello/<name>')
def hello(name=None):
    '''
    mapping a test route
    '''
    d_input = {'name': name}
    return render_template(TEMPLATES['hello'], **d_input)


@app.route('/app1')
def app1_template():
    '''
    mapping a basic dash app route
    '''
    return render_template(TEMPLATES['app1'], dash_url='app_dash1')


@app.route('/app2')
def app2_template():
    '''
    mapping the main dash app route
    '''
    return render_template(TEMPLATES['app2'], dash_url='app_dash2')

# run the website
if __name__ == '__main__':
    app.run(debug=True)
