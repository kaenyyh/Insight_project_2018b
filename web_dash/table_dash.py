import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State, Event
import plotly.plotly as py
from plotly.graph_objs import *
from scipy.stats import rayleigh
from flask import Flask
import numpy as np
import pandas as pd
import os
import sqlite3
import datetime as dt

app = dash.Dash('streaming-usage-app')
server = app.server

from cassandra.cluster import Cluster

CASSANDRA_SERVER    = ['35.162.115.222', '54.68.116.229', '50.112.36.122', '52.88.106.70']
CASSANDRA_NAMESPACE = "playground"
cluster = Cluster(CASSANDRA_SERVER)
session = cluster.connect()
session.execute("USE " + CASSANDRA_NAMESPACE)




app.layout = html.Div([
    html.Div([
        html.H2("Streaming Wikipedia Usage Monitor"),
        html.Img(src="https://s3-us-west-1.amazonaws.com/plotly-tutorials/logo/new-branding/dash-logo-by-plotly-stripe-inverted.png"),
    ], className='banner'),
    html.Div([
        html.Div([
            html.H3("Submitted Wikipedia Edits")
        ], className='Title'),
        html.Div([
            dcc.Graph(id='wiki-edit'),
        ], className='twelve columns wiki-edit'),
        dcc.Interval(id='wiki-edit-update', interval=1000, n_intervals=0),
    ], className='row wiki-edit-row'),
    html.Div([
        html.Div([
 	    html.H4("Latest 5 'Malicous' Users: "),
        ], className='Title'),
        html.Div([
            html.H5(id='tableoutput')
        ]),
        dcc.Interval(id='table-update', interval=1000, n_intervals=0),
    ], style={'width': '49%', 'display': 'inline-block'}), 
    html.Div([
	html.Div([
            html.H4("Edits of latest 'malicious' user: ")
        ],  className='Title'),
	html.Div([
	    dcc.Graph(id='histogramout'),
        ]),
        dcc.Interval(id='histogram-update', interval=1000, n_intervals=0),
    ], style={'width': '49%', 'display': 'inline-block'})
], style={'padding': '0px 10px 15px 10px',
          'marginLeft': 'auto', 'marginRight': 'auto', "width": "900px",
          'boxShadow': '0px 0px 5px 5px rgba(204,204,204,0.4)'})



@app.callback(
    Output(component_id='tableoutput', component_property='children'),
    [Input('table-update', 'n_intervals')]
)
def generate_table(interval):
    news_rec = session.execute('SELECT * FROM testtable')
    materialized_news = list(news_rec)
    df = pd.DataFrame(materialized_news, columns=['id','time','count'])

    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in df.columns])] +

        # Body
        [html.Tr([
            html.Td(df.iloc[i][col]) for col in df.columns
        ]) for i in range(5)]
    )

@app.callback(
    Output('histogramout','figure' ),
    [Input('histogram-update', 'n_intervals')]
)
def get_malicious_user(interval):
    rows = session.execute("SELECT * FROM testtable ")

    revnum = []
    for i in range(1, 11):
        revnum.append(rows[i].count)

    bin_val = np.histogram(revnum, bins=10)

    trace = Bar(
#        x=bin_val[1],
#        y=bin_val[0],
	y=revnum,
        marker=Marker(
#            color='#7F7F7F'
	color='#42C4F7'
        ),
        showlegend=False,
        hoverinfo='x+y'
    ) 

    layout = Layout(
        xaxis=dict(
            title='Wind Speed (mph)',
            showgrid=False,
            showline=False,
            fixedrange=True
        ),
        yaxis=dict(
            showgrid=False,
            showline=False,
            zeroline=False,
            title='Number of Samples',
            fixedrange=True
        ),
        margin=Margin(
            t=50,
            b=20,
            r=5
        ),
        autosize=True,
        bargap=0.01,
        bargroupgap=0,
        hovermode='closest',
        legend=Legend(
            x=0.175,
            y=-0.2,
            orientation='h'
        )
    )
    return Figure(data=[trace], layout=layout)


@app.callback(Output('wiki-edit', 'figure'), [Input('wiki-edit-update', 'n_intervals')])
def gen_wind_speed(interval):
    now = dt.datetime.now()
    sec = now.second
    minute = now.minute
    hour = now.hour

    total_time = (hour * 3600) + (minute * 60) + (sec)

    rows = session.execute("SELECT count FROM testtable ")

    revnum = []
    for i in range(200):
        revnum.append(rows[i].count)


#    month = []
#    for i in range(75):
#        month.append(rows[i].date)

#    con = sqlite3.connect("./Data/wind-data.db")
#    df = pd.read_sql_query('SELECT Speed, SpeedError, Direction from Wind where\
#                            rowid > "{}" AND rowid <= "{}";'
#                            .format(total_time-200, total_time), con)

    trace = Scatter(
	y=revnum,
#        y=revnum,
        line=Line(
            color='#42C4F7'
        ),
        hoverinfo='skip',
#        error_y=ErrorY(
#            type='data',
#            array=df['SpeedError'],
#            thickness=1.5,
#            width=2,
#            color='#B4E8FC'
#        ),
        mode='lines'
    )

    layout = Layout(
        height=450,
        xaxis=dict(
            range=[0, 200],
            showgrid=False,
            showline=False,
            zeroline=False,
            fixedrange=True,
            tickvals=[0, 50, 100, 150, 200],
	    ticktext=['0', '50', '100', '150', '200'],
#            ticktext=['200', '150', '100', '50', '0'],
            title='Time Elapsed (sec)'
        ),
        yaxis=dict(
            range=[min(0, min(revnum)),
                   max(50,1.3* max(revnum))],
            showline=False,
            fixedrange=True,
            zeroline=False,
	    nticks=10
#            nticks=max(6, round(max(revnum)/10))
        ),
        margin=Margin(
            t=45,
            l=50,
            r=50
        )
    )

    return Figure(data=[trace], layout=layout)




external_css = ["https://cdnjs.cloudflare.com/ajax/libs/skeleton/2.0.4/skeleton.min.css",
                "https://cdn.rawgit.com/plotly/dash-app-stylesheets/737dc4ab11f7a1a8d6b5645d26f69133d97062ae/dash-wind-streaming.css",
                "https://fonts.googleapis.com/css?family=Raleway:400,400i,700,700i",
                "https://fonts.googleapis.com/css?family=Product+Sans:400,400i,700,700i"]


for css in external_css:
    app.css.append_css({"external_url": css})

if 'DYNO' in os.environ:
    app.scripts.append_script({
        'external_url': 'https://cdn.rawgit.com/chriddyp/ca0d8f02a1659981a0ea7f013a378bbd/raw/e79f3f789517deec58f41251f7dbb6bee72c44ab/plotly_ga.js'
    })

if __name__ == '__main__':

#    from cassandra.cluster import Cluster

#    CASSANDRA_SERVER    = ['35.162.115.222', '54.68.116.229', '50.112.36.122', '52.88.106.70']
#    CASSANDRA_NAMESPACE = "playground"
#    cluster = Cluster(CASSANDRA_SERVER)
#    session = cluster.connect()
#    session.execute("USE " + CASSANDRA_NAMESPACE)
    
    app.run_server(debug=True, host="0.0.0.0", port = 80)
