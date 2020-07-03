from datetime import datetime
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html


controls = dbc.FormGroup(
    [
        dbc.FormGroup([
                dbc.Row(dbc.Label("Instrument:")),
                dbc.Row(dcc.Input(
                    id="input1",
                    style={
                        'width': '160px',
                        'padding': '10px 10px'},
                    value='PETR4',
                    type='text',
                    debounce=True))]),
        dbc.FormGroup([
                dbc.Row(dbc.Label("Initial Date:")),
                dbc.Row(dcc.DatePickerSingle(
                    id="input2",
                    style={'width': '150px'},
                    clearable=True,
                    is_RTL=False,
                    first_day_of_week=2,
                    month_format='MMM Do, YYYY',
                    placeholder='DD-MM-YYYY',
                    display_format='DD-MM-YYYY',
                    date=datetime(2020, 4, 1))),
                ]),
        dbc.FormGroup([
                dbc.Row(dbc.Label("")),
                dbc.Row(dbc.Button(
                    "Refresh",
                    id="refresh-button",
                    size="lg",
                    style={'width': '162px'},
                    color="primary",
                    # color="dark",
                    block=True,
                    className="mr-4")),
                ]),
    ]
)

layout = dbc.Container(
    [
        html.Br(),
        html.H1("Time Series Explorer"),
        html.Hr(),
        html.Br(),
        dbc.Row(
            [
                dbc.Col(controls, md=2),
                dbc.Col(dcc.Graph(id="my-graph"), md=9),
            ],
            align="center",
        ),
    ],
    fluid=True,
)
