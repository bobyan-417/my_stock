import plotly.plotly as py
import plotly.graph_objs as go

import datetime
py.sign_in('biaoyan417', 'api_key')
def to_unix_time(dt):
    epoch =  datetime.datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000

x = [datetime.datetime(year=2013, month=10, day=04),
    datetime.datetime(year=2013, month=11, day=05),
    datetime.datetime(year=2013, month=12, day=06)]

data = [go.Scatter(
            x=x,
            y=[1, 3, 6])]

layout = go.Layout(xaxis = dict(
                   range = [to_unix_time(datetime.datetime(2013, 10, 17)),
                            to_unix_time(datetime.datetime(2013, 11, 20))]
    ))

fig = go.Figure(data = data, layout = layout)
py.iplot(fig)