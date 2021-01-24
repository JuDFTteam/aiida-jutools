# Imports
import numpy as np
import pandas as pd

from bokeh.io import output_file, save, curdoc
from bokeh.layouts import gridplot, column, row
from bokeh.models import ColumnDataSource, Select, BoxSelectTool
from bokeh.models.tools import HoverTool
from bokeh.plotting import figure, show

import helpers


# Settings
#input_filename = 'combined_property_wf042.json'
# or
input_filename = 'combined_property_parser.json'
output_filename ='Interactive_visualization_app.html'
TOOLS = 'pan, wheel_zoom, box_select, reset, save'

# Read file
df = helpers.read_json_file(input_filename)
output_file(output_filename)
OPTIONS, UNITS = helpers.get_options_and_units(df)


# Create the scatter plot
source = ColumnDataSource(data=dict(x=[], y=[], suuid=[], formula=[], duuid=[]))
TOOLTIPS = [('Index', '$index'),
            ('Sturture_node uuid', '@suuid'),
            ('Input formula', '@formula'),
            ('Dict_node uuid', '@duuid'),
            ('Value', '($x, $y)')]  # Hover tools for scatter plot
p = figure(plot_width=600,
            plot_height=600,
            toolbar_location='above',
            x_axis_location=None,
            y_axis_location=None,
            title='Linked Histograms',
            tools=TOOLS,
            tooltips=TOOLTIPS)
# Render
r = p.circle(x='x',
                y='y',
                color='#3a5785',
                alpha=0.6,
                selection_color='red',
                selection_fill_alpha=0.8,
                nonselection_fill_color='grey',
                nonselection_fill_alpha=0.2,
                source=source)

# Create the horizontal histogram
hsource = ColumnDataSource(data=dict(left=[], right=[], top=[], bottom=[]))
ph = figure(toolbar_location=None,
            plot_width=p.plot_width,
            plot_height=200,
            x_range=p.x_range,
            min_border=10,
            min_border_left=50,
            y_axis_location='right',
            tools='hover',
            tooltips=[('Count', '@top')])
ph.yaxis.major_label_orientation = np.pi / 4
# Render
ph.quad(bottom='bottom',
        left='left',
        right='right',
        top='top',
        color='#3a5785',
        alpha=0.6,
        source=hsource)

# Create the vertical histogram
vsource = ColumnDataSource(data=dict(left=[], right=[], top=[], bottom=[]))
pv = figure(toolbar_location=None,
            plot_width=200,
            plot_height=p.plot_height,
            y_range=p.y_range,
            min_border=10,
            y_axis_location='right',
            tools='hover',
            tooltips=[('Count', '@right')])
pv.xaxis.major_label_orientation = np.pi / 4
# Render
pv.quad(left='left',
        bottom='bottom',
        top='top',
        right='right',
        color='#3a5785',
        alpha=0.6,
        source=vsource)

# Set up widgets
select_widget1 = Select(value=OPTIONS[0], options=OPTIONS)
select_widget2 = Select(value=OPTIONS[1], options=OPTIONS)

# Define callback function and activate
def callback():
    '''
    Callback function for Bokeh visualization.
    '''
    xcol, ycol = select_widget1.value, select_widget2.value
    p.title.text = "Properties visualization for " + xcol + " and " + ycol
    xunit, yunit = UNITS[OPTIONS.index(xcol)], UNITS[OPTIONS.index(ycol)] 
    filtered_df, xdata, ydata = helpers.filter_missing_value(df, xcol, ycol)
    source.data = dict(
            x=filtered_df[xcol],
            y=filtered_df[ycol],
            suuid=filtered_df['structure_uuid'],
            formula=filtered_df['formula'],
            duuid=filtered_df['dict_uuid'])
    hhist, hedges = np.histogram(xdata, bins=20)
    hsource.data = dict(
            left=hedges[:-1],
            right=hedges[1:],
            top=hhist,
            bottom=np.zeros(hhist.shape))
    ph.xaxis.axis_label = xcol + ' (' + xunit + ')'
    vhist, vedges = np.histogram(ydata, bins=20)
    vsource.data = dict(
            left=np.zeros(vhist.shape),
            right=vhist,
            top=vedges[1:],
            bottom=vedges[:-1])
    pv.yaxis.axis_label = ycol + ' (' + yunit + ')'

select_widget1.on_change('value', lambda attr, old, new: callback())
select_widget2.on_change('value', lambda attr, old, new: callback())      

# Set up layout
mainplot = gridplot([[p, pv], [ph, None]], merge_tools=False)
widgetbox = column(select_widget1, select_widget2)
layout = row(mainplot, widgetbox)

# Initialize
callback()

curdoc().add_root(layout)