# Imports
import numpy as np
import pandas as pd

from bokeh.io import output_file, save, curdoc
from bokeh.layouts import gridplot, column, row
from bokeh.models import ColumnDataSource, RadioGroup, Select, Slider, PreText
from bokeh.models.tools import HoverTool
from bokeh.plotting import figure, show

import helpers
from helpers import MAP, INVMAP


# Settings
input_filename = 'combined_properties_all.xlsx'
output_filename ='Interactive_visualization_app.html'
TOOLS = 'pan, wheel_zoom, box_select, reset, save'
maker_size=10

# Read file
dfs = helpers.read_excel_file(input_filename)
df_all, OPTIONS_all, UNITS_all = {}, {}, {}
versions, mversions = [], []
for key, df in dfs.items():
        df = helpers.filter_unavailable_df(df)
        if not df.empty:
                df_all[key] = df
                OPTIONS_all[key], UNITS_all[key] = helpers.get_attrs_and_units(df)
                mversions.append(key)
                versions.append(INVMAP[key])
output_file(output_filename)


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
                size=maker_size,
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
radio_button_widget = RadioGroup(name="Workflow version", labels=versions, active=0, margin=(50,0,0,20))
select_widget1 = Select(title="X Axis", 
                        value=OPTIONS_all[mversions[0]][0], 
                        options=OPTIONS_all[mversions[0]],
                        margin=(20,0,0,20))
select_widget2 = Select(title="Y Axis", 
                        value=OPTIONS_all[mversions[0]][1], 
                        options=OPTIONS_all[mversions[0]],
                        margin=(10,0,0,20))
slider_widget = Slider(title="Number of bins", start=5, end=50, step=1, value=20, margin=(20,0,0,20))
numerical_statistics = PreText(text='', width=200, margin=(20,0,0,20))
categorical_statistics = PreText(text='', width=200, margin=(20,0,0,20))

# Define callback functions
def update_options():
        '''
        Callback function for updating options of select widgets.
        '''
        version = versions[radio_button_widget.active]
        mversion = MAP[version]
        df, OPTIONS = df_all[mversion], OPTIONS_all[mversion]
        
        select_widget1.remove_on_change('value', us)
        select_widget1.options = OPTIONS
        select_widget1.value = OPTIONS[0]
        select_widget1.on_change('value', us)

        select_widget2.remove_on_change('value', us)
        select_widget2.options = OPTIONS
        select_widget2.value = OPTIONS[1]      
        select_widget2.on_change('value', us)        
        
        update_sel()
        categorical_statistics.text = df['formula'].describe().to_string()

def update_sel():
        '''
        Callback function for select widgets.
        '''
        # Get df, options, units for the current version
        version = versions[radio_button_widget.active]
        mversion = MAP[version]
        df, OPTIONS, UNITS = df_all[mversion], OPTIONS_all[mversion], UNITS_all[mversion]
        # Get data and units for the selected options
        xcol, ycol = select_widget1.value, select_widget2.value    
        filtered_df, xdata, ydata = helpers.filter_missing_value(df, xcol, ycol)    
        xunit, yunit = UNITS[OPTIONS.index(xcol)], UNITS[OPTIONS.index(ycol)] 
        # Get nbins
        nbins = slider_widget.value
        
        source.data = dict(
                x=filtered_df[xcol],
                y=filtered_df[ycol],
                suuid=filtered_df['structure_uuid'],
                formula=filtered_df['formula'],
                duuid=filtered_df['dict_uuid'])
        p.title.text = "Properties visualization"# for " + xcol + " and " + ycol
        hhist, hedges = np.histogram(xdata, bins=nbins)
        hsource.data = dict(
                left=hedges[:-1],
                right=hedges[1:],
                top=hhist,
                bottom=np.zeros(hhist.shape))
        if xunit:
                ph.xaxis.axis_label = f"{xcol} ({xunit})"
        else:
                ph.xaxis.axis_label = f"{xcol}"
        vhist, vedges = np.histogram(ydata, bins=nbins)
        vsource.data = dict(
                left=np.zeros(vhist.shape),
                right=vhist,
                top=vedges[1:],
                bottom=vedges[:-1])
        if yunit:
                pv.yaxis.axis_label = f"{ycol} ({yunit})"
        else:
                pv.yaxis.axis_label = f"{ycol}"
        
        numerical_statistics.text = df[[xcol, ycol]].describe().to_string()

# Activate callback functions
uo = lambda attr, old, new: update_options()
us = lambda attr, old, new: update_sel()
radio_button_widget.on_change('active', uo) 
select_widget1.on_change('value', us)
select_widget2.on_change('value', us)  
slider_widget.on_change('value',us)

# Set up layout
mainplot = gridplot([[p, pv], [ph, None]], merge_tools=False)
widgetbox = column(radio_button_widget, 
                select_widget1, 
                select_widget2, 
                slider_widget, 
                numerical_statistics,
                categorical_statistics )
layout = row(mainplot, widgetbox)

# Initialize
update_options()

curdoc().add_root(layout)
curdoc().title = "Properties visualization"