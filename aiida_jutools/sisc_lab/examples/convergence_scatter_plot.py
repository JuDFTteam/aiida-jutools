# -*- coding: utf-8 -*-
import matplotlib
import sys, os
from pprint import pprint
import time
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as pp
import matplotlib.patches as mppatches
import matplotlib.lines as mlines
from mpl_toolkits.axes_grid1 import make_axes_locatable
from collections import Counter
#from aiida.orm import load_node
#from aiida.common.constants import elements as PeriodicTableElements
from masci_tools.vis.plot_methods import default_histogram
from masci_tools.vis.plot_methods import multiple_scatterplots, multiaxis_scatterplot, single_scatterplot
from masci_tools.vis.plot_methods import single_scatterplot, multi_scatter_plot, set_plot_defaults, histogram
#from aiida_fleur.tools.element_econfig_list import get_spin_econfig, get_econfig, rek_econ
#from aiida_fleur.tools.element_econfig_list import econfiguration, get_coreconfig

#_atomic_numbers = {data['symbol']: num for num, data in PeriodicTableElements.items()}

htr2ev = 27.21138602


def plot_convergence_scatter2(frame,
                              frame2,
                              iteration=None,
                              print_prozent=True,
                              prozent=0.0,
                              saveas='convergence_scf_oqmd3.pdf'):

    # all together with shared axis
    from mpl_toolkits.axes_grid1 import make_axes_locatable
    from masci_tools.vis.plot_methods import set_plot_defaults
    set_plot_defaults(show=False)

    set_plot_defaults(use_axis_fromatter=False,
                      figsize=(16, 9),
                      labelfonstsize=21,
                      ticklabelsize=20)

    fig, ax3 = pp.subplots(figsize=(16, 9))
    divider = make_axes_locatable(ax3)
    ax1 = divider.append_axes('top', 1.2, pad=0.2, sharex=ax3)
    ax4 = divider.append_axes('right', 1.2, pad=0.2, sharey=ax3)

    difference_charge_spin = frame2['difference_charge_spin']
    difference_energy_spin = frame2['difference_energy_spin']
    x3 = list(frame['distance'])  #difference_charge
    y3 = list(frame['energy_differ'])  #difference_energy
    markersize = list(frame['markersize'])  #iterations
    markercolor = list(frame['markercolor'])
    x2 = [i for i in range(0, 10000)]
    y2 = x2

    ax3 = multi_scatter_plot(
        x3,
        y3,
        markersize,
        xlabel='Charge distance [me/bohr^3]',
        ylabel=u'Energy diff [htr]',
        title='',
        scale=['log', 'log'],
        legend=False,
        #xticks=xticks,
        color=markercolor,
        limits=[[0.0000001, 1100], [0.00000000001, 1100]],
        saveas='convergence_scf_oqmd',
        alpha=0.5,
        marker='o',
        label='spin',
        axis=ax3)  #markerstyle)

    rectangle2 = mppatches.Patch(color='r',
                                 alpha=0.6,
                                 label='Collinear magnetism')
    rectangle1 = mppatches.Patch(color='b', alpha=0.6, label='Non magnetic')
    circle1 = mlines.Line2D(range(1),
                            range(1),
                            linestyle='',
                            color='k',
                            alpha=0.3,
                            marker='o',
                            markerfacecolor='k',
                            markersize=5.0,
                            label='< 20 Iterations')
    circle2 = mlines.Line2D(range(1),
                            range(1),
                            linestyle='',
                            color='k',
                            alpha=0.3,
                            marker='o',
                            markerfacecolor='k',
                            markersize=10.0,
                            label='<100 Iterations')
    circle3 = mlines.Line2D(range(1),
                            range(1),
                            linestyle='',
                            color='k',
                            alpha=0.3,
                            marker='o',
                            markerfacecolor='k',
                            markersize=15.0,
                            label='=240 Iterations')
    convl = mlines.Line2D(range(1),
                          range(1),
                          linestyle='--',
                          color='k',
                          alpha=1.0,
                          marker='',
                          label='Convergence criterion')

    handles = [circle1, circle2, circle3, rectangle1, rectangle2, convl]
    legends_defaults = {
        'bbox_to_anchor': (0.05, 0.97),
        'fontsize': 17,
        'linewidth': 3.0,
        'borderaxespad': 0,
        'loc': 2,
        'fancybox': True,
        'framealpha': 1.0
    }  #'title' : 'Legend',
    loptions = legends_defaults.copy()
    linewidth = loptions.pop('linewidth', 1.5)
    leg = ax3.legend(
        handles=handles, **loptions
    )  #bbox_to_anchor=loptions['anchor'],loc=loptions['loc'], title=legend_title, borderaxespad=0., fancybox=True)
    leg.get_frame().set_linewidth(linewidth)
    ax3.axvline(ymin=0,
                ymax=1.0,
                x=0.000005,
                linewidth=2.0,
                linestyle='--',
                color='k')
    ax3 = single_scatterplot(
        x2,
        y2,
        u'Charge distance [me/a$_0$$^3$]',
        ylabel=u'Energy difference [htr]',  #last $\Delta$ E [htr]',
        marker='',
        title='',
        plotlabel='',
        axis=ax3)

    # Histogram top
    x = difference_charge_spin
    bins = [10**i for i in np.linspace(-7, 3, 70)]
    #ax1.set_xscale('log')
    ax1.xaxis.tick_top()
    ax1.xaxis.set_label_position('top')
    histcolors = ['r', 'b']
    ax1.axvline(ymin=0,
                ymax=1.0,
                x=0.000005,
                linewidth=2.0,
                linestyle='--',
                color='k')
    ax1, n, bins2, patches = histogram(
        x,
        bins=bins,
        legend=False,
        xlabel='',  #charge distance [me/bohr^3]',
        ylabel=u'N',
        histtype='barstacked',
        title='',
        cumulative=False,
        color=histcolors,
        limits=[[0.0000001, 1100], [0, 1200]],
        saveas='convergence_scf_oqmd3',
        alpha=0.6,
        axis=ax1,
        return_hist_output=True)  #markerstyle)

    #ax1.text(0.5, 0.5, 'N = {} + {}'.format(len(difference_charge_spin[0]), len(difference_charge_spin[1])),
    #        horizontalalignment='center',
    #        verticalalignment='center', transform=ax1.transAxes, fontsize=22)
    ax1.text(
        0.5,
        0.7,
        r'$N_{tot}$ =           +',  #+ #str(len(difference_charge_spin[0])) + r'+'+ str(len(difference_charge_spin[1])),
        #r'\textcolor{blue}{{}}'.format(str(len(difference_charge_spin[0]))) + r'+'+ r'\textcolor{red}{{}}'.format(str(len(difference_charge_spin[1]))),
        horizontalalignment='center',
        verticalalignment='center',
        transform=ax1.transAxes,
        fontsize=22)
    ax1.text(0.54,
             0.7,
             '{}'.format(str(len(difference_charge_spin[0]))),
             color='red',
             horizontalalignment='center',
             verticalalignment='center',
             transform=ax1.transAxes,
             fontsize=22)
    ax1.text(0.64,
             0.7,
             '{}'.format(str(len(difference_charge_spin[1]))),
             color='blue',
             horizontalalignment='center',
             verticalalignment='center',
             transform=ax1.transAxes,
             fontsize=22)
    if iteration is not None:
        ax1.text(0.9,
                 0.7,
                 u'Iter = ' + str(iteration),
                 horizontalalignment='center',
                 verticalalignment='center',
                 transform=ax1.transAxes,
                 fontsize=22)

    if print_prozent:  # maybe to slow
        #total = 0
        #converged = 0
        #for dataset in difference_energy_spin:
        #    total = total + len(dataset)
        #    for datapkt in dataset:
        #        if datapkt <= 0.000005:
        #            converged = converged + 1
        prozent = prozent  #converged/total*100
        ax1.text(0.055,
                 0.7,
                 u'{:3.0f}%'.format(prozent),
                 horizontalalignment='center',
                 verticalalignment='center',
                 transform=ax1.transAxes,
                 fontsize=22)
        ax1.text(0.25,
                 0.7,
                 u'{:3.0f}%'.format(100 - prozent),
                 horizontalalignment='center',
                 verticalalignment='center',
                 transform=ax1.transAxes,
                 fontsize=22)

    ###### Histogram for energy, right
    x4 = difference_energy_spin
    bins1 = [10**i for i in np.linspace(-11, 3, 50)]
    #ax4.set_yscale('log')
    histcolors = ['r', 'b']
    ax4.yaxis.tick_right()
    ax4.yaxis.set_label_position('right')
    #ax.invert_xaxis()
    #ax.text(0, 200, 'N = {} + {}'.format(len(difference_charge_spin[0]), len(difference_charge_spin[1])), fontsize=16)

    #ax.axvline(ymin=0, ymax=1.0, x=0.000005, linewidth=2.0, linestyle='--', color='k')
    ax4, n, bins2, patches = histogram(
        x4,
        bins=bins1,
        legend=False,
        xlabel=u'N',  #charge distance [me/bohr^3]',
        ylabel=u'',
        histtype='barstacked',
        orientation='horizontal',
        title='',
        cumulative=False,
        color=histcolors,
        limits=[[0, 1200], [0.00000000001, 1100]],
        saveas='convergence_scf_oqmd3',
        alpha=0.6,
        axis=ax4,
        return_hist_output=True)  #markerstyle)
    #ax4.text(0.5, 0.5, 'N = {} + {}'.format(len(difference_charge_spin[0]), len(difference_charge_spin[1])), #'matplotlib',
    #        horizontalalignment='center',
    #        verticalalignment='center', transform=ax.transAxes, fontsize=22)
    #x = single_scatterplot(n, title='', xlabel='charge distance [me/bohr^3]', ylabel=u'counts')
    #pp.show()
    fig.savefig(saveas, transparent=False, format='png')
    return fig
