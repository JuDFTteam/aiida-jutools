SiScLab-2020 project
####################

In this folder you can put/dump all your files, they will be automaticly part of this python package.
So you do not have to worry about packaging.

just put every python function you write into `aiida-jutools/aiida-jutools/sisclab/helpers.py`

you can then import these functions in you notebooks by:

```
from aiida_jutools.sisclab.helpers import <xyfunction>
```
(if case of import problems see below)

As a project target we have put 2 'deliverable' python notbooks which you will have to implement.
The overall goal is that we can execute them on any database we have.
For the project you will do so on the small database you have.
And we will execute it once you are finished on a/some larger database(s) and put the output here.
You can export the results of the notebooks for your report and presentation. Also we could collect the timings for different database sizes to get an idea of the scaling behavior of your work. Either the individual plots, or the whole notebook as .pdf. by hidding the input code lines.
Also, your are a team. The task are easily splittable so you can devide them amoung yourselves to save time, but also review the work of the other team members and provide them with feedback.

Have Fun! Johannes, Jens & Daniel

# Plotfunctions
We already have a lib of plot functions. I have put them in the subfolder masci_tools/vis/

I recommend (and would be very happy) to use those and extent those.
you can import them with
```
from .masci_tools.vis.plot_methods import <some_function>
from .masci_tools-vis.bokeh_plots import <some_function>
```
# Examples

In the example folder you find some older notebooks from Jens,
You may get some inspiration from them (for example how one can visualize the structure information) and you can copy/reuse code from them if you think it is useful for your task.
The 'AiiDA_statistics_py3_Jens.ipynb' contains the rough start of a small meta analysis as you did as a first task.
The second one 'inpgen_evaluation_oqmd.ipynb' contains a actual dirty analysis of some data from a project.
The 'MP_convergence_scf_clean_150_240.png' file contains a plot to give you an idea how a non-interactive version of a special plot for the structure-propetry visualizer may look like. I.e where we want to go with the 'generalized' version which is your deliverable. The code for this plot you find in the 'convergence_scatter_plot.py' file

#####
In case of import problems: you probably have to install the aiida-jutools repo.
for this go to the to folder in the git repository and execute
(cd aiida-jutools)
```
pip install -e .
```
This requires aiida so you can only install it somewhere where you have already aiida installed i.e on quantum mobile for example.
