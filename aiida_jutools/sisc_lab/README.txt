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

As a project target I have put 2 'deliverable' python notbooks which you will have to implement.
The overall goal is that we can execute them on any database we have.
For the project you will do so on the small database you have.
And we will execute it once you are finished on a alarger database and put the output here.
You can export the results of the notebooks for your report and presentation. Either the individual plots, or the whole notebook as .pdf. by hidding the input code lines.


Have Fun! Johannes, Jens & Daniel

#####
In case of import problems: you probably have to install the aiida-jutools repo.
for this go to the to folder in the git repository and execute
(cd aiida-jutools)
```
pip install -e .
```
This requires aiida so you can only install it somewhere where you have already aiida installed i.e on quantum mobile for example.
