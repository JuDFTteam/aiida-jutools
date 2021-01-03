SiScLab2020 Project Resources
=============================

**[master file](../../README.md)**

# Preface

This is an incomplete, sorted dump for resources/stuff posted in the SiScLab2020, project @PGI-1, Slack channel (non-public). From the perspective of co-supervisor wasmer. To give the project proceedings a bit of structure, have a place to pre-formulate ideas, and find old things more quickly.


<!-- emacs markdown-mode auto-generated message: -->
<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [SiScLab2020 Project Resources](#sisclab2020-project-resources)
- [Preface](#preface)
- [Central resources](#central-resources)
- [Datasets](#datasets)
- [AiiDA resources](#aiida-resources)
- [Python](#python)
    - [Core Python](#core-python)
    - [Python Ecosystem](#python-ecosystem)
        - [Lists](#lists)
        - [Serialization](#serialization)
    - [Python development](#python-development)
        - [General](#general)
        - [Deployment](#deployment)
        - [Dev tools](#dev-tools)
            - [IDEs](#ides)
- [Data science](#data-science)
    - [General](#general-1)
        - [Lists](#lists-1)
    - [Data analysis & visualization](#data-analysis--visualization)
- [Git](#git)

<!-- markdown-toc end -->


# Central resources

- [SiScLab module description](https://online.rwth-aachen.de/RWTHonline/ee/ui/ca2/app/desktop/#/slc.cm.reg/student/modules/detail/10102/253?$ctx=design=ca;lang=en).

- [Project repository](https://github.com/JuDFTteam/aiida-jutools/tree/SiscLab2020/aiida_jutools/sisc_lab).

- Communication: Slack channel, non-public.

- Examples for the kind of desired statistics & visualizations for the deliverables / project outcome:
  - [Example for deliverable `D1`](https://www.materialscloud.org/explore/pyrene-mofs/statistics): materialscloud/explore > dataset 'pyrene-based MOFs' > statistics.
  - [Example for deliverable `D2`](https://www.materialscloud.org/discover/curated-cofs#mcloudHeader): materialscloud/discover > db MOFs for carbon capture > interacive plot.

- Project libraries:

Note: these are for now copy-pasted into the project repository subfolder, so you don't 

  - [masci_tools/vis/bokeh_plots.py](https://github.com/JuDFTteam/masci-tools/blob/develop/masci_tools/vis/bokeh_plots.py)
  - [masci_tools/vi/plot_methods.py](https://github.com/JuDFTteam/masci-tools/blob/develop/masci_tools/vis/plot_methods.py)

# Datasets

- [Introductory tutorial dataset](https://aiida-tutorials.readthedocs.io/en/latest/pages/2020_Intro_Week/sections/basics.html#importing-data).
- Project databases for development:
  - [whole_database_test.tar.gz (63.8 MB) ](https://iffcloud.fz-juelich.de/s/rFnRozRjpYd2gXW), dec12.
  - [small_database_sisc_project1.tar.gz (23.1 MB) ](https://iffcloud.fz-juelich.de/s/aJXQfMe7EaRYMPR), dec14.
 
- Institute databases, published, to use for production run / evaluation of deliverables:
  - [JuCLS database](https://archive.materialscloud.org/record/2020.139).
    - [Artistic visualization](http://www.aiida.net/graphs/nggallery/image/aiida_work1_1/).
  - [JuDiT database](https://archive.materialscloud.org/record/2020.94).

# AiiDA resources

- [Quantum Mobile virtual machine](https://quantum-mobile.readthedocs.io/en/latest/).

- [AiiDA documentation](https://aiida.readthedocs.io/projects/aiida-core/en/latest/):
  - [How-To Guides](https://aiida.readthedocs.io/projects/aiida-core/en/latest/howto/):
    - [Importing & querying](https://aiida.readthedocs.io/projects/aiida-core/en/latest/howto/data.html)
    - [Explore graph](https://aiida.readthedocs.io/projects/aiida-core/en/latest/howto/exploring.html)
    - [Visualize graph](https://aiida.readthedocs.io/projects/aiida-core/en/latest/howto/visualising_graphs/visualising_graphs.html)
  - [Topics](https://aiida.readthedocs.io/projects/aiida-core/en/latest/topics/index.html):
    - [Provenance](https://aiida.readthedocs.io/projects/aiida-core/en/latest/topics/provenance/index.html)
    - [Data types](https://aiida.readthedocs.io/projects/aiida-core/en/latest/topics/data_types.html)
    - [Advanced querying](https://aiida.readthedocs.io/projects/aiida-core/en/latest/topics/database.html)

- [AiiDA tutorials](https://aiida-tutorials.readthedocs.io/en/latest/):
  - 2020 virtual workshop: 
    - [AiiDA v1.0 cheatsheet](https://aiida-tutorials.readthedocs.io/en/latest/_downloads/5f69c3f9ce1f31f1f959b3da46fdc99d/cheatsheet.pdf).
    - [4.3 Querying for data](https://aiida-tutorials.readthedocs.io/en/latest/pages/2020_Intro_Week/sections/data.html#querying-for-data).


- [materialscloud.org/explore/connect](https://www.materialscloud.org/explore/connect): browse local/remote database graphically via your browser.

# Python

## Core Python

- [Python 3 Module of the Week](https://pymotw.com/3/) Very good series to learn the Python 3 STL.


## Python Ecosystem
### Lists

- [github.com/awesome-python](https://github.com/vinta/awesome-python).

### Serialization

- json
- csv
- pickle
  - [Pickling in Python - The Very Basics](https://ianlondon.github.io/blog/pickling-basics/)
  - [PyMOTW-3 - pickle — Object Serialization](https://pymotw.com/3/pickle/)

## Python development

### General

- [Python package best practices](https://education.molssi.org/python-package-best-practices/index.html)

### Deployment

- [Python Packages tutorial](https://www.pythontutorial.net/python-basics/python-packages/)
- [Python Package tutorial](https://www.programiz.com/python-programming/package)

### Dev tools

#### IDEs
- spyder
- vscode
  - [vscode remote development](https://code.visualstudio.com/docs/remote/ssh), where the remote maybe a virtual machine or an actual physically remote computer. This means your IDE runs on your local system, but can access environments, codes and notebooks lying on the VM / remote machine.
  - [vscode live share](https://visualstudio.microsoft.com/de/services/live-share/) collaborative development.
- pycharm (student license)
  - [pycharm remote development](https://www.jetbrains.com/help/pycharm/creating-project-remotely.html).
  - [pycharm code with me](https://plugins.jetbrains.com/plugin/14896-code-with-me) collaborative development.

# Data science

## General

### Lists

- [Awesome Python Data Science](https://github.com/krzjoa/awesome-python-data-science#data-manipulation)
- [Awesome Pipeline](https://github.com/pditommaso/awesome-pipeline) Has some data science pipeline/workflow tools.
- [awesome-etl](https://github.com/pawl/awesome-etl) "A curated list of notable ETL (extract, transform, load) frameworks, libraries and software."

## Data analysis & visualization

### Visualization Types

- [data-to-viz.com](https://www.data-to-viz.com/): flowchart, leads you to the most appropriate graph for your data. It links to the code (python) to build it and lists common caveats you should avoid.
- [coolinfographics.com](https://coolinfographics.com/dataviz-guides) list of dataviz-guides. more conceptual, less code.
- [Financial Times Visual Vocabulary](https://ft-interactive.github.io/visual-vocabulary/)

# Git

- [Johannes' git_howto for student project work](https://git.rwth-aachen.de/sisc/git_howto).
- [Learn Git in 15 minutes](https://www.youtube.com/watch?v=USjZcfj8yxE).
- [Git cheatsheet](https://i.redd.it/8341g68g1v7y.png).








