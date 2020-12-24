SiScLab2020 Project Meetings
============================

**[master file](../../README.md)**

# Disclaimer

Incomplete minutes of project meetings. Order: newest on top, descending.

# Meeting 2020-12-18, Friday

## Minutes johannes

zhipeng:

presentation

[...]

jens:

structuredata:

how many atoms are there

count number of sites

zhipeng:

number of elements == number of atoms?

jens:

no. for us number of sites == number of atoms.

i don't know if StructureData stores this on its own but don't think so.

johannes:

about zhipeng's element table:

jens:

no, elements is also interesting

[...]

but things like crystal symmetry are not there by default

so would have to store this in node extras, or even calculate first for that.

sijie:

presentation

[...]

stopped writing

[...]

going forward:

students share their work on jutools repository branch SiScLab2020

students prefer to work over christmas holidays

jens will write up tasks to open-end, students free how much to advance till next meeting

next meeting friday, january 8. 

# Meeting 2020-12-11, Friday

## Minutes johannes

Daniel: rough direction to focus on next days:
- look at database
- try to find out what kind of physical properties
- calculations and results
- what kind of results are there
- think of how to get some kind of results of these results
- table on one side structure and other side results
- e.g. to visualize
- should be able to generate this table

jens: mostly fully agree.
- first step would some easy meta analysis of the database.
- how many nodes, how many calcuations, what kinds of workflows
- nice exercise

daniel: ultimate goal
- have some research data with some kind of structure
- the task is once you have data with such nice structure
- explore, look at what you have
- analysis on what

johannes:
- how long did calculations run
- how many calculations did fail

daniel:
- we have on one hand now this database with complex structure
- in order to analyze it you should to cast it into something simple
- in the ends seeing is believing, so plots
- so start playing with that
- and in the second step one could think about how to generalize it (Johannes side-note: ie from python notebooks --> to python functions and classes)
- somebody more data extraction, somebody else more into visualizing, interactive plots, up to you

jens:
- will post git tutorial
- check if everybody has git access
- will write up next step

zhipeng:
- so idea is aiida querybuilder --> pandas --> plots?

jens:
- basically yes.

[...]

*stopped writing minutes for the most part*

[...]

jens:
- start from high metadata perspective
- then get more and more specific

sijie:
- so what is the goal? querying, plotting, what then?

jens:
- got lots of databases like this in the institute
- so idea would be somebody has some databases, fires up notebook, and gets all these nice data analysis / visualization tools for her/his database out of the box from that.
- question is what are you more into - crack down into the data, or do more visualizations
- if you want some analysis. could do some correlation analysis of the data. perforamnce of things. would be quite some options once we have the data and have a basic understanding if you want the project to go into a different direction. but would like to start slow / open-ended.

[...]

*stopped writing*

[...] 

