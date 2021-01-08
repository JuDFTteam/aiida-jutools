SiScLab2020 Project Meetings
============================

**[master file](../../README.md)**

# Disclaimer

Incomplete minutes of project meetings. Order: newest on top, descending.

# Meeting 2021-01-08, Friday

## Minutes johannes

jens:
- please tell us how much time spent
- split like two do D1, two do D2? Answer: yes.
- show us what you did.
- then will show results on small db.

how much time spent:
- zhipeng: last time ~3-4 days, this time also 3-4 days, so ca.
- sijie: this time 2 days for coding, 2 days for reviewing discussing
- anna: last time 4-5 days, this time about 4 days
- miao: also more or less

jens:
- D1 first half:
  - nice, integrate into the helpers now like the others did, follow the style guides
  - showing static version with browser with just the results.


jens:
- structure barchart:
  - have to think of how to adjust the axis and the binning to the data that you have
  - like the hover tool that you have but doesn't make sense to display the
    uuids and formula at once, e.g. cut uuids off at some point.
    - zhipeng: how exactly?
    - jens: set of formulas. if you just plot the statistical view like a
      histogram it doesn't make sense to plot all uuids, or maybe cut it off at
      around 20 uuids.

  - since interactive it's still fine
  - order by the periodic table not by insertion
  - liked what you did in that part
- then comes some formula process states plot which doesn't make much sense
- last plots failed in my case, was too large

zhipeng:
- yes had smaller dataset
- will add scatterplot for formula. how to sort them? nodes with formula, uuid. what should x and y axis be.
  - jens: number of sites, number of kinds, but could get much overlap. let's
    say for a large dataset, don't have a good idea. yet. more important for the
    second tool. there they will spread out more there you can see ... maybe for
    first tool there's further information for structureplot like spacegroup.
    - zhipeng: gave us some plots from materialscloud.
    - jens: sent many links.
    - johannes: see repo README > resources.
    - zhipeng: there could list [...] is it possbile to do this with the bulk visualization?
    - jens: my view this list is not very helpful.
    - zhipeng: was also my concern for structure data. there are so many nodes.
    - jens: perhaps a list of structure thumbnails with nodes and connection
    - jens: but what i want to know is number of atoms, that's important to know. i
      what to get a felling of what kinds of materials are in there. so number
      of atoms, types of elements.
- processnode: dunno how to plot it. shows us the processnode information. cause
  of all them are finished so all get value 1. and x axis is the id. how to do that?
  - jens: in this case maybe barchart. would just like to see statistical view how many failed how many succeeded.
- zhipeng: workflow barchart:
  - did something wrong b cause many failed?
  - jens: could be in that dataset. when workflow fails, not successful is just
    = worst case. but there are more states. like finished but didn't do what it
    was supposed to do, then have different exitdcode. would nice to have a
    barchart to see all these exitcodes in detail (how many with what). with all
    of the messages as dictionary. for example all of the relaxations failed
    cause had problems with this feature.
  - zhipeng: yes had printed this in the list. all the exitcodes were 0.
  - jens: could be in your database. will look in his database.
- data provenance:
  - zhipeng: outgoing, incoming
  - jens: stopped for his db after 20min cause took too long. don't loop over
    all nodes. instead get all links directly and from that get the info. maybe
    that's faster. anyway nice info. tells us now there are lots of structure
    data but not very connected. would be nice to have a barchart and no
    simulations for them. for publication last column should be zero as check,
    because everything should be connected. how shallow is the tree, how deep,
    how many orphaned nodes lying around.

sijie:
- wrote helper module for first part, three functions, `get_structure_workflow`,
- many NaN values, problem for scatter plotting stuff. none of the entries have both values at the same time.
- Jens: yes also failed with my database. you can also assume they have the same
  attribute type if it's the same workflow subtype. but you're right the database you got was too small for that.
- Jens: like how you designed the helper, separated the data collection from the
  plot and so on. for the others, i today uploaded a short styyle guide how to
  wrote proper python code. your code sijie is already very good. some of the
  other codes should get up to that standard.
- sijie: if iuse total energy and sdisatance charge have 14 nodes for plotting.
- jens: problem already know can't give you a million nodes db, so have to give
  cherrypick workflows for you st that you get a probper dataset and takes some
  time. then run it for a proper dataset and measure the performance. but that
  wont be possible to run on quantum mobile.
- johanne: with as much code as possible inside the modules,not the notebook.
  just interfaces there.

sijie:
- next part: structure project.
- visualization: could be better.
- jens: bokeh hard work to make plots nice at the end. just get some basic viz at the start
- sijie: xdata, many data were redundant (repeated) ie duplicates.
- jens: yes, could be if exactly the same, like test runs with same result.
  could prune them out. but have to be careful for that, for a real database,
  could throw results away that we don't want to throw away. esp. when most
  results lie in a very small region.
- jens: sijie: can try to work on larger database. in meantime, will try to
  handpick some nice workflows.


jens:
- will do packaging, write unit tests later (supervisors)
- will calculate how many hours spent, how many left
- then will send remaining tasks based on that, and you're done, can write the report

jens:
- meet in 2 weeks
- have a rough final version of the notebook by then (ie code)
- then can just export it to html file. then have also large part for
  presentation and report.

# Meeting 2020-12-18, Friday

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
