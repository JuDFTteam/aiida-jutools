Here a sumup of common good practice when programming in python and vizualising things:


Pyhton:
#######

We program mostly under the google python style guide, which differs a bit from pep8
see: https://google.github.io/styleguide/pyguide.html
Below you find points which are important for you.

Naming conventions:
-------------------
no spaces in any names

- Class names: CamelCase
- file names: all_lower_case_with_underscores
- functions names: all_lower_case_with_underscores

Imports:
--------
All imports should be at the top level of a module/file/notebook.
Things should be imported once per module/file/notebook

Only in some circumstances do imports go at the top (first lines) of a functions

Documentation:
--------------

In code documentation is very important.
It helps you and others to understand your code.
All functions, classes and modules come with a """ docstring at the top, 
which will be utilized by a lot of code tools.
for exmaple in a jupyter notebook this will be displayed via

?object_name
or object_name?

Tools:
------
We will not go into this and we will also not enforce you to use it in this project.

There are a lot of tools, who out format code and check for it to enforce it.
see for example pytlint, yapf, pre-commit,etc.

Plotting:
#########

- All plots need proper axis labels.

