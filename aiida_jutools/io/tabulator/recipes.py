# -*- coding: utf-8 -*-
###############################################################################
# Copyright (c), Forschungszentrum Jülich GmbH, IAS-1/PGI-1, Germany.         #
#                All rights reserved.                                         #
# This file is part of the aiida-jutools package.                             #
# (AiiDA JuDFT tools)                                                         #
#                                                                             #
# The code is hosted on GitHub at https://github.com/judftteam/aiida-jutools. #
# For further information on the license, see the LICENSE.txt file.           #
# For further information please visit http://judft.de/.                      #
#                                                                             #
###############################################################################
"""Tools for working with AiiDA IO: tabulation: recipes."""

import abc as _abc
import typing as _typing

import numpy as _np
from masci_tools.util import python_util as _masci_python_util

import aiida_jutools as _jutools


class Recipe(_abc.ABC):
    def __init__(self,
                 exclude_list: dict = None,
                 include_list: dict = None,
                 transformer: _jutools.io.tabulator.Transformer = None,
                 **kwargs):
        """Initialize a tabulator object.

        The attributes :py:attr:`~.include_list` and :py:attr:`~.exclude_list` control whic properties
        are to be tabulated. They may be set in a derived class definition, or at runtime on an instance.

        Subclasses define the nature of the objects to be tabulated by making assumptions on their
        property structure. That way, if both include and exclude list are empty, by default the 'complete'
        set of properties of the objects will be tabulated, where the subclass defines the notion of 'complete'.

        If neither exclude nor include list is given, the full set of properties according to implementation
        will be tabulated.

        Format of the include and exclude lists:



        :param exclude_list: Optional list of properties to exclude. May be set later.
        :param include_list: Optional list of properties to include. May be set later.
        :param transform: Specifies special transformations for certain properties for tabulation.
        :param kwargs: Additional keyword arguments for subclasses.
        """
        # note: for the in/ex lists, using the public setter here,
        # to trigger conversion
        self._exclude_list = exclude_list if exclude_list else {}
        self._include_list = include_list if include_list else {}
        self.transformer = transformer

        self._scalar_types = (bool, int, float, str, complex)
        self._nonscalar_types = (list, tuple, set, _np.ndarray)
        self._nested_types = tuple([dict])
        self._simple_types = tuple(
            set(self._scalar_types).union(
                set(self._nonscalar_types))
        )

    @property
    def exclude_list(self) -> dict:
        return self._exclude_list

    @exclude_list.setter
    def exclude_list(self, exclude_list: _typing.Union[dict, list]):
        self._exclude_list = exclude_list
        if isinstance(exclude_list, dict):
            self._to_keypaths()

    @property
    def include_list(self) -> dict:
        return self._include_list

    @include_list.setter
    def include_list(self, include_list: _typing.Union[dict, list]):
        self._include_list = include_list
        if isinstance(include_list, dict):
            self._to_keypaths()

    def _to_keypaths(self):
        """Generate paths from a possibly nested dictionary.

        This method can be used for handling include lists, exclude lists, and when writing
        new :py:class:`~Transformer` transform methods.

        List of paths to each value within the dict as tuples (path, value).

        convert from with-List to with-None format for convert to keypaths

        convert to keypaths (upper: done inside this one anyway)
        """

        def _to_keypaths_recursive(sub_dict: dict,
                                   path: list = []):
            paths = []
            for k, v in sub_dict.items():
                if isinstance(v, dict):
                    paths += _to_keypaths_recursive(v, path + [k])
                paths.append((path + [k], v))
            return paths

        for in_or_ex, a_dict in {'in': self._include_list,
                                 'out': self._exclude_list}.items():

            # precondition: not already keypaths format
            is_list = isinstance(a_dict, list)
            is_all_lists = is_list and all(isinstance(path, list) for path in a_dict)
            if is_all_lists:
                continue

            # if empty, convert to empty list. if not empty, convert to keypaths
            if not a_dict:
                keypaths = []
            else:
                # convert from include list with-list format with-none format:
                # same-level subkeys mentioned as list [k1,k2] -> dict {k1:None, k2:None}.
                _a_dict = _masci_python_util.modify_dict(a_dict=a_dict,
                                                         transform_value=lambda v: {k: None for k in v}
                                                         if isinstance(v, list) else v,
                                                         to_level=99)

                keypaths = _to_keypaths_recursive(sub_dict=_a_dict,
                                                  path=[])
                # the result consists of sets of subpaths. For each subset, there is
                # an additianal entry where the value contains the whole subdict from
                # which the paths were generated. We are not interested in those duplicate
                # entries, so remove them.
                keypaths = [tup for tup in keypaths if not isinstance(tup[1], dict)]

                # now list should be like [(path1, None), (path2, None), ...],
                # or at least of type _typing.List[_typing.Tuple[list, _typing.Any]].
                # check that. if not, something is wrong.
                # otherwise, just return the paths.
                if all(tup[1] is None for tup in keypaths):
                    keypaths = [tup[0] for tup in keypaths]

            # postcondition: keypaths format
            is_list = isinstance(keypaths, list)
            is_all_lists = is_list and all(isinstance(path, list) for path in keypaths)
            if not is_all_lists:
                raise TypeError(f"Could not generate keypaths of required type {_typing.List[list]} "
                                f"from {in_or_ex}clude list. Either specified list in wrong format "
                                f"(see class init docstring for examples), or list generated from "
                                f"autolist stumbled over untreated special case for some unpacked "
                                f"property.")

            if in_or_ex == 'in':
                self._include_list = keypaths
            elif in_or_ex == 'out':
                self._exclude_list = keypaths
