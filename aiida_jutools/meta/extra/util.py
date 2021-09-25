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
"""Tools for working with AiiDA metadata / annotations: extras: util."""

import abc
import abc as _abc
import typing as _typing

import masci_tools.util.python_util as _masci_python_util
from aiida import orm as _orm

import aiida_jutools as _jutools


class ExtraForm(_abc.ABC):
    """Abstract base class for forms for standardized extras.

    The extra's key and value are write-protected. This is the purpose of the form. Setting the value (or its
    subvalues) can be implemented in two ways: a) via (recommended: mandatory, validated) constructor arguments,
    b) via Python property getters & setters. In the latter case, if the value is not a nested data structure,
    then a 'value' setter may be defined instead.

    Each subclass must implement the abstract mtehods :py:meth:`~.clear`, :py:meth:`~.load` and :py:meth:`~.validate`.

    Note: Currently does not support versioning, does not rely on external schema & form files. For design
    considerations for implementing the latter, see the proposal "Proposal for an AiiDA extras metadata
    standard & implementation", URL: https://iffmd.fz-juelich.de/8JZ8aLxyQSWD2byc5GKX2Q (retrieved 2021-09-20).
    This standardization approach only makes sense if versioning is implemented, especially wrt single-value extras.
    """

    def __init__(self):
        self._key = None
        self._value = None
        self._error_report_key = "ERROR_REPORT"

    @property
    def key(self) -> str:
        """The extra's key. Read-only."""
        return self._key

    @property
    def value(self) -> _typing.Any:
        """The extra's value. Read-only. Set (sub)values via individual form methods."""
        return self._value

    @abc.abstractmethod
    def load(self,
             entity: _orm.EntityExtrasMixin,
             **kwargs):
        """Load an entity's extra's value into this form.

        :param entity: The entity from which to load the extra.
        :param kwargs: Specific form arguments.
        """
        pass

    @abc.abstractmethod
    def validate(self,
                 **kwargs) -> bool:
        """Validate value content. Return True for valid, False for invalid.

        Form may except if validation fails. This is up to the form's implementation.

        :param kwargs: Specific form arguments.
        """
        pass

    @abc.abstractmethod
    def clear(self):
        """Reset form (clear all data from its value, leaving only the value's skeleton.) """
        # regarding clear error report: either call super().clear() in implementing method, or do it yourself.
        self._value.pop(self._error_report_key, None)

    def insert(self,
               entity: _orm.EntityExtrasMixin,
               validate: bool = False,
               overwrite: bool = False,
               **kwargs) -> bool:
        """Insert this form's extra into a given entity's extras.

        :param entity: Entity into whose extras to insert.
        :param validate: True: validate before insertion. Validation may except.
        :param overwrite: False: Do not insert if already set.
        :param kwargs: Specific form arguments.
        :return: True if inserted, False if not.
        """
        valid = self.validate(**kwargs) if validate else True

        form_cls = _jutools.meta.ExtraFormFactory(form_name='kkr_constants_version')
        form = form_cls()
        form.load(entity=entity)

        if valid and (overwrite or not form.value):
            entity.set_extra(self.key, self._value)
            return True
        return False

    def insert_error_report(self, error_report: str,
                            append_timestamp: bool = True,
                            overwrite: bool = False) -> bool:
        """Insert an error report into the form's value.

        Will append a timestamp to the error report.

        :param error_report: the error report.
        :param append_timestamp: Append timestamp to error report string.
        :param overwrite: False: Do not insert if already set.
        :return: True if inserted, False if not.
        """
        has_report = self._value.get(self._error_report_key, None)
        if not has_report or overwrite:
            error_report = error_report + f" Timestamp: {_masci_python_util.now()}." \
                if append_timestamp else error_report
            self._value[self._error_report_key] = error_report
            return True
        return False


def ExtraFormFactory(form_name: str = None) -> _typing.Optional[_typing.Type[ExtraForm]]:
    """Factory for standardized extra forms.

    Extra forms allow to use commonly used extras in a standardized format. This help in realizing data
    FAIR-ness across different AiiDA databases.

    Available extra forms:

    - `'kkr_constants_version'`: Extra documenting conversion constants versions used by aiida-kkr calc / workflow.

    Note: This factory is useful for use in runtime (interpreter, notebook). For use in library, direct form import
    from :py:mod:`~.forms` should be preferred.

    :param form_name: name of the extra form. Usually identical to name of the extra. None prints a list.
    :return: The class of the ExtraForm for instantiation.
    """
    from aiida_jutools.meta.extra.forms import KkrConstantsVersionExtraForm

    forms = {
        'kkr_constants_version': KkrConstantsVersionExtraForm
    }

    if form_name not in forms:
        print(f"Warning: No {ExtraForm.__name__} with name '{form_name}' exists. Available "
              f"forms : {list(forms.keys())}.")
        return None
    return forms[form_name]
