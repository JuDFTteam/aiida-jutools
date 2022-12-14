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
"""Tools for working with AiiDA metadata / annotations: extras: forms: KkrConstantsVersion."""

import typing as _typing

import masci_tools.util.constants as _masci_constants
from aiida import orm as _orm

from aiida_jutools.plugins.kkr import KkrConstantsVersion as _KkrConstantsVersion, \
    get_runtime_kkr_constants_version as _get_runtime_kkr_constants_version
from ..util import ExtraForm as _ExtraForm


class KkrConstantsVersionExtraForm(_ExtraForm):
    """Extra form for setting the conversion constants versions used by an AiiDA calc / workflow.

    This form serves as an example for the implementation of :py:meth:`~.util.ExtraForm.__init__`. As such, it
    showcases both suggested options to set the form value (subvalues): via the constructor, or via property
    getters / setters. Normally, only one of the ways would be implemented, preferably the former. In that case,
    the constructor arguments would be mandatory.
    """

    def __init__(self,
                 constants_version: _KkrConstantsVersion = None,
                 ANG_BOHR_KKR: float = None,
                 RY_TO_EV_KKR: float = None):
        """Extends :py:meth:`~.util.ExtraForm.__init__`.

        :param constants_version: version of the conversion constants.
        :param ANG_BOHR_KKR: value of the Angstrom to Bohr conversion constant.
        :param RY_TO_EV_KKR: value of the Rydberg to electron Volt conversion constant.
        """
        super().__init__()
        self._key = 'kkr_constants_version'
        self._value = {
            'constants_version': constants_version.name if constants_version else constants_version,
            'ANG_BOHR_KKR': ANG_BOHR_KKR,
            'RY_TO_EV_KKR': RY_TO_EV_KKR
        }

    def load(self,
             entity: _orm.EntityExtras,
             **kwargs):
        """This methods extends :py:meth:`~.util.ExtraForm.load`.

        Additional `kwargs` commands:

        - `silent : bool = True`. False: print warnings.

        :param entity:
        :type entity:
        :param kwargs:
        :type kwargs:
        """
        ent_value = entity.get_extra(self._key, default=None)
        if ent_value and isinstance(ent_value, dict):
            form_subkeys = set(self._value.keys())
            ent_subkeys = set(ent_value.keys())
            for form_subkey in form_subkeys:
                self._value[form_subkey] = ent_value.get(form_subkey, None)

            silent = kwargs.get('silent', None)
            if (
                    silent is not None
                    and not silent
                    and form_subkeys != ent_subkeys
            ):
                print(f"Warning: Entity {entity} has standardized extra '{self.key}', "
                      f"but subvalue keys are non-standard or contain an error report: {ent_subkeys}.")

    def validate(self, **kwargs) -> bool:
        assert isinstance(self.value, dict), "Value must be a dictionary."
        assert all(subval is not None for subval in self.value.values()), "All subvalues must be set."

        valid_versions = [kcv.name for kcv in [_KkrConstantsVersion.NEW,
                                               _KkrConstantsVersion.INTERIM,
                                               _KkrConstantsVersion.OLD]]
        assert self.value['constants_version'] in valid_versions, f"Constants version must be one of {valid_versions}."
        # TODO also validate conversion constants values?
        return True

    def clear(self):
        super().clear()
        for value_key in self._value:
            self._value[value_key] = None

    def get_from_runtime(self,
                         silent: bool = False) -> bool:
        """Determine the form's value from runtime.

        :param silent: False: Print warnings.
        :return: True if determined successfully, False if not.
        """
        constants_version = _get_runtime_kkr_constants_version(silent=silent)
        lookup = {'ANG_BOHR_KKR': constants_version.lookup(constant_name='ANG_BOHR_KKR',
                                                           silent=silent),
                  'RY_TO_EV_KKR': constants_version.lookup(constant_name='RY_TO_EV_KKR',
                                                           silent=silent)}
        runtime = {'ANG_BOHR_KKR': _masci_constants.ANG_BOHR_KKR,
                   'RY_TO_EV_KKR': _masci_constants.RY_TO_EV_KKR}
        # compare with runtime values

        a2b_match = lookup['ANG_BOHR_KKR'] == runtime['ANG_BOHR_KKR']
        r2e_match = lookup['RY_TO_EV_KKR'] == runtime['RY_TO_EV_KKR']
        all_match = a2b_match and r2e_match

        if not all_match:
            constants_version = _KkrConstantsVersion.UNDEFINED
            error_report = f"Warning: Determined runtime version {constants_version}, but tabulated constants " \
                           f"values do not match runtime values. One possible reason is that values in " \
                           f"masci-tools have been updated, and this form is not up to date."
            if not silent:
                print(error_report + " I will mark this as undefined, set the runtime values, "
                                     "and include an error report.")
            self.insert_error_report(error_report=error_report, overwrite=True)

        self.constants_version = constants_version
        self.ANG_BOHR_KKR = runtime['ANG_BOHR_KKR']
        self.RY_TO_EV_KKR = runtime['RY_TO_EV_KKR']

        return all_match

    @property
    def constants_version(self) -> _typing.Optional[_KkrConstantsVersion]:
        """Get/set the version of the conversion constants. Internally stored as string."""
        constants_version = self._value.get('constants_version')
        try:
            return _KkrConstantsVersion[constants_version]
        except KeyError as err:
            return None

    @constants_version.setter
    def constants_version(self, constants_version: _KkrConstantsVersion):
        """Set the version of the conversion constants."""
        self._value['constants_version'] = constants_version.name

    @property
    def ANG_BOHR_KKR(self) -> _typing.Optional[float]:
        """Get/set the value of the Angstrom to Bohr conversion constant."""
        return self._value.get('ANG_BOHR_KKR', None)

    @ANG_BOHR_KKR.setter
    def ANG_BOHR_KKR(self, ANG_BOHR_KKR: float):
        """Get/set the value of the Angstrom to Bohr conversion constant."""
        self._value['ANG_BOHR_KKR'] = ANG_BOHR_KKR

    @property
    def RY_TO_EV_KKR(self) -> _typing.Optional[float]:
        """Get/set the value of the Rydberg to electron Volt conversion constant."""
        return self._value.get('RY_TO_EV_KKR', None)

    @RY_TO_EV_KKR.setter
    def RY_TO_EV_KKR(self, RY_TO_EV_KKR: float):
        """Set the value of the Rydberg to electron Volt conversion constant."""
        self._value['RY_TO_EV_KKR'] = RY_TO_EV_KKR
