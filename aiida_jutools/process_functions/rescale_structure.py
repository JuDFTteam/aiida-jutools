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
"""CalcFunction for rescaling StructureData."""

import aiida.engine as _aiida_engine
import aiida.orm as _orm


@_aiida_engine.calcfunction
def rescale_structure(input_structure: _orm.StructureData,
                      scale_factor: _orm.Float) -> _orm.StructureData:
    """
    Rescales a crystal structure. Keeps the provenance in the database.

    :param input_structure, a StructureData node (pk, or uuid)
    :param scale_factor, float scaling factor for the cell
    :return: New StrcutureData node with rescalled structure, which is linked to input Structure
              and None if inp_structure was not a StructureData

    copied and modified from aiida_fleur.tools.StructureData_util
    """

    the_ase = input_structure.get_ase()
    new_ase = the_ase.copy()
    new_ase.set_cell(the_ase.get_cell() * float(scale_factor), scale_atoms=True)
    return _orm.StructureData(ase=new_ase)
