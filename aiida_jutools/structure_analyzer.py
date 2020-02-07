#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Collection of functions to extract, process and output symmetry-related information.
Currently works for cif files.
To be extended for various data formats: aiida structure, POSCAR, ...
"""

# standard imports
import os
import sys, getopt
import math
import numpy
import string
from datetime import datetime
from fractions import gcd
from sympy import nsimplify
from pprint import pprint

# aiida imports
from aiida.plugins import DataFactory

# CIF files
import pymatgen
from CifFile import ReadCif
import spglib

# local imports
from terminal_colors import *
# CSQ color for sequence
# CDB color for structure database
# CRC color for recipe
# CWF color for workflow
# CC1 color for communication / important statements 1
# CC2 color for communication / important statements 2
# CWR color for warning messages
# CER color for error messages
# COK color for OK messages
# CIN color for info messages
from ptable import ptable

__copyright__ = (u"Copyright (c), 2019-2020, Forschungszentrum Jülich GmbH, "
                 "IAS-1/PGI-1, Germany. All rights reserved.")
__license__ = "MIT license, see LICENSE.txt file"
__version__ = "0.1"
__contributors__ = u"Roman Kováčik"

# aiida DataFactory 
StructureData = DataFactory('structure')
DictData = DataFactory('dict')
CifData = DataFactory('cif')

# tolerance for number of decimal places
roundtol = 8 # long
roundtom = 6 # medium
roundtos = 4 # short

"""
letter case swap for sake of Wyckoff letter sorting
SG 47 has 27 different Wyckoff positions abc...xyzA, only one with uppercase 'A'
in standard sorting uppercase precedes lowercase, so case is swapped for sorting
"""
def caseswap(x):
    return ''.join([c.lower() if 'A' <= c <= 'Z' else c.upper() for c in x])

"""
compare symmetry operations for 2 hall numbers
SG 68 will likely cause trouble, because spglib (due to legacy reasons?)
has degenerate hall_numbers with respct to the symmetry operations
"""
def compare_hall_numbers(sg,cifspg):

    # list of first hall numbers corresponding to space group numbers (index)
    # 531 (SG 231), of course, does not actually exist
    spacegroup_to_hall_number = [
        1,   2,   3,   6,   9,  18,  21,  30,  39,  57,
        60,  63,  72,  81,  90, 108, 109, 112, 115, 116,
        119, 122, 123, 124, 125, 128, 134, 137, 143, 149,
        155, 161, 164, 170, 173, 176, 182, 185, 191, 197,
        203, 209, 212, 215, 218, 221, 227, 228, 230, 233,
        239, 245, 251, 257, 263, 266, 269, 275, 278, 284,
        290, 292, 298, 304, 310, 313, 316, 322, 334, 335,
        337, 338, 341, 343, 349, 350, 351, 352, 353, 354,
        355, 356, 357, 358, 359, 361, 363, 364, 366, 367,
        368, 369, 370, 371, 372, 373, 374, 375, 376, 377,
        378, 379, 380, 381, 382, 383, 384, 385, 386, 387,
        388, 389, 390, 391, 392, 393, 394, 395, 396, 397,
        398, 399, 400, 401, 402, 404, 406, 407, 408, 410,
        412, 413, 414, 416, 418, 419, 420, 422, 424, 425,
        426, 428, 430, 431, 432, 433, 435, 436, 438, 439,
        440, 441, 442, 443, 444, 446, 447, 448, 449, 450,
        452, 454, 455, 456, 457, 458, 460, 462, 463, 464,
        465, 466, 467, 468, 469, 470, 471, 472, 473, 474,
        475, 476, 477, 478, 479, 480, 481, 482, 483, 484,
        485, 486, 487, 488, 489, 490, 491, 492, 493, 494,
        495, 497, 498, 500, 501, 502, 503, 504, 505, 506,
        507, 508, 509, 510, 511, 512, 513, 514, 515, 516,
        517, 518, 520, 521, 523, 524, 525, 527, 529, 530, 531
    ]

    # if SG number is known (sg != 0), only corresponding hall numbers are examined
    # otherwise all hall numbers are examined
    if sg != 0:
        hall_num0 = spacegroup_to_hall_number[sg-1]
        hall_num1 = spacegroup_to_hall_number[sg]
    else:
        hall_num0 = spacegroup_to_hall_number[0]
        hall_num1 = spacegroup_to_hall_number[230]

    match = []

    # symmetry operations are compared and list of matching hall numbers is returned
    # only one match should be found (except SG 68), this has to be fixed
    for hall_num in range(hall_num0, hall_num1):

        symops = spglib.get_symmetry_from_database(hall_num)

        rot1 = symops['rotations'] ; shaper1 = numpy.shape(rot1)
        rot2 = cifspg['rotations'] ; shaper2 = numpy.shape(rot2)

        trans1 = symops['translations'] ; shapet1 = numpy.shape(trans1)
        trans2 = cifspg['translations'] ; shapet2 = numpy.shape(trans2)

        if (shaper1[0] != shaper2[0]) or (shapet1[0] != shapet2[0]) or (shaper1[0] != shapet1[0]):
            continue

        matched = [0] * shaper1[0]

        for idx1 in range(0,len(trans1)):
            idxmatch = False
            for idx2 in range(0,len(trans2)):
                if (matched[idx2] == 0) and numpy.allclose(rot1[idx1],rot2[idx2]) and numpy.allclose(trans1[idx1],trans2[idx2]):
                    matched[idx2] = 1
                    idxmatch = True
            if idxmatch == False:
                break
        if all( x == 1 for x in matched ):
           match.append(hall_num)

    return(match)

"""
check whether all entries match, i.e. len(myset) == 1; if not print warning and set alright = False
"""
def check_setdif(mydict,key0,key1,strict,setdiflist):

    global prompt

    myset = set(mydict[key0][key1].values())
    if len(myset) != 1:
        if strict:
            print(prompt+CWR+' !!! inconsistent info['+"'"+key0+"'"+']['+"'"+key1+"']" + CEND +
                  " : [ "+', '.join(str("'"+k+"' = ")+str(CWR+v+CEND) for k,v in sorted(mydict[key0][key1].items()))+' ]'+CEND)
            setdiflist[0] = False
        else:
            setdiflist[1] = True
            setdiflist[2] += " ; '"+key1+"' : [ "+', '.join(str("'"+k+"' = ")+str("'"+v+"'") for k,v in sorted(mydict[key0][key1].items()))+' ]'

"""
check consistency between original cif file, aiida cif data and spglib output
"""
def cif_check(cif_read,cif_data,spgl):

    global prompt
    global verbose

    info0 = [
        'sg',
        'str',
    ]
    info1 = {
        'number' : 'sg',
        'hall_number' : 'sg',
        'choice' : 'sg',
        'international_full' : 'sg',
        'international_short' : 'sg',
        'hall_symbol' : 'sg',
        'schoenflies' : 'sg',
        'chemical_formula_sum' : 'str',
        'cell_formula_units_z' : 'str',
    }
    info2 = [
        'read',
        'data',
        'spgd',
        'spgo',
    ]

    info = {}

    hallnum_spgd = spgl['hall_number']
    hallnum_spgo  = 0
    sg_spgd = spglib.get_spacegroup_type(hallnum_spgd)
    sg_spgo  = {}

    pr_4s = 'checking Hall number: '
    match = compare_hall_numbers(spgl['number'], spgl)
    if match == []:
        match2 = compare_hall_numbers(0, spgl)
        pr_m4s2 = pr_4s + str(match2)
    pr_m4s = pr_4s + str(match)
    if len(match) == 1:
        hallnum_spgo = match[0]
        sg_spgo = spglib.get_spacegroup_type(match[0])
        info['check'] = 1
        info['message'] = 'OK ... ' + pr_m4s + ' one matching Hall number found'
    else:
        if len(match) > 1:
            info['check'] = 2
            info['message'] = '\n' + CWR + ' !!! something went wrong ' + pr_m4s + ' more than one >> Hall number << matching' + CEND
        else:
            info['check'] = 0
            info['message'] = '\n' + CWR + ' !!! something went wrong ' + pr_m4s2 + CEND
        if len(match2) == 1:
            info['check'] = -1
            info['message'] = 'Maybe OK ... ' + pr_m4s2 + ' one matching Hall number in different space group found'

    alright = True
    difdet = False
    prdifdet = ''
    setdiflist = [True, False, CWR+' !!! original and default choices differ'+CEND]
    for key0 in info0:
        info[key0] = {}
        for key1,val1 in info1.items():
            if key0 == val1 and info[key0].get(key1) == None:
                info[key0][key1] = {}
            if key0 == 'sg' and key1 == 'number':
                info[key0][key1]['read'] = str(cif_read.get(cif_read.keys()[0]).get('_symmetry_Int_Tables_number'))
                info[key0][key1]['data'] = str(cif_data.get_spacegroup_numbers()[0])
                info[key0][key1]['spgo'] = str(sg_spgo[key1])
                info[key0][key1]['spgd'] = str(sg_spgd[key1])
                check_setdif(info,key0,key1,True,setdiflist)
            if key0 == 'sg' and key1 == 'hall_number':
                info[key0][key1]['spgo'] = str(hallnum_spgo)
                info[key0][key1]['spgd'] = str(hallnum_spgd)
                check_setdif(info,key0,key1,False,setdiflist)
            if key0 == 'sg' and key1 == 'choice':
                info[key0][key1]['spgo'] = str(sg_spgo[key1])
                info[key0][key1]['spgd'] = str(sg_spgd[key1])
                check_setdif(info,key0,key1,False,setdiflist)
            if key0 == 'sg' and key1 == 'international_full':
                info[key0][key1]['read'] = cif_read.get(cif_read.keys()[0]).get('_symmetry_space_group_name_h-m')
                info[key0][key1]['spgo'] = str(sg_spgo[key1])
                info[key0][key1]['spgd'] = str(sg_spgd[key1])
            if key0 == 'sg' and key1 == 'international_short':
                info[key0][key1]['spgo'] = str(sg_spgo[key1])
                info[key0][key1]['spgd'] = str(sg_spgd[key1])
            if key0 == 'sg' and key1 == 'hall_symbol':
                info[key0][key1]['spgo'] = str(sg_spgo[key1])
                info[key0][key1]['spgd'] = str(sg_spgd[key1])
            if key0 == 'sg' and key1 == 'schoenflies':
                info[key0][key1]['spgo'] = str(sg_spgo[key1])
                info[key0][key1]['spgd'] = str(sg_spgd[key1])
            if key0 == 'sg' and key1 == 'pointgroup_schoenflies':
                info[key0][key1]['spgo'] = str(sg_spgo[key1])
                info[key0][key1]['spgd'] = str(sg_spgd[key1])
            if key0 == 'sg' and key1 == 'pointgroup_international':
                info[key0][key1]['spgo'] = str(sg_spgo[key1])
                info[key0][key1]['spgd'] = str(sg_spgd[key1])
            if key0 == 'str' and key1 == 'chemical_formula_sum':
                info[key0][key1]['read'] = cif_read.get(cif_read.keys()[0]).get('_chemical_formula_sum')
                info[key0][key1]['data'] = cif_data.attributes['formulae'][0]
            if key0 == 'str' and key1 == 'cell_formula_units_z':
                info[key0][key1]['read'] = cif_read.get(cif_read.keys()[0]).get('_cell_formula_units_z')
            # this list could be extended if needed
    if setdiflist[1]:
        print(prompt + '       ' + pr_4s + setdiflist[2])
    if verbose or len(match) != 1:
        print(prompt + '\n' + prompt + '       ' + pr_4s + 'INFO' + '\n' + prompt + '\n' + prompt)
        for key0 in info0:
            if key0 in info:
                for key1 in info1:
                    if key1 in info[key0]:
                        for key2 in info2:
                            if key2 in info[key0][key1]:
                                prff = "%s['%s']['%s']['%s']"+' '*(10-len(key2))+" = %-40s"
                                print(prompt + '       ' + pr_4s + prff % (' : info',key0,key1,key2,info[key0][key1][key2]))
                        if info[key0][key1] != {}:
                            print(prompt)
                if info[key0] != {}:
                    print(prompt)

    return info

"""
constructs dictionary with symmetry related information
currently works only for 3-dimensional lattice in 3-dimensional space: space groups [3,3]
extension could be written in the future for:
layer groups [3,2]
rod groups [3,1]
"""
def construct_symmetry_dict(dim,sgn,sgs,pgs):

    crystal_system = None
    crystal_family = None
    lattice_centering = None
    point_group_center_of_inversion = None
    point_group_chiral = None
    point_group_polar = None

    sdict = {}
    sdict['[system,lattice] dimension'] = dim

    if dim == [3,3]:

        sgn2cs = [ [1,2,'triclinic'], [3,15,'monoclinic'], [16,74,'orthorhombic'], [75,142,'tetragonal'], [143,167,'trigonal'], [168,194,'hexagonal'], [195,230,'cubic'] ]
        sgn2cf = [ [1,2,'a'], [3,15,'m'], [16,74,'o'], [75,142,'t'], [143,194,'h'], [195,230,'c'] ]
        sgn2ic = [ [2,2], [10,15], [47,74], [83,88], [123,142], [147,148], [162,167], [175,176], [191,194], [200,206], [221,230] ]
        ltt = [True,True] ; ltf = [True,False] ; lft = [False,True] ; lff = [False,False]
        pgs2pgpc = {
            "1" : ltt, "2" : ltt, "3" : ltt, "4" : ltt, "6" : ltt,
            "m" : ltf, "mm2" : ltf, "3m" : ltf, "4mm" : ltf, "6mm" : ltf,
            "222" : lft, "422" : lft, "622" : lft, "32" : lft, "23" : lft, "432" : lft,
            "-1" : lff, "-4" : lff, "-3" : lff, "-6" : lff,
            "2/m" : lff, "mmm" : lff, "4/m" : lff, "-42m" : lff, "4/mmm" : lff, "-3m" : lff, "6/m" : lff, "-62m" : lff, "6/mmm" : lff, "m-3" : lff, "-43m" : lff,  "m-3m" : lff
        }

        if isinstance(sgn, int):
            point_group_center_of_inversion = False
            for sr in sgn2cs:
                if sgn >= sr[0] and sgn <= sr[1]:
                    crystal_system = sr[2]
            for sr in sgn2cf:
                if sgn >= sr[0] and sgn <= sr[1]:
                    crystal_family = sr[2]
            for sr in sgn2ic:
                if sgn >= sr[0] and sgn <= sr[1]:
                    point_group_center_of_inversion = True

        lattice_centering = str(sgs[0])
        if crystal_family == 'm' and lattice_centering != 'P':
            lattice_centering = 'S'
        if crystal_family == 'o' and ( lattice_centering == 'A' or lattice_centering == 'B' or lattice_centering == 'C' ):
            lattice_centering = 'S'

        point_group_polar = pgs2pgpc.get(pgs)[0]
        point_group_chiral = pgs2pgpc.get(pgs)[1]

        sdict['crystal_family'] = crystal_family
        sdict['crystal_system'] = crystal_system
        sdict['lattice_centering'] = lattice_centering
        sdict['point_group'] = str(pgs)
        sdict['point_group_center_of_inversion'] = point_group_center_of_inversion
        sdict['point_group_polar'] = point_group_polar
        sdict['point_group_chiral'] = point_group_chiral
        sdict['space_group_number'] = int(sgn)
        sdict['space_group_symbol'] = str(sgs)
        
    return sdict

"""
Wyckoff representative positions are determined
and a lot of information is generated in xproto and labels dictionaries
pymg_dict and spgl are expected to be sorted, i.e. to form groups of equivalent sites w.r.t. wyckoff positions
function currently works only for conventional unit cells
"""
def determine_wyckrep(pymg_dict,spgl,symmetry_dict):

    global roundtol
    global roundtom
    global roundtos

    # prototype and labels dictionary
    xproto = {}
    labels = {}

    # consistency check for number of sites
    nsites_pymg = len(pymg_dict['sites'])
    nsites_spgl = len(spgl['wyckoffs'])
    if nsites_pymg != nsites_spgl:
        xproto["status"] = "ERROR"
        xproto["message"] = "nsites_pymg (" + str(nsites_pymg) + ") != nsites_spgl (" + str(nsites_spgl) + ")"
        return (xproto,labels)
    else:
        nsites_tot = nsites_pymg

    # list of representative site indices, its length should be equal to number of individual wyckoff positions
    repidxlist = sorted(list(set(spgl['equivalent_atoms'])))
    eqdict = {} # dictionary for representative sites
    # loop: representative sites
    # grouping them by representative label, setting up various information about occupation, labeling, ...
    for idx,repidx in enumerate(repidxlist):
        eqdict[repidx] = {}
        eqdict[repidx]['idx_eq'] = idx # index of representative site in eqdict
        eqdict[repidx]['sites_label'] = pymg_dict['sites'][repidx]['label'] # site labels, copy from pymg_dict
        eqdict[repidx]['sites_species'] = pymg_dict['sites'][repidx]['species'] # site species, copy from pymg_dict
        eqdict[repidx]['sites_conc'] = round(sum([x['occu'] for x in eqdict[repidx]['sites_species']]),roundtol) # sum of occupations over species at representative site
        eqdict[repidx]['mult_nom'] = (spgl['equivalent_atoms'] == repidx).sum() # nominal multiplicity = wyckoff multiplicity
        eqdict[repidx]['mult_prox'] = proximate_multiplicity(eqdict[repidx]['mult_nom'],eqdict[repidx]['sites_conc']) # proximate multiplicity
        eqdict[repidx]['list_eleocc'] = [[eleocc["element"],eleocc["occu"]] for eleocc in eqdict[repidx]['sites_species']] # list of [element, occupation] pairs to be used in wyck
        eqdict[repidx]['list_eleocc'] = sorted(eqdict[repidx]['list_eleocc'])
        # setting up representative label eqdict_label
        if eqdict[repidx]['mult_prox'] > 0: # reasonably high (> 0) occupancy of the Wyckoff positions
            mult_max_eq = max([list(spgl['equivalent_atoms']).count(x) for x in repidxlist if pymg_dict['sites'][x]['label'] == eqdict[repidx]['sites_label']])
            mult_ind = [proximate_multiplicity(mult_max_eq,x[1]) for x in eqdict[repidx]['list_eleocc']] # individual proximate multiplicity
            if all(mi == 0 for mi in mult_ind):
                eqdict_label = ' '.join([x[0]+':'+str(x[1]) for x in eqdict[repidx]['list_eleocc']])
            else:
                if sum(mi > 0 for mi in mult_ind) == 1:
                    eqdict_label = ' '.join([eqdict[repidx]['list_eleocc'][y][0] for y,x in enumerate(mult_ind) if x > 0])
                else:
                    eqdict_label = ' '.join([eqdict[repidx]['list_eleocc'][y][0]+':'+str(eqdict[repidx]['list_eleocc'][y][1]) for y,x in enumerate(mult_ind) if x > 0])
        else:
            eqdict_label = 'X'
        # assigning group index to representative site, based on eqdict_label
        label_sum_not_none = sum(x != None for x in list(set([x.get('label') for x in eqdict.values()])))
        label_match = list(set([x.get('idx_grp') for x in eqdict.values() if x.get('label') == eqdict_label ]))
        if label_match == []:
            eqdict[repidx]['idx_grp'] = label_sum_not_none
        else:
            eqdict[repidx]['idx_grp'] = label_match[0]
        eqdict[repidx]['label'] = eqdict_label
    # end loop: representative sites

    # initialize wyck list
    ngrp = len(set([x['idx_grp'] for x in eqdict.values()]))
    wyck = [None]*ngrp
    for idx in range(0,ngrp):
        wyck[idx] = ['', [[0,0],[0,0,0]], [], '']
        
    # using spgl positions because they depend on a particular setting/choice
    rspg = [x[1] for x in sorted(zip(spgl['std_mapping_to_primitive'].tolist(),spgl['std_positions'].tolist()))[:]]

    # loop: over all expanded positions
    for idx,site in enumerate(pymg_dict['sites']):
        equa = spgl['equivalent_atoms'][idx]
        sidx = eqdict[equa]['idx_grp']
        ### rabc = site['abc']
        rabc = rspg[idx]
        if idx == equa:
            wyck[sidx][3] = eqdict[equa]['label']
            wycklabel = spgl['wyckoffs'][idx]
            wyckmult = eqdict[equa]['mult_nom']
            wyckocc = eqdict[equa]['sites_conc']
            wyckmo = eqdict[equa]['mult_prox']
            wyck[sidx][2].append([wycklabel, wyckmult, [None,None,None], wyckocc, wyckmo, eqdict[equa]['list_eleocc']])
            widx = len(wyck[sidx][2])-1
            repwyck = [10.0,20.0,30.0]
            wyck[sidx][1][0][0] += wyckmult
            wyck[sidx][1][0][1] += wyckmo
            wyck[sidx][1][1][0] += wyckmult
            wyck[sidx][1][1][1] += wyckmo
            wyck[sidx][1][1][2] += wyckmult*wyckocc
        # representative Wyckoff position is determined in order of decreasing importance according to following rules
        # all 3 coordinates are equal, then this value is closest to 0
        # last 2 coordinates are equal, then this value is closest to 0, then first value is closest to 0
        # no coordinates are equal, then z is closer to 0, then y is closer to 0, then x is closer to 0
        deg3rwyck = set([round(repwyck[x],roundtol) for x in range(0,3)])
        deg3rabc = set([round(rabc[x],roundtol) for x in range(0,3)])
        deg2rwyck = set([round(repwyck[x],roundtol) for x in range(1,3)])
        deg2rabc = set([round(rabc[x],roundtol) for x in range(1,3)])
        if len(deg3rwyck) > len(deg3rabc):
            repwyck = rabc
        elif len(deg3rwyck) == len(deg3rabc):
            if len(deg2rwyck) > len(deg2rabc):
                repwyck = rabc
            elif len(deg2rwyck) == len(deg2rabc):
                arwyck2 = abs(round(repwyck[2],roundtol)) ; arabc2 = abs(round(rabc[2],roundtol))
                if  arabc2 < arwyck2:
                    repwyck = rabc
                elif arabc2 == arwyck2:
                    arwyck1 = abs(round(repwyck[1],roundtol)) ; arabc1 = abs(round(rabc[1],roundtol))
                    if  arabc1 < arwyck1:
                        repwyck = rabc
                    elif arabc1 == arwyck1:
                        arwyck0 = abs(round(repwyck[0],roundtol)) ; arabc0 = abs(round(rabc[0],roundtol))
                        if  arabc0 < arwyck0:
                            repwyck = rabc
        wyck[sidx][2][widx][2] = [round(repwyck[x],roundtol) for x in range(0,3)]
    # end loop: all expanded positions

    # copy wyck to wycksort
    wycksort = wyck

    cfuzid = max([x[1][0][0] for x in wycksort[:] if x[1][0][0] == int(x[1][0][0])] or [0])
    cfuzre = max([x[1][0][1] for x in wycksort[:] if x[1][0][1] == int(x[1][0][1])] or [0])

    for idx, wyckele in enumerate(wyck):
        # sort wyckoff positions alphabetically by their labels within group
        wycksort[idx][2] = sorted(wyckele[2], key=lambda a: (caseswap(a[0]), a[2][::-1]))
        if wycksort[idx][3] != 'X':
            # temporary group label from sequence of wyckoff labels within group
            wycksort[idx][0] = ''.join([x[0] for x in wycksort[idx][2][:]])
        else:
            # temporary group label = '~' if group is vacant
            wycksort[idx][0] = '~'
        if cfuzid != 0 and wycksort[idx][1][0][0] != 0 and wycksort[idx][1][0][0] == int(wycksort[idx][1][0][0]):
            cfuzid = int(gcd(cfuzid,wycksort[idx][1][0][0]))
        if cfuzre != 0 and wycksort[idx][1][0][1] != 0 and wycksort[idx][1][0][1] == int(wycksort[idx][1][0][1]):
            cfuzre = int(gcd(cfuzre,wycksort[idx][1][0][1]))
    if cfuzid != 0:
        for idx, wyckele in enumerate(wycksort):
            wycksort[idx][1][0][0] = wycksort[idx][1][0][0]//cfuzid
    if cfuzre != 0:
        for idx, wyckele in enumerate(wycksort):
            wycksort[idx][1][0][1] = wycksort[idx][1][0][1]//cfuzre

    # sort groups by temporary label
    wycksort = sorted(wycksort, key=lambda a: (caseswap(a[:][0]), a[:][2][0][2][::-1]))
    # label groups by A, B, C, ... if group is not vacant (label group by V if it is vacant) and label prototype
    # number of groups should not exceed 21 (U is 21st letter), exception (however unlikely) should be taken care of
    simple_stoich = []
    supersimple_stoich = ""
    xproto["nrw"] = "" # name constructed from letters and their sequence indices of representative Wyckoff positions
    idxw = 0
    nsites_occ = 0
    for idx, wyckele in enumerate(wycksort):
        if wycksort[idx][0] != '~':
            wycksort[idx][0] = chr(idx+65) # replace temporary label by uppercase letter
            simple_stoich.append(wycksort[idx][0] + str(wycksort[idx][1][0][0])) # append label with multiplicity
            supersimple_stoich = supersimple_stoich + wycksort[idx][0] # append label and its multiplicity if it is > 1
            if wycksort[idx][1][0][0] != 1:
                supersimple_stoich = supersimple_stoich + str(wycksort[idx][1][0][0])
            for xidx,x in enumerate(wyckele[2]):
                xproto["nrw"] = xproto["nrw"] + x[0] + str(idxw) + '.'
                idxw += 1
            xproto["nrw"] = xproto["nrw"][:-1] + '|'
            nsites_occ += wycksort[idx][1][1][0]
        else:
            wycksort[idx][0] = 'V'
    xproto["nrw"] = xproto["nrw"][:-1]
    xproto["nsc"] = spgl['choice']
    pearson = symmetry_dict["crystal_family"]+symmetry_dict["lattice_centering"]+str(nsites_occ)
    xproto["nprot"] = '_'.join(simple_stoich) + '__' + pearson + '__' + str(spgl['number']) # prototype name

    # number of formula units in unit cell (Z)
    zlist_nom = []
    zlist_prox = []
    wocc = [0]*3
    for idx, wyckele in enumerate(wycksort):
        if wyckele[1][0][0] != 0:
            zlist_nom.append(wyckele[1][1][0]//wyckele[1][0][0])
        if wyckele[1][0][1] != 0:
            zlist_prox.append(wyckele[1][1][1]//wyckele[1][0][1])
        wocc[0] += wyckele[1][1][0]
        wocc[1] += wyckele[1][1][1]
        wocc[2] += wyckele[1][1][2]

    zset_nom = list(set(zlist_nom))
    zset_prox = list(set(zlist_prox))
    if len(zset_nom) != 1 or len(zset_prox) != 1:
        xproto["status"] = "ERROR"
        xproto["message"] = "zset_nom, zset_prox = (" + str(zset_nom) + ", " + str(zset_prox) + ")"
        return (xproto,labels)

    # 'occ_structure' - occupation in nested lists, base for next entries
    occ_structure = [[
        [[
            [[
                z[0],z[1]
            ] for z in y[5]],y[1]
        ] for y in x[2]],x[0]
    ] for x in wycksort]
    occ_flat = sorted(sum([[[z[0],z[1]*y[1]] for z in y[0]] for y in sum([x[0] for x in occ_structure],[])],[]))
    occ_element_alpha = [[ele,round(sum(y[1] for y in occ_flat if y[0] == ele),roundtom)] for ele in sorted(list(set([x[0] for x in occ_flat])))]
    occ_element_atnum = sorted(occ_element_alpha, key=lambda a: (ptable[a[0]][0]))
    occ_element_eniupac = sorted(occ_element_alpha, key=lambda a: (ptable[a[0]][3]))
    occ_element_gp = sorted(occ_element_alpha, key=lambda a: (ptable[a[0]][2] , -ptable[a[0]][1]))
    occ_element_pg = sorted(occ_element_alpha, key=lambda a: (-ptable[a[0]][1] , ptable[a[0]][2]))
    # 'wyck_*' entries with general stoichiometry including Wyckoff letters
    stoich_wyck_all = ' '.join([x[0]+str(x[1][0][0])+'['+''.join([y[0] for y in x[2]])+']' for x in wycksort])
    stoich_wyck_sum = ' '.join([x[0]+str(x[1][0][0])+'['+
                                  '-'.join(sorted(
                                      [k+'*'+str(v) for k,v in dict(zip(
                                          [str(y[1])+y[0] for y in x[2]],
                                          [[y[0] for y in x[2]].count(i) for i in [y[0] for y in x[2]]]
                                          )).items()],
                                      key=lambda a: (a.lstrip(string.digits))
                                  ))
                                  +']' for x in wycksort])

    # 'formula_*' entries with different sorting of elements
    # sorting by: alphabet - alpha, atomic number - atnum, eniupac - IUPAC convention, gp - group and period, pg - period and group
    # including {} for easier latex output in 'occ_element_*' entries
    xproto["stoich"] = {}
    xproto["stoich"]["occ_element_full"] = ' '.join(['{'+x[0]+'}{'+str(x[1]).rstrip('0').rstrip('.')+'}' for x in occ_element_alpha])
    xproto["stoich"]["occ_element_rat"] = ' '.join(['{'+x[0]+'}{'+str(round(1.*x[1]/zset_nom[0],roundtos)).rstrip('0').rstrip('.')+'}' for x in occ_element_alpha])
    xproto["stoich"]["occ_element_rednom"] = ' '.join(['{'+x[0]+'}{'+str(round(1.*x[1]/wocc[0],roundtom))+'}' for x in occ_element_alpha])
    xproto["stoich"]["occ_element_redreal"] = ' '.join(['{'+x[0]+'}{'+str(round(1.*x[1]/wocc[2],roundtom))+'}' for x in occ_element_alpha])
    xproto["stoich"]["formula_alpha"] = ' '.join([x[0]+str(round(1.*x[1]/zset_nom[0],roundtos)).rstrip('0').rstrip('.') for x in occ_element_alpha])
    xproto["stoich"]["formula_atnum"] = ' '.join([x[0]+str(round(1.*x[1]/zset_nom[0],roundtos)).rstrip('0').rstrip('.') for x in occ_element_atnum])
    xproto["stoich"]["formula_eniupac"] = ' '.join([x[0]+str(round(1.*x[1]/zset_nom[0],roundtos)).rstrip('0').rstrip('.') for x in occ_element_eniupac])
    xproto["stoich"]["formula_gp"] = ' '.join([x[0]+str(round(1.*x[1]/zset_nom[0],roundtos)).rstrip('0').rstrip('.') for x in occ_element_gp])
    xproto["stoich"]["formula_pg"] = ' '.join([x[0]+str(round(1.*x[1]/zset_nom[0],roundtos)).rstrip('0').rstrip('.') for x in occ_element_pg])
    xproto["stoich"]["occ_structure"] = occ_structure
    xproto["stoich"]["wyck_all"] = stoich_wyck_all
    xproto["stoich"]["wyck_sum"] = stoich_wyck_sum
    # nominal stoichiometry
    # combined general and explicit formulas in 'form_nom_*' entries
    xproto["stoich"]["form_nom_full"] = [
        '   '.join(
            [x[1]+':['+'  '.join(
                ['('+' '.join(['{'+z[0]+'}{'+str(z[1])+'}' for z in y[0]])+')('+str(y[1])+')' for y in x[0]]
                )+']' for x in occ_structure]
            ),
        1
    ]
    xproto["stoich"]["form_nom_rat"] = [
        '   '.join(
            [x[1]+':['+'  '.join(
                ['('+' '.join(['{'+z[0]+'}{'+str(z[1])+'}' for z in y[0]])+')('+str(round(1.*y[1]/zset_nom[0],roundtos)).rstrip('0').rstrip('.')+')' for y in x[0]]
                )+']' for x in occ_structure]
            ),
        zset_nom[0]
    ]
    xproto["stoich"]["form_nom_ratexp"] = [
        ' '.join(
            [x[1]+':'+''.join(
                ['('+' '.join([z[0]+'_'+str(round(z[1]*y[1]/zset_nom[0],roundtos)).rstrip('0').rstrip('.') for z in y[0]])+')' for y in x[0]]
                ) for x in occ_structure]
            ),
        zset_nom[0]
    ]
    xproto["stoich"]["form_nom_red"] = [
        '   '.join(
            [x[1]+':['+'  '.join(
                ['('+' '.join(['{'+z[0]+'}{'+str(z[1])+'}' for z in y[0]])+')('+str(round(1.*y[1]/wocc[0],roundtom))+')' for y in x[0]]
                )+']' for x in occ_structure]
            ),
        round(wocc[0],roundtom)
    ]
    xproto["stoich"]["off"] = False # off-stoichiometry
    xproto["stoich"]["off_strong"] = False # strong off-stoichiometry
    # real stoichiometry
    if wocc[2] != wocc[0]:
        xproto["stoich"]["form_real_full"] = [
            '   '.join(
                [x[1]+':['+'  '.join(
                    ['('+' '.join(
                        ['{'+z[0]+'}{'+str(round(z[1]*y[1],roundtom))+'}' for z in y[0]]
                        )+')' for y in x[0]]
                    )+']' for x in occ_structure]
                ),
            1
        ]
        xproto["stoich"]["form_real_rednom"] = [
            '   '.join(
                [x[1]+':['+'  '.join(
                    ['('+' '.join(
                        ['{'+z[0]+'}{'+str(round(z[1]*y[1]/wocc[0],roundtom))+'}' for z in y[0]]
                        )+')' for y in x[0]]
                    )+']' for x in occ_structure]
                ),
            round(wocc[0],roundtom)
        ]
        xproto["stoich"]["form_real_redreal"] = [
            '   '.join(
                [x[1]+':['+'  '.join(
                    ['('+' '.join(
                        ['{'+z[0]+'}{'+str(round(z[1]*y[1]/wocc[2],roundtom))+'}' for z in y[0]]
                        )+')' for y in x[0]]
                    )+']' for x in occ_structure]
                ),
            round(wocc[2],roundtom)
        ]
        xproto["stoich"]["off"] = True
    if zset_nom[0] != zset_prox[0]:
        xproto["stoich"]["off_strong"] = True

    # representative Wyckoff positions (constructed by unique rules, see above)
    # useful for library of prototypes
    xproto["repwyck"] = wycksort

    xproto["lattice"] = pymg_dict["lattice"]

    xproto["status"] = "OK"

    # labels for unique identification of structures, v for verbose, c for concise
    labels["symmetry_c"] = str(spgl['number'])+':'+spgl['choice']
    labels["symmetry_v"] = ' '.join([
            str(spgl['number'])+':'+spgl['choice'],
            '('+spglib.get_spacegroup_type(spgl['hall_number'])['international_full']+')',
            '['+str(symmetry_dict['[system,lattice] dimension'][0])+','+str(symmetry_dict['[system,lattice] dimension'][1])+']'
    ])
    labels["structure_v"] = pearson+' : '+'Z='+str(zset_nom[0])+' : '+stoich_wyck_sum
    labels["structure_c"] = pearson+' '+supersimple_stoich
    labels["chemistry"] = xproto["stoich"]["formula_eniupac"]

    return (xproto,labels)

"""
nearest integer Wyckoff position multiplicity for vacant structure sites with concentration 0 < conc < 1
"""
def proximate_multiplicity(mult,conc):

    default = 0
    if conc < 0. or conc > 1.0 or mult < 1:
        return
    divlist = [x for x in range(1,mult+1) if 1.0*mult/x==int(1.0*mult/x)]
    for idx,idiv in enumerate(divlist):
        if idx < len(divlist)-1:
            if conc >= 1./math.sqrt(divlist[idx]*divlist[idx+1]):
                return mult//idiv
        else:
            if conc >= 1./idiv/2:
                return mult//idiv
    return default

"""
matching particular prototype (xproto), constructed by determine_wyckrep function, against dictionary of prototypes (protos)
"""
def prototype_match(protos,xproto):

    tolerance = 1e-5

    proto_name = xproto["nprot"]
    proto_spec = proto_name.split('__')[0].split('_')

    iproto = protos.get(proto_name)

    # condition: prototype match
    if iproto:
        match = [None]*len(iproto)

        # loop: over wyckoff sequence variants (vidx,vproto)
        for vidx,vproto in enumerate(iproto):
            proto_spec_wyck = xproto["nrw"].split('|')
            match[vidx] = {}
            match[vidx]["id_names"] = vproto["id_names"]
            match[vidx]["items"] = {
                "id_setting_choice" : False,
                "id_wyckoff_sequence" : False,
                "id_spec_nrw_len": False
            }
            match[vidx]["occupied"] = {}
            if xproto["nsc"] == vproto["id"]["nsc"]:
                match[vidx]["items"]["id_setting_choice"] = True
            if xproto["nrw"] == vproto["id"]["nrw"]:
                match[vidx]["items"]["id_wyckoff_sequence"] = True
            if len(proto_spec_wyck) == len(proto_spec):
                match[vidx]["items"]["id_spec_nrw_len"] = True

            # condition: all scalar items match
            if all(x == True for x in match[vidx]["items"].values()):
                # initvar
                nspec = len(proto_spec)
                nwyck = 0
                for y in vproto["repwyck"][:]:
                    for z in y[2]:
                        nwyck += 1
                match[vidx]["occupied"]["species"] = [None]*nspec
                match[vidx]["occupied"]["wyckpos"] = [None]*nwyck
                pidx = 0
                wyckdict = {}

                # matching: lattice parameters
                par_lat = vproto["params"]["lat"]
                (a, b, c, alpha, beta, gamma) = [xproto['lattice'][k] for k in ('a', 'b', 'c', 'alpha', 'beta', 'gamma')]
                match[vidx]["lattice"] = [None]*len(par_lat)
                # loop: over lattice parameters
                for ilpar,dlpar in enumerate(par_lat.items()):
                    xlpar = eval(dlpar[0])
                    if xlpar >= dlpar[1][0] and xlpar <= dlpar[1][1]:
                        match[vidx]["lattice"][ilpar] = True

                # loop: over species
                for sidx in range(0,nspec):
                    vspec = str(vproto["repwyck"][sidx][0]) + str(vproto["repwyck"][sidx][1])
                    xspec = str(xproto["repwyck"][sidx][0]) + str(xproto["repwyck"][sidx][1][0][0])

                    # condition: species match
                    if vspec == xspec:
                        match[vidx]["occupied"]["species"][sidx] = True
                        proto_wlm = proto_spec_wyck[sidx].split('.')

                        # loop: over wyckoff positions
                        for widx in range(0,len(proto_wlm)):
                            match[vidx]["occupied"]["wyckpos"][pidx] = [False,False,False,False]
                            vwyck = vproto["repwyck"][sidx][2][widx]
                            xwyck = xproto["repwyck"][sidx][2][widx]
                            # matching: wyckoff (label,multiplicity)
                            vwlm = str(vwyck[0]) + str(vwyck[1])
                            xwlm = str(xwyck[0]) + str(xwyck[1])
                            if vwlm == xwlm:
                                match[vidx]["occupied"]["wyckpos"][pidx][0] = True
                            # matching: wyckoff position
                            for ridx in range(0,3):
                                vr = vwyck[2][ridx]
                                xr = xwyck[2][ridx]
                                # creating dynamic local variables for wyckoff coordinates,
                                # and storing reference to them in wyckdict dictionary, to be deleted later
                                xwycki = xwyck[0]+str(pidx)+chr(ridx+120)
                                if xwycki[0].isalpha():
                                    exec("%s = %f" % (xwycki, xr), None)
                                    wyckdict[xwycki] = xr
                                # checking match for fixed coordinates
                                if isinstance(vr, (int,float)):
                                    if abs(vr-xr) <= tolerance:
                                        match[vidx]["occupied"]["wyckpos"][pidx][ridx+1] = True
                                # checking match for coordinates with allowed range
                                if isinstance(vr, str):
                                    par_vr = vproto["params"]["repwyck"].get(vr)
                                    if xr >= par_vr[0] and xr <= par_vr[1]:
                                        match[vidx]["occupied"]["wyckpos"][pidx][ridx+1] = True
                            # matching: concentration
                            vc = vproto["params"]["repwyck"].get(str(vwyck[0])+str(pidx)+'c')
                            if vc and isinstance(vc[0], (int,float)) and isinstance(vc[1], (int,float)):
                                # this is not fully finished, one should take into consideration natural boundaries from approximated stoichiometry
                                xc = xwyck[3]
                                if xc >= vc[0] and xc <= vc[1] and xc <= 1.0:
                                    match[vidx]["occupied"]["wyckpos"][pidx].append(True)
                                else:
                                    match[vidx]["occupied"]["wyckpos"][pidx].append(False)
                            pidx += 1
                        # end loop: wyckoff positions

                    else:
                        match[vidx]["_status"] = "ERROR"
                        match[vidx]["_message"] = "species do not match: vspec = " + vspec + " xspec = " + xspec
                        return match
                    # end condition: species match

                # end loop: species

                # match for individual variants (vidx,vproto)
                match[vidx]["items"]["lattice"] = all(x == True for x in match[vidx]["lattice"])
                match[vidx]["items"]["occupied_species"] = all(x == True for x in match[vidx]["occupied"]["species"])
                match[vidx]["items"]["occupied_wyckpos"] = all([x == True for sublist in match[vidx]["occupied"]["wyckpos"] for x in sublist])

                match[vidx]["_status"] = True

                # useful for KKR - generating Wyckoff representatives for vacant sites
                if vproto.get("vacant") != None:

                    match[vidx]["vacant"] = [None] * len(vproto["vacant"])

                    # loop: over vacant variants
                    for vacidx,vacelem in enumerate(vproto["vacant"]):

                        match[vidx]["vacant"][vacidx] = vacelem

                        # creating dynamic local variables for vacant Wyckoff coordinates,
                        # and storing reference to them in vacpardict dictionary, to be deleted later
                        vacpardict = {}
                        for vacpark,vacparv in vproto["params"]["vacant"][vacidx].items():
                            if vacpark[0].isalpha():
                                exec("%s = %f" % (vacpark, vacparv), None)
                                vacpardict[vacpark] = vacparv
                        # interpreting vacant Wyckoff coordinates if they are strings
                        for vacwyckidx in range(0,len(match[vidx]["vacant"][vacidx][2])):
                            for ridx in range(0,3):
                                if isinstance(match[vidx]["vacant"][vacidx][2][vacwyckidx][2][ridx], str):
                                    match[vidx]["vacant"][vacidx][2][vacwyckidx][2][ridx] = eval(match[vidx]["vacant"][vacidx][2][vacwyckidx][2][ridx])
                        # deleting dynamically created local variables for vacant Wyckoff coordinates
                        for iwd in vacpardict.keys():
                            exec("del %s" % (iwd), None)
                        
                    # end loop: vacant variants

                # deleting dynamically created local variables for Wyckoff coordinates
                for iwd in wyckdict.keys():
                    exec("del %s" % (iwd), None)
            
            else:
                match[vidx]["_status"] = False
                
            # end condition: all scalar items match

        # end loop: over wyckoff sequence variants (vidx,vproto)

    else:
        match = [{}]
        match[0]["_status"] = None
    # end condition: prototype match

    return match

"""
does as function name says
"""
def construct_spglib_input_from_pymatgen(pymg):

    # the sites list is expected to be sorted by pymatgen
    lattice = pymg.as_dict()['lattice']['matrix']
    positions = [x['abc'] for x in pymg.as_dict()['sites']]
    numbers = [] ; xold = None ; idxnum = 0
    for x in pymg.as_dict()['sites']:
        if x['label'] != xold:
            idxnum += 1
        numbers.append(idxnum)
        xold = x['label']

    return (lattice,positions,numbers)

"""
pymatgen structure for KKR calculations
vacant sites are created if previously defined in prototype library
"""
def construct_pymg_kkr(pymg,spgo,prototype):

    pymg_kkr = pymg

    vacar = []
    okar = []

    vacrep = []
    for reps in prototype['repwyck']:
        if reps[0] == 'V':
            for wyck in reps[2]:
                vacrep.append(wyck[0:3])

    for (iproto,proto) in enumerate(prototype['match']):
        if proto['_status'] == True:
            varstr = proto['id_names']['var'][1]
            if type(proto['id_names']['var'][0]) == int:
                varstr = varstr + ' ' + str(proto['id_names']['var'][0])
            if type(proto['id_names']['var'][2]) == str:
                varstr = varstr + ' ' + str(proto['id_names']['var'][2])
            protoid = ' ) ( '.join(['( var: '+varstr,'lnl: '+proto['id_names']['lnl'][0][0],'pnl: '+' , '.join(proto['id_names']['pnl'])+' )'])
            for (ivac,vac) in enumerate(proto['vacant']):
                wvl = []
                multmatch = []
                for (iwv,wyckvac) in enumerate(vac[2]):
                    if not wyckvac in vacrep:
                        wvl.append(None)
                        multmatch.append(True)
                        posvac = numpy.array(wyckvac[2])
                        wvl[iwv] = []
                        for (tran,rot) in zip(spgo['translations'],spgo['rotations']):
                            wvl[iwv].append(numpy.around((rot.dot(posvac)+tran),decimals=12)%1)
                        wvl[iwv] = numpy.unique(numpy.asarray(wvl[iwv]), axis=0).tolist()
                        if len(wvl[iwv]) != wyckvac[1]:
                            multmatch[iwv] = False
                    else:
                        print(prompt + '    ' + str(iproto) + ' ' +str(ivac) + ' vacant site ' + str(wyckvac) + ' found to be partially occupied, skipping ...')
                if all(multmatch):
                    status = protoid+' + alt. '+str(ivac)+' :: star of wyckoff positions '+str([len(x) for x in wvl])
                    vacar.append([status, wvl])
                    okar.append(True)
                else:
                    status = protoid+' + alt. '+str(iwv)+' :: ERROR in star of wyckoff positions '+str([len(x) for x in wvl])
                    okar.append(False)
                    print(prompt + '    ' + status)

    if len(vacar) > 0:
        print(prompt + '    Vacant sites alternatives:')
    for (ix,x) in enumerate(vacar):
        print(prompt + '      ' + str(ix) + ' : ' + x[0])
        if len(x[1]) > 0:
            for iy,y in enumerate(x[1]):
                ysim = []
                for z in y:
                    ysim.append([nsimplify(w) if len(str(nsimplify(w)))<=3 else w for w in z])
                print(prompt + '        ' + 'v' + str(iy) + ' : ' + ' '.join([str(z).replace(' ','') for z in ysim]))
    if len(vacar) > 1:
        ans = input(prompt + '    Choose index of one of the listed alternatives: ')
        if ans.isdigit() and int(ans) >= 0 and int(ans) < len(vacar):
            vacitem = vacar[int(ans)][1]
    elif len(vacar) == 1:
        vacitem = vacar[0][1]
    else:
        vacitem = []

    pymgprop = None
    if pymg_kkr.site_properties.get('kind_name'):
        pymgprop = {'kind_name' : 'X'}

    for x in vacitem:
        for pos in x:
            pymg_kkr.append({'X': 1.0}, pos, validate_proximity = True, properties=pymgprop)

    ok = len(okar) == 0 or any(okar)

    return (pymg_kkr,ok)

"""
extracting/generating (hopefully) unique identifier to be used for labeling purpose
"""
def get_sis(path):

    # last field (by __ separator) of file basename (without .ext) should contain string like this: aa_nnnnn
    # if it does not, creation date-time is used
    lf = path.split('/')[-1].split('.')[0].split('__')[-1].split('_')
    if len(lf)==2 and lf[0].isalpha() and lf[1].isdigit():
        sis = ' '.join(lf)
    else:
        sis = lf[0] + ' ' + datetime.fromtimestamp(os.path.getctime(path)).strftime("%Y%m%d-%H:%M:%S.%f")

    return sis

"""
generating reasonable parameters for kkr calculations based on longest bravais vector of conventional unit cell
"""
def determine_kkr_parameters(conv,prim):

    global roundtos
    global roundtol

    params = {}
    nvd = []

    alatbasis = max(conv.cell_lengths)
    pymg = prim.get_pymatgen()

    bveclen = pymg.lattice.reciprocal_lattice.abc

    for d in numpy.arange(0.2, 20, 0.2):
        nnlist = pymg.get_all_neighbors_py(d)
        if min([len(x) for x in nnlist]) > 300:
            break

    distlist = sorted(list(set([round(y[1]+0.5e-8,roundtol) for x in nnlist for y in x])))
    distmaxph = max(distlist)+0.5
    step = round(3/alatbasis+0.05,1)
    for r in numpy.arange(step,15*step,step):
        rang = r*alatbasis
        if rang > distmaxph:
            distlist.append(rang)

    oldmin = 0
    for dist in distlist:
        nnlistcount = [len(x) for x in pymg.get_all_neighbors_py(round(dist+0.5e-8,roundtol))]
        nnlistcountmin = min(nnlistcount)
        if nnlistcountmin > oldmin:
            rinalatbasis = round(dist/alatbasis+0.5e-8,roundtol)
            rinalatbasissimple = str(nsimplify(rinalatbasis,tolerance=1e-6))
            if len(rinalatbasissimple) <=16:
                rsimple = rinalatbasissimple
            else:
                rsimple = round(rinalatbasis+0.5e-4,roundtos)
            nvd.append([round(dist+0.5e-8,roundtol),nnlistcountmin,max(nnlistcount),sum(nnlistcount),rinalatbasis,rsimple])
        oldmin = min(nnlistcount)

    kmeshtempr_mRy = tuple([round(128*x,1) for x in bveclen])

    params['alatbasis'] = alatbasis
    params['bbasis'] = pymg.lattice.reciprocal_lattice.matrix
    params['bveclen'] = bveclen
    params['kmeshtempr_mRy'] = kmeshtempr_mRy
    params['neighbors_vs_distance'] = nvd

    return params

"""
main function
"""
def analyze_symmetry(dd):

    global prompt
    global verbose
    global prototypes

    # variables, constants
    dim = [3,3]
    system = {}

    # data format
    # currently supported: cif file
    # future implementation: aiida structure data, POSCAR file
    fmt = dd.get('fmt')
    if not fmt:
        fmt = ''

    # dictionary containing library of prototypes
    prototypes = dd.get('prototypes')
    if not prototypes:
        prototypes = {}

    # prompt to prepend standard output
    prompt = dd.get('prompt')
    if not prompt:
        prompt = ''
    # verbose output
    verbose = dd.get('verbose')
    if not verbose:
        verbose = False

    # ok flag to proceed with output
    ok = False

    # condition: cif file
    if fmt == 'cif':

        """
        minimal dd should contain:
        dd = {
            'fmt' : 'cif',
            'cifpath' : /absolute/path/to/file__id_nnnnnn.cif,
            'outmode' : ['a_cif','a_conv','a_prim','a_primkkr']
        }
        at least one outmode to make sense of calling this function
        """

        path = dd.get('cifpath')
        
        data_structure_original = {}
        check_cif = {}

        cif_read = ReadCif(path) # dictionary generated directly from cif file
        cif_data = CifData(file=path) # aiida CifData
        
        sis = get_sis(path) # unique identifier

        # data_structure_original dictionary is stored in outmodes: 'a_conv','a_prim','a_primkkr'
        data_structure_original = {
            "data_format" : fmt,
            "data_path_file" : path,
            "file_content" : cif_data.get_content()
        }

        astr_conv = cif_data.get_structure() # conventional unit cell aiida StructureData
        astr_prim = cif_data.get_structure(primitive_cell=True) # primitive unit cell aiida StructureData

        pymg_conv = astr_conv.get_pymatgen() # conventional unit cell pymatgen structure

        # following line could be replaced by "from aiida.tools.data.structure import structure_to_spglib_tuple", if it works correctly for disordered structures
        (lattice,positions,numbers) = construct_spglib_input_from_pymatgen(pymg_conv) # (lattice,positions,numbers) input for spglib
        spgd = spglib.get_symmetry_dataset((lattice,positions,numbers)) # spglib symmetry dataset corresponding to pymg_conv and astr_conv
        check_cif = cif_check(cif_read,cif_data,spgd) # check_cif dictionary contains some cross-checks about symmetry and structure
        

        if check_cif['check'] == 1: # if check is successful
            # original choice/setting is used to get spgo_conv spgilb symmetry dataset
            spgo_conv = spglib.get_symmetry_dataset((lattice,positions,numbers),hall_number=int(check_cif['sg']['hall_number']['spgo']))
            # system properties, will be later moved to external function as soon as it becomes more complex
            system['kind'] = 'homogeneous'
            system['components'] = [{}]
            system['components_interface'] = None
            system['components'][0]['action'] = None
            system['components'][0]['complexity'] = 'single'
            system['components'][0]['composition'] = 'atomic'
            system['components'][0]['objects'] = [{}]
            system['components'][0]['objects_interface'] = None
            system['components'][0]['objects'][0]['character'] = ['crystalline']
            if astr_conv.is_alloy:
                system['components'][0]['objects'][0]['character'].append('alloyed')
            if astr_conv.has_vacancies:
                system['components'][0]['objects'][0]['character'].append('vacant')
            system['components'][0]['objects'][0]['object'] = 'bulk'
            system['components'][0]['objects'][0]['boundary_conditions'] = {
                'periodic' : astr_conv.pbc,
                'vacuum_at_infinity' : [0,0,0,0,0,0],
                'vacuum_interface' : [0,0,0,0,0,0]
            }
            system['components'][0]['objects_boundary_conditions'] = {'status' : 'inherited'}
            system['components_boundary_conditions'] = {'status' : 'inherited'}
            # specification of sole component based on properties of objects (here only one) contained within
            system['components'][0]['specification'] = ' '.join([
                ' '.join(system['components'][0]['objects'][0]['character']),
                system['components'][0]['objects'][0]['object']
                ])
            # specification of whole system based on property of its components (here only one)
            system['specification'] = ' '.join([
                system['components'][0]['complexity'],
                system['components'][0]['composition'],
                system['components'][0]['specification']
                ])
            
            ok = True

    # end condition: cif file
            
    # at this point the following has to be available:
    # - pymg_conv
    # - spgo_conv
    # - sis

    output = {}

    if ok:
        # this has to be generalized as more formats (aiida structure, POSCAR) will be included
        # dictionary with symmetry information is created
        if dim == [3,3]:
            symmetry = construct_symmetry_dict(dim,spgo_conv['number'],spgo_conv['international'],spgo_conv['pointgroup'])

        (prototype,labels) = determine_wyckrep(pymg_conv.as_dict(),spgo_conv,symmetry)
        labels['id'] = sis
        system['components'][0]['objects'][0]['action'] = None
        system['components'][0]['objects'][0]['target_labels'] = labels
        # node label set-up
        label = ' <> '.join([labels['symmetry_v'],labels['structure_v'],labels['chemistry'],labels['id']])
        prototype['label'] = label
        prototype['match'] = prototype_match(prototypes,prototype)

        outmode = dd.get('outmode')
        if not outmode:
            outmode = []

        # AIIDA structure conventional
        if 'a_conv' in outmode:

            if fmt == 'cif':
                astr_conv.set_extra("data_structure_original", data_structure_original)
                astr_conv.set_extra("check_cif", check_cif)

            astr_conv.set_extra("system", system)
            astr_conv.set_extra("prototype", prototype)
            astr_conv.set_extra("symmetry", symmetry)

            astr_conv.label = label
            astr_conv.description = 'aiida structure conventional'

            output["aiida_structure_conventional"] =  astr_conv

        # AIIDA structure primitive
        if 'a_prim' in outmode:

            if fmt == 'cif':
                astr_prim.set_extra("data_structure_original", data_structure_original)
                astr_prim.set_extra("check_cif", check_cif)

            astr_prim.set_extra("system", system)
            astr_prim.set_extra("prototype", prototype)
            astr_prim.set_extra("symmetry", symmetry)

            astr_prim.label = label
            astr_prim.description = 'aiida structure primitive'

            output["aiida_structure_primitive"] =  astr_prim

        # AIIDA structure primitive for KKR
        if 'a_primkkr' in outmode:

            (pymg_kkr,pymg_kkr_ok) = construct_pymg_kkr(pymg_conv,spgo_conv,prototype)

            if pymg_kkr_ok:

                astr_prim_kkr = StructureData(pymatgen=pymg_kkr.get_primitive_structure(tolerance=0.01))

                for (ikind,kind) in enumerate(astr_prim_kkr.attributes['kinds']):
                    sumw = round(sum(kind['weights']),roundtom)
                    if sumw < 1.0:
                        astr_prim_kkr.attributes['kinds'][ikind]['symbols'] += ('X',)
                        astr_prim_kkr.attributes['kinds'][ikind]['weights'] += (round(1.0-sumw,roundtom),)

                if fmt == 'cif':
                    astr_prim_kkr.set_extra("data_structure_original", data_structure_original)
                    astr_prim_kkr.set_extra("check_cif", check_cif)

                astr_prim_kkr.set_extra("prototype", prototype)
                astr_prim_kkr.set_extra("symmetry", symmetry)
                astr_prim_kkr.set_extra("system", system)

                astr_prim_kkr.label = label
                astr_prim_kkr.description = 'aiida structure primitive kkr'

                output["aiida_structure_primitive_kkr"] =  astr_prim_kkr

        # AIIDA cif
        if 'a_cif' in outmode:

            cif_data.label = label
            cif_data.description = 'aiida cif'

            output["aiida_cif"] =  cif_data

    return output
