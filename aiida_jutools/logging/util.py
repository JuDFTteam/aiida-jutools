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
"""AiiDA JuTools: logging: utils."""

import enum as _enum
import typing as _typing


class LogLevel(_enum.Enum):
    INFO = 0
    WARNING = 1
    ERROR = 2


def log(l: LogLevel = None,
        e: _typing.Type[Exception] = None,
        o: _typing.Any = None,
        f=None,
        m: str = "") -> _typing.Optional[Exception]:
    """Basic logging. Prints to standard sys.stdout.

    Schema: [l: ][c][.][f][: ][msg]

    Legend: l=level, c=class, f=function, m=msg. []=optional.

    Example: "Info: Foor.bar(): Writing to file myfile.txt."

    TODO replace with real logging / aiida logging.

    :param l: logging level.
    :param e: exception class to raise, usually for level 'Error'.
    :param o: object or class.
    :param f: function or method.
    :param m: message body.
    :return: exception if 'Error', else print and return nothing.
    """
    l = f"{l.name.title()}: " if l else ""

    cls = o if (type(o) is type) else o.__class__
    cls_name = cls.__name__ if o else ""
    cf_sep = "." if (o and f) else ""
    func_name = f"{f.__name__}()" if f else ""
    cf = f"{cls_name}{cf_sep}{func_name}"

    fm_sep = ": " if cf else ""
    m = f"{l}{cf}{fm_sep}{m}"

    if e:
        return e(m)
    print(m)

