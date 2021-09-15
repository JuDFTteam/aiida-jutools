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
"""Tools for working with AiiDA process submission: blocking submission."""

import dataclasses as _dc
import datetime as _datetime
import time as _time
import typing as _typing

from aiida import orm as _orm, tools as _aiida_tools, engine as _aiida_engine
from aiida.engine import processes as _aiida_processes
from masci_tools.util import python_util as _masci_python_util

import aiida_jutools as _jutools


# from process.util import query_processes, get_process_states


@_dc.dataclass
class BlockingSubmissionControllerSettings:
    """Settings for :py:class:`~BlockingSubmissionController`. See its documentation for details.

    Time unit of 'wait' attributes is minutes.

    :param dry_run: True: don't submit, simulate with secondss_per_min=1 instead of 60
    :param max_top_processes_running: wait in line if surpassed: top processes (type of called builder)
    :param max_all_processes_running: wait in line if surpassed: all processes (top & children)
    :param wait_for_submit: interval to recheck if line is free now
    :param max_wait_for_submit: max time to wait in line, give up afterwards
    :param wait_after_submit: if could submit, wait this long until returning
    :param resubmit_failed: True: if found failed process of same label in group, resubmit. Default False.
    :param resubmit_failed_as_restart: True: submit get_builder_restarted() from failed instead of builder.
           Default True.
    :param delete_if_stalling: True: delete nodes of 'stalling' top processes. Default True.
    :param delete_if_stalling_dry_run: True: if delete_if_stalling, simulate delete_if_stalling to 'try it out'.
    :param max_wait_for_stalling: delete top process (node & descendants) if running this long. To avoid congestion.
    """
    dry_run: bool = True
    max_top_processes_running: int = 30
    max_all_processes_running: int = 60
    wait_for_submit: int = 5
    max_wait_for_submit: int = 240
    wait_after_submit: int = 2
    resubmit_failed: bool = False
    resubmit_failed_as_restart: bool = False
    delete_if_stalling: bool = False
    delete_if_stalling_dry_run: bool = False
    max_wait_for_stalling: int = 240


class BlockingSubmissionController:
    """Class for supervised process submission to daemon. See constructor docstring for full info."""

    def __init__(self,
                 settings: BlockingSubmissionControllerSettings,
                 quota_querier: _jutools.computer.QuotaQuerier = None):
        """Class for supervised process submission to daemon. Use e.g. in a loop of many submissions.

        This submission controller assumes that processes intended for submission are uniquely identified
        by their group affiliation and ``label``. It is intended to be called inside a loop, supplying
        a process builder in each iteration for submission. Before each submission, the controller checks
        if an identical process exists which already finished successfully, and if so, skips it, in the most
        basic setting. It uses  distinction by ``process_state`` and ``exit_status`` for that. Submission timings
        and further conditions can be fine-tuned via the :py:class:`~BlockingSubmissionControllerSettings` class.

        If you distinguish your processes by their ``extras`` rather than group and ``label``, or if you want
        concurrent submission of batches of processes on each iteration, use the AiiDA submission controller instead.
        It can be found at https://github.com/aiidateam/aiida-submission-controller.

        :param settings: supervisor settings
        :param quota_querier: computer quota querier for main code. Optional.
        """
        # set settings
        self.settings = settings
        self.__tmp_guard_against_delete_if_stalling()
        self.quotaq = quota_querier
        # queue for submitted workchains, k=wc, v=run_time in min
        self._submitted_top_processes = []

    @property
    def submitted_top_processes(self) -> _typing.List[_orm.ProcessNode]:
        """Database nodes of top processes submitted to the verdi daemon."""
        return self._submitted_top_processes

    def submit(self,
               builder: _aiida_processes.ProcessBuilder,
               groups: _typing.Union[_orm.Group, _typing.List[_orm.Group]] = None) -> _typing.Tuple[
        _orm.ProcessNode, bool]:
        """Submit calculation but wait if more than max running processes are running already.

        The waiting is a blocking operation. If you run this (inside a loop) in a Jupyter cell, you cannot execute
        other cells while the controller is running. But you can close the notebook (keeping the kernel running in
        background), and the controller continues to run until you interrupt the kernel.

        Note: Processes are identified by their label (``builder.metadata.label``). Meaning: If the supervisor
        finds a process node labeled 'A' in one of the groups with state 'finished_ok' (process state ``finished``,
        exit status ``0``), it will load and return that node instead of submitting.

        Note: If ``quota_querier`` is set, computer of main code of builder must be the same as computer set in
        ``quota_querier``. This is not checked as a builder may have several codes using different computers.
        Example for the plugin ``aiida-kkr``: For the workflow ``kkr_imp_wc``, the computer for which the kkrimp code,
        which is another input for the builder, is configured. Meaning, in that case, the quota_querier's computer
        must be the same as ``builder['kkrimp'].computer``.

        :param builder: code builder. ``metadata.label`` must be set!
        :param groups: restrict to processes in a group or list of groups (optional)
        :return: tuple (next process, is process from db True or from submit False) or (None,None) if submit failed
        """
        self.__tmp_guard_against_delete_if_stalling()

        wc_label = builder.metadata.label
        if not wc_label:
            raise ValueError("builder.metadata.label not set. This method doesn't work without an identifying process"
                             "label.")
        wc_process_label = builder._process_class.get_name()

        # get workchains from group(s)
        if isinstance(groups, _orm.Group):
            groups = [groups]
        workchains = []
        for group in groups:
            workchains.extend(
                _jutools.process.query_processes(label=wc_label,
                                                 process_label=wc_process_label,
                                                 group=group).all(flat=True))
        # remove duplicates (ie if same wc in several groups)
        _wc_uuids = []

        def _is_duplicate(wc):
            if wc.uuid not in _wc_uuids:
                _wc_uuids.append(wc.uuid)
                return False
            return True

        workchains[:] = [wc for wc in workchains if not _is_duplicate(wc)]
        # classify db workchains by process state
        workchains_finished_ok = [proc for proc in workchains if proc.is_finished_ok]  # finished and exit status 0
        workchains_terminated = [proc for proc in workchains if proc.is_terminated]  # finished, excepted or killed
        workchains_not_terminated = [proc for proc in workchains if
                                     not proc.is_terminated]  # created, waiting or running

        if len(workchains) > 1:
            print(
                f"INFO: '{wc_label}': found multiple ({len(workchains)}) results in group(s) "
                f"{[group.label for group in groups]}, pks: {[wc.pk for wc in workchains]}")

        # handle for settings
        s = self.settings
        seconds_per_min = 1 if s.dry_run else 60

        def num_running(granularity: int):
            if granularity == 0:  # top processes
                return _jutools.process.query_processes(process_label=wc_process_label,
                                                        process_states=_jutools.process.get_process_states(
                                                            terminated=False)).count()
            if granularity == 1:  # all processes
                return _jutools.process.query_processes(
                    process_states=_jutools.process.get_process_states(terminated=False)).count()

        # load or submit workchain/calc
        # (in the following comments, 'A:B' is used as exemplary wc_label value)
        if workchains_finished_ok:
            # found A:B in db and finished_ok
            next_process_is_from_db = True
            next_process = _orm.load_node(workchains_finished_ok[0].pk)
            print(f"loaded '{wc_label}' from db, finished_ok")
        else:
            # not found A:B in db with state finished_ok. try submitting
            if workchains_terminated and not s.resubmit_failed:
                # found A:B in db with state terminated and not_finished_ok, and no 'resubmit_failed'
                next_process_is_from_db = True
                next_process = _orm.load_node(workchains_terminated[0].pk)
                info = f"process state {next_process.attributes['process_state']}"
                info = info if not next_process.attributes.get('exit_status', None) else \
                    info + f", exit status {next_process.attributes['exit_status']}"
                print(f"loaded '{wc_label}' from db, {info}, (retry modus {s.resubmit_failed})")

            elif workchains_not_terminated:
                # found A:B in db with state not terminated, so it's currently in the queue already
                next_process_is_from_db = False
                next_process = _orm.load_node(workchains_not_terminated[0].pk)
                self._submitted_top_processes.append(next_process)
                print(f"'{wc_label}' is not terminated")

            else:
                # not found A:B in db, so never submitted yet (or deleted since)
                # or found in db not_finished_ok, but terminated, and 'retry'
                # so only option left is submit
                _builder = builder

                info = f"staging submit '{wc_label}' "
                if workchains_terminated and s.resubmit_failed:
                    info_failed = [f"pk {wc.pk}, state {wc.attributes['process_state']}, " \
                                   f"exit status {wc.attributes.get('exit_status', None)}"
                                   for wc in workchains_terminated]
                    info += f", resubmit (previously failed: {info_failed})"
                    if s.resubmit_failed_as_restart:
                        wc_failed_first = workchains_terminated[0]
                        _builder = workchains_terminated[0].get_builder_restart()

                        # some things are not copied from the original builder, such as
                        # node label and description. so do that manually.
                        _builder.metadata.label = builder.metadata.label
                        _builder.metadata.description = builder.metadata.description

                        info += f", restart from first found previously failed, pk={wc_failed_first.pk}"

                        # check that supplied builder metadata correspond with found workchain
                        if (builder.metadata.label != wc_failed_first.label) or (
                                builder.metadata.description != wc_failed_first.description):
                            info += f"(WARNING: label, description supplied via builder ('{builder.metadata.label}', " \
                                    f"{builder.metadata.description}) do not correspond to label, description from " \
                                    f"first found previously failed ('{wc_failed_first.label}', " \
                                    f"{wc_failed_first.description}). Will use those supplied via builder.))"
                info += " ..."
                print(info)

                submitted = False

                if self.quotaq and not self.quotaq.is_min_free_space_left():
                    raise IOError(f"Abort: not enough free space {self.quotaq.settings.min_free_space} "
                                  f"left on remote workdir. Check this object's quotaq.")

                waited_for_submit = 0
                while waited_for_submit <= s.max_wait_for_submit:
                    # entered submit waiting line (length of one workchain with many subprocesses)
                    if s.delete_if_stalling or (not s.delete_if_stalling and s.delete_if_stalling_dry_run):
                        # delete stalling nodes and remove from watch queue
                        def stalling(wc):
                            # DEVNOTE TODO: measuring stalling time via delta= python_util.now()-wc.time does not work
                            #               as expected. It SHOULD delete all top processes that appear in verdi process
                            #               list, with time in verdi process list > max_stalling_time. Instead:
                            # - at every new blocking submit, all wc's deltas are back to zero.
                            # - python_util.now() as is now measureus UTC. wrong offset (+1 compared to ctime? need
                            # localization first?)
                            # - sometimes it DOES delete nodes, but i'm not sure if it was correct for those.
                            is_stalling = (_masci_python_util.now() - wc.mtime) > _datetime.timedelta(
                                minutes=s.max_wait_for_stalling)
                            # print(f"wc {wc.label} pk {wc.pk} last change time {python_util.now() - wc.mtime}, is
                            # stalling {is_stalling}")
                            if is_stalling:
                                info_msg_suffix = "would now delete its nodes nodes (delete_if_stalling dry run)" \
                                    if s.delete_if_stalling_dry_run else "deleting all its nodes"
                                info_msg = f"INFO: process pk={wc.pk} label='{wc.label}' exceeded max stalling " \
                                           f"time {s.max_wait_for_stalling} min, {info_msg_suffix} ..."
                                print(info_msg)

                                if not s.delete_if_stalling_dry_run:
                                    # note: we do not need to kill the top processnode's process first.
                                    # deleting its nodes will also kill all not terminated connected processes.
                                    _aiida_tools.delete_nodes(pks=[wc.pk], dry_run=False, force=True, verbosity=1)
                            return is_stalling

                        # print("while wait, check if any stalling")
                        self._submitted_top_processes[:] = [
                            wc for wc in self._submitted_top_processes if not stalling(wc)]

                    if num_running(0) > s.max_top_processes_running or num_running(1) > s.max_all_processes_running:
                        # process queue is too full, wait
                        waited_for_submit += s.wait_for_submit  # in minutes
                        _time.sleep(s.wait_for_submit * seconds_per_min)
                    else:
                        # process queue is not too full, can submit
                        print(f"try submit (waited {waited_for_submit} min, "
                              f"queued: {num_running(0)} top, {num_running(1)} all processes; "
                              f"wait another {s.wait_after_submit} minutes after submission)")
                        if not s.dry_run:
                            next_process = _aiida_engine.submit(_builder)
                            self._submitted_top_processes.append(next_process)
                            for group in groups:
                                group.add_nodes([next_process])
                            print(f"submitted {wc_label}, pk {next_process.pk}")
                            _time.sleep(s.wait_after_submit * seconds_per_min)
                        else:
                            print(f"dry_run: would now submit {wc_label}")
                            next_process = None
                        # submitted. exit waiting line
                        submitted = True
                        break
                next_process_is_from_db = False

                if not submitted:
                    print(f"WARNING: submission of '{wc_label}' timed out after {waited_for_submit} min waiting time.")
                    next_process, next_process_is_from_db = None, None
        return next_process, next_process_is_from_db

    def __tmp_guard_against_delete_if_stalling(self):
        """Setting ``delete_if_stalling`` True is currently not safe. This disables the option if user has set it.
        DEVNOTE: TODO see resp. DEVNOTEs in submit()
        """
        if self.settings.delete_if_stalling:
            print(f"WARNING: {BlockingSubmissionControllerSettings.__name__}.delete_if_stalling=True is currently "
                  f"not supported. Will instead set delete_if_stalling_dry_run=True to show what the setting "
                  f"*would* do.")
            self.settings.delete_if_stalling = False
            self.settings.delete_if_stalling_dry_run = True
