# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2023 ScyllaDB

import logging
import re
import os
import contextlib
from sdcm.loader import CqlStressCassandraStressExporter
from sdcm.prometheus import nemesis_metrics_obj
from sdcm.sct_events.loaders import CqlStressCassandraStressEvent
from sdcm.stress_thread import CassandraStressThread
from sdcm.utils.common import SoftTimeoutContext
from sdcm.utils.docker_remote import RemoteDocker


LOGGER = logging.getLogger(__name__)


class CqlStressCassandraStressThread(CassandraStressThread):
    DOCKER_IMAGE_PARAM_NAME = 'stress_image.cql-stress-cassandra-stress'

    def __init__(self, loader_set, stress_cmd, timeout, stress_num=1, keyspace_num=1, keyspace_name='', compaction_strategy='',  # pylint: disable=too-many-arguments
                 profile=None, node_list=None, round_robin=False, client_encrypt=False, stop_test_on_failure=True,
                 params=None):
        super().__init__(loader_set=loader_set, stress_cmd=stress_cmd, timeout=timeout,
                         stress_num=stress_num, node_list=node_list,  # pylint: disable=too-many-arguments
                         round_robin=round_robin, stop_test_on_failure=stop_test_on_failure, params=params,
                         keyspace_num=keyspace_num, keyspace_name=keyspace_name, profile=profile,
                         client_encrypt=client_encrypt, compaction_strategy=compaction_strategy)

    def create_stress_cmd(self, cmd_runner, keyspace_idx, loader):  # pylint: disable=too-many-branches
        stress_cmd = self.stress_cmd

        if "no-warmup" not in stress_cmd:
            # add no-warmup to stress_cmd if it's not there. See issue #5767
            stress_cmd = re.sub(
                r'(cql-stress-cassandra-stress [\w]+)', r'\1 no-warmup', stress_cmd)

        if self.keyspace_name:
            stress_cmd = stress_cmd.replace(
                " -schema ", " -schema keyspace={} ".format(self.keyspace_name))
        elif 'keyspace=' not in stress_cmd:  # if keyspace is defined in the command respect that
            stress_cmd = stress_cmd.replace(
                " -schema ", " -schema keyspace=keyspace{} ".format(keyspace_idx))

        if self.compaction_strategy and "compaction(" not in stress_cmd:
            stress_cmd = stress_cmd.replace(
                " -schema ", f" -schema 'compaction(strategy={self.compaction_strategy})' ")

        if '-col' in stress_cmd:
            # In cassandra-stress, '-col n=' parameter defines number of columns called (C0, C1, ..., Cn) respectively.
            # It accepts a distribution as value, however, only 'FIXED' distribution is accepted for the cql mode.
            # In cql-stress, we decided to accept a number instead.
            # To support the c-s syntax in test-cases files, we replace occurrences
            # of n=FIXED(k) to n=k.
            stress_cmd = re.sub(r' (\'?)n=[\s]*fixed\(([0-9]+)\)(\'?)', r' \1n=\2\3',
                                stress_cmd, flags=re.IGNORECASE)

        if '-rate' in stress_cmd:
            # In cassandra-stress either '-rate throttle=' or '-rate fixed=' parameter is accepted.
            # In cql-stress we decided to change it so 'fixed' is a boolean flag
            # specifying whether the tool should display coordination-omission fixed latencies.
            stress_cmd = re.sub(r' fixed=[\s]*([0-9]+/s)',
                                r' throttle=\1 fixed', stress_cmd)

        if '-pop' in stress_cmd:
            # '-pop seq=x..y' syntax is not YET supported by cql-stress.
            # TODO remove when seq=x..y syntax is supported.
            stress_cmd = re.sub(r' seq=[\s]*([\d]+\.\.[\d]+)',
                                r" 'dist=SEQ(\1)'", stress_cmd)

        if self.node_list and '-node' not in stress_cmd:
            stress_cmd += " -node "
            if self.loader_set.test_config.MULTI_REGION:
                # The datacenter name can be received from "nodetool status" output. It's possible for DB nodes only,
                # not for loader nodes. So call next function for DB nodes
                datacenter_name_per_region = self.loader_set.get_datacenter_name_per_region(
                    db_nodes=self.node_list)
                if loader_dc := datacenter_name_per_region.get(loader.region):
                    stress_cmd += f"datacenter={loader_dc} "
                else:
                    LOGGER.error("Not found datacenter for loader region '%s'. Datacenter per loader dict: %s",
                                 loader.region, datacenter_name_per_region)

            node_ip_list = [n.cql_address for n in self.node_list]
            stress_cmd += ",".join(node_ip_list)
        return stress_cmd

    def _run_cs_stress(self, loader, loader_idx, cpu_idx, keyspace_idx):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        # TODO:
        # - Add support for profile yaml once cql-stress supports 'user' command.
        # - Adjust metrics collection once cql-stress displays the metrics grouped by operation (mixed workload).

        cleanup_context = contextlib.nullcontext()
        os.makedirs(loader.logdir, exist_ok=True)

        stress_cmd_opt = self.stress_cmd.split(
            "cql-stress-cassandra-stress", 1)[1].split(None, 1)[0]

        log_id = self._build_log_file_id(loader_idx, cpu_idx, keyspace_idx)
        log_file_name = \
            os.path.join(
                loader.logdir, f'cql-stress-cassandra-stress-{stress_cmd_opt}-{log_id}.log')

        LOGGER.debug('cassandra-stress local log: %s', log_file_name)

        cmd_runner_name = loader.ip_address

        cpu_options = ""
        if self.stress_num > 1:
            cpu_options = f'--cpuset-cpus="{cpu_idx}"'

        cmd_runner = cleanup_context = RemoteDocker(loader, self.docker_image_name,
                                                    command_line="-c 'tail -f /dev/null'",
                                                    extra_docker_opts=f'{cpu_options} '
                                                    '--network=host '
                                                    f'--label shell_marker={self.shell_marker}'
                                                    f' --entrypoint /bin/bash')
        stress_cmd = self.create_stress_cmd(cmd_runner, keyspace_idx, loader)
        LOGGER.info('Stress command:\n%s', stress_cmd)

        tag = f'TAG: loader_idx:{loader_idx}-cpu_idx:{cpu_idx}-keyspace_idx:{keyspace_idx}'

        if self.stress_num > 1:
            node_cmd = f'STRESS_TEST_MARKER={self.shell_marker}; taskset -c {cpu_idx} {stress_cmd}'
        else:
            node_cmd = f'STRESS_TEST_MARKER={self.shell_marker}; {stress_cmd}'
        node_cmd = f'echo {tag}; {node_cmd}'

        with cleanup_context, \
                CqlStressCassandraStressExporter(instance_name=cmd_runner_name,
                                                 metrics=nemesis_metrics_obj(),
                                                 stress_operation=stress_cmd_opt,
                                                 stress_log_filename=log_file_name,
                                                 loader_idx=loader_idx, cpu_idx=cpu_idx), \
                CqlStressCassandraStressEvent(node=loader, stress_cmd=self.stress_cmd,
                                              log_file_name=log_file_name) as cs_stress_event:
            try:
                # prolong timeout by 5% to avoid killing cassandra-stress process
                hard_timeout = self.timeout + int(self.timeout * 0.05)
                with SoftTimeoutContext(timeout=self.timeout, operation="cql-stress-cassandra-stress"):
                    result = cmd_runner.run(
                        cmd=node_cmd, timeout=hard_timeout, log_file=log_file_name, retry=0)
            except Exception as exc:  # pylint: disable=broad-except
                self.configure_event_on_failure(
                    stress_event=cs_stress_event, exc=exc)

        return loader, result, cs_stress_event
