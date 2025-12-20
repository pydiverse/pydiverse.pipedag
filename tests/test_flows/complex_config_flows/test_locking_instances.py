# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import pytest

from pydiverse.pipedag.backend.lock.zookeeper import KazooClient
from pydiverse.pipedag.context import StageLockContext
from pydiverse.pipedag.core.config import PipedagConfig
from tests.fixtures.instances import with_instances
from tests.test_flows.complex_config_flows.test_instance_selection import (
    cfg_file_path,
    check_result,
    get_flow,
)

_ = cfg_file_path
pytestmark = [with_instances("postgres")]


def do_test_lock_manager_instances(cfg_file_path, instance):
    # At this point, an instance is chosen from multi-pipedag-instance
    # configuration file
    pipedag_config = PipedagConfig(cfg_file_path)
    cfg = pipedag_config.get(instance=instance)

    flow, out1, out2 = get_flow(cfg.attrs, pipedag_config)

    with StageLockContext():
        result = flow.run(config=cfg)
        check_result(result, out1, out2)


@pytest.mark.parametrize("instance", ["lock_zookeeper"])
@pytest.mark.skipif(KazooClient is None, reason="requires kazoo")
def test_lock_manager_instances_kazoo(cfg_file_path, instance):
    do_test_lock_manager_instances(cfg_file_path, instance)


@pytest.mark.parametrize("instance", ["lock_file"])
def test_lock_manager_instances(cfg_file_path, instance):
    do_test_lock_manager_instances(cfg_file_path, instance)
