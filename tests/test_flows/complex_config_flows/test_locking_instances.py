from __future__ import annotations

import pytest

from pydiverse.pipedag.context import StageLockContext
from pydiverse.pipedag.core.config import PipedagConfig
from tests.test_flows.complex_config_flows.test_instance_selection import (
    cfg_file_path,
    check_result,
    get_flow,
)

_ = cfg_file_path


@pytest.mark.parametrize("instance", ["lock_zookeeper", "lock_file"])
def test_lock_manager_instances(cfg_file_path, instance):
    # At this point, an instance is chosen from multi-pipedag-instance
    # configuration file
    pipedag_config = PipedagConfig(cfg_file_path)
    cfg = pipedag_config.get(instance=instance)

    flow, out1, out2 = get_flow(cfg.attrs, pipedag_config)

    with StageLockContext():
        result = flow.run(config=cfg)
        check_result(result, out1, out2)
