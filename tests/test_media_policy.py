"""infer_policy.ClassPolicy against the shared C/Python fixture.

The fixture (policy_fixture.json) is the contract: the device's
nn_infer/src/policy.c and this Python mirror must agree bit for bit,
because either side may evaluate the policy for a given camera."""

import json
from pathlib import Path

import pytest

from tests.media_helper import import_media

infer_policy = import_media("infer_policy")

_FIX = json.loads((Path(__file__).parent / "policy_fixture.json").read_text())


@pytest.mark.parametrize("case", _FIX["cases"],
                         ids=[c["name"] for c in _FIX["cases"]])
def test_fixture_case(case):
    p = infer_policy.ClassPolicy(
        agg=case["policy"]["agg"],
        start_x1000=case["policy"]["start"],
        stop_x1000=case["policy"]["stop"])
    for i, conf in enumerate(case["feed"]):
        agg, _changed = p.feed(conf)
        assert agg == case["agg"][i], \
            f"step {i}: agg {agg} != {case['agg'][i]}"
        assert p.detected == case["detected"][i], \
            f"step {i}: detected {p.detected} != {case['detected'][i]}"


def test_agg_clamped():
    assert infer_policy.ClassPolicy(agg=0).agg == 5        # 0 → default
    assert infer_policy.ClassPolicy(agg=999).agg == infer_policy.AGG_MAX
    assert infer_policy.ClassPolicy(agg=-3).agg == 1


def test_feed_reports_state_change_once():
    p = infer_policy.ClassPolicy(agg=1, start_x1000=500, stop_x1000=500)
    _, changed = p.feed(900)
    assert changed and p.detected
    _, changed = p.feed(900)
    assert not changed                       # still detected, no edge
    _, changed = p.feed(0)
    assert changed and not p.detected
