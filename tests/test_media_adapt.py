"""AdaptController: the queueing-delay congestion controller.

Pins the 2026-08-18 bufferbloat lesson: on a reliable uplink an
over-committed encoder loses NOTHING — it builds a standing queue on
the camera (drops stayed 0 while HLS ran 35 s behind live).  Congestion
must therefore come from one-way delay, probing must be bounded by
measured goodput, and a standing queue outranks the operator's bitrate
floor."""

import struct

from tests.media_helper import import_media

vs = import_media("video_service")


class _FakeSess:
    def __init__(self):
        self.cmds = []                       # [(cmd, value)]

    def send(self, b):
        self.cmds.append((b[1], struct.unpack(">I", b[2:6])[0]))


class _OneWindow:
    """threading.Event stand-in: lets _loop run exactly one iteration."""
    def __init__(self):
        self.calls = 0

    def wait(self, _t):
        self.calls += 1
        return self.calls > 1


def make_ac(**kw):
    ac = vs.AdaptController(_FakeSess(), **kw)
    ac._refresh_policy = lambda: None        # no hub in unit tests
    return ac


def window(ac, *, frames=30, kbytes=100, gaps=0, owd=None):
    """Feed one 1-second measurement window through the loop."""
    ac._frames, ac._bytes, ac._gaps = frames, kbytes * 1000, gaps
    ac._owd_min = owd
    ac._stop = _OneWindow()
    ac.sess.cmds.clear()
    ac._loop()
    return dict(ac.sess.cmds)


def test_idle_window_changes_nothing():
    ac = make_ac(start_bitrate=2_000_000)
    cmds = window(ac, frames=0)
    assert cmds == {} and ac.br == 2_000_000


def test_queue_alone_is_congestion_even_with_zero_drops():
    """THE bufferbloat pin: gaps==0 the whole time, br must still back off."""
    ac = make_ac(start_bitrate=3_000_000)
    window(ac, kbytes=100, owd=10.0)         # baseline owd
    br_before = ac.br
    window(ac, kbytes=100, gaps=0, owd=10.0 + ac.QUEUE_HI_MS + 200)
    assert ac.br < br_before                 # backed off on delay alone


def test_probe_is_bounded_by_goodput_not_br_max():
    """A ~0.8 Mbps link must never be commanded to BR_MAX (the old loop
    pinned there and built a 34 s queue)."""
    ac = make_ac(start_bitrate=1_000_000)
    for _ in range(20):                       # calm link, low goodput
        window(ac, kbytes=100, owd=5.0)       # ~800 kbps
    assert ac.br < vs.AdaptController.BR_MAX
    assert ac.br <= max(vs.AdaptController.BR_MIN,
                        int(ac.net_kbps * 1000 * ac.PROBE_FRAC))


def test_standing_queue_outranks_operator_floor():
    """fps mode: the floor holds normally, but honouring it during a
    standing queue buys unbounded latency, so quality must yield."""
    ac = make_ac(start_bitrate=3_000_000)
    ac.mode = "fps"
    ac.br_floor = 3_000_000
    window(ac, kbytes=100, owd=10.0)          # baseline
    for _ in range(6):                        # persistent standing queue
        window(ac, kbytes=100, owd=10.0 + ac.QUEUE_HI_MS + 500)
    assert ac.br < ac.br_floor                # floor yielded while draining


def test_calm_link_respects_floor():
    ac = make_ac(start_bitrate=3_000_000)
    ac.mode = "fps"
    ac.br_floor = 2_000_000
    for _ in range(10):
        window(ac, kbytes=800, owd=5.0)       # plenty of goodput, no queue
    assert ac.br >= ac.br_floor


def test_quality_mode_pins_fps_and_sheds_bitrate():
    ac = make_ac(fps=30, start_bitrate=3_000_000)
    ac.mode = "quality"
    ac.target_fps = 5
    cmds = window(ac, kbytes=100, owd=5.0)
    assert cmds.get(ac.CMD_FPS_DIV) == 6      # 30/5
    br_before = ac.br
    window(ac, kbytes=100, gaps=3, owd=5.0)   # loss counts as congestion too
    assert ac.br < br_before


def test_bitrate_never_leaves_bounds():
    ac = make_ac(start_bitrate=1_000_000)
    window(ac, kbytes=50, owd=10.0)
    for _ in range(30):
        window(ac, kbytes=50, gaps=5, owd=10.0 + ac.QUEUE_HI_MS + 800)
    assert ac.br >= vs.AdaptController.BR_MIN
    for _ in range(60):
        window(ac, kbytes=5000, owd=5.0)
    assert ac.br <= vs.AdaptController.BR_MAX
