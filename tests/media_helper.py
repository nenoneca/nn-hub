"""Import machinery for media-host modules.

media-host/ is deployed as loose files (not a package), and video_service
imports GStreamer/scipy at module scope.  Tests stub those host-only deps
so the pure logic under test imports hermetically on any machine."""

import sys
import types
from pathlib import Path

MEDIA = Path(__file__).resolve().parent.parent / "media-host"


def _stub(name, **attrs):
    if name in sys.modules:
        return sys.modules[name]
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m


def import_media(modname):
    if str(MEDIA) not in sys.path:
        sys.path.insert(0, str(MEDIA))
    gi = _stub("gi", require_version=lambda *a, **k: None)
    repo = _stub("gi.repository", Gst=types.SimpleNamespace(),
                 GLib=types.SimpleNamespace())
    gi.repository = repo
    # The REAL scipy when it is installed: a stub cached in sys.modules
    # leaks into every later test in the same run, and video_service's
    # motion detector then fails with "scipy.ndimage has no attribute
    # binary_dilation" (the nnvideo detect/zero-copy tests, full runs only).
    try:
        import scipy.ndimage  # noqa: F401
    except Exception:
        scipy = _stub("scipy")
        scipy.ndimage = _stub("scipy.ndimage")
    _stub("nn_accel_pb2")
    import importlib
    return importlib.import_module(modname)
