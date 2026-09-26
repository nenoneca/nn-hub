"""Zero-copy frame path: views over the decoder's buffer, the letterbox
written straight into the inference pool, the record parser's direct path,
and the motion detector keeping only a downscaled luma between ticks."""
import mmap
import os
import struct
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "media-host"))
from nnvideo.frame import DmaMaps, Frame  # noqa: E402


def _i420(w, h, seed=1):
    rng = np.random.default_rng(seed)
    return rng.integers(0, 256, (h * 3 // 2, w), np.uint8)


def test_i420_frame_views_share_memory_and_letterbox_matches_reference():
    from yolox_detector import yuv420_to_rgb
    w, h = 96, 64
    yuv = _i420(w, h)
    fr = Frame.from_i420_array(yuv, w, h)
    assert np.shares_memory(fr.y, fr._mem) and fr.y.shape == (h, w), "luma is a view, not a copy"
    u, v = fr.chroma()
    assert u.shape == (h // 2, w // 2) and np.shares_memory(u, fr._mem)
    assert np.array_equal(fr.i420(), yuv) and np.shares_memory(fr.i420(), fr._mem)
    assert fr.shape == (h, w, 3)
    # letterbox into a pool slot: compare with the reference conversion sampled the same way
    size = 48
    dst = np.zeros((size, size, 3), np.uint8)
    s = fr.write_letterbox(dst, size)
    nw, nh = int(w * s), int(h * s)
    ref = yuv420_to_rgb(yuv, w, h)
    iy = (np.arange(nh) / s).astype(int); ix = (np.arange(nw) / s).astype(int)
    ref_small = ref[iy][:, ix].astype(int)
    diff = np.abs(dst[:nh, :nw].astype(int) - ref_small)
    assert diff.max() <= 3, f"integer BT.601 within rounding of the float reference (max diff {diff.max()})"
    assert (dst[nh:, :, :] == 114).all(), "bottom pad written"
    # a second write with pad=False leaves the border alone
    dst[nh:, :, :] = 7
    fr2 = Frame.from_i420_array(_i420(w, h, seed=2), w, h)
    fr2.write_letterbox(dst, size, pad=False)
    assert (dst[nh:, :, :] == 7).all()


def test_gray8_frame_over_an_mmap_is_a_view_and_release_is_idempotent():
    w, h = 32, 16
    fd = os.memfd_create("frame", 0)
    os.ftruncate(fd, w * h)
    luma = np.arange(w * h, dtype=np.uint8).reshape(h, w)
    os.pwrite(fd, luma.tobytes(), 0)
    maps = DmaMaps()
    mm = maps.get(fd, w * h)
    assert maps.get(fd, w * h) is mm, "one mapping per descriptor"
    fr = Frame("GRAY8", w, h, mm, fd=-1)          # fd=-1: no dmabuf sync ioctl on a memfd
    assert np.array_equal(fr.y, luma) and fr.chroma() is None and fr.i420().shape == (h, w)
    dst = np.zeros((16, 16, 3), np.uint8)
    s = fr.write_letterbox(dst, 16)
    assert s == 0.5 and (dst[:8, :16, 0] == dst[:8, :16, 1]).all(), "luma replicated to grey RGB"
    fr.release(); fr.release()
    assert fr.released and fr._mem is None
    maps.close(); os.close(fd)


def test_padded_stride_frame_views_skip_the_padding():
    w, h, stride = 20, 8, 32
    buf = np.zeros((h * 3 // 2) * stride, np.uint8)
    y = np.arange(h * w, dtype=np.uint8).reshape(h, w)
    buf[:h * stride].reshape(h, stride)[:, :w] = y
    fr = Frame("I420", w, h, buf.tobytes(), stride=stride)
    assert np.array_equal(fr.y, y)
    assert fr.i420().shape == (h * 3 // 2, w)


def test_record_parser_direct_path_matches_reassembly():
    from video_service import RecordParser, NN_REC_VIDEO, NN_REC_STATUS
    def rec(typ, flags, seq, ts, pl):
        return struct.pack("<BBHQI", typ, flags, seq, ts, len(pl)) + pl
    stream = rec(NN_REC_VIDEO, 2, 1, 100, b"a" * 5000) + rec(NN_REC_STATUS, 0, 2, 101, b"{}") + rec(NN_REC_VIDEO, 4, 3, 102, b"b" * 300)
    want = [(NN_REC_VIDEO, 2, 1, 100, b"a" * 5000), (NN_REC_STATUS, 0, 2, 101, b"{}"), (NN_REC_VIDEO, 4, 3, 102, b"b" * 300)]
    # whole records per feed: the direct path, no reassembly buffer touched
    p = RecordParser()
    assert p.feed(stream) == want and p.buf == bytearray()
    # arbitrary splits: identical output
    for cut in (1, 15, 16, 17, 5015, 5030, len(stream) - 1):
        p = RecordParser()
        out = p.feed(stream[:cut]) + p.feed(stream[cut:])
        assert out == want, f"split at {cut}"
    # a tail that spans stays pending and completes on the next feed
    p = RecordParser()
    out = p.feed(stream[:5100])
    assert len(out) == 2 and p.buf
    assert p.feed(stream[5100:]) == want[2:]


def test_motion_detector_keeps_only_downscaled_luma_and_hands_the_frame_on():
    from video_service import MotionDetector
    w, h = 64, 48
    det = MotionDetector(interval_ms=0, diff_thresh=18, proc_width=32)
    got = []
    det.on_tick = lambda ts, ratio, boxes, fr: got.append((ratio, boxes, fr))
    def frame(shift):
        y = np.zeros((h, w), np.uint8); y[10:30, 10 + shift:30 + shift] = 200
        return Frame.from_i420_array(np.concatenate([y.reshape(-1), np.full(w * h // 2, 128, np.uint8)]).reshape(h * 3 // 2, w), w, h)
    f0, f1, f2 = frame(0), frame(0), frame(20)
    det.feed_frame(f0)
    assert f0.released and det._prev_small is not None and det._prev_small.dtype == np.int16
    assert det._prev.shape == (24, 32), "debug routes get the downscaled luma"
    det.feed_frame(f1); det.feed_frame(f2)
    assert got[0][0] == 0.0 and got[1][0] > 0 and got[1][1]
    assert got[1][2] is f2 and not f2.released, "the tick's frame is handed to the callback, not released"
    assert got[0][2] is f1 and not f1.released, "every tick hands its frame on; the consumer releases it"


def test_single_process_prints_carry_the_camera_tag(capsys):
    import threading
    import video_service as vs
    from nnvideo.pipeline import current_camera
    vs.SINGLE_PROCESS = True
    try:
        tok = current_camera.set("cam9")
        vs.print(">> hello from a pipeline run")
        current_camera.reset(tok)
        done = threading.Event()
        threading.Thread(target=lambda: (vs.print(">> hello from a helper"), done.set()), name="hls-ts:cam7").start()
        done.wait(2)
        vs.print(">> untagged")
    finally:
        vs.SINGLE_PROCESS = False
    out = capsys.readouterr().out
    assert "[cam9/svc] >> hello from a pipeline run" in out
    assert "[cam7/svc] >> hello from a helper" in out
    assert "\n>> untagged" in out
