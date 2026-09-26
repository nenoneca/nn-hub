"""A decoded frame that is never copied on its way to detection or inference.

Two sources, measured on the OrangePi (CIX P1, GStreamer 1.22, 2026-09-21):

* GRAY8 from the hardware decoder with capture-io-mode=dmabuf: the buffer is
  a DMABUF; Python mmaps the descriptor once per pool buffer (they are
  reused, so the map cache stays small) and reads luma as a numpy view.
  Nothing is copied — the decoder's own pages are what the detector reads.
* I420 (or any format the decoder will not export as DMABUF): gst-python's
  Buffer.map() hands back a `bytes` object, i.e. one copy made by the
  binding; the frame keeps that object and every consumer reads views of
  it.  No second copy.

A Frame holds the GstSample (so the decoder cannot recycle the buffer
underneath us) until `release()`; the detect pipeline releases it right
after the motion tick unless inference needs it, and the inference client
releases it when the request has been written into the shared pool.
`__del__` is the safety net for a frame dropped from a queue.
"""
from __future__ import annotations

import fcntl
import mmap
import struct
from typing import Optional

import numpy as np

# DMA_BUF_IOCTL_SYNC = _IOW('b', 0, struct dma_buf_sync{u64 flags})
_DMA_BUF_IOCTL_SYNC = 0x40086200
_SYNC_READ, _SYNC_START, _SYNC_END = 1, 0, 4


class DmaMaps:
    """mmap cache keyed by descriptor: a decoder's capture pool reuses the
    same buffers (and descriptors) for the life of the graph."""

    def __init__(self):
        self._maps: dict[tuple[int, int], mmap.mmap] = {}

    def get(self, fd: int, size: int) -> mmap.mmap:
        m = self._maps.get((fd, size))
        if m is None or m.closed:
            m = mmap.mmap(fd, size, mmap.MAP_SHARED, mmap.PROT_READ)
            self._maps[(fd, size)] = m
        return m

    def close(self) -> None:
        for m in self._maps.values():
            try:
                m.close()
            except Exception:
                pass
        self._maps.clear()


def _sync(fd: int, flags: int) -> None:
    try:
        fcntl.ioctl(fd, _DMA_BUF_IOCTL_SYNC, struct.pack("Q", flags))
    except OSError:
        pass                       # not a dmabuf, or the driver does not need it


class Frame:
    __slots__ = ("fmt", "w", "h", "stride", "_mem", "_sample", "_fd", "released", "ts_ms")

    def __init__(self, fmt: str, w: int, h: int, mem, *, sample=None, fd: int = -1,
                 stride: int | None = None, ts_ms: int = 0):
        self.fmt, self.w, self.h = fmt, w, h
        self.stride = stride or w
        self._mem = mem                  # mmap (GRAY8 dmabuf) or bytes (I420 map) or ndarray
        self._sample = sample
        self._fd = fd
        self.released = False
        self.ts_ms = ts_ms
        if fd >= 0:
            _sync(fd, _SYNC_START | _SYNC_READ)

    # ── constructors ─────────────────────────────────────────────────────
    @classmethod
    def from_i420_array(cls, yuv: np.ndarray, w: int, h: int, ts_ms: int = 0) -> "Frame":
        return cls("I420", w, h, np.ascontiguousarray(yuv), ts_ms=ts_ms)

    @classmethod
    def from_luma_array(cls, y: np.ndarray, w: int, h: int, ts_ms: int = 0) -> "Frame":
        return cls("GRAY8", w, h, np.ascontiguousarray(y), ts_ms=ts_ms)

    # ── views ────────────────────────────────────────────────────────────
    def _flat(self) -> np.ndarray:
        return np.frombuffer(self._mem, dtype=np.uint8) if not isinstance(self._mem, np.ndarray) \
            else self._mem.reshape(-1)

    @property
    def y(self) -> np.ndarray:
        """Luma (h, w) as a view."""
        n = self.h * self.stride
        return self._flat()[:n].reshape(self.h, self.stride)[:, :self.w]

    def chroma(self) -> tuple[np.ndarray, np.ndarray] | None:
        """(u, v) at half resolution as views, or None for a luma-only frame."""
        if self.fmt != "I420":
            return None
        f = self._flat()
        h2, s2 = self.h // 2, self.stride // 2
        yo = self.h * self.stride
        u = f[yo:yo + h2 * s2].reshape(h2, s2)[:, :self.w // 2]
        v = f[yo + h2 * s2:yo + 2 * h2 * s2].reshape(h2, s2)[:, :self.w // 2]
        return u, v

    def i420(self) -> np.ndarray:
        """The (h*3/2, w) I420 array the legacy helpers expect: a view when
        the layout is tight, a luma-only (h, w) view for GRAY8."""
        if self.fmt != "I420":
            return self.y
        if self.stride == self.w:
            return self._flat()[:self.h * 3 // 2 * self.w].reshape(self.h * 3 // 2, self.w)
        u, v = self.chroma()
        return np.concatenate([self.y.reshape(-1), u.reshape(-1), v.reshape(-1)]).reshape(self.h * 3 // 2, self.w)

    @property
    def shape(self) -> tuple[int, int, int]:
        """What the event engine reads for its box coordinate space."""
        return (self.h, self.w, 3)

    # ── inference input ──────────────────────────────────────────────────
    def letterbox_scale(self, size: int) -> tuple[float, int, int]:
        s = min(size / self.w, size / self.h)
        return s, int(self.w * s), int(self.h * s)

    def write_letterbox(self, dst: np.ndarray, size: int, pad: bool = True) -> float:
        """Write the frame, scaled to fit `size`x`size`, into `dst`
        (size, size, 3) uint8 — the inference pool slot itself.  Scaling is
        done on the planes BEFORE colour conversion, so the conversion runs
        on 0.4 MP instead of 2.5 MP, and the result lands where the
        inference daemon reads it: no RGB frame, no canvas, no tobytes().
        Returns the scale factor for undoing boxes.  `pad=False` skips the
        border (written once per slot by the caller)."""
        s, nw, nh = self.letterbox_scale(size)
        iy = (np.arange(nh) / s).astype(np.intp)
        ix = (np.arange(nw) / s).astype(np.intp)
        ys = self.y[iy][:, ix]                                  # (nh, nw) sampled luma, uint8 view-gather
        out = dst[:nh, :nw]
        ch = self.chroma()
        if ch is None:
            out[:, :, 0] = ys; out[:, :, 1] = ys; out[:, :, 2] = ys
        else:
            u, v = ch
            uy = np.minimum(iy // 2, u.shape[0] - 1); ux = np.minimum(ix // 2, u.shape[1] - 1)
            y32 = ys.astype(np.int32)
            us = u[uy][:, ux].astype(np.int32) - 128
            vs = v[uy][:, ux].astype(np.int32) - 128
            # BT.601, integer arithmetic (x256), same coefficients as yuv420_to_rgb
            out[:, :, 0] = np.clip(y32 + ((359 * vs) >> 8), 0, 255)
            out[:, :, 1] = np.clip(y32 - ((88 * us + 183 * vs) >> 8), 0, 255)
            out[:, :, 2] = np.clip(y32 + ((454 * us) >> 8), 0, 255)
        if pad:
            if nh < size:
                dst[nh:, :, :] = 114
            if nw < size:
                dst[:nh, nw:, :] = 114
        return s

    def rgb(self) -> np.ndarray:
        """Full-size RGB (a new array) for consumers that still need one."""
        from yolox_detector import yuv420_to_rgb
        return yuv420_to_rgb(self.i420(), self.w, self.h)

    # ── lifetime ─────────────────────────────────────────────────────────
    def release(self) -> None:
        if self.released:
            return
        self.released = True
        if self._fd >= 0:
            _sync(self._fd, _SYNC_END | _SYNC_READ)
        self._sample = None          # the decoder may recycle the buffer now
        self._mem = None

    def __del__(self):
        try:
            self.release()
        except Exception:
            pass
