"""The one thing that crosses from a handler into a pipeline, and from one
pipeline into the next.  Pipelines never reach into a camera object: every
input is in the request, every setting is read from the store at run start."""
from __future__ import annotations

import itertools
import time
from dataclasses import dataclass, field
from typing import Any

_ids = itertools.count(1)


def next_id() -> int:
    return next(_ids)


class Kind:
    """What the data is.  Pipelines dispatch on it; queues apply their drop
    policy per (camera, kind)."""
    AU = "au"                    # one H.264 access unit
    AUDIO = "audio"              # one AAC/ADTS record
    STATUS = "status"            # device NN_REC_STATUS (settings, caps, errors)
    HEARTBEAT = "heartbeat"      # device 'H' record
    DETECT_RECORD = "detect_record"   # device-side inference result record
    FRAME = "frame"              # decoded frame for detection
    MOTION = "motion"            # motion result (ratio, boxes)
    BOXES = "boxes"              # object boxes from inference
    SEGMENT = "segment"          # an HLS segment appeared
    UPLOAD = "upload"            # an event recording ready to upload
    COMMAND = "command"          # an operator/pipeline command for the camera
    TICK = "tick"                # periodic timer
    CONNECT = "connect"          # handler: a camera session started
    DISCONNECT = "disconnect"    # handler: a camera session ended


@dataclass(slots=True)
class Request:
    camera_id: str
    pipeline: str                # identifier: which pipeline this wants
    kind: str
    data: Any = None             # bytes/memoryview for media, dict for control
    ts_device: float | None = None
    meta: dict = field(default_factory=dict)
    id: int = field(default_factory=next_id)
    ts_arrival: float = field(default_factory=time.time)

    def emit(self, pipeline: str, kind: str, data: Any = None, **meta) -> "Request":
        """A follow-on request that keeps the camera and device timestamp."""
        m = dict(self.meta); m.update(meta); m.setdefault("parent", self.id)
        return Request(self.camera_id, pipeline, kind, data, self.ts_device, m)

    def __repr__(self) -> str:                   # compact, for logs
        return f"<Request #{self.id} {self.camera_id}/{self.pipeline} {self.kind}>"
