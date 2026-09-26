"""nnvideo — one process, every camera: generic pipelines with queues.

Design: https://claude.ai/code/artifact/586ea33c-e7de-4030-8963-0b784fa6f612

    camera handler ──Request(camera_id, pipeline, kind, data)──▶ PipelineQueue
                                                                    │ N workers
                                                          Pipeline.run(req, settings)
                                                                    │ emits
                                                          next PipelineQueue …

Settings live in a SQLite store (`Store`) and are read at the start of every
run (`Settings.get`), so a change is live on the next request and survives a
restart.  Ports are never fixed per camera: connection facts live in the
volatile `Registry`.
"""
from .request import Request, Kind, next_id          # noqa: F401
from .queue import PipelineQueue, Policy, QueueFull   # noqa: F401
from .settings import Store, Settings                 # noqa: F401
from .pipeline import Pipeline, Engine, Mode          # noqa: F401
from .registry import Registry                        # noqa: F401
