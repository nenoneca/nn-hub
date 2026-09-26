"""Named label sets, so a device advertises an IDENTIFIER instead of 80 strings.

A camera's capability record carries `"labels": "coco80"` (plus the class
count); the hub expands that to names for validation and for the UI. Sending
the full list from an MCU would cost ~900 B of flash and wire per connect for
data that is identical across every device using the model.
"""
from __future__ import annotations

COCO80 = [
    "person", "bicycle", "car", "motorcycle", "airplane", "bus", "train",
    "truck", "boat", "traffic light", "fire hydrant", "stop sign",
    "parking meter", "bench", "bird", "cat", "dog", "horse", "sheep", "cow",
    "elephant", "bear", "zebra", "giraffe", "backpack", "umbrella", "handbag",
    "tie", "suitcase", "frisbee", "skis", "snowboard", "sports ball", "kite",
    "baseball bat", "baseball glove", "skateboard", "surfboard",
    "tennis racket", "bottle", "wine glass", "cup", "fork", "knife", "spoon",
    "bowl", "banana", "apple", "sandwich", "orange", "broccoli", "carrot",
    "hot dog", "pizza", "donut", "cake", "chair", "couch", "potted plant",
    "bed", "dining table", "toilet", "tv", "laptop", "mouse", "remote",
    "keyboard", "cell phone", "microwave", "oven", "toaster", "sink",
    "refrigerator", "book", "clock", "vase", "scissors", "teddy bear",
    "hair drier", "toothbrush",
]

SETS = {"coco80": COCO80}


def _names_from(infer: dict) -> list[str] | None:
    cls = infer.get("classes")
    if isinstance(cls, list) and cls:
        return [c["name"] if isinstance(c, dict) else str(c) for c in cls]
    ls = infer.get("labels")
    if isinstance(ls, str) and ls in SETS:
        return list(SETS[ls])
    if isinstance(cls, int) and cls == 80:
        return list(COCO80)
    return None


def names_for(caps: dict | None) -> list[str] | None:
    """Class names configurable for this camera, or None when unknown.

    A camera has up to two engines and either may define the vocabulary:
    "infer" is the device's edge model, "service_infer" is the media
    service's own detector.  The ESP32 cameras have only the latter — using
    just the device capability left their class picker empty.

    Resolution per engine: explicit list -> named label set -> a count of 80
    (everything we ship is COCO-trained, so cameras flashed before `labels`
    existed stay configurable)."""
    caps = caps or {}
    out, seen = [], set()
    for key in ("infer", "service_infer"):
        got = _names_from(caps.get(key) or {})
        for n in got or []:
            if n not in seen:
                seen.add(n); out.append(n)
    if not out:                       # legacy shape: caps IS the infer dict
        got = _names_from(caps if isinstance(caps, dict) else {})
        return got
    return out
