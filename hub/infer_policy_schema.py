"""Validation + normalisation for the per-camera inference policy doc.

Shape (see nn-media-stream/docs/INFERENCE_POLICY_DESIGN.md):

    {"version": 7,
     "engines": {"edge":    {"enabled": true,  "classes": {...}},
                 "service": {"enabled": false, "classes": {...}}}}

    class entry: {"capture": bool, "agg": 1..max_agg, "start": 0..1,
                  "stop": 0..1 (<= start)}

Policy is CLAMPED rather than rejected wherever a value is merely
out-of-range: a UI can't produce an invalid pair, and an API client is
better served by "here is what I stored" than by a 400 it has to interpret.
Genuinely unknown input (a class the device's model can't detect, a
malformed engine) IS rejected — silently dropping it would leave the user
believing a rule is active when nothing evaluates it.
"""
from __future__ import annotations

AGG_MAX_DEFAULT = 30
ENGINES = ("edge", "service")

# Inference rate cap, per camera AND per engine.  The camera may deliver 30
# fps; running the detector on every frame burns the accelerator for no gain
# and makes `agg` cover a far shorter time window than it appears to.
FPS_MIN, FPS_MAX, FPS_DEFAULT = 1, 15, 5


class PolicyError(ValueError):
    pass


def _clamp(v, lo, hi):
    return lo if v < lo else (hi if v > hi else v)


def normalise_class(entry: dict, max_agg: int = AGG_MAX_DEFAULT) -> dict:
    if not isinstance(entry, dict):
        raise PolicyError("class entry must be an object")
    try:
        agg = int(entry.get("agg", 5) or 5)
        start = float(entry.get("start", 0.6))
        stop = float(entry.get("stop", start))
    except (TypeError, ValueError):
        raise PolicyError("agg/start/stop must be numbers")
    start = _clamp(start, 0.0, 1.0)
    stop = _clamp(stop, 0.0, 1.0)
    return {
        "capture": bool(entry.get("capture", True)),
        "agg": _clamp(agg, 1, max(1, int(max_agg))),
        "start": round(start, 3),
        # the invariant the device also re-clamps locally
        "stop": round(min(stop, start), 3),
    }


def normalise(doc: dict, caps: dict | None = None,
              prev: dict | None = None) -> dict:
    """Validate/normalise a policy doc, bumping version off `prev`.

    caps: the device's advertised capability doc ({"infer": {...}} or the
    inner dict) — used to bound `agg` and to reject classes the model does
    not support.  When caps are unknown (device never connected) any class
    name is accepted, since refusing would make a camera unconfigurable
    until it comes online.
    """
    if not isinstance(doc, dict):
        raise PolicyError("policy must be an object")
    infer = (caps or {}).get("infer", caps) or {}
    max_agg = int(infer.get("max_agg") or AGG_MAX_DEFAULT)
    from .labelsets import names_for
    names = names_for(caps)
    # "motion" is a pseudo-class: the service's motion ratio fed through the
    # SAME aggregation/hysteresis as an object class, so noise can't hold a
    # recording open by crossing an instantaneous threshold once.
    known = (set(names) | {"motion"}) if names else None

    engines_in = doc.get("engines") or {}
    if not isinstance(engines_in, dict):
        raise PolicyError("engines must be an object")
    for name in engines_in:
        if name not in ENGINES:
            raise PolicyError(f"unknown engine {name!r}")

    out_engines = {}
    for name in ENGINES:
        e = engines_in.get(name)
        if e is None:
            continue
        if not isinstance(e, dict):
            raise PolicyError(f"engine {name} must be an object")
        classes_in = e.get("classes") or {}
        if not isinstance(classes_in, dict):
            raise PolicyError(f"engine {name}: classes must be an object")
        classes = {}
        for cname, entry in classes_in.items():
            if known is not None and cname not in known:
                raise PolicyError(
                    f"class {cname!r} is not supported by this device's model")
            classes[cname] = normalise_class(entry, max_agg)
        try:
            fps = int(e.get("fps", FPS_DEFAULT) or FPS_DEFAULT)
        except (TypeError, ValueError):
            fps = FPS_DEFAULT
        out_engines[name] = {"enabled": bool(e.get("enabled", True)),
                             "fps": _clamp(fps, FPS_MIN, FPS_MAX),
                             "classes": classes}

    prev_ver = int((prev or {}).get("version") or 0)
    return {"version": prev_ver + 1, "engines": out_engines}


def class_names(caps: dict | None = None) -> list[str]:
    """Class names for the UI ([] when the device hasn't told us)."""
    from .labelsets import names_for
    return names_for(caps) or []


def defaults(caps: dict | None = None, classes=("person",)) -> dict:
    """A sane starting policy: capture the named classes on whichever
    engines exist, everything else inert."""
    infer = (caps or {}).get("infer", caps) or {}
    max_agg = int(infer.get("max_agg") or AGG_MAX_DEFAULT)
    cls = {c: normalise_class({"capture": True, "agg": 5, "start": 0.6,
                               "stop": 0.4}, max_agg) for c in classes}
    return {"version": 1,
            "engines": {e: {"enabled": True, "fps": FPS_DEFAULT,
                            "classes": dict(cls)}
                        for e in ENGINES}}
