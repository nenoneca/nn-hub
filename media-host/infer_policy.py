"""Per-class capture policy with hysteresis — Python mirror of
nn_infer/src/policy.c.

The two implementations MUST agree bit for bit: the device may evaluate the
policy (edge inference) or the service may (service inference), and a class
must not behave differently depending on who ran it.  Both keep the ring in
conf_x1000 INTEGERS with an O(1) running sum, so neither drifts.

tests/policy_fixture.json is executed against both.
"""
from __future__ import annotations

AGG_MAX = 30


class ClassPolicy:
    __slots__ = ("capture", "agg", "start", "stop", "_ring", "_head", "_sum",
                 "detected")

    def __init__(self, capture: bool = True, agg: int = 5,
                 start_x1000: int = 600, stop_x1000: int = 400):
        self.capture = bool(capture)
        self.agg = min(max(int(agg) or 5, 1), AGG_MAX)
        self.start = int(start_x1000)
        # invariant: stop <= start (a config that violates it would make a
        # class that can never leave detect)
        self.stop = min(int(stop_x1000), self.start)
        self._ring = [0] * self.agg
        self._head = 0
        self._sum = 0
        self.detected = False

    def feed(self, conf_x1000: int) -> tuple[int, bool]:
        """Feed this frame's highest confidence for the class (0 = absent).

        Returns (aggregate_x1000, state_changed).  O(1): running sum, no
        rescan of the window."""
        c = int(conf_x1000)
        self._sum -= self._ring[self._head]       # value leaving the window
        self._sum += c
        self._ring[self._head] = c                # push back
        self._head = (self._head + 1) % self.agg

        agg = self._sum // self.agg
        was = self.detected
        if not self.detected:
            if agg >= self.start:
                self.detected = True
            elif agg < self.stop:
                pass
        else:
            if agg < self.stop:
                self.detected = False
        return agg, (was != self.detected)
