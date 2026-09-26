"""The access-unit splitter in transcoded_client (boundary rules only —
the real-capture sweep stays a bench tool).  A wrong boundary is silent:
the decoder just discards most frames."""

from tests.media_helper import import_media

tc = import_media("transcoded_client")
boundary = tc.ShmAuWriter._au_boundary      # staticmethod

SPS = b"\x00\x00\x01\x67ss"
PPS = b"\x00\x00\x01\x68pp"
IDR = b"\x00\x00\x01\x65" + b"I" * 20
SLICE = b"\x00\x00\x01\x41" + b"P" * 20


def _cut(buf):
    f = boundary
    try:
        return f(buf)                       # staticmethod / free function
    except TypeError:
        return f(None, buf)                 # plain method fallback


def test_unfinished_au_is_not_cut():
    assert _cut(SPS + PPS + IDR) == -1


def test_next_slice_opens_next_au():
    assert _cut(SPS + PPS + IDR + SLICE) == len(SPS + PPS + IDR)
    assert _cut(SLICE + SLICE) == len(SLICE)


def test_parameter_sets_belong_to_following_au():
    assert _cut(SLICE + SPS + PPS + IDR) == len(SLICE)


def test_four_byte_start_code_cut_at_zero_byte():
    four = b"\x00\x00\x00\x01\x41" + b"P" * 8
    assert _cut(SLICE + four) == len(SLICE)


def test_fragmented_arrival_no_slice_lost():
    stream = (SPS + PPS + IDR + SLICE * 5) * 8
    acc, aus = bytearray(), []
    for i in range(0, len(stream), 7):       # pathological fragmentation
        acc.extend(stream[i:i + 7])
        while True:
            cut = _cut(bytes(acc))
            if cut <= 0:
                break
            aus.append(bytes(acc[:cut]))
            acc = bytearray(acc[cut:])
    if acc:
        aus.append(bytes(acc))
    assert b"".join(aus) == stream           # nothing lost or reordered
