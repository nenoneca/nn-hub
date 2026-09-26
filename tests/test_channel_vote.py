from hub import channel_vote as cv


def _scan(voter, **dbm):
    d = {ch: None for ch in cv.CHANNELS}
    d.update({int(k[2:]): v for k, v in dbm.items()})
    return {"voter": voter, "weight": 1.0, "dbm": d}


def test_parse_allowed():
    assert cv.parse_allowed("11-13,25") == {11, 12, 13, 25}
    assert cv.parse_allowed("") == set(range(11, 27))
    assert cv.parse_allowed("9-12,30") == {11, 12}


def test_scan_from_bytes_signed_and_missing():
    b = bytes([(-96) & 0xff] * 15 + [127])
    s = cv.scan_from_bytes(b)
    assert s[11] == -96 and s[26] is None


def test_borda_points_and_order():
    scans = [_scan("a", ch15=-90, ch20=-95, ch25=-99),
             _scan("b", ch15=-97, ch20=-94, ch25=-98),
             _scan("c", ch15=-80, ch20=-96, ch25=-93)]
    r = cv.vote(scans, k=3, allowed={15, 20, 25}, quiet_dbm=-120)
    pts = {x["channel"]: x["points"] for x in r}
    # a: 25>20>15  b: 25>15>20  c: 20>25>15
    assert pts == {25: 3 + 3 + 2, 20: 2 + 1 + 3, 15: 1 + 2 + 1}
    assert r[0]["channel"] == 25


def test_veto_excludes_channel_and_forces_move():
    scans = [_scan("a", ch15=-40, ch25=-95), _scan("b", ch15=-99, ch25=-96)]
    r = cv.vote(scans, k=2, allowed={15, 25}, veto_dbm=-60)
    assert r[-1]["channel"] == 15 and r[-1]["vetoed_by"] == ["a"]
    d = cv.decide(15, r, k=2)
    assert d["move"] and d["winner"] == 25 and "busy level" in d["reason"]


def test_hysteresis_keeps_current_when_close():
    scans = [_scan("a", ch20=-95, ch25=-96), _scan("b", ch20=-96, ch25=-95)]
    r = cv.vote(scans, k=2, allowed={20, 25}, quiet_dbm=-120)
    d = cv.decide(20, r, k=2, margin=1, quiet_dbm=-120)
    assert d["move"] is False


def test_current_already_best_and_no_usable():
    scans = [_scan("a", ch25=-99, ch20=-80)]
    d = cv.decide(25, cv.vote(scans, k=3, allowed={20, 25}))
    assert d["move"] is False and d["winner"] == 25
    d2 = cv.decide(25, cv.vote([_scan("a", ch25=-30)], allowed={25}))
    assert d2["winner"] is None and d2["move"] is False


def test_weight_scales_points():
    s = _scan("gw", ch25=-99, ch20=-90); s["weight"] = 2.0
    r = cv.vote([s], k=2, allowed={20, 25}, quiet_dbm=-120)
    assert {x["channel"]: x["points"] for x in r} == {25: 4.0, 20: 2.0}


def test_quiet_floor_ties_and_clear_current_stays():
    # one voter: 25 at -90, 11 at -99 -- both below the -85 quiet level
    scans = [_scan("gw", ch11=-99, ch24=-99, ch25=-90, ch12=-80)]
    r = cv.vote(scans, k=3, allowed={11, 12, 24, 25})
    pts = {x["channel"]: x["points"] for x in r}
    assert pts[11] == pts[24] == pts[25] == 2.0 and pts[12] == 0
    d = cv.decide(25, r, k=3)
    assert d["move"] is False and "clear at every device" in d["reason"]


def test_noisy_current_moves_to_clear():
    scans = [_scan("a", ch15=-70, ch25=-95), _scan("b", ch15=-75, ch25=-97)]
    d = cv.decide(15, cv.vote(scans, k=1, allowed={15, 25}), k=1)
    assert d["move"] and d["winner"] == 25
