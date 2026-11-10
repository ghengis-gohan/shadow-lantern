#!/usr/bin/env python3
"""
selftest.py — push every sim profile in lines.json through reduce_frame + match_spectrum and print the verdict.
Run after editing lines.json or the matcher. Exit code 1 if a profile's expected class is wrong.

    python3 node/spectral/app/tools/selftest.py

Expectation per profile comes from the profile name: held-out and unlisted profiles must read "unknown",
continuum-only must read "continuum", empty must read "quiet", everything else must match its own name.
"""
import json, os, sys
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import spectral as S  # noqa: E402

table = json.load(open(S.CFG["lines_file"]))
wl = S.pixel_wavelengths(json.load(open(S.CFG["cal_file"])))
sigma = table["sim"].get("line_fwhm_nm", 15.0) / 2.355
rng = np.random.default_rng(7)
held = {c for c, s in table["classes"].items() if s.get("held_out")}
bad = 0

print("%-14s %-12s %-5s %-5s %s" % ("profile", "class", "p", "nov", "materials"))
for name, prof in table["sim"]["profiles"].items():
    sig = np.full(S.N_PX, prof.get("continuum", 0.0))
    for wl0, amp in prof["lines"]:
        sig += amp * np.exp(-0.5 * ((wl - wl0) / sigma) ** 2)
    raw = np.clip(60 + 800 * sig + rng.normal(0, 3.0, S.N_PX), 0, 1023)
    norm, peaks, noise, cont = S.reduce_frame(raw, None, wl, S.CFG)
    cls, p, mats, lines, nov, scores = S.match_spectrum(peaks, cont, table, S.CFG)

    if not prof["lines"]:
        want = "continuum" if prof.get("continuum", 0) >= S.CFG["cont_thr"] else "quiet"
    elif name in held or name not in table["classes"]:
        want = "unknown"
    else:
        want = name
    ok = cls == want
    bad += not ok
    extra = "" if ok else "   <-- expected %s" % want
    if name in held:
        extra += "   (held-out score %.2f)" % scores.get(name, 0)
    print("%-14s %-12s %-5.2f %-5.2f %s%s" % (name, cls, p, nov, " ".join(lines), extra))

print("\n%s" % ("all profiles as expected" if not bad else "%d mismatch(es)" % bad))
sys.exit(1 if bad else 0)
