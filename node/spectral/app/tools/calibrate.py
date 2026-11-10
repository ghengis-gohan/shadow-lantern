#!/usr/bin/env python3
"""
calibrate.py — fit the pixel->wavelength polynomial from known sources (your emitter LEDs).

Day-one procedure:
  1. Run the spectral service against the Uno. Light ONE emitter channel at a time.
  2. For each channel read the peak pixel:   curl -s localhost:8081/latest | python3 -m json.tool | grep -A3 peaks
     (or pass --from-node to let this tool grab the strongest peak itself)
  3. Collect (pixel, nm) pairs and fit:
       calibrate.py --pair 143.2 591 --pair 180.5 611 --pair 61.0 465 --pair 95.4 520 --pair 229.8 770 --pair 22.1 385 \
                    --out calibration.json
     Use the LEDs' MEASURED peak wavelengths from the datasheet (591, 611, ...) not the marketing number.
  4. Restart the service with the new calibration.json (or rebuild the image).

Degree 2 needs >=3 pairs, degree 3 needs >=5. With 6 emitter channels, degree 2 is the honest choice.
"""
import argparse, json, sys, urllib.request
import numpy as np


def grab_peak(url):
    latest = json.load(urllib.request.urlopen(url + "/latest", timeout=3))
    if not latest.get("peaks"):
        sys.exit("no peaks in the latest frame; is a channel lit?")
    pk = max(latest["peaks"], key=lambda p: p["h"])
    return pk["px"], pk["nm"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", nargs=2, type=float, action="append", metavar=("PIXEL", "NM"), default=[])
    ap.add_argument("--from-node", metavar="NM", type=float, action="append", default=[],
                    help="read the strongest peak's pixel from the running node and pair it with NM")
    ap.add_argument("--node", default="http://localhost:8081")
    ap.add_argument("--degree", type=int, default=2)
    ap.add_argument("--out", default="calibration.json")
    a = ap.parse_args()

    pairs = list(a.pair)
    for nm in a.from_node:
        input("Light the %.0f nm channel only, then press Enter..." % nm)
        px, seen = grab_peak(a.node)
        print("  strongest peak at pixel %.2f (currently mapped to %.1f nm)" % (px, seen))
        pairs.append((px, nm))

    if len(pairs) < a.degree + 1:
        sys.exit("need at least %d pairs for degree %d" % (a.degree + 1, a.degree))
    px = np.array([p for p, _ in pairs]); nm = np.array([n for _, n in pairs])
    coef = np.polyfit(px, nm, a.degree)[::-1]            # low order first, matching calibration.json
    resid = nm - sum(c * px ** i for i, c in enumerate(coef))
    print("coefficients:", [float("%.6g" % c) for c in coef])
    print("residuals (nm):", [float("%.2f" % r) for r in resid], " rms %.2f" % np.sqrt((resid ** 2).mean()))
    json.dump({"coefficients": [float(c) for c in coef] + [0.0] * (6 - len(coef)),
               "source": "fit from %d pairs, degree %d" % (len(pairs), a.degree),
               "pairs": [[float(p), float(n)] for p, n in pairs]}, open(a.out, "w"), indent=2)
    print("wrote", a.out)


if __name__ == "__main__":
    main()
