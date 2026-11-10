#!/usr/bin/env python3
"""
spectral.py — Lantern spectral node service.

Pipeline, all of it visible:
  frame (288 ADC values from the Uno)  ->  dark subtract  ->  baseline removal  ->  peak find
  ->  match peaks against lines.json  ->  class / materials / novelty  ->  compact event
  ->  SQLite priority queue  ->  acked uplink (UDP or XBee serial)  ->  ground station

There is no trained model here. The "model slot" is match_spectrum(); a classifier that returns
(cls, p, materials, lines, novelty) from (peaks, continuum) replaces that one function later.

Sources (SPECTRO_SOURCE):
  serial:///dev/spectro?baud=115200   the Uno
  replay:///path/to/frames.txt        recorded raw lines, looped (RECORD_FILE writes them)
  sim                                 synthetic frames from lines.json "sim" profiles

Uplink (UPLINK):
  udp://host:port                      WiFi / bench testing
  serial:///dev/xbee?baud=9600         XBee transparent mode, NDJSON both ways

Protocol with the ground (NDJSON, one object per line):
  node -> ground   {"hb":t,"n":id,"q":pending}            1 Hz heartbeat, q = unacked events
  node -> ground   {"t":..,"n":id,"s":seq,"cls":..,"p":..,"m":[..],"l":[..],"nov":..}   event
  node -> ground   {"sp":{"n":id,"s":seq,"t":t,"v":[72 ints]}}  downsampled spectrum, bulk, when idle
  ground -> node   {"ack":seq}  {"hb":t}  {"cmd":"integ","us":20000}  {"cmd":"dark"}
Link is "up" when anything arrived from the ground within LINK_TIMEOUT_S. Events are resent until
acked, oldest first, so an outage costs latency, never data.
"""
import json, logging, os, socket, sqlite3, sys, threading, time, queue
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import numpy as np

log = logging.getLogger("spectral")
N_PX = 288


def env(k, d=None, cast=str):
    v = os.environ.get(k)
    return d if v in (None, "") else cast(v)


CFG = {
    "node_id":        env("NODE_ID", "spec-1"),
    "source":         env("SPECTRO_SOURCE", "serial:///dev/spectro?baud=115200"),
    "uplink":         env("UPLINK", "udp://127.0.0.1:5005"),
    "db":             env("TELEMETRY_DB", "/var/telemetry/spectral.db"),
    "lines_file":     env("LINES_FILE", os.path.join(os.path.dirname(__file__), "lines.json")),
    "cal_file":       env("CAL_FILE", os.path.join(os.path.dirname(__file__), "calibration.json")),
    "dark_file":      env("DARK_FILE", "/var/telemetry/dark.npy"),
    "record_file":    env("RECORD_FILE", ""),
    "http_port":      env("HTTP_PORT", 8081, int),
    "tol_nm":         env("MATCH_TOL_NM", 8.0, float),
    "peak_rel":       env("PEAK_MIN_REL", 0.15, float),
    "peak_snr":       env("PEAK_MIN_SNR", 5.0, float),
    "peak_abs":       env("PEAK_MIN_ABS", 20.0, float),     # ADC counts (~2 % of 10-bit full scale); below this is noise
    "debounce":       env("EVENT_DEBOUNCE_FRAMES", 2, int),
    "novelty_thr":    env("NOVELTY_THRESHOLD", 0.35, float),
    "cont_thr":       env("CONTINUUM_THRESHOLD", 0.08, float),
    "link_timeout":   env("LINK_TIMEOUT_S", 5.0, float),
    "bulk_period":    env("BULK_PERIOD_S", 2.0, float),
    "keepalive":      env("EVENT_KEEPALIVE_S", 5.0, float),
    "anomaly_period": env("ANOMALY_REPEAT_S", 2.0, float),
    "sim_speed":      env("SIM_HZ", 5.0, float),
}


# ----------------------------------------------------------------------------- frame sources
def parse_frame(line: bytes):
    """'S,seq,integ,v0..v287' -> (kind, seq, integ_us, ndarray) or None."""
    try:
        parts = line.decode("ascii", "ignore").strip().split(",")
        if len(parts) != N_PX + 3 or parts[0] not in ("S", "D"):
            return None
        return parts[0], int(parts[1]), int(parts[2]), np.asarray(parts[3:], dtype=np.float64)
    except ValueError:
        return None


class SerialSource:
    def __init__(self, url):
        import serial
        u = urlparse(url)
        baud = int(parse_qs(u.query).get("baud", ["115200"])[0])
        self.ser = serial.Serial(u.path, baud, timeout=2)
        self.bad = 0
        log.info("serial source %s @ %d", u.path, baud)

    def frames(self):
        while True:
            line = self.ser.readline()
            if not line:
                continue
            if line.startswith(b"#"):
                log.info("uno: %s", line.decode(errors="ignore").strip())
                continue
            fr = parse_frame(line)
            if fr is None:
                self.bad += 1
                continue
            yield line, fr

    def command(self, s):
        self.ser.write((s + "\n").encode())


class ReplaySource:
    def __init__(self, url, hz):
        self.path = urlparse(url).path
        self.period = 1.0 / hz
        log.info("replay source %s", self.path)

    def frames(self):
        while True:
            with open(self.path, "rb") as f:
                for line in f:
                    fr = parse_frame(line)
                    if fr:
                        yield line, fr
                        time.sleep(self.period)

    def command(self, s):
        pass


class SimSource:
    """Synthesizes raw ADC frames from lines.json 'sim' profiles on the configured schedule."""

    def __init__(self, table, wl, hz):
        self.sim = table["sim"]
        self.wl = wl
        self.period = 1.0 / hz
        self.sigma = self.sim.get("line_fwhm_nm", 15.0) / 2.355
        self.seq = 0
        self.integ = 20000
        self.current = None
        log.info("sim source: schedule %s", [p for p, _ in self.sim["schedule"]])

    def frames(self):
        rng = np.random.default_rng(1)
        while True:
            for name, secs in self.sim["schedule"]:
                self.current = name
                prof = self.sim["profiles"][name]
                t_end = time.time() + secs
                while time.time() < t_end:
                    sig = np.full(N_PX, prof.get("continuum", 0.0))
                    for wl0, amp in prof["lines"]:
                        sig += amp * np.exp(-0.5 * ((self.wl - wl0) / self.sigma) ** 2)
                    raw = 60 + 800 * sig + rng.normal(0, 3.0, N_PX)      # dark offset, 10-bit-ish scale, noise
                    raw = np.clip(raw, 0, 1023)
                    fr = ("S", self.seq, self.integ, raw)
                    line = ("S,%d,%d," % (self.seq, self.integ) + ",".join(str(int(v)) for v in raw) + "\n").encode()
                    self.seq += 1
                    yield line, fr
                    time.sleep(self.period)

    def command(self, s):
        if s.startswith("I"):
            self.integ = int(s[1:])


# ----------------------------------------------------------------------------- processing
def pixel_wavelengths(cal):
    p = np.arange(N_PX, dtype=np.float64)
    return sum(c * p ** i for i, c in enumerate(cal["coefficients"]))


def rolling_min(x, w):
    from numpy.lib.stride_tricks import sliding_window_view
    pad = w // 2
    return sliding_window_view(np.pad(x, pad, mode="edge"), w).min(axis=1)


def rolling_mean(x, w):
    pad = w // 2
    return np.convolve(np.pad(x, pad, mode="edge"), np.ones(w) / w, mode="valid")


def reduce_frame(raw, dark, wl, cfg):
    """raw -> (normalized spectrum, peaks[(wl, height, pixel)], noise_sigma, continuum_level)."""
    x = raw - (dark if dark is not None else 0.0)
    base = rolling_mean(rolling_min(x, 31), 31)         # slow floor = continuum + residual dark
    cont = float(np.clip(base.mean() / 900.0, 0, 1))    # 0..1 share of full scale
    x = np.clip(x - base, 0, None)
    med = np.median(x)
    mad = float(np.median(np.abs(x - med)) * 1.4826 + 1e-6)
    top = float(x.max())
    thr = max(cfg["peak_snr"] * mad, cfg["peak_rel"] * top, cfg["peak_abs"])
    peaks = []
    for i in range(2, N_PX - 2):
        if x[i] >= thr and x[i] > x[i - 1] and x[i] >= x[i + 1] and x[i] >= x[i - 2] and x[i] >= x[i + 2]:
            y0, y1, y2 = x[i - 1], x[i], x[i + 1]
            d = y0 - 2 * y1 + y2
            off = 0.5 * (y0 - y2) / d if d != 0 else 0.0
            pix = i + float(np.clip(off, -1, 1))
            peaks.append((float(np.interp(pix, np.arange(N_PX), wl)), float(y1), round(pix, 2)))
    norm = x / max(top, 1.0)
    return norm, peaks, mad, cont


def match_spectrum(peaks, cont, table, cfg):
    """THE MODEL SLOT. Rules today; a classifier tomorrow.

    materials : a material is present when enough of its lines sit under peaks (min_fraction).
    class fit : share of peak ENERGY a class's lines explain, scaled by how many of the class's
                materials actually showed up. A class that explains every peak and whose materials
                are all present scores 1.0.
    novelty   : 1 - best class fit among classes that are NOT held out. High novelty = "I have seen
                these elements, but never in this combination" — which is exactly what an ablating
                heat shield looks like to a model trained on meteors and flares.
    Returns (cls, p, materials, lines, novelty, scores) ; scores includes held-out classes for display.
    """
    tol = cfg["tol_nm"]
    present = {}
    for mat, spec in table["materials"].items():
        hits = [ln for ln in spec["lines"] if any(abs(pw - ln) <= tol for pw, _, _ in peaks)]
        if hits and len(hits) / len(spec["lines"]) >= spec.get("min_fraction", 1.0):
            present[mat] = hits
    total = sum(h for _, h, _ in peaks) or 1.0

    scores = {}
    for cls, spec in table["classes"].items():
        mats = spec["materials"]
        cls_lines = [ln for m in mats for ln in table["materials"][m]["lines"]]
        explained = sum(h for pw, h, _ in peaks if any(abs(pw - ln) <= tol for ln in cls_lines))
        coverage = sum(1 for m in mats if m in present) / len(mats)
        scores[cls] = round((explained / total) * (coverage ** 0.5), 2) if peaks else 0.0

    live = {c: s for c, s in scores.items() if not table["classes"][c].get("held_out")}
    best = max(live, key=live.get) if live else None
    best_s = live[best] if best else 0.0
    novelty = round(1.0 - best_s, 2) if peaks else 0.0

    if not peaks:
        cls, p = ("continuum", round(cont, 2)) if cont >= cfg["cont_thr"] else ("quiet", 1.0)
    elif novelty > cfg["novelty_thr"] or best is None:
        cls, p = "unknown", novelty
    else:
        cls, p = best, best_s
    lines = ["%s%d" % (m, round(ln)) for m, hits in present.items() for ln in hits]
    return cls, p, sorted(present), lines[:6], novelty, scores


def downsample(norm, n=72):
    return [int(v) for v in np.clip(norm.reshape(n, -1).mean(axis=1) * 255, 0, 255)]


class EventPolicy:
    """Emit on change, on anomaly every ANOMALY_REPEAT_S, otherwise a keepalive every EVENT_KEEPALIVE_S."""

    def __init__(self, cfg):
        self.cfg, self.key, self.t = cfg, None, 0.0
        self.cand, self.cand_n = None, 0

    def should_emit(self, cls, mats, now):
        key = (cls, tuple(mats))
        if key != self.key:
            # debounce: a new verdict must hold for EVENT_DEBOUNCE_FRAMES consecutive frames
            self.cand_n = self.cand_n + 1 if key == self.cand else 1
            self.cand = key
            if self.cand_n < self.cfg["debounce"]:
                return None
            reason = "change"
        elif cls == "unknown" and now - self.t >= self.cfg["anomaly_period"]:
            reason = "anomaly"
        elif now - self.t >= self.cfg["keepalive"]:
            reason = "keepalive"
        else:
            return None
        self.key, self.t = key, now
        return reason


# ----------------------------------------------------------------------------- store
class Store:
    def __init__(self, path):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, t REAL, body TEXT, acked INTEGER DEFAULT 0)")
        self.db.execute("CREATE TABLE IF NOT EXISTS spectra(id INTEGER PRIMARY KEY, t REAL, body TEXT, sent INTEGER DEFAULT 0)")
        self.lock = threading.Lock()

    def add_event(self, ev):
        with self.lock:
            cur = self.db.execute("INSERT INTO events(t, body) VALUES(?, '')", (ev["t"],))
            ev["s"] = cur.lastrowid
            self.db.execute("UPDATE events SET body=? WHERE id=?", (json.dumps(ev, separators=(",", ":")), ev["s"]))
            self.db.commit()
        return ev["s"]

    def pending_events(self, limit=5):
        with self.lock:
            return self.db.execute("SELECT id, body FROM events WHERE acked=0 ORDER BY id LIMIT ?", (limit,)).fetchall()

    def count_pending(self):
        with self.lock:
            return self.db.execute("SELECT COUNT(*) FROM events WHERE acked=0").fetchone()[0]

    def ack_upto(self, seq):
        with self.lock:
            self.db.execute("UPDATE events SET acked=1 WHERE id<=? AND acked=0", (seq,))
            self.db.commit()

    def add_spectrum(self, sp):
        with self.lock:
            self.db.execute("INSERT INTO spectra(t, body) VALUES(?, ?)", (sp["t"], json.dumps(sp, separators=(",", ":"))))
            self.db.execute("DELETE FROM spectra WHERE id < (SELECT MAX(id) FROM spectra) - 2000")   # keep ~1 h
            self.db.commit()

    def next_unsent_spectrum(self):
        with self.lock:
            return self.db.execute("SELECT id, body FROM spectra WHERE sent=0 ORDER BY id LIMIT 1").fetchone()

    def mark_spectrum_sent(self, sid):
        with self.lock:
            self.db.execute("UPDATE spectra SET sent=1 WHERE id=?", (sid,))
            self.db.commit()


# ----------------------------------------------------------------------------- uplink
class Uplink(threading.Thread):
    def __init__(self, cfg, store, state, source):
        super().__init__(daemon=True)
        self.cfg, self.store, self.state, self.source = cfg, store, state, source
        u = urlparse(cfg["uplink"])
        self.kind = u.scheme
        self.buf = b""
        if self.kind == "udp":
            self.addr = (u.hostname, u.port)
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.settimeout(0.05)
        elif self.kind == "serial":
            import serial
            baud = int(parse_qs(u.query).get("baud", ["9600"])[0])
            self.ser = serial.Serial(u.path, baud, timeout=0.05)
        else:
            raise SystemExit("UPLINK must be udp://host:port or serial:///dev/xbee?baud=9600")
        self.last_rx = 0.0
        self.last_hb = 0.0
        self.last_resend = 0.0
        self.last_bulk = 0.0
        log.info("uplink %s", cfg["uplink"])

    def send(self, obj):
        data = (json.dumps(obj, separators=(",", ":")) + "\n").encode()
        try:
            if self.kind == "udp":
                self.sock.sendto(data, self.addr)
            else:
                self.ser.write(data)
            self.state["tx_bytes"] += len(data)
        except OSError as e:
            log.debug("send failed: %s", e)

    def recv_lines(self):
        try:
            if self.kind == "udp":
                data, _ = self.sock.recvfrom(4096)
            else:
                data = self.ser.read(self.ser.in_waiting or 1)
        except (socket.timeout, OSError):
            return []
        self.buf += data
        lines, self.buf = self.buf.split(b"\n")[:-1], self.buf.split(b"\n")[-1]
        return lines

    def handle(self, line):
        try:
            msg = json.loads(line)
        except ValueError:
            return
        self.last_rx = time.time()
        if "ack" in msg:
            self.store.ack_upto(int(msg["ack"]))
        if msg.get("cmd") == "integ":
            self.source.command("I%d" % int(msg["us"]))
        if msg.get("cmd") == "dark":
            self.state["dark_request"] = True

    def run(self):
        while True:
            now = time.time()
            for line in self.recv_lines():
                self.handle(line)
            up = (now - self.last_rx) < self.cfg["link_timeout"]
            if up != self.state["link_up"]:
                log.info("link %s", "UP" if up else "DOWN")
            self.state["link_up"] = up
            pending = self.store.count_pending()
            self.state["pending"] = pending

            if now - self.last_hb >= 1.0:
                self.send({"hb": int(now), "n": self.cfg["node_id"], "q": pending})
                self.last_hb = now

            if up and pending and now - self.last_resend >= 1.0:
                for _, body in self.store.pending_events(5):      # oldest first; acks retire them
                    self.send(json.loads(body))
                self.last_resend = now

            if up and not pending and now - self.last_bulk >= self.cfg["bulk_period"]:
                row = self.store.next_unsent_spectrum()
                if row:
                    self.send({"sp": json.loads(row[1])})
                    self.store.mark_spectrum_sent(row[0])
                self.last_bulk = now
            time.sleep(0.02)


# ----------------------------------------------------------------------------- http
class Hub:
    """Fan-out of SSE messages to connected browsers."""

    def __init__(self):
        self.subs, self.lock = [], threading.Lock()

    def publish(self, kind, obj):
        msg = "event: %s\ndata: %s\n\n" % (kind, json.dumps(obj))
        with self.lock:
            for q in list(self.subs):
                try:
                    q.put_nowait(msg)
                except queue.Full:
                    pass

    def subscribe(self):
        q = queue.Queue(maxsize=200)
        with self.lock:
            self.subs.append(q)
        return q

    def unsubscribe(self, q):
        with self.lock:
            if q in self.subs:
                self.subs.remove(q)


def make_handler(state, hub, store):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _json(self, code, obj):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            p = urlparse(self.path).path
            if p == "/healthz":
                fresh = time.time() - state["last_frame_t"] < 5.0
                return self._json(200 if fresh else 503, {"ok": fresh, "frames": state["frames"], "link_up": state["link_up"], "pending": state["pending"]})
            if p == "/latest":
                return self._json(200, state["latest"])
            if p == "/status":
                s = {k: v for k, v in state.items() if k != "latest"}
                return self._json(200, s)
            if p == "/events":
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                q = hub.subscribe()
                try:
                    while True:
                        try:
                            self.wfile.write(q.get(timeout=15).encode())
                        except queue.Empty:
                            self.wfile.write(b": keepalive\n\n")
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass
                finally:
                    hub.unsubscribe(q)
                return
            self._json(404, {"error": "not found"})

        def do_POST(self):
            p = urlparse(self.path).path
            if p == "/dark":
                state["dark_request"] = True
                return self._json(202, {"ok": True, "note": "cover the slit; averaging the next 20 frames"})
            self._json(404, {"error": "not found"})

    return H


# ----------------------------------------------------------------------------- main
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    cfg = CFG
    table = json.load(open(cfg["lines_file"]))
    cal = json.load(open(cfg["cal_file"]))
    wl = pixel_wavelengths(cal)
    if cal.get("source") == "placeholder":
        log.warning("calibration.json is a PLACEHOLDER; wavelengths are approximate until you calibrate")

    dark = None
    if os.path.exists(cfg["dark_file"]):
        dark = np.load(cfg["dark_file"])
        log.info("loaded dark frame %s", cfg["dark_file"])

    src = cfg["source"]
    if src == "sim":
        source = SimSource(table, wl, cfg["sim_speed"])
    elif src.startswith("replay://"):
        source = ReplaySource(src, cfg["sim_speed"])
    else:
        source = SerialSource(src)

    state = {"node_id": cfg["node_id"], "frames": 0, "last_frame_t": 0.0, "link_up": False, "pending": 0,
             "tx_bytes": 0, "dark_request": False, "latest": {}, "wl": [round(w, 1) for w in wl], "events": 0}
    store = Store(cfg["db"])
    hub = Hub()
    policy = EventPolicy(cfg)
    Uplink(cfg, store, state, source).start()
    httpd = ThreadingHTTPServer(("0.0.0.0", cfg["http_port"]), make_handler(state, hub, store))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    log.info("http on :%d  (/healthz /latest /status /events ; POST /dark)", cfg["http_port"])

    rec = open(cfg["record_file"], "ab") if cfg["record_file"] else None
    dark_acc, last_sp = [], 0.0

    for line, (kind, seq, integ, raw) in source.frames():
        now = time.time()
        state["frames"] += 1
        state["last_frame_t"] = now
        if rec:
            rec.write(line)

        if state["dark_request"] or kind == "D":
            dark_acc.append(raw)
            if len(dark_acc) >= 20:
                dark = np.mean(dark_acc, axis=0)
                os.makedirs(os.path.dirname(cfg["dark_file"]) or ".", exist_ok=True)
                np.save(cfg["dark_file"], dark)
                log.info("dark frame saved (%d frames, mean %.1f)", len(dark_acc), dark.mean())
                dark_acc, state["dark_request"] = [], False
            continue

        norm, peaks, noise, cont = reduce_frame(raw, dark, wl, cfg)
        cls, p, mats, lines, nov, scores = match_spectrum(peaks, cont, table, cfg)

        latest = {"t": round(now, 2), "seq": seq, "integ_us": integ, "cls": cls, "p": p, "materials": mats,
                  "lines": lines, "novelty": nov, "continuum": round(cont, 3), "noise": round(noise, 2),
                  "class_scores": scores,      # includes held-out classes: "what the hidden class would have said"
                  "peaks": [{"nm": round(w, 1), "h": round(h, 1), "px": px} for w, h, px in peaks],
                  "spectrum": [round(float(v), 3) for v in norm]}
        if src == "sim":
            latest["sim_profile"] = source.current
        state["latest"] = latest
        hub.publish("spectrum", {"t": latest["t"], "cls": cls, "p": p, "novelty": nov, "peaks": latest["peaks"], "spectrum": latest["spectrum"]})

        reason = policy.should_emit(cls, mats, now)
        if reason:
            ev = {"t": int(now), "n": cfg["node_id"], "cls": cls, "p": p, "m": mats[:4], "l": lines[:4], "nov": nov}
            store.add_event(ev)
            state["events"] += 1
            hub.publish("event", ev)
            log.info("event %s %-10s p=%.2f nov=%.2f %s (%s)", ev["s"], cls, p, nov, lines, reason)

        if now - last_sp >= cfg["bulk_period"]:
            store.add_spectrum({"n": cfg["node_id"], "s": seq, "t": int(now), "v": downsample(norm)})
            last_sp = now


if __name__ == "__main__":
    main()
