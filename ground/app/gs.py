#!/usr/bin/env python3
"""
gs.py — Lantern ground station: ingest, acks, correlation, human decision, dashboard.

Listens for NDJSON from any node over UDP (:5005) and/or an XBee on serial. For every message it
answers so the node knows the link is alive: {"ack":seq} for events, {"hb":t} on its own 1 Hz beat.
That answer IS the link-state signal on the node side; no ack, no link.

Correlation: an anomaly (cls == "unknown") from one node and an event from a different node within
CORRELATION_WINDOW_S becomes an alert. Two platforms that never spoke to each other agreeing in time
is the constellation story in one line of SQL.

Decision: POST /label {"event_id":..,"verdict":"confirm|dismiss","note":".."}. Confirm fires the EDA
webhook if EDA_WEBHOOK is set (an Event-Driven Ansible rulebook with a webhook source). Every verdict
is also a training label for later.

Dashboard: /  (plotly served locally so the venue needs no internet).
"""
import json, logging, os, queue, socket, sqlite3, sys, threading, time, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

log = logging.getLogger("gs")
HERE = os.path.dirname(os.path.abspath(__file__))


def env(k, d=None, cast=str):
    v = os.environ.get(k)
    return d if v in (None, "") else cast(v)


CFG = {
    "udp_port":    env("GS_UDP_PORT", 5005, int),
    "serial":      env("GS_SERIAL", ""),                       # e.g. /dev/xbee?baud=9600
    "http_port":   env("HTTP_PORT", 8080, int),
    "db":          env("GS_DB", "/var/telemetry/ground.db"),
    "window":      env("CORRELATION_WINDOW_S", 10.0, float),
    "node_timeout": env("NODE_TIMEOUT_S", 5.0, float),
    "eda_webhook": env("EDA_WEBHOOK", ""),
}


class Store:
    def __init__(self, path):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, node TEXT, seq INTEGER, t REAL, rx_t REAL, cls TEXT, body TEXT,
                                            UNIQUE(node, seq));
          CREATE TABLE IF NOT EXISTS alerts(id INTEGER PRIMARY KEY, t REAL, nodes TEXT, event_ids TEXT, verdict TEXT DEFAULT '', note TEXT DEFAULT '');
          CREATE TABLE IF NOT EXISTS labels(id INTEGER PRIMARY KEY, t REAL, event_id INTEGER, verdict TEXT, note TEXT);
        """)
        self.lock = threading.Lock()

    def add_event(self, ev):
        """Returns (row_id, is_new). Duplicates (resends after a lost ack) are recognized, not double-counted."""
        with self.lock:
            try:
                cur = self.db.execute("INSERT INTO events(node, seq, t, rx_t, cls, body) VALUES(?,?,?,?,?,?)",
                                      (ev["n"], ev["s"], ev["t"], time.time(), ev.get("cls", ""), json.dumps(ev)))
                self.db.commit()
                return cur.lastrowid, True
            except sqlite3.IntegrityError:
                row = self.db.execute("SELECT id FROM events WHERE node=? AND seq=?", (ev["n"], ev["s"])).fetchone()
                return row[0], False

    def neighbours(self, ev_id, node, t, window):
        with self.lock:
            return self.db.execute("SELECT id, node, cls, body FROM events WHERE node!=? AND ABS(t-?)<=? AND id!=? ORDER BY ABS(t-?) LIMIT 5",
                                   (node, t, window, ev_id, t)).fetchall()

    @staticmethod
    def _ids(ids):
        return "," + ",".join(str(i) for i in ids) + ","        # ",3,17," so LIKE '%,3,%' can't match 13 or 31

    def add_alert(self, t, nodes, ids):
        with self.lock:
            cur = self.db.execute("INSERT INTO alerts(t, nodes, event_ids) VALUES(?,?,?)", (t, json.dumps(nodes), self._ids(ids)))
            self.db.commit()
            return cur.lastrowid

    def recent_alert_for(self, ev_id):
        with self.lock:
            return self.db.execute("SELECT id FROM alerts WHERE event_ids LIKE ? AND t > ?", ("%%,%d,%%" % ev_id, time.time() - 60)).fetchone()

    def add_label(self, ev_id, verdict, note):
        with self.lock:
            self.db.execute("INSERT INTO labels(t, event_id, verdict, note) VALUES(?,?,?,?)", (time.time(), ev_id, verdict, note))
            self.db.execute("UPDATE alerts SET verdict=?, note=? WHERE event_ids LIKE ?", (verdict, note, "%%,%d,%%" % ev_id))
            self.db.commit()

    def recent(self, table, n=50):
        with self.lock:
            cur = self.db.execute("SELECT * FROM %s ORDER BY id DESC LIMIT ?" % table, (n,))
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    def labels_csv(self):
        with self.lock:
            rows = self.db.execute("SELECT l.t, e.node, e.seq, e.cls, e.body, l.verdict, l.note FROM labels l JOIN events e ON e.id=l.event_id ORDER BY l.id").fetchall()
        out = ["label_t,node,seq,cls,verdict,note,event_json"]
        for t, node, seq, cls, body, verdict, note in rows:
            out.append("%d,%s,%s,%s,%s,%s,%s" % (t, node, seq, cls, verdict, (note or "").replace(",", ";"), body.replace(",", ";")))
        return "\n".join(out) + "\n"


class Hub:
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
        q = queue.Queue(maxsize=500)
        with self.lock:
            self.subs.append(q)
        return q

    def unsubscribe(self, q):
        with self.lock:
            if q in self.subs:
                self.subs.remove(q)


class Ground:
    def __init__(self, cfg):
        self.cfg = cfg
        self.store = Store(cfg["db"])
        self.hub = Hub()
        self.nodes = {}            # node_id -> {"addr":..., "transport":..., "last":t, "pending":q, "cls":..., "events":n}
        self.spectra = {}          # node_id -> latest downsampled spectrum
        self.lock = threading.Lock()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", cfg["udp_port"]))
        self.sock.settimeout(0.05)
        self.ser = None
        self.serbuf = b""
        if cfg["serial"]:
            import serial
            u = urlparse("serial://" + cfg["serial"]) if not cfg["serial"].startswith("serial://") else urlparse(cfg["serial"])
            self.ser = serial.Serial(u.path, int(parse_qs(u.query).get("baud", ["9600"])[0]), timeout=0.05)
            log.info("serial listener %s", cfg["serial"])
        log.info("udp listener :%d", cfg["udp_port"])

    # ---- transport
    def send(self, node, obj):
        data = (json.dumps(obj, separators=(",", ":")) + "\n").encode()
        n = self.nodes.get(node)
        if not n:
            return
        try:
            if n["transport"] == "udp":
                self.sock.sendto(data, n["addr"])
            elif self.ser:
                self.ser.write(data)
        except OSError:
            pass

    def rx_loop(self):
        while True:
            try:
                data, addr = self.sock.recvfrom(8192)
                for line in data.split(b"\n"):
                    if line.strip():
                        self.handle(line, "udp", addr)
            except (socket.timeout, OSError):
                pass
            if self.ser:
                try:
                    self.serbuf += self.ser.read(self.ser.in_waiting or 1)
                    *lines, self.serbuf = self.serbuf.split(b"\n")
                    for line in lines:
                        if line.strip():
                            self.handle(line, "serial", None)
                except OSError:
                    pass

    def hb_loop(self):
        while True:
            now = time.time()
            snapshot = {}
            with self.lock:
                for nid, n in self.nodes.items():
                    n["up"] = (now - n["last"]) < self.cfg["node_timeout"]
                    snapshot[nid] = {k: v for k, v in n.items() if k != "addr"}
                    snapshot[nid]["age"] = round(now - n["last"], 1)
            for nid in snapshot:
                self.send(nid, {"hb": int(now)})
            self.hub.publish("nodes", snapshot)
            time.sleep(1.0)

    # ---- protocol
    def handle(self, line, transport, addr):
        try:
            msg = json.loads(line)
        except ValueError:
            return
        now = time.time()
        if "hb" in msg and "n" in msg:
            self.touch(msg["n"], transport, addr, now, pending=msg.get("q", 0))
            return
        if "sp" in msg:
            sp = msg["sp"]
            self.touch(sp["n"], transport, addr, now)
            self.spectra[sp["n"]] = sp
            self.hub.publish("spectrum", sp)
            return
        if "n" in msg and "s" in msg and "cls" in msg:
            self.touch(msg["n"], transport, addr, now, cls=msg["cls"])
            self.send(msg["n"], {"ack": msg["s"]})
            ev_id, new = self.store.add_event(msg)
            if new:
                with self.lock:
                    self.nodes[msg["n"]]["events"] += 1
                rec = dict(msg, id=ev_id, rx_t=round(now, 1), late=round(now - msg["t"], 1))
                self.hub.publish("event", rec)
                log.info("event %-8s #%-4s %-12s p=%.2f nov=%.2f late=%.0fs %s", msg["n"], msg["s"], msg["cls"], msg.get("p", 0), msg.get("nov", 0), now - msg["t"], msg.get("l", []))
                self.correlate(ev_id, msg)

    def touch(self, node, transport, addr, now, **kw):
        with self.lock:
            n = self.nodes.setdefault(node, {"transport": transport, "addr": addr, "last": 0, "pending": 0, "cls": "", "events": 0, "up": False})
            n.update(transport=transport, addr=addr or n["addr"], last=now, **kw)

    def correlate(self, ev_id, ev):
        """Anomaly here + anything from another node inside the window = alert. Rule-based on purpose."""
        interesting = ev.get("cls") == "unknown" or ev.get("cls", "").startswith("infrasound")
        if not interesting:
            return
        nb = self.store.neighbours(ev_id, ev["n"], ev["t"], self.cfg["window"])
        nb = [r for r in nb if r[2] == "unknown" or r[2].startswith("infrasound")]
        if not nb:
            return
        if self.store.recent_alert_for(ev_id) or any(self.store.recent_alert_for(r[0]) for r in nb):
            return
        nodes = sorted({ev["n"], *[r[1] for r in nb]})
        ids = [ev_id] + [r[0] for r in nb]
        aid = self.store.add_alert(ev["t"], nodes, ids)
        alert = {"id": aid, "t": ev["t"], "nodes": nodes, "event_ids": ids,
                 "summary": "%s: %s + %s within %.0fs" % (" & ".join(nodes), ev["cls"], ", ".join(sorted({r[2] for r in nb})), self.cfg["window"]),
                 "events": [json.loads(r[3]) for r in nb] + [ev]}
        self.hub.publish("alert", alert)
        log.warning("ALERT #%d %s", aid, alert["summary"])

    def label(self, ev_id, verdict, note):
        self.store.add_label(ev_id, verdict, note)
        self.hub.publish("label", {"event_id": ev_id, "verdict": verdict, "note": note, "t": time.time()})
        log.info("label event %d -> %s %s", ev_id, verdict, note)
        if verdict == "confirm" and self.cfg["eda_webhook"]:
            try:
                body = json.dumps({"event_id": ev_id, "verdict": verdict, "note": note, "t": time.time()}).encode()
                req = urllib.request.Request(self.cfg["eda_webhook"], data=body, headers={"Content-Type": "application/json"})
                urllib.request.urlopen(req, timeout=3)
                log.info("EDA webhook fired")
            except Exception as e:
                log.warning("EDA webhook failed: %s", e)


def plotly_path():
    try:
        import plotly
        p = os.path.join(os.path.dirname(plotly.__file__), "package_data", "plotly.min.js")
        return p if os.path.exists(p) else None
    except ImportError:
        return None


def make_handler(g):
    PLOTLY = plotly_path()

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype="application/json"):
            if isinstance(body, (dict, list)):
                body = json.dumps(body).encode()
            elif isinstance(body, str):
                body = body.encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            p = urlparse(self.path).path
            if p == "/" or p == "/index.html":
                return self._send(200, open(os.path.join(HERE, "static", "index.html"), "rb").read(), "text/html")
            if p == "/static/plotly.min.js":
                if PLOTLY:
                    return self._send(200, open(PLOTLY, "rb").read(), "application/javascript")
                self.send_response(302)
                self.send_header("Location", "https://cdn.plot.ly/plotly-2.35.2.min.js")
                self.end_headers()
                return
            if p == "/healthz":
                return self._send(200, {"ok": True, "nodes": len(g.nodes)})
            if p == "/api/state":
                with g.lock:
                    nodes = {k: {kk: vv for kk, vv in v.items() if kk != "addr"} for k, v in g.nodes.items()}
                return self._send(200, {"nodes": nodes, "events": g.store.recent("events", 50), "alerts": g.store.recent("alerts", 20),
                                        "labels": g.store.recent("labels", 50), "spectra": g.spectra})
            if p == "/api/labels.csv":
                return self._send(200, g.store.labels_csv(), "text/csv")
            if p == "/stream":
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                q = g.hub.subscribe()
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
                    g.hub.unsubscribe(q)
                return
            self._send(404, {"error": "not found"})

        def do_POST(self):
            p = urlparse(self.path).path
            n = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(n) or b"{}")
            if p == "/label":
                if body.get("verdict") not in ("confirm", "dismiss"):
                    return self._send(400, {"error": "verdict must be confirm|dismiss"})
                g.label(int(body["event_id"]), body["verdict"], body.get("note", ""))
                return self._send(200, {"ok": True})
            if p == "/cmd":                                      # re-task a node: {"node":"spec-1","cmd":"integ","us":40000}
                g.send(body.pop("node"), body)
                return self._send(200, {"ok": True})
            self._send(404, {"error": "not found"})

    return H


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    g = Ground(CFG)
    threading.Thread(target=g.rx_loop, daemon=True).start()
    threading.Thread(target=g.hb_loop, daemon=True).start()
    httpd = ThreadingHTTPServer(("0.0.0.0", CFG["http_port"]), make_handler(g))
    log.info("dashboard http://0.0.0.0:%d  (plotly %s)", CFG["http_port"], "local" if plotly_path() else "CDN fallback")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
