#!/usr/bin/env python3
"""
infra_node.py — second constellation node: infrasound (simulated today, Gem logger tomorrow).

Runs anywhere with Python 3 — the UNO Q's Debian side is the intended host for demo day. Speaks the
same NDJSON protocol as the spectral node, so the ground station treats it as a peer: heartbeat, acked
events, nothing else. No dependencies beyond the standard library.

  python3 infra_node.py --ground 192.168.1.50:5005 --node infra-1            # press Enter to fire an event
  python3 infra_node.py --ground 192.168.1.50:5005 --auto 30                  # random event every ~30 s
  python3 infra_node.py --ground 192.168.1.50:5005 --watch /path/to/trigger   # fire when that file's mtime changes

Event body: {"t":..,"n":"infra-1","s":seq,"cls":"infrasound_event","p":0.8,"f":2.3,"amp":0.41,"nov":0}
  f   = dominant frequency Hz (bolide/explosion infrasound sits around 0.5–5 Hz)
  amp = peak-to-peak Pa, relative

When the Gem arrives, replace fire_event()'s payload with the real detection (STA/LTA on the
microbarometer stream) and nothing else changes.
"""
import argparse, json, os, random, select, socket, sqlite3, sys, time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ground", default="127.0.0.1:5005")
    ap.add_argument("--node", default="infra-1")
    ap.add_argument("--auto", type=float, default=0, help="mean seconds between random events (0 = manual only)")
    ap.add_argument("--watch", default="", help="fire an event when this file's mtime changes")
    ap.add_argument("--db", default=os.path.expanduser("~/infra_node.db"))
    a = ap.parse_args()

    host, port = a.ground.rsplit(":", 1)
    addr = (host, int(port))
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(0.05)

    db = sqlite3.connect(a.db)
    db.execute("CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, t REAL, body TEXT, acked INTEGER DEFAULT 0)")

    def send(obj):
        sock.sendto((json.dumps(obj, separators=(",", ":")) + "\n").encode(), addr)

    def pending():
        return db.execute("SELECT id, body FROM events WHERE acked=0 ORDER BY id LIMIT 5").fetchall()

    def fire_event():
        ev = {"t": int(time.time()), "n": a.node, "cls": "infrasound_event",
              "p": round(random.uniform(0.6, 0.95), 2), "f": round(random.uniform(0.8, 4.0), 1),
              "amp": round(random.uniform(0.1, 0.9), 2), "nov": 0}
        cur = db.execute("INSERT INTO events(t, body) VALUES(?, '')", (ev["t"],))
        ev["s"] = cur.lastrowid
        db.execute("UPDATE events SET body=? WHERE id=?", (json.dumps(ev, separators=(",", ":")), ev["s"]))
        db.commit()
        print("event #%d fired: f=%.1f Hz amp=%.2f" % (ev["s"], ev["f"], ev["amp"]), flush=True)

    last_rx = last_hb = last_send = 0.0
    stdin_ok = sys.stdin is not None and sys.stdin.isatty()      # EOF (< /dev/null) would read as "Enter" forever
    next_auto = time.time() + random.expovariate(1 / a.auto) if a.auto else None
    watch_m = os.path.getmtime(a.watch) if a.watch and os.path.exists(a.watch) else None
    print("infra node %s -> %s:%d   (%s)" % (a.node, host, int(port), "auto" if a.auto else "press Enter to fire"), flush=True)

    while True:
        now = time.time()
        try:
            data, _ = sock.recvfrom(4096)
            last_rx = now
            for line in data.split(b"\n"):
                if not line.strip():
                    continue
                msg = json.loads(line)
                if "ack" in msg:
                    db.execute("UPDATE events SET acked=1 WHERE id<=?", (int(msg["ack"]),))
                    db.commit()
        except (socket.timeout, OSError, ValueError):
            pass
        up = now - last_rx < 5.0

        if now - last_hb >= 1.0:
            q = db.execute("SELECT COUNT(*) FROM events WHERE acked=0").fetchone()[0]
            send({"hb": int(now), "n": a.node, "q": q})
            last_hb = now
            sys.stdout.write("\r link %-4s  buffered %d   " % ("UP" if up else "DOWN", q))
            sys.stdout.flush()

        if up and now - last_send >= 1.0:
            for _, body in pending():
                send(json.loads(body))
            last_send = now

        if next_auto and now >= next_auto:
            fire_event()
            next_auto = now + random.expovariate(1 / a.auto)
        if a.watch and os.path.exists(a.watch):
            m = os.path.getmtime(a.watch)
            if watch_m is not None and m != watch_m:
                fire_event()
            watch_m = m
        if stdin_ok and select.select([sys.stdin], [], [], 0)[0]:
            if sys.stdin.readline() == "":
                stdin_ok = False
            else:
                fire_event()
        time.sleep(0.05)


if __name__ == "__main__":
    main()
