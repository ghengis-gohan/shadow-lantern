# Lantern — DDIL edge sensing kit

Sense → classify at the edge → survive a thin link → human decides → fleet learns.
First skin: a spectral node that names the material it sees and says *unknown* when it can't,
plus a second node (infrasound) so two platforms that never talk to each other can agree in time.

```
node/spectro_fw/      Arduino Uno firmware: drives the Hamamatsu C12880MA, streams 288 values/frame over USB
node/spectral/        Python service for the Jetson: dark/baseline, peaks, line match, SQLite queues, acked uplink, HTTP
node/manifests/       MicroShift kustomization baked into the OS image (/etc/microshift/manifests/)
node/os/              files copied onto the bootc image root: udev names, greenboot check; Containerfile snippet
ground/               ground station: ingest, acks + heartbeat, correlation, confirm/dismiss, dashboard
nodes/infra_node.py   second node (infrasound, simulated) — runs on the UNO Q's Linux side or any box
run-local.sh          the whole chain on one laptop, no hardware
```

## 0. Prove the chain with no hardware (5 minutes)

```bash
pip install numpy plotly
./run-local.sh            # then open http://localhost:8080
```
Watch the sim cycle: sodium → meteor → potassium → LED clutter → **ablative_tps** (held out → `unknown`, novelty ≈ 0.8) → strontium.
When the infrasound node fires inside the 10 s window of an `unknown`, an alert appears. Confirm or dismiss it.
Cut the UDP port with iptables; the node card flips to LINK DOWN and "buffered on node" climbs; restore it and the
events arrive with `late Ns` on them. That is the whole demo, in software.

## 1. Spectrometer front-end (Uno)

```bash
cd node/spectro_fw && pio run -t upload && pio device monitor
```
Wiring is in `src/main.cpp`. You should see `S,<seq>,20000,<288 values>` lines. `I40000` sets 40 ms integration.

## 2. Spectral service on the Jetson

Bench-test on the Jetson *before* containerizing — same code:
```bash
pip install numpy pyserial
SPECTRO_SOURCE=serial:///dev/ttyACM0?baud=115200 UPLINK=udp://<ground-ip>:5005 TELEMETRY_DB=/tmp/s.db DARK_FILE=/tmp/dark.npy \
  python3 node/spectral/app/spectral.py
curl -s localhost:8081/latest | python3 -m json.tool | head -40
```
Day one, in order:
1. **Dark frame.** Cover the slit: `curl -X POST localhost:8081/dark` (averages 20 frames, saves `dark.npy`).
2. **Calibrate.** Light one emitter channel at a time and run `node/spectral/app/tools/calibrate.py --from-node 591 --from-node 611 --from-node 465 --from-node 520 --from-node 770 --from-node 385 --out node/spectral/app/calibration.json`. Use the LEDs' *measured* peak wavelengths. Restart.
3. **Record.** Set `RECORD_FILE=/var/telemetry/frames.txt`. Every frame is kept; `SPECTRO_SOURCE=replay:///var/telemetry/frames.txt` replays it anywhere. These are the demo's golden runs and the training set.

Then build and push:
```bash
podman build -t quay.io/<org>/lantern-spectral:0.1 node/spectral && podman push quay.io/<org>/lantern-spectral:0.1
```
Edit `node/manifests/kustomization.yaml` (image name) and `configmap.yaml` (UPLINK → ground IP, or the XBee).

## 3. Into the OS image

Append `node/os/Containerfile.snippet` to your node Containerfile, rebuild, push, rebuild the ISO (or `bootc upgrade` on an installed node).
Quay credentials go in the pull secret (`/etc/crio/openshift-pull-secret`) so MicroShift can pull the app image.
On the node:
```bash
bootc status
export KUBECONFIG=/var/lib/microshift/resources/kubeadmin/kubeconfig
oc get pods -n lantern -w
curl -s localhost:8081/healthz
```
greenboot's `50_lantern_check.sh` fails the boot if the pod isn't ready in 300 s, so a broken image rolls back. Make sure the node has
network on the first boot so the app image can be pulled once; embedding it for fully offline boot is the next OS-image task.

## 4. Ground station

Laptop or N100 with podman:
```bash
podman build -t quay.io/<org>/lantern-ground:0.1 ground
podman run --rm --net=host -v lantern-gs:/var/telemetry quay.io/<org>/lantern-ground:0.1
```
On the N100 under MicroShift: `ground/manifests/`. Field link: set `GS_SERIAL=/dev/xbee?baud=9600` here and `UPLINK=serial:///dev/xbee?baud=9600` on the node.
`EDA_WEBHOOK=http://<eda>:5000/endpoint` makes **Confirm** fire an Event-Driven Ansible rulebook. `/api/labels.csv` is the operator label export.

## 5. Second node

On the UNO Q (SSH in):
```bash
scp nodes/infra_node.py arduino@<unoq>:~/ && ssh arduino@<unoq> python3 infra_node.py --ground <ground-ip>:5005 --node infra-1
```
Press Enter to fire an infrasound event; `--auto 30` for random. When the Gem logger arrives, `fire_event()` gets the real detection.

## Protocol (NDJSON, one object per line, both transports)

| direction | message | purpose |
|---|---|---|
| node → ground | `{"hb":t,"n":id,"q":pending}` | 1 Hz heartbeat; `q` = events not yet acked |
| node → ground | `{"t":..,"n":id,"s":seq,"cls":..,"p":..,"m":[..],"l":[..],"nov":..}` | event, ~40–60 bytes; resent until acked |
| node → ground | `{"sp":{"n":id,"s":seq,"t":t,"v":[72 ints]}}` | downsampled spectrum, only when idle and link up |
| ground → node | `{"ack":seq}` · `{"hb":t}` · `{"cmd":"integ","us":N}` · `{"cmd":"dark"}` | acks, keepalive, re-tasking |

Link = "up" on a node when anything arrived from the ground in the last 5 s. Nothing is ever dropped; outages cost latency.

## The model slot

`match_spectrum()` in `spectral.py` is the classifier: peaks → materials (by line hits) → class (best material-set match among
classes **not** marked `held_out`) → novelty (share of peak energy no known line explains). `ablative_tps` and `uhtc` are
held out in `lines.json`, so the TPS signature reads as `unknown` with high novelty. That is the honest demo: the model never names
the threat; it learns the background and flags the residual. Flip `held_out` to see what a trained class would say.
