"""
Coordinator node.

Distributed systems ideas used here
  1. Client-server over sockets : coordinators talk to each other over raw TCP
                                  sockets (election + heartbeat). The browser
                                  and the venues are reached over HTTP.
  2. Bully election             : the highest ID alive is always the leader.
                                  Real ELECTION / OK / COORDINATOR messages.
  3. Lamport logical clock      : every message carries a counter, so all
                                  events can be shown in one agreed order.
  4. Berkeley clock sync        : the leader collects every node's clock,
                                  averages them and tells each node how far
                                  to move its own clock.
  5. RPC style calls            : the leader calls the venue servers to book.
"""
import json
import os
import socket
import socketserver
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Flask, Response, jsonify, request

NODE_ID = int(os.environ.get("NODE_ID", "1"))
HTTP_PORT = int(os.environ.get("HTTP_PORT", "5000"))
ELECTION_PORT = int(os.environ.get("ELECTION_PORT", "6000"))
CLOCK_SKEW = float(os.environ.get("CLOCK_SKEW_SECONDS", "0"))
HEARTBEAT_SECONDS = float(os.environ.get("HEARTBEAT_SECONDS", "2"))
SYNC_EVERY_SECONDS = float(os.environ.get("SYNC_EVERY_SECONDS", "30"))


def parse_map(text, cast=str):
    """'1=host:6000,2=host2:6000' -> {1: 'host:6000', 2: 'host2:6000'}"""
    out = {}
    for part in filter(None, (p.strip() for p in text.split(","))):
        k, v = part.split("=", 1)
        out[cast(k)] = v
    return out


PEERS_TCP = parse_map(os.environ.get("PEERS_TCP", ""), int)    # other coordinators, socket address
PEERS_HTTP = parse_map(os.environ.get("PEERS_HTTP", ""), int)  # other coordinators, http base url
VENUES = parse_map(os.environ.get("VENUES", "A=http://127.0.0.1:5001,B=http://127.0.0.1:5002"))

app = Flask(__name__)

# ------------------------------------------------------------------ clocks
lamport = 0
lamport_lock = threading.Lock()
clock_adjustment = 0.0


def now():
    return time.time() + CLOCK_SKEW + clock_adjustment


def tick(received=0):
    global lamport
    with lamport_lock:
        lamport = max(lamport, received) + 1
        return lamport


# ------------------------------------------------------------------ logs
local_events = []
local_lock = threading.Lock()


def log_local(kind, detail):
    ts = tick()
    with local_lock:
        local_events.append({
            "id": len(local_events) + 1,
            "source": f"Coordinator {NODE_ID}",
            "kind": kind,
            "detail": detail,
            "lamport": ts,
            "wall_time": round(now(), 3),
        })
    print(f"[C{NODE_ID} L={ts}] {kind}: {detail}", flush=True)
    return ts


# ------------------------------------------------------------------ bully election (TCP sockets)
leader_id = None
state_lock = threading.Lock()
election_lock = threading.Lock()
electing = False
coordinator_seen = threading.Event()


def send_msg(addr, msg, timeout=1.0):
    """Send one JSON line over a TCP socket and read one JSON line back."""
    host, port = addr.rsplit(":", 1)
    try:
        with socket.create_connection((host, int(port)), timeout=timeout) as s:
            s.settimeout(timeout)
            s.sendall((json.dumps(msg) + "\n").encode())
            data = b""
            while not data.endswith(b"\n"):
                chunk = s.recv(4096)
                if not chunk:
                    break
                data += chunk
            if data:
                reply = json.loads(data.decode())
                tick(reply.get("lamport", 0))
                return reply
    except (OSError, ValueError):
        return None
    return None


def become_leader():
    global leader_id
    with state_lock:
        leader_id = NODE_ID
    log_local("ELECTION", f"No higher node answered. Node {NODE_ID} is now the leader")
    for pid, addr in PEERS_TCP.items():
        send_msg(addr, {"type": "COORDINATOR", "from": NODE_ID, "lamport": tick()})


def start_election():
    global electing, leader_id
    with election_lock:
        if electing:
            return
        electing = True
    try:
        for _attempt in range(3):
            coordinator_seen.clear()
            higher = {pid: a for pid, a in PEERS_TCP.items() if pid > NODE_ID}
            log_local("ELECTION", f"Node {NODE_ID} starts an election, asking higher nodes {sorted(higher) or 'none'}")
            got_ok = False
            for pid, addr in higher.items():
                reply = send_msg(addr, {"type": "ELECTION", "from": NODE_ID, "lamport": tick()})
                if reply and reply.get("type") == "OK":
                    log_local("ELECTION", f"Node {pid} replied OK, so it takes over the election")
                    got_ok = True
            if not got_ok:
                become_leader()
                return
            if coordinator_seen.wait(timeout=4):
                return
            log_local("ELECTION", "Higher node never announced itself, retrying")
    finally:
        with election_lock:
            electing = False


class PeerHandler(socketserver.StreamRequestHandler):
    def handle(self):
        global leader_id
        try:
            msg = json.loads(self.rfile.readline().decode())
        except ValueError:
            return
        ts = tick(msg.get("lamport", 0))
        kind, sender = msg.get("type"), msg.get("from")

        if kind == "PING":
            reply = {"type": "PONG", "from": NODE_ID, "leader": leader_id}
        elif kind == "ELECTION":
            reply = {"type": "OK", "from": NODE_ID}
            if sender is not None and sender < NODE_ID:
                threading.Thread(target=start_election, daemon=True).start()
        elif kind == "COORDINATOR":
            reply = {"type": "ACK", "from": NODE_ID}
            if sender is not None and sender < NODE_ID:
                # A lower node claims leadership while we are alive: bully it.
                threading.Thread(target=start_election, daemon=True).start()
            else:
                with state_lock:
                    leader_id = sender
                coordinator_seen.set()
                log_local("ELECTION", f"Node {sender} announced itself as the leader")
        else:
            reply = {"type": "ERROR"}
        reply["lamport"] = ts
        self.wfile.write((json.dumps(reply) + "\n").encode())


class ThreadedTCP(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


def heartbeat_loop():
    global leader_id
    misses = 0
    time.sleep(1.5)
    start_election()
    while True:
        time.sleep(HEARTBEAT_SECONDS)
        with state_lock:
            current = leader_id
        if current is None:
            start_election()
        elif current != NODE_ID:
            reply = send_msg(PEERS_TCP.get(current, ""), {"type": "PING", "from": NODE_ID, "lamport": tick()}) if current in PEERS_TCP else None
            if reply:
                misses = 0
            else:
                misses += 1
                if misses >= 2:
                    log_local("ELECTION", f"Leader node {current} stopped answering heartbeats")
                    misses = 0
                    with state_lock:
                        leader_id = None
                    threading.Thread(target=start_election, daemon=True).start()
        else:
            misses = 0


# ------------------------------------------------------------------ berkeley clock sync
clock_status = {"last_sync": None, "rounds": 0, "history": []}


def read_clock(name, base_url):
    """Ask a node for its time. Compensate for network delay (half the round trip)."""
    t0 = now()
    try:
        r = requests.get(base_url + "/time", timeout=2)
        t1 = now()
        remote = r.json()["time"] + (t1 - t0) / 2
        return {"name": name, "url": base_url, "offset": remote - t1, "rtt_ms": round((t1 - t0) * 1000, 2)}
    except (requests.RequestException, ValueError, KeyError):
        return None


def collect_offsets():
    nodes = []
    for vid, url in VENUES.items():
        nodes.append(read_clock(f"Venue {vid}", url))
    for pid, url in PEERS_HTTP.items():
        nodes.append(read_clock(f"Coordinator {pid}", url))
    return [n for n in nodes if n]


def berkeley_sync():
    global clock_adjustment
    nodes = collect_offsets()
    offsets = {"Coordinator %d (leader)" % NODE_ID: 0.0}
    offsets.update({n["name"]: n["offset"] for n in nodes})
    average = statistics.mean(offsets.values())

    log_local("CLOCK_SYNC", "Berkeley round started, offsets vs leader: " +
              ", ".join(f"{k}={v:+.3f}s" for k, v in offsets.items()))

    adjustments = {}
    for n in nodes:
        delta = average - n["offset"]
        try:
            requests.post(n["url"] + "/adjust", json={"delta": delta}, timeout=2)
            adjustments[n["name"]] = delta
        except requests.RequestException:
            pass
    clock_adjustment += average
    adjustments["Coordinator %d (leader)" % NODE_ID] = average

    after = {n["name"]: n["offset"] for n in collect_offsets()}
    spread_before = max(offsets.values()) - min(offsets.values())
    spread_after = (max(after.values()) - min(after.values())) if after else 0.0
    result = {
        "time": round(now(), 3),
        "offsets_before": {k: round(v, 3) for k, v in offsets.items()},
        "average_offset": round(average, 3),
        "adjustments": {k: round(v, 3) for k, v in adjustments.items()},
        "spread_before_s": round(spread_before, 3),
        "spread_after_s": round(spread_after, 3),
    }
    clock_status["last_sync"] = result
    clock_status["rounds"] += 1
    clock_status["history"] = (clock_status["history"] + [result])[-5:]
    log_local("CLOCK_SYNC", f"Berkeley round done, clock spread {spread_before:.3f}s -> {spread_after:.3f}s")
    return result


def sync_loop():
    time.sleep(8)
    while True:
        with state_lock:
            am_leader = leader_id == NODE_ID
        if am_leader:
            try:
                berkeley_sync()
            except Exception as exc:  # keep the thread alive
                print("sync error", exc, flush=True)
        time.sleep(SYNC_EVERY_SECONDS)


# ------------------------------------------------------------------ http api
@app.after_request
def cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return resp


@app.route("/<path:_p>", methods=["OPTIONS"])
def options(_p):
    return ("", 204)


def is_leader():
    with state_lock:
        return leader_id == NODE_ID


@app.route("/health")
def health():
    return jsonify(status="ok", node=NODE_ID)


@app.route("/leader-status")
def leader_status():
    with state_lock:
        lid = leader_id
    return jsonify(node_id=NODE_ID, is_leader=(lid == NODE_ID), leader_id=lid,
                   lamport=tick(), wall_time=round(now(), 3))


@app.route("/election-log")
def election_log():
    with local_lock:
        return jsonify(log=[e for e in local_events if e["kind"] == "ELECTION"])


@app.route("/time")
def get_time():
    return jsonify(node=NODE_ID, time=now())


@app.route("/adjust", methods=["POST"])
def adjust():
    global clock_adjustment
    delta = float(request.get_json(force=True).get("delta", 0))
    clock_adjustment += delta
    log_local("CLOCK_ADJUSTED", f"clock moved by {delta:+.3f}s by the leader")
    return jsonify(ok=True)


@app.route("/api/venues")
def venues():
    return jsonify(venues=[{"id": k, "url": v} for k, v in VENUES.items()])


@app.route("/api/seats/<vid>")
def seats(vid):
    if vid not in VENUES:
        return jsonify(error="Unknown venue"), 404
    try:
        r = requests.get(f"{VENUES[vid]}/seats", params={"lamport": tick()}, timeout=3)
        data = r.json()
        tick(data.get("lamport", 0))
        return jsonify(data)
    except requests.RequestException:
        return jsonify(error=f"Venue {vid} is not reachable"), 503


def forward(vid, path, payload, label):
    ts = log_local("REQUEST", f"{label} sent to Venue {vid}")
    payload = dict(payload, lamport=ts)
    try:
        r = requests.post(f"{VENUES[vid]}{path}", json=payload, timeout=4)
        data = r.json()
        tick(data.get("lamport", 0))
        return jsonify(data), r.status_code
    except requests.RequestException:
        return jsonify(ok=False, error=f"Venue {vid} is not reachable"), 503


def leader_guard():
    with state_lock:
        lid = leader_id
    return jsonify(ok=False, error="This node is not the leader", leader_id=lid), 409


@app.route("/api/book", methods=["POST"])
def book():
    if not is_leader():
        return leader_guard()
    d = request.get_json(force=True)
    vid = d.get("venue")
    if vid not in VENUES:
        return jsonify(ok=False, error="Unknown venue"), 404
    return forward(vid, "/book", {"seat": d.get("seat"), "user": d.get("user", "guest")},
                   f"Book {d.get('seat')} for {d.get('user', 'guest')}")


@app.route("/api/cancel", methods=["POST"])
def cancel():
    if not is_leader():
        return leader_guard()
    d = request.get_json(force=True)
    vid = d.get("venue")
    if vid not in VENUES:
        return jsonify(ok=False, error="Unknown venue"), 404
    return forward(vid, "/cancel", {"seat": d.get("seat")}, f"Cancel {d.get('seat')}")


def gather_events():
    """Pull events from every venue and merge them with this node's own events."""
    merged = []
    with local_lock:
        merged.extend(local_events)

    def fetch(url):
        try:
            r = requests.get(f"{url}/events", params={"lamport": tick()}, timeout=2)
            data = r.json()
            tick(data.get("lamport", 0))
            return data["events"]
        except (requests.RequestException, ValueError, KeyError):
            return []

    # Ask all venues at the same time so one slow venue cannot delay the rest.
    with ThreadPoolExecutor(max_workers=max(1, len(VENUES))) as pool:
        for events in pool.map(fetch, VENUES.values()):
            merged.extend(events)
    merged.sort(key=lambda e: (e["lamport"], e["source"], e["id"]))
    return merged


@app.route("/api/events")
def api_events():
    return jsonify(events=gather_events()[-80:], lamport=tick())


@app.route("/api/sync", methods=["POST"])
def api_sync():
    if not is_leader():
        return leader_guard()
    return jsonify(ok=True, result=berkeley_sync())


@app.route("/api/clock-status")
def api_clock_status():
    return jsonify(node_id=NODE_ID, is_leader=is_leader(), wall_time=round(now(), 3),
                   adjustment=round(clock_adjustment, 3), **clock_status)


@app.route("/api/export-log")
def export_log():
    """Plain text log used as input for the Hadoop word count."""
    lines = []
    for e in gather_events():
        lines.append(f"{e['source']} {e['kind']} {e['detail']}")
    return Response("\n".join(lines) + "\n", mimetype="text/plain")


if __name__ == "__main__":
    server = ThreadedTCP(("0.0.0.0", ELECTION_PORT), PeerHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=sync_loop, daemon=True).start()
    print(f"[Coordinator {NODE_ID}] http {HTTP_PORT}, election socket {ELECTION_PORT}, "
          f"peers {PEERS_TCP}, venues {VENUES}", flush=True)
    app.run(host="0.0.0.0", port=HTTP_PORT, threaded=True)
