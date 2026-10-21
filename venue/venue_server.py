"""
Venue server: owns the seats of ONE theatre.

Distributed systems ideas used here
  1. Mutual exclusion : a conditional MongoDB update atomically grants each
                        seat to at most one concurrent request.
  2. Lamport clock    : every message carries a logical timestamp, so events
                        from different machines can be put in one agreed order.
  3. Physical clock   : this server has its own (deliberately wrong) wall
                        clock. The leader coordinator corrects it with the
                        Berkeley algorithm through the /time and /adjust calls.
"""
import os
import threading
import time

from flask import Flask, jsonify, request
from pymongo import MongoClient, ReturnDocument, UpdateOne
from pymongo.errors import PyMongoError

VENUE_ID = os.environ.get("VENUE_ID", "A")
VENUE_NAME = os.environ.get("VENUE_NAME", "Venue " + VENUE_ID)
PORT = int(os.environ.get("PORT", "5001"))
# Pretend this machine's clock is wrong by this many seconds.
CLOCK_SKEW = float(os.environ.get("CLOCK_SKEW_SECONDS", "0"))
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DATABASE = os.environ.get("MONGO_DATABASE", "seatsync")

ROWS = "ABCDE"
COLS = range(1, 7)

app = Flask(__name__)
mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
seat_collection = mongo_client[MONGO_DATABASE]["seats"]

# ---------- persistent seat state and in-memory event/clock state ----------
seat_ids = [f"{r}{c}" for r in ROWS for c in COLS]
events = []                                                 # event log
state_lock = threading.Lock()                               # protects clock + events

lamport = 0
clock_adjustment = 0.0       # set by Berkeley sync


def seat_key(seat):
    return f"{VENUE_ID}:{seat}"


def read_seats():
    documents = seat_collection.find(
        {"venue_id": VENUE_ID}, {"_id": 0, "seat": 1, "owner": 1}
    )
    return {document["seat"]: document["owner"] for document in documents}


def initialize_seats():
    seat_collection.bulk_write([
        UpdateOne(
            {"_id": seat_key(seat)},
            {"$setOnInsert": {"venue_id": VENUE_ID, "seat": seat, "owner": None}},
            upsert=True,
        )
        for seat in seat_ids
    ])


def now():
    """This venue's own wall clock: real time + wrong skew + corrections."""
    return time.time() + CLOCK_SKEW + clock_adjustment


def tick(received=0):
    """Lamport rule: on receive take max(local, received) + 1."""
    global lamport
    with state_lock:
        lamport = max(lamport, received) + 1
        return lamport


def log_event(kind, detail, ts):
    with state_lock:
        events.append({
            "id": len(events) + 1,
            "source": VENUE_NAME,
            "kind": kind,
            "detail": detail,
            "lamport": ts,
            "wall_time": round(now(), 3),
        })


@app.after_request
def cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


@app.route("/health")
def health():
    try:
        mongo_client.admin.command("ping")
        return jsonify(status="ok", venue=VENUE_NAME, storage="mongodb")
    except PyMongoError:
        return jsonify(status="error", venue=VENUE_NAME, error="MongoDB is unavailable"), 503

@app.route("/seats")
def get_seats():
    ts = tick(int(request.args.get("lamport", 0)))
    try:
        return jsonify(venue=VENUE_NAME, venue_id=VENUE_ID, seats=read_seats(), lamport=ts)
    except PyMongoError:
        return jsonify(error="Booking storage is unavailable", lamport=ts), 503

@app.route("/book", methods=["POST"])
def book():
    data = request.get_json(force=True)
    seat = data.get("seat")
    user = data.get("user", "guest")
    ts = tick(int(data.get("lamport", 0)))

    if seat not in seat_ids:
        return jsonify(ok=False, error="No such seat", lamport=ts), 404

    try:
        # A conditional MongoDB update is atomic across venue-server replicas.
        booked = seat_collection.find_one_and_update(
            {"_id": seat_key(seat), "owner": None},
            {"$set": {"owner": user}},
            return_document=ReturnDocument.AFTER,
        )
        if booked is None:
            current = seat_collection.find_one({"_id": seat_key(seat)}, {"owner": 1})
            if current is None:
                return jsonify(ok=False, error="Seat is not initialized", lamport=ts), 503
            log_event("BOOK_REJECTED", f"{user} wanted {seat}, already taken by {current['owner']}", ts)
            return jsonify(ok=False, error=f"Seat {seat} is already booked", lamport=ts), 409
        log_event("BOOKED", f"{user} booked {seat}", ts)
        return jsonify(ok=True, seat=seat, user=user, lamport=ts)
    except PyMongoError:
        return jsonify(ok=False, error="Booking storage is unavailable", lamport=ts), 503

@app.route("/cancel", methods=["POST"])
def cancel():
    data = request.get_json(force=True)
    seat = data.get("seat")
    ts = tick(int(data.get("lamport", 0)))
    if seat not in seat_ids:
        return jsonify(ok=False, error="No such seat", lamport=ts), 404
    try:
        result = seat_collection.update_one(
            {"_id": seat_key(seat), "owner": {"$ne": None}},
            {"$set": {"owner": None}},
        )
        if result.modified_count == 0:
            return jsonify(ok=False, error="Seat is not booked", lamport=ts), 409
        log_event("CANCELLED", f"{seat} released", ts)
        return jsonify(ok=True, seat=seat, lamport=ts)
    except PyMongoError:
        return jsonify(ok=False, error="Booking storage is unavailable", lamport=ts), 503


@app.route("/events")
def get_events():
    since = int(request.args.get("since", 0))
    ts = tick(int(request.args.get("lamport", 0)))
    with state_lock:
        out = [e for e in events if e["id"] > since]
    return jsonify(events=out, lamport=ts)


# ---------- clock synchronisation (Berkeley) ----------
@app.route("/time")
def get_time():
    """Coordinator asks: what time does your clock show?"""
    return jsonify(venue=VENUE_NAME, time=now(), skew_hint=CLOCK_SKEW)


@app.route("/adjust", methods=["POST"])
def adjust():
    """Coordinator says: move your clock by delta seconds."""
    global clock_adjustment
    delta = float(request.get_json(force=True).get("delta", 0))
    clock_adjustment += delta
    ts = tick()
    log_event("CLOCK_ADJUSTED", f"clock moved by {delta:+.3f}s", ts)
    return jsonify(ok=True, new_time=now(), lamport=ts)


if __name__ == "__main__":
    try:
        mongo_client.admin.command("ping")
        initialize_seats()
    except PyMongoError as exc:
        raise SystemExit(f"Cannot start venue server: MongoDB is unavailable: {exc}") from exc
    print(f"[{VENUE_NAME}] port {PORT}, clock skew {CLOCK_SKEW:+}s")
    app.run(host="0.0.0.0", port=PORT, threaded=True)
