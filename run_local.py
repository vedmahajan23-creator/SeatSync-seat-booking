"""
Run the application services on one laptop without Docker. MongoDB must be
running locally first (for example, `docker compose up -d mongodb`).

    python run_local.py

Type these commands in the same window while it runs:
    kill 2     stop Coordinator 2 (simulates a crash)
    start 2    bring Coordinator 2 back
    status     show which processes are alive
    quit       stop everything
"""
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
VENUES = "A=http://127.0.0.1:5001,B=http://127.0.0.1:5002"

PROCS = {
    "venue_a": ("venue/venue_server.py", {"VENUE_ID": "A", "VENUE_NAME": "Jai Ganesh, Akurdi", "PORT": "5001", "CLOCK_SKEW_SECONDS": "-3"}),
    "venue_b": ("venue/venue_server.py", {"VENUE_ID": "B", "VENUE_NAME": "Elpro, Pune", "PORT": "5002", "CLOCK_SKEW_SECONDS": "7"}),
    "1": ("coordinator/coordinator_node.py", {"NODE_ID": "1", "HTTP_PORT": "5000", "ELECTION_PORT": "6001",
          "PEERS_TCP": "2=127.0.0.1:6002", "PEERS_HTTP": "2=http://127.0.0.1:5010", "VENUES": VENUES, "CLOCK_SKEW_SECONDS": "0"}),
    "2": ("coordinator/coordinator_node.py", {"NODE_ID": "2", "HTTP_PORT": "5010", "ELECTION_PORT": "6002",
          "PEERS_TCP": "1=127.0.0.1:6001", "PEERS_HTTP": "1=http://127.0.0.1:5000", "VENUES": VENUES, "CLOCK_SKEW_SECONDS": "2"}),
}
running = {}


def start(name):
    script, env = PROCS[name]
    if name in running and running[name].poll() is None:
        print(f"{name} is already running")
        return
    running[name] = subprocess.Popen([sys.executable, os.path.join(HERE, script)],
                                     env={**os.environ, "PYTHONUNBUFFERED": "1", **env},
                                     cwd=HERE, stdin=subprocess.DEVNULL)
    print(f"started {name}")


def stop(name):
    p = running.get(name)
    if p and p.poll() is None:
        p.terminate()
        p.wait(timeout=5)
        print(f"stopped {name}")
    else:
        print(f"{name} is not running")


def main():
    for name in ["venue_a", "venue_b", "1", "2"]:
        start(name)
        time.sleep(1.5)
    print("\nServe the frontend from the frontend folder on port 8080. Commands: kill 2, start 2, kill 1, start 1, status, quit\n")
    try:
        for line in sys.stdin:
            parts = line.split()
            if not parts:
                continue
            if parts[0] == "quit":
                break
            if parts[0] == "status":
                for n, p in running.items():
                    print(f"  {n}: {'running' if p.poll() is None else 'stopped'}")
            elif parts[0] in ("kill", "start") and len(parts) == 2 and parts[1] in PROCS:
                (stop if parts[0] == "kill" else start)(parts[1])
            else:
                print("unknown command")
    except KeyboardInterrupt:
        pass
    finally:
        for n in list(running):
            if running[n].poll() is None:
                running[n].terminate()
        print("everything stopped")


if __name__ == "__main__":
    main()
