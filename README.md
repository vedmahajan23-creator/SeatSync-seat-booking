# SeatSync — Distributed Systems Mini Project

SeatSync is a distributed seat-booking demo for exploring distributed-system design. It runs a browser UI, two venue services, two elected coordinators, and MongoDB. It includes interactive teaching labs for topics that are not part of the live booking path.

> **Coursework scope:** this is a practical teaching project, not a production booking service and not a production implementation of every syllabus algorithm. The main application, simulated labs, and explanatory notes are identified separately below.

## Requirements

- Docker Desktop with Docker Compose (recommended), or Python 3.10+ and MongoDB 7 for local Python execution.
- For Kubernetes: a local Kubernetes cluster such as Minikube or kind, plus `kubectl` and Docker.
- For the optional Hadoop exercise: Docker Compose and a Java runtime/compiler inside the selected Hadoop image.

## Run the application with Docker Compose

From PowerShell in the project directory:

```powershell
docker compose up -d --build
```

Open:

- Booking application: <http://localhost:8080>
- Interactive syllabus labs: <http://localhost:8080/labs.html>

Check service state and logs:

```powershell
docker compose ps
docker compose logs -f
```

Stop the application but keep saved bookings:

```powershell
docker compose down
```

Bookings are persisted in the `mongodb_data` named volume. To intentionally erase the database and all bookings, run `docker compose down -v`; this removes the persistent volume.

### Demonstrate leader failover

In another PowerShell window, from the project directory:

```powershell
docker compose stop coordinator_2
```

Wait a few seconds, refresh the page, and observe Coordinator 1 taking over. Then restore the higher-ID node:

```powershell
docker compose start coordinator_2
```

Coordinator 2 should reclaim leadership after its election. To verify booking persistence, book a seat, restart the venue service, then reload the seat map:

```powershell
docker compose restart venue_a
```

## Run the automated booking tests

The tests use an isolated temporary MongoDB database and remove it afterward. Start MongoDB first, either with the Compose service or a local MongoDB server:

```powershell
docker compose up -d mongodb
py -m pip install -r requirements.txt
py -m unittest discover -s tests -v
```

## Run without Docker for the application services

MongoDB is still required. Start only the MongoDB service in the background:

```powershell
docker compose up -d mongodb
```

Then install Python packages and start the venue/coordinator processes:

```powershell
py -m pip install -r requirements.txt
py run_local.py
```

Open <http://localhost:8080> only if the frontend is being served separately. For the simplest frontend setup, from a second PowerShell window run:

```powershell
py -m http.server 8080 --directory frontend
```

The local processes are controlled in the `run_local.py` window:

```text
kill 2
start 2
status
quit
```

The browser page uses the same local coordinator and venue ports as the Compose configuration. Avoid running the local Python services and their Docker equivalents at the same time because they bind the same ports.

## Kubernetes

Kubernetes manifests and local-cluster commands are in [`k8s/README.md`](./k8s/README.md). The manifests deploy the frontend, both venue services, both coordinators, and MongoDB with a persistent volume claim. For a local cluster, build and load the application images before deploying.

## What is implemented

| Syllabus theme | Project coverage |
|---|---|
| Distributed application, client-server architecture, service communication | **Implemented:** browser → coordinator → venue services using HTTP/JSON; coordinators exchange TCP election/heartbeat messages. |
| Containerization and orchestration | **Implemented:** Docker Compose; Kubernetes manifests are also provided for local-cluster deployment. |
| Persistence and mutual exclusion | **Implemented:** venue services share MongoDB and use an atomic conditional update per seat, preventing concurrent double-booking across venue-service instances. MongoDB itself is a single local instance, not a replicated database cluster. |
| Bully election and basic fault handling | **Implemented:** two coordinators elect a leader and detect a missed heartbeat. This is a demo; it is not quorum consensus and is vulnerable to network partitions/split brain. |
| Lamport logical clocks | **Implemented in application messages/events:** event ordering uses Lamport timestamps. |
| Physical clock synchronization | **Implemented:** simplified Berkeley synchronization among coordinator and venue clocks. |
| Hadoop / MapReduce | **Optional implementation:** the `hadoop/` directory starts a small HDFS/YARN cluster and runs a word-count job on booking activity. |
| Vector clocks, global state, ring election, mutual-exclusion algorithms, deadlock detection, beacon protocol | **Educational labs/simulations:** interactive exercises in `frontend/labs.html`; these are not used to coordinate real bookings. |
| WebRTC and message/stream patterns | **Educational demos:** the labs simulate RPC, queue and stream behavior; WebRTC creates two local browser peers and exchanges a data-channel message. It does not provide a deployed multi-user signaling service. |
| Distributed computation, virtualization, multimedia, middleware, architectures and design issues | **Explanatory material:** concept notes in the labs; this project does not implement a multimedia pipeline, virtualization platform, or general-purpose middleware. |
| Serverless, distributed file systems, blockchain, distributed database trade-offs and named case studies | **Study notes:** the labs explain these systems and trade-offs. Hadoop demonstrates HDFS and batch processing, but there is no blockchain, serverless deployment, Cloudflare/AWS/Megaport integration, or multi-node database. |
| Lodha–Kshemkalyani fair mutual exclusion and Knapp deadlock classification | **Self-study notes only:** the labs outline the topics, not full implementations or formal proofs. |

See the labs page for a complete map of concepts and explicit limits: <http://localhost:8080/labs.html>.

## Suggested demonstration

1. Show the booking app and point out the separate venue services and coordinator cluster.
2. Book a seat, then restart its venue. The booking should remain because its state is in MongoDB.
3. Stop Coordinator 2 and observe failover to Coordinator 1; restart it and show the new election.
4. Open **Syllabus labs** and demonstrate Lamport/vector clocks, a simulated election, mutual exclusion, deadlock cycle detection, and WebRTC local data-channel messaging.
5. Run the optional Hadoop word-count job after creating booking activity.

## Optional Hadoop word count

With the booking application running and some activity recorded, run from the project root:

```powershell
.\hadoop\run_wordcount.bat
```

The script fetches the activity log from a coordinator, starts the separate Hadoop cluster, runs MapReduce, and prints common words. HDFS and YARN web UIs are at <http://localhost:9870> and <http://localhost:8088>.

## Known limitations and next steps

- MongoDB is a single-node service in this local project. It provides persistent, atomic seat updates, but not database high availability or cross-region replication.
- Booking documents persist; the event feed, Lamport counters, election history, and clock synchronization history are in memory and reset when their services restart.
- The coordinators do not use quorum consensus; a network partition may cause both nodes to believe they are leader. The venue's atomic seat update still prevents a double booking for an individual seat.
- Cancellation is a simple demo operation and does not include user authentication or authorization.
- Most syllabus topics are represented by educational explanations or explicitly labelled simulations. For an assessed presentation, state this distinction and explain one or more limits/design trade-offs.
