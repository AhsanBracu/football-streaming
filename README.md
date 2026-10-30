# Live Football Analytics with a Streaming Data Pipeline

ID2221 Data-Intensive Computing (KTH), Group 28: Md Ahsanul Karim, Md. Sakibul Islam, Singvalliyappa Velayutham.

The pipeline turns a raw stream of football match events into a live, minute-by-minute view of
**which team is on top**. Recorded World Cup 2022 matches are replayed as if they were live. Spark
computes per-team statistics (pressures, passes, shots, xG) over sliding 5-minute windows of match
time, and a web dashboard updates within seconds of each event.

The report is in [`report/Football_Stream_Project_Report.pdf`](report/Football_Stream_Project_Report.pdf).

## Architecture

```
StatsBomb JSON ──► HDFS ──► Python producer ──► Kafka ──► Spark Structured ──► MongoDB ──► Node.js server ──► React dashboard
 (download)       (store)   (replays events     (topic     Streaming           (replica   (Change Stream      (browser,
                            at original timing)  football-  (sliding windows)    set)       → WebSocket)        live charts)
                                                 events)
```

Everything runs in Docker Compose on one machine:

| Service | Image / code | Web UI on your machine |
|---|---|---|
| HDFS namenode + datanode | `apache/hadoop:3.3.6` | http://localhost:9870 |
| Kafka (KRaft, 1 broker) | `apache/kafka:4.1.0` | — |
| Kafka UI | `kafbat/kafka-ui` | http://localhost:8080 |
| Spark master + 1 worker (2 cores) | `apache/spark:3.5.3` | http://localhost:8081 (master), http://localhost:8082 (worker) |
| MongoDB (single-node replica set `rs0`) | `mongo:7.0` | `mongodb://localhost:27018/?directConnection=true` |
| Dashboard | `dashboard/` (Node 24, Express, ws, React, Recharts) | **http://localhost:3000** |
| Producer (run on demand) | `producer/` (Python) | — |

## Repository layout

```
docker-compose.yml        all services, with memory caps
scripts/                  download_statsbomb.py, upload_to_hdfs.sh, create_topics.sh, run_spark_job.sh
producer/replay.py        reads matches from HDFS (WebHDFS) and replays them into Kafka
spark/streaming_job.py    Kafka → parse → sliding windows → MongoDB (foreachBatch upsert)
dashboard/server/         Express + WebSocket server, pushes MongoDB Change Stream updates
dashboard/client/         React + Recharts dashboard (built into the dashboard image)
evaluation/               latency.py, match_summary.py and their results (results.csv, match_summary.csv)
report/                   report source (LaTeX), figure and PDF
```

## Requirements

- **Docker Desktop** with at least **4 GB of memory** (Settings → Resources). Every service has a
  memory cap so the whole stack fits in about 3.5 GB.
- **Python 3** on your machine, for the download script and the evaluation scripts.
  The evaluation scripts need `pymongo`: `python3 -m pip install pymongo`.
- Free ports: 3000, 8080, 8081, 8082, 9094, 9870, 27018.

Tested on an Apple-silicon Mac (8 GB RAM). The Hadoop image is Intel-only and runs through emulation
there, which works but is slower.

## Dataset

[StatsBomb Open Data](https://github.com/statsbomb/open-data), FIFA World Cup 2022 (competition 43,
season 106). We use three matches (8.9 MB), which are not included in this zip:

| Match id | Match | Events |
|---|---|---|
| 3857276 | Canada – Morocco 1–2 | 3 388 |
| 3857271 | England – Iran 6–2 | 3 649 |
| 3857296 | Croatia – Belgium 0–0 | 4 116 |

## How to run

All commands are run from the project folder.

**1. Download the dataset** (into `data/statsbomb/`):
```bash
python3 scripts/download_statsbomb.py --limit 3
```

**2. Start all services.** The first time this builds the dashboard image and pulls the other images,
which takes a few minutes.
```bash
docker compose up -d
docker compose ps        # wait until mongodb shows "(healthy)"
```

**3. Create the Kafka topic and upload the data to HDFS.** Both scripts wait until Kafka/HDFS are ready.
```bash
./scripts/create_topics.sh
./scripts/upload_to_hdfs.sh
```

**4. Start the Spark job (terminal 1).** It keeps running; stop it with Ctrl+C.
```bash
./scripts/run_spark_job.sh --reset
```
Wait until it prints `batch 0: 0 windows written` (about 30 s; the first run also downloads the
Kafka and MongoDB connectors). `--reset` starts with an empty checkpoint; without it, the job
continues where it stopped.

**5. Open the dashboard:** http://localhost:3000

**6. Replay matches (terminal 2).** Start this only after step 4 shows `batch 0`: the job reads only
messages sent after it started, so earlier events would be missed.
```bash
docker compose run --rm producer --speed 10                    # all 3 matches at once, 10x faster than real time
docker compose run --rm producer --speed 10 --match 3857276    # only Canada – Morocco
```
At speed 10 a match takes about 10 minutes. The dashboard shows the newest replay automatically;
older replays can be chosen in the selector at the top right.

## Evaluation

**Football results**, after a replay has finished. This prints, per team, passes, pressures, shots,
xG, goals and how often the team out-pressed its opponent, and writes `evaluation/match_summary.csv`:
```bash
python3 evaluation/match_summary.py
```

**End-to-end latency** (producer → Kafka → Spark → MongoDB → consumer). Start the script first, then a
replay in another terminal; press Ctrl+C about 30 s after the replay ends. The summary (median, p95,
max) is appended to `evaluation/results.csv`:
```bash
python3 evaluation/latency.py --label "1 match, speed 10"
docker compose run --rm producer --speed 10 --match 3857276
```

Our results (one run each): 1 match at speed 10 → median 4.9 s; 1 match at speed 60 → 5.1 s;
3 matches at speed 30 → 9.7 s. See the report for the discussion.

## Troubleshooting

| Problem | Cause and fix |
|---|---|
| `run_spark_job.sh` prints `Waiting for HDFS to be ready...` for a while | Normal right after `docker compose up`: HDFS stays read-only (safe mode) for ~30 s after start. |
| The first minutes of a replay are missing | The producer was started before Spark's `batch 0`. Rerun the replay once the job is running. |
| A container stops with exit code 137 | Out of memory. Give Docker more memory, or stop Kafka UI: `docker compose stop kafka-ui`. |
| Port already in use | Another program uses one of the ports above (e.g. a local MongoDB or Node server). Stop it or change the left side of the port mapping in `docker-compose.yml`. |
| Changes to `producer/replay.py` have no effect | `docker compose run` reuses the old image. Rebuild: `docker compose build producer`. |
| The dashboard shows no data | Check that the Spark job is running (terminal 1) and that a replay was started after it. |

## Stopping

```bash
docker compose down        # stop everything, keep the data (HDFS, Kafka, MongoDB volumes)
docker compose down -v     # stop everything and delete all data
```
