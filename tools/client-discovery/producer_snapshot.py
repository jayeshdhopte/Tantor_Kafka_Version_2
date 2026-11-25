"""Collect broker-observed Produce requests and publish topic discovery snapshots."""

import argparse
from datetime import datetime, timedelta, timezone
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
import uuid
from zoneinfo import ZoneInfo


COMPLETED = re.compile(r"\bCompleted request:\s*(?=\{)")
TIMESTAMP = re.compile(r"^\[?(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?)")
CONNECTION = re.compile(r"[\[\]a-fA-F0-9:.]+:\d+-([\[\]a-fA-F0-9:.]+):\d+(?:-\d+-\d+)?")
LEGACY = re.compile(
    r"RequestHeader\(apiKey=(?:PRODUCE|0)[, ]+[^)]*?\bclientId=([^,\s)]+)[^)]*\)"
)
LEGACY_REMOTE = re.compile(r"\bfrom connection ([^\s,]+)")
LEGACY_TOPIC = re.compile(r"\btopic=([a-zA-Z0-9._-]+)(?=[,\s)\]]|$)")
TOPIC = re.compile(r"[a-zA-Z0-9._-]{1,249}")
KAFKA_ID = re.compile(r"[a-zA-Z0-9_-]{22}")
TOPIC_ID = re.compile(r"\bTopic:\s+([a-zA-Z0-9._-]+)\s+TopicId:\s+([A-Za-z0-9_-]+)")
SCAN_LIMIT = 4 * 1024 * 1024
SNAPSHOT_LIMIT = 2 * 1024 * 1024


def request_time(line, zone):
    match = TIMESTAMP.search(line)
    if not match:
        return None
    try:
        stamp = datetime.fromisoformat(match[1].replace(",", ".").replace("Z", "+00:00"))
        return (stamp if stamp.tzinfo else stamp.replace(tzinfo=zone)).astimezone(timezone.utc)
    except ValueError:
        return None


def request_fields(line):
    completed = COMPLETED.search(line)
    if completed:
        record, _ = json.JSONDecoder().raw_decode(line[completed.end():])
        header = record["requestHeader"]
        api_key = header["requestApiKey"]
        if type(api_key) is not int or api_key != 0:
            return ()
        topics = [item.get("name") or "id:" + (item.get("topicId") or "")
                  for item in record["request"]["topicData"]]
        return record["connection"], header.get("clientId"), topics

    header = LEGACY.search(line)
    remote = LEGACY_REMOTE.search(line)
    if header and remote:
        return remote[1], header[1], LEGACY_TOPIC.findall(line)
    return ()


def recent_requests(log, zone, now, window):
    if not log.is_file():
        raise ValueError("Request log must be a regular file")
    with log.open("rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError("Request log must be a regular file")
        size = stream.seek(0, os.SEEK_END)
        offset = max(0, size - SCAN_LIMIT)
        stream.seek(offset)
        lines = stream.read(SCAN_LIMIT).decode("utf-8", errors="replace").split("\n")
    if offset:
        lines = lines[1:]
    if lines and lines[-1]:
        lines.pop()
    cutoff = now - timedelta(minutes=window)
    timestamps = [stamp for line in lines if (stamp := request_time(line, zone)) is not None]
    complete = bool(timestamps) and (offset == 0 or min(timestamps) <= cutoff)
    found = {}
    for line in lines:
        seen = request_time(line, zone)
        if seen is None or seen < cutoff or seen > now + timedelta(seconds=30):
            continue
        try:
            fields = request_fields(line)
            if not fields:
                continue
            connection, client_id, topics = fields
            peer = CONNECTION.fullmatch(connection)
            if not peer or not isinstance(client_id, str) or not client_id.strip() or len(client_id) > 255:
                complete = False
                continue
            ip = ipaddress.ip_address(peer[1].strip("[]"))
            if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped:
                ip = ip.ipv4_mapped
            valid_topics = [topic for topic in topics if isinstance(topic, str)
                            and (TOPIC.fullmatch(topic) or (topic.startswith("id:") and KAFKA_ID.fullmatch(topic[3:])))]
            if not valid_topics or len(valid_topics) != len(topics):
                complete = False
            for topic in valid_topics:
                key = (str(ip), client_id, topic)
                found[key] = max(found.get(key, seen), seen)
        except (ValueError, KeyError, TypeError, AttributeError):
            complete = False
    return complete, [
        {"client_ip": ip, "client_id": client_id, "topic": topic, "last_seen": seen.isoformat(),
         "client_type": "PRODUCER", "evidence_source": "broker_request_telemetry", "mapping_confidence": "CONFIRMED"}
        for (ip, client_id, topic), seen in sorted(found.items(), key=lambda row: row[1], reverse=True)
    ]


def collect(args):
    properties = dict(line.split("=", 1) for line in args.metadata.read_text().splitlines()
                      if "=" in line and not line.lstrip().startswith("#"))
    if properties.get("cluster.id") != args.kafka_cluster_id:
        raise ValueError("Broker metadata belongs to a different Kafka cluster")
    available, rows = recent_requests(
        args.request_log, timezone.utc if args.timezone == "UTC" else ZoneInfo(args.timezone),
        datetime.now(timezone.utc), args.lookback
    )
    print(json.dumps({"available": available and len(rows) <= 2000, "producers": rows[:2000]}))


def publish(args):
    payload = json.load(sys.stdin)
    observations = payload["reports"]
    topic_names = dict((topic_id, name) for name, topic_id in TOPIC_ID.findall(payload["topic_descriptions"]))
    if not 0 <= len(observations) <= args.brokers_total or args.brokers_total < 1:
        raise ValueError("Broker inventory and results disagree")
    available = 0
    combined = {}
    for report in observations:
        complete = report["available"]
        for row in report.get("producers", []):
            if row["topic"].startswith("id:"):
                topic = topic_names.get(row["topic"][3:])
                if topic is None:
                    complete = False
                    continue
                row["topic"] = topic
            key = (row["client_ip"], row["client_id"], row["topic"])
            current = combined.get(key)
            if current is None or row["last_seen"] > current["last_seen"]:
                combined[key] = row
        available += int(complete)
    snapshot = {
        "schema_version": 1, "cluster_id": str(args.cluster_id), "kafka_cluster_id": args.kafka_cluster_id,
        "snapshot_timestamp": args.captured_at,
        "telemetry_coverage": {"brokers_total": args.brokers_total, "brokers_available": available},
        "producers": list(combined.values()),
    }
    data = (json.dumps(snapshot, separators=(",", ":")) + "\n").encode()
    if len(data) > SNAPSHOT_LIMIT:
        raise ValueError("Snapshot exceeds 2 MiB. Reduce the collection lookback.")
    target = args.directory / (str(args.cluster_id) + ".json")
    with tempfile.NamedTemporaryFile(dir=args.directory, delete=False) as stream:
        pending = Path(stream.name)
        try:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
            stream.close()
            os.chmod(pending, 0o640)
            os.replace(pending, target)
        finally:
            pending.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    read = subcommands.add_parser("collect")
    read.add_argument("--metadata", required=True, type=Path)
    read.add_argument("--request-log", required=True, type=Path)
    read.add_argument("--kafka-cluster-id", required=True)
    read.add_argument("--timezone", required=True)
    read.add_argument("--lookback", type=int, choices=range(1, 61), default=15)
    write = subcommands.add_parser("publish")
    write.add_argument("--cluster-id", required=True, type=uuid.UUID)
    write.add_argument("--kafka-cluster-id", required=True)
    write.add_argument("--captured-at", required=True)
    write.add_argument("--brokers-total", type=int, required=True)
    write.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "collect":
        collect(args)
    else:
        publish(args)


if __name__ == "__main__":
    main()
