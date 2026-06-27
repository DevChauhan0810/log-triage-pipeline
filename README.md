# Raw Log Triage Pipeline — Track 2 (Log-to-JSON)

A command-line utility that turns a raw, noisy server-log dump into a clean,
validated JSON report. It filters out benign noise locally, then uses a local
Gemma model (via Ollama) to analyse each failing server and produce a
hypothesis + suggested remediation for every incident.

Built for the AI-First Developer Efficiencies hackathon, Track 2.

## What it does

Instead of returning a single error, the pipeline groups failures by server,
reconstructs each server's recent timeline, and asks Gemma to reason about the
pattern of events rather than a single isolated line.

## Pipeline (5 stages)

1. Read the raw log file.
2. Filter - keep only suspicious candidate windows (a failing line plus neighbours).
3. Group - for every failing server, gather its timeline and keep recent events.
4. Analyse - send each server's events to Gemma in parallel for hypothesis + fix.
5. Validate - parse, repair, enforce schema, retry once, then save the batch.

## Setup

    ollama pull gemma2:2b
    pip install ollama

## Run

    python triage.py

Reads the log named in LOG_FILE and writes triage_report.json.

## Output schema

Each entry: service_name, timestamp, error_severity, hypothesis,
suggested_remediation, server_ip, timeline_events, timeline_failures.

## Configuration

Tunable constants at the top of triage.py: LOG_FILE, EVENTS_PER_SERVER,
MAX_WORKERS, CONTEXT_LINES.

## Data

Tested against the public Loghub HDFS sample (HDFS_2k.log).
Produces a 64-server triage report.
