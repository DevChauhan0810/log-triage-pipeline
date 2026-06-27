#  LOG TRIAGE PIPELINE  (fast multi-server version)
#  Finds every failing server, takes only the LAST few events
#  of each server's timeline, and analyzes them with Gemma in
#  PARALLEL for speed. Validates each result and saves a JSON
#  array report.


import re
import json
import ollama
from concurrent.futures import ThreadPoolExecutor, as_completed

LOG_FILE = "HDFS_2k.log"            # change to your actual filename
OUTPUT_FILE = "triage_report.json"  # the batch report we produce

EVENTS_PER_SERVER = 4   # how many of the most-recent events to send to Gemma
MAX_WORKERS = 8         # how many servers to analyze in parallel

# Words that signal something might be wrong.
TROUBLE_WORDS = ["error", "fatal", "exception", "failed", "warn", "critical"]

# Keys each result object must contain.
REQUIRED_KEYS = [
    "service_name",
    "timestamp",
    "error_severity",
    "hypothesis",
    "suggested_remediation",
]

IP_RE = re.compile(r"10\.\d+\.\d+\.\d+")
SEVERITY_RANK = {"INFO": 0, "WARN": 1, "ERROR": 2, "FATAL": 3, "CRITICAL": 4}


#  READING + HELPERS

def read_log(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.readlines()


def severity_of(line):
    for word, rank in SEVERITY_RANK.items():
        if f" {word} " in line:
            return rank
    return 0


def is_failure(line):
    return severity_of(line) >= SEVERITY_RANK["WARN"]


def timestamp_key(line):
    parts = line.split()
    if len(parts) >= 2:
        return (parts[0], parts[1])
    return ("", "")


#  GROUPING BY FAILING SERVER  (single pass, fast)


def build_server_map(lines):
    """
    One pass over the log. Returns:
      - failing_servers: ordered list of server IPs seen on a failure line
      - history_map: {server_ip: [all its lines]}
    """
    history_map = {}
    failing_servers = []
    seen_failing = set()

    for ln in lines:
        ips = IP_RE.findall(ln)
        # record this line under every server it mentions
        for ip in set(ips):
            history_map.setdefault(ip, []).append(ln)
        # the failing server is the FIRST ip on a failure line
        if is_failure(ln) and ips:
            first = ips[0]
            if first not in seen_failing:
                seen_failing.add(first)
                failing_servers.append(first)

    return failing_servers, history_map


def recent_events(history, n):
    """Return the last n events of a server's timeline, sorted by time."""
    ordered = sorted(history, key=timestamp_key)
    return ordered[-n:]


#  JSON VALIDATION


def clean_json_text(text):
    text = text.strip()
    if "```" in text:
        text = text.split("```")[1].replace("json", "", 1)
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1:
        text = text[start:end + 1]
    return text.strip()


def validate(text):
    try:
        data = json.loads(clean_json_text(text))
    except json.JSONDecodeError:
        return None
    for key in REQUIRED_KEYS:
        if key not in data or not str(data[key]).strip():
            return None
    return data


#  PROMPT

SYSTEM_PROMPT = """You are a log triage engine. You will be given the most recent log events from ONE server (identified by its IP address).
At least one event is a failure (WARN, ERROR, exception, or failed).

Study the events, form a HYPOTHESIS about what was happening to this server, and recommend a remediation.
You MUST extract real values from the logs. Never return empty fields.

Return a JSON object with exactly these five keys:
- service_name: the component from the failing line, e.g. "dfs.DataNode$DataXceiver"
- timestamp: the date and time numbers at the start of the failing line, e.g. "081109 214043"
- error_severity: the severity word from the failing line, e.g. "WARN"
- hypothesis: one short sentence on what was likely happening to this server
- suggested_remediation: one short sentence recommending a fix

Example:
{"service_name": "dfs.DataNode$DataXceiver", "timestamp": "081109 214043", "error_severity": "WARN", "hypothesis": "The server hit intermittent exceptions while otherwise serving blocks normally, pointing to transient network issues.", "suggested_remediation": "Check network connectivity and NIC health on this node."}"""


def ask_gemma(prompt_text, temperature=0.3):
    response = ollama.generate(
        model="gemma2:2b",
        system=SYSTEM_PROMPT,
        prompt=prompt_text,
        format="json",
        options={"temperature": temperature, "num_predict": 250},
    )
    return response["response"]


def analyze_one(server, history):
    """Analyze a single server. Returns a result dict or None. Thread-safe."""
    events = recent_events(history, EVENTS_PER_SERVER)
    total = len(history)
    fails = sum(1 for ln in history if is_failure(ln))
    log_text = "".join(events)

    result = validate(ask_gemma(log_text, temperature=0.3))
    if result is None:
        result = validate(ask_gemma(log_text, temperature=0.1))
    if result is None:
        return server, None

    result["server_ip"] = server
    result["timeline_events"] = total
    result["timeline_failures"] = fails
    return server, result


#  MAIN PIPELINE  (parallel)

def main():
    all_lines = read_log(LOG_FILE)
    servers, history_map = build_server_map(all_lines)

    print(f"Total lines in file:        {len(all_lines)}")
    print(f"Unique failing servers:     {len(servers)}")
    print(f"Events sent per server:     {EVENTS_PER_SERVER}")
    print(f"Parallel workers:           {MAX_WORKERS}")
    print("=" * 55)

    if not servers:
        print("No failing servers found. Nothing to triage.")
        return

    report = []
    failed = []
    done = 0

    # analyze servers concurrently
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(analyze_one, s, history_map[s]): s
            for s in servers
        }
        for fut in as_completed(futures):
            server, result = fut.result()
            done += 1
            if result is None:
                failed.append(server)
                print(f"[{done}/{len(servers)}] {server} ... FAILED")
            else:
                report.append(result)
                print(f"[{done}/{len(servers)}] {server} ... ok")

    # keep the report in a stable order (by server)
    report.sort(key=lambda r: r["server_ip"])

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("=" * 55)
    print(f"Analyzed OK:  {len(report)} servers")
    print(f"Failed:       {len(failed)} servers")
    print(f"[OK] Report saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()