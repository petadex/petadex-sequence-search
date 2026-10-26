"""SNS → ntfy relay for the search CloudWatch alarms.

Subscribed to the `petadex-search-alarms` SNS topic. Each alarm state change
becomes one short push to the private ntfy topic in env `NTFY_TOPIC` (a
secret: anyone who knows it can read and post to it). Messages are status
only (which alarm, what state), never ARNs, IPs or log lines.

Deployed by scripts/ops/setup_search_alarms.sh; not part of the search image.
"""
import json
import os
import urllib.request

NTFY_URL = "https://ntfy.sh/"

# alarm name → (title, message when firing, priority when firing)
ALARMS = {
    "petadex-search-worker-throttles": (
        "PETadex search failing",
        "Search workers are being throttled, so searches are failing (fail-fast). "
        "Check worker reserved concurrency and running jobs.",
        5,
    ),
    "petadex-search-unavailable": (
        "PETadex search unavailable",
        "Search is refusing everyone: the hourly/daily budget is used up, "
        "or the admission check can't run. Check orchestrator logs.",
        4,
    ),
    "petadex-search-refused": (
        "PETadex search refused a user",
        "A search was refused by the per-client limit or because search was busy.",
        3,
    ),
}


def notify(title, message, priority, tags):
    req = urllib.request.Request(
        NTFY_URL + os.environ["NTFY_TOPIC"],
        data=message.encode(),
        headers={"Title": title, "Priority": str(priority), "Tags": tags},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=8) as resp:
        resp.read()


def handler(event, context):
    for record in event.get("Records", []):
        alarm = json.loads(record["Sns"]["Message"])
        name, state = alarm.get("AlarmName", ""), alarm.get("NewStateValue", "")
        title, message, priority = ALARMS.get(
            name, ("PETadex search alarm", f"Alarm {name} changed state.", 3))
        if state == "ALARM":
            notify(title, message, priority, "warning")
        elif state == "OK" and alarm.get("OldStateValue") == "ALARM":
            notify(f"{title}: resolved", "Back to normal.", 2, "white_check_mark")
        # INSUFFICIENT_DATA and OK-from-INSUFFICIENT_DATA are not news.
        print(f"{name}: {alarm.get('OldStateValue')} -> {state}")
