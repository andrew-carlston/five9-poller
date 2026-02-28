"""Five9 Agent State Poller — cron entry point.

Called every minute by Render cron job.
Grabs Five9 agent state snapshot and writes to Supabase.
"""

import os
import sys
import time
from datetime import datetime, timezone

import requests
import xml.etree.ElementTree as ET

# ── Config from environment ──────────────────────────────────
FIVE9_USER = os.environ.get("FIVE9_USER", "")
FIVE9_PASS = os.environ.get("FIVE9_PASS", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

FIVE9_SOAP_URL = "https://api.five9.com/wssupervisor/v14/SupervisorWebService"
FIVE9_NS = "http://service.supervisor.ws.five9.com/"


def five9_soap_call(method, body_xml=""):
    soap = f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:ser="{FIVE9_NS}">
  <soapenv:Header/>
  <soapenv:Body>
    <ser:{method}>{body_xml}</ser:{method}>
  </soapenv:Body>
</soapenv:Envelope>"""
    return requests.post(
        FIVE9_SOAP_URL,
        auth=(FIVE9_USER, FIVE9_PASS),
        headers={"Content-Type": "text/xml; charset=UTF-8", "SOAPAction": ""},
        data=soap,
        timeout=30,
    )


def main():
    start = time.time()
    snapshot_ts = datetime.now(timezone.utc).isoformat()

    # Set session parameters
    resp = five9_soap_call("setSessionParameters", """<viewSettings>
        <rollingPeriod>Today</rollingPeriod>
        <statisticsRange>CurrentDay</statisticsRange>
      </viewSettings>""")
    if resp.status_code != 200:
        print(f"FAIL: Session setup returned {resp.status_code}")
        sys.exit(1)

    # Pull agent states
    resp = five9_soap_call("getStatistics",
                           "<statisticType>AgentState</statisticType>")
    if resp.status_code != 200:
        print(f"FAIL: getStatistics returned {resp.status_code}")
        sys.exit(1)

    # Parse XML
    root = ET.fromstring(resp.text)
    for elem in root.iter():
        if "}" in elem.tag:
            elem.tag = elem.tag.split("}", 1)[1]

    columns_node = root.find(".//columns/values")
    if columns_node is None:
        print("FAIL: No columns in response")
        sys.exit(1)
    columns = [d.text or "" for d in columns_node.findall("data")]

    rows = []
    for row_node in root.findall(".//rows"):
        values_node = row_node.find("values")
        if values_node is None:
            continue
        values = [d.text or "" for d in values_node.findall("data")]
        agent = dict(zip(columns, values))

        if agent.get("State") == "Logged Out":
            continue

        rows.append({
            "snapshot_ts": snapshot_ts,
            "username": agent.get("Username", ""),
            "full_name": agent.get("Full Name", ""),
            "state": agent.get("State", ""),
            "reason_code": agent.get("Reason Code", ""),
            "state_since": agent.get("State Since", ""),
            "state_duration": agent.get("State Duration", ""),
            "campaign_name": agent.get("Campaign Name", ""),
            "call_type": agent.get("Call Type", ""),
            "media_availability": agent.get("Media Availability", ""),
        })

    if not rows:
        print(f"OK: 0 active agents (all logged out)")
        sys.exit(0)

    # Write to Supabase
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/five9_agent_snapshots",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
        json=rows,
        timeout=30,
    )

    if resp.status_code not in (200, 201):
        print(f"FAIL: Supabase returned {resp.status_code}: {resp.text[:300]}")
        sys.exit(1)

    elapsed = round(time.time() - start, 2)
    print(f"OK: {len(rows)} agents written | {elapsed}s")


if __name__ == "__main__":
    main()
