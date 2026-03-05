#!/usr/bin/env python3
"""
Pediatric Asthma Reddit Tracker — Web Dashboard

Serves a Chart.js dashboard with 6 modules, auto-scrapes every 6 hours,
and provides API endpoints for all data.

Usage:
    python asthma_tracker_web.py
    # Dashboard at http://localhost:8053
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import asthma_tracker as tracker

PORT = 8053
SCRAPE_INTERVAL = 6 * 60 * 60  # 6 hours


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

class Scheduler:
    def __init__(self, interval: float = SCRAPE_INTERVAL):
        self.interval = interval
        self.last_run = None
        self.next_run = None
        self.running = False
        self._stop_event = threading.Event()
        self._run_now_event = threading.Event()
        self._lock = threading.Lock()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        self._update_next_run()

    def stop(self):
        self._stop_event.set()

    def _update_next_run(self):
        self.next_run = (datetime.utcnow() + timedelta(seconds=self.interval)).isoformat(timespec="seconds") + "Z"

    def _loop(self):
        while not self._stop_event.is_set():
            slept = 0.0
            while slept < self.interval:
                if self._stop_event.is_set():
                    return
                if self._run_now_event.is_set():
                    self._run_now_event.clear()
                    break
                chunk = min(30.0, self.interval - slept)
                self._stop_event.wait(timeout=chunk)
                slept += chunk
            self._do_scrape()
            self._update_next_run()

    def _do_scrape(self):
        with self._lock:
            self.running = True
        try:
            tracker.run_scrape(limit=200, filter_today=False)
            self.last_run = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            if not os.environ.get("ASTHMA_TRACKER_NO_PUBLISH"):
                gen = Path(__file__).parent / "generate_site.py"
                if gen.exists():
                    try:
                        subprocess.Popen(
                            [sys.executable, str(gen), "--push"],
                            cwd=str(gen.parent),
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                        )
                    except Exception:
                        pass
        except Exception as e:
            tracker.log.error(f"Scheduled scrape failed: {e}")
        finally:
            with self._lock:
                self.running = False

    def run_now(self) -> dict:
        with self._lock:
            if self.running:
                return {"ok": False, "error": "Scrape already in progress."}
        self._run_now_event.set()
        for _ in range(100):
            time.sleep(0.05)
            with self._lock:
                if self.running:
                    break
        else:
            return {"ok": False, "error": "Scrape did not start."}
        for _ in range(900):
            time.sleep(1.0)
            with self._lock:
                if not self.running:
                    return {"ok": True, "last_run": self.last_run}
        return {"ok": True, "last_run": self.last_run, "note": "Scrape still running."}

    def status(self) -> dict:
        last = self.last_run
        if not last:
            conn = tracker.get_db()
            row = conn.execute("SELECT scraped_at FROM scrape_runs ORDER BY id DESC LIMIT 1").fetchone()
            if row:
                last = row[0]
            conn.close()
        return {
            "interval_hours": self.interval / 3600,
            "last_run": last,
            "next_run": self.next_run,
            "running": self.running,
        }


scheduler = Scheduler()


# ---------------------------------------------------------------------------
# HTML Dashboard
# ---------------------------------------------------------------------------

HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Pediatric Asthma Reddit Tracker</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
:root {
  --bg: #0f1117;
  --surface: #1a1d27;
  --border: #2a2d3a;
  --text: #e1e4ed;
  --muted: #8b90a0;
  --accent: #7c6ef0;
  --accent2: #e06090;
  --green: #4ade80;
  --yellow: #f0c040;
  --red: #f06060;
  --cyan: #40d0d0;
  --fear: #f0a040;
  --concordant: #4ade80;
  --discordant: #f06060;
  --uncertain: #f0c040;
}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;font-size:14px;line-height:1.5}
a{color:var(--accent)}
header{background:var(--surface);border-bottom:1px solid var(--border);padding:12px 20px;display:flex;align-items:center;gap:12px;flex-wrap:wrap}
header h1{font-size:16px;font-weight:600;white-space:nowrap}
header select,header input[type=date]{background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:6px 10px;font-size:13px}
.preset-btn{background:var(--bg);color:var(--muted);border:1px solid var(--border);border-radius:6px;padding:5px 10px;cursor:pointer;font-size:12px}
.preset-btn:hover,.preset-btn.active{background:var(--accent);color:#fff;border-color:var(--accent)}
.scrape-btn{background:var(--accent);color:#fff;border:none;border-radius:6px;padding:6px 14px;cursor:pointer;font-size:13px;font-weight:500}
.scrape-btn:hover{opacity:0.85}
.scrape-btn:disabled{opacity:0.5;cursor:not-allowed}
.date-label{color:var(--muted);font-size:12px;margin:0 4px}
.sched-bar{background:var(--surface);border-bottom:1px solid var(--border);padding:6px 20px;font-size:12px;color:var(--muted);display:flex;align-items:center;gap:8px}
.sched-dot{width:8px;height:8px;border-radius:50%;display:inline-block}
.sched-dot.idle{background:var(--green)}.sched-dot.running{background:var(--yellow);animation:pulse 1s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.4}}
nav.tab-bar{background:var(--surface);border-bottom:1px solid var(--border);padding:0 20px;display:flex;gap:0}
.tab-btn{background:none;border:none;color:var(--muted);padding:12px 20px;cursor:pointer;font-size:14px;font-weight:500;border-bottom:2px solid transparent}
.tab-btn:hover{color:var(--text)}
.tab-btn.active{color:var(--accent);border-bottom-color:var(--accent)}
main{max-width:1320px;margin:0 auto;padding:20px}
.tab-content{display:none}.tab-content.active{display:block}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:24px}
.stat{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:14px;text-align:center}
.stat-val{font-size:22px;font-weight:700;color:var(--accent)}
.stat-label{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;margin-top:4px}
.stat-val.fear{color:var(--fear)}
.stat-val.green{color:var(--green)}
.stat-val.red{color:var(--red)}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:20px}
@media(max-width:960px){.grid{grid-template-columns:1fr}}
.card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:20px;min-height:200px}
.card.full{grid-column:1/-1}
.card h2{font-size:15px;font-weight:600;margin-bottom:12px;display:flex;align-items:center;gap:8px}
.card h2 .badge{font-size:11px;padding:2px 8px;border-radius:10px;font-weight:400}
.chart-wrap{position:relative;height:300px}
.bar-rows{display:flex;flex-direction:column;gap:4px}
.bar-row{display:flex;align-items:center;gap:8px}
.bar-label{width:160px;font-size:12px;text-align:right;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.bar-track{flex:1;height:20px;background:var(--bg);border-radius:4px;overflow:hidden;position:relative}
.bar-fill{height:100%;border-radius:4px;transition:width 0.3s}
.bar-count{width:40px;font-size:12px;text-align:right;color:var(--muted)}
.bar-fill.ics{background:var(--accent)}.bar-fill.oral{background:var(--red)}.bar-fill.broncho{background:var(--cyan)}
.bar-fill.bio{background:var(--accent2)}.bar-fill.leuko{background:var(--yellow)}.bar-fill.combo{background:var(--green)}
.bar-fill.device{background:var(--muted)}.bar-fill.default{background:var(--accent)}
.bar-fill.env{background:var(--green)}.bar-fill.viral{background:var(--red)}.bar-fill.neb{background:var(--yellow)}
.bar-fill.concordant{background:var(--concordant)}.bar-fill.discordant{background:var(--discordant)}
.bar-fill.uncertain{background:var(--uncertain)}.bar-fill.unclear{background:var(--muted)}
.sent-bar{display:inline-block;width:80px;height:10px;border-radius:3px;vertical-align:middle}
.fear-bar{display:inline-block;width:60px;height:10px;border-radius:3px;vertical-align:middle}
.empty{color:var(--muted);text-align:center;padding:40px;font-style:italic}
.pill{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;border:1px solid var(--border);margin:1px}
.pill.pos{border-color:var(--green);color:var(--green)}.pill.neg{border-color:var(--red);color:var(--red)}
.pill.neu{border-color:var(--muted);color:var(--muted)}.pill.fear{border-color:var(--fear);color:var(--fear)}
.pill.definite{border-color:var(--accent);color:var(--accent)}.pill.likely{border-color:var(--cyan);color:var(--cyan)}
.methods-box{display:none;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:12px;margin-top:12px;font-size:12px;color:var(--muted)}
.methods-box.show{display:block}
.methods-btn{background:none;border:1px solid var(--border);color:var(--muted);border-radius:4px;padding:2px 8px;cursor:pointer;font-size:11px}
.methods-btn:hover{color:var(--text);border-color:var(--text)}
table.explorer{width:100%;border-collapse:collapse;font-size:13px}
table.explorer th{text-align:left;padding:8px;border-bottom:1px solid var(--border);color:var(--muted);font-size:11px;text-transform:uppercase}
table.explorer td{padding:8px;border-bottom:1px solid var(--border)}
table.explorer tr:hover{background:rgba(124,110,240,0.05)}
table.explorer tr.expanded-row{background:var(--bg)}
table.explorer tr.expanded-row td{padding:12px 16px}
.post-text{color:var(--muted);font-size:12px;max-height:150px;overflow-y:auto;margin-bottom:8px;white-space:pre-wrap}
.comment-item{border-left:2px solid var(--border);padding:6px 12px;margin:6px 0;font-size:12px}
.comment-meta{color:var(--muted);font-size:11px;margin-bottom:2px}
.explorer-controls{display:flex;gap:8px;align-items:center;margin-bottom:12px}
.explorer-controls select,.explorer-controls button{background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:6px 12px;font-size:13px}
.explorer-controls button:hover{border-color:var(--accent)}
/* Validate tab */
.facet-bar{display:flex;gap:0;margin-bottom:16px}
.facet-btn{background:none;border:1px solid var(--border);color:var(--muted);padding:8px 16px;cursor:pointer;font-size:13px}
.facet-btn:first-child{border-radius:6px 0 0 6px}.facet-btn:last-child{border-radius:0 6px 6px 0}
.facet-btn.active{background:var(--accent);color:#fff;border-color:var(--accent)}
.val-card{background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:16px;margin-bottom:12px}
.val-title{font-weight:600;margin-bottom:8px}
.val-text{color:var(--muted);font-size:13px;max-height:120px;overflow-y:auto;margin-bottom:10px;white-space:pre-wrap}
.val-actions{display:flex;gap:8px;flex-wrap:wrap}
.vote-btn{padding:6px 14px;border-radius:6px;cursor:pointer;font-size:13px;border:1px solid var(--border);background:var(--surface);color:var(--text)}
.vote-btn:hover{border-color:var(--accent)}
.vote-btn.selected{background:var(--accent);color:#fff;border-color:var(--accent)}
.vote-btn.yes{border-color:var(--green)}.vote-btn.yes.selected{background:var(--green)}
.vote-btn.no{border-color:var(--red)}.vote-btn.no.selected{background:var(--red)}
.likert-btn{min-width:40px;text-align:center}
.likert-btn[data-score="-1"]{border-color:var(--red)}.likert-btn[data-score="-1"].selected{background:var(--red)}
.likert-btn[data-score="-0.5"]{border-color:#f09060}.likert-btn[data-score="-0.5"].selected{background:#f09060}
.likert-btn[data-score="0"]{border-color:var(--muted)}.likert-btn[data-score="0"].selected{background:var(--muted)}
.likert-btn[data-score="0.5"]{border-color:#80c080}.likert-btn[data-score="0.5"].selected{background:#80c080}
.likert-btn[data-score="1"]{border-color:var(--green)}.likert-btn[data-score="1"].selected{background:var(--green)}
.val-stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:12px;margin-bottom:16px}
.val-stat{background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:10px;text-align:center}
.val-stat .val{font-size:20px;font-weight:700;color:var(--accent)}.val-stat .lbl{font-size:11px;color:var(--muted)}
.checkbox-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:4px;margin-bottom:8px}
.checkbox-grid label{font-size:12px;display:flex;align-items:center;gap:4px;cursor:pointer}
/* Feedback tab */
.feedback-wrap{max-width:600px;margin:0 auto}
.feedback-compose{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:20px;margin-bottom:20px}
.feedback-compose textarea{width:100%;background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:10px;min-height:80px;resize:vertical;font-family:inherit;font-size:14px}
.char-counter{text-align:right;font-size:11px;color:var(--muted);margin:4px 0 8px}
.feedback-submit{background:var(--accent);color:#fff;border:none;border-radius:6px;padding:8px 20px;cursor:pointer;font-size:14px}
.feedback-submit:hover{opacity:0.85}
.feedback-error{color:var(--red);font-size:12px;margin-top:6px}
.fb-item{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:12px;margin-bottom:8px;display:flex;gap:12px;align-items:flex-start}
.fb-vote{display:flex;flex-direction:column;align-items:center;gap:2px;min-width:40px}
.fb-vote button{background:none;border:none;cursor:pointer;font-size:18px;color:var(--muted)}
.fb-vote button:hover,.fb-vote button.voted{color:var(--accent)}
.fb-vote .count{font-size:14px;font-weight:600;color:var(--text)}
.fb-body{flex:1;font-size:13px}.fb-time{font-size:11px;color:var(--muted);margin-top:4px}
/* Heatmap */
.heatmap-wrap{overflow-x:auto}
table.heatmap{border-collapse:collapse;font-size:11px;width:100%}
table.heatmap th{padding:4px 6px;text-align:left;color:var(--muted);font-weight:400;border-bottom:1px solid var(--border)}
table.heatmap th.rotate{writing-mode:vertical-rl;transform:rotate(180deg);max-height:100px;text-align:right}
table.heatmap td{padding:4px 6px;text-align:center;border-bottom:1px solid var(--border)}
table.heatmap .row-label{text-align:right;color:var(--muted);white-space:nowrap;max-width:120px;overflow:hidden;text-overflow:ellipsis}
.heat-cell{min-width:28px;font-size:10px;color:rgba(255,255,255,0.8);border-radius:2px}
</style>
</head>
<body>

<header>
  <h1>Pediatric Asthma Tracker</h1>
  <select id="subFilter" onchange="saveState();loadAll()">
    <option value="">All subreddits</option>
    <option value="Asthma">r/Asthma</option>
    <option value="Parenting">r/Parenting</option>
    <option value="beyondthebump">r/beyondthebump</option>
    <option value="Mommit">r/Mommit</option>
    <option value="daddit">r/daddit</option>
    <option value="AskDocs">r/AskDocs</option>
    <option value="Allergies">r/Allergies</option>
    <option value="medical">r/medical</option>
    <option value="Pediatrics">r/Pediatrics</option>
    <option value="NewParents">r/NewParents</option>
  </select>
  <button class="preset-btn" onclick="setPreset(1)">24h</button>
  <button class="preset-btn" onclick="setPreset(7)">Week</button>
  <button class="preset-btn" onclick="setPreset(30)">Month</button>
  <button class="preset-btn" onclick="setPreset(90)">90d</button>
  <button class="preset-btn" onclick="setPreset(365)">1Y</button>
  <button class="preset-btn" onclick="setPreset(0)">All</button>
  <span id="customDateWrap" style="display:none">
    <span class="date-label">From</span><input type="date" id="dateFrom" onchange="saveState();loadAll()">
    <span class="date-label">To</span><input type="date" id="dateTo" onchange="saveState();loadAll()">
  </span>
  <button class="preset-btn" onclick="document.getElementById('customDateWrap').style.display='inline'">Custom</button>
  <button class="scrape-btn" id="scrapeBtn" onclick="triggerScrape()">Scrape Now</button>
</header>

<div class="sched-bar">
  <span class="sched-dot idle" id="schedDot"></span>
  <span id="schedText">Loading...</span>
</div>

<nav class="tab-bar">
  <button class="tab-btn active" onclick="switchTab('dashboard',this)">Dashboard</button>
  <button class="tab-btn" onclick="switchTab('validate',this)">Validate</button>
  <button class="tab-btn" onclick="switchTab('feedback',this)">Feedback</button>
</nav>

<main>
<!-- ==================== DASHBOARD TAB ==================== -->
<div class="tab-content active" id="tabDashboard">

<div class="stats" id="statsRow"></div>

<div class="grid">

<!-- Chart 1: Medication Mentions -->
<div class="card">
  <h2>Medication Mentions <button class="methods-btn" onclick="toggleMethods('m-meds')">Methods</button></h2>
  <div class="methods-box" id="m-meds">Post-level medication detection using regex patterns for ~35 medications in 8 classes (ICS, oral corticosteroids, bronchodilators, biologics, leukotriene modifiers, combination inhalers, devices). Only posts passing both asthma gate and pediatric gate are analyzed.</div>
  <div id="medBars" class="bar-rows"></div>
</div>

<!-- Chart 1b: Medication Sentiment + Fear -->
<div class="card">
  <h2>Medication Sentiment &amp; Fear</h2>
  <div style="font-size:11px;color:var(--muted);margin-bottom:6px">
    <span style="display:inline-block;width:10px;height:10px;background:#4ade80;border-radius:2px;margin-right:3px;vertical-align:middle"></span>Positive Sentiment
    <span style="display:inline-block;width:10px;height:10px;background:#f06060;border-radius:2px;margin:0 3px 0 12px;vertical-align:middle"></span>Negative Sentiment
    <span style="display:inline-block;width:10px;height:10px;background:#8b90a0;border-radius:2px;margin:0 3px 0 12px;vertical-align:middle"></span>Neutral
    <span style="display:inline-block;width:10px;height:10px;background:#f0a040;border-radius:2px;margin:0 3px 0 12px;vertical-align:middle"></span>Fear Score (0-1)
  </div>
  <div class="chart-wrap"><canvas id="sentChart"></canvas></div>
</div>

<!-- Chart 1c: Daily Trend -->
<div class="card full">
  <h2>Daily Medication Trend (Top 5)</h2>
  <div class="chart-wrap"><canvas id="trendChart"></canvas></div>
</div>

<!-- Chart 6: Singulair Deep Dive -->
<div class="card">
  <h2>Singulair Behavioral Effects <button class="methods-btn" onclick="toggleMethods('m-sing')">Methods</button></h2>
  <div class="methods-box" id="m-sing">Tracks 9 behavioral effects (nightmares, aggression, mood changes, suicidal ideation, sleep disturbances, anxiety, depression, personality changes, hyperactivity) in posts mentioning Singulair/montelukast. Also tracks 4 discourse categories: black box warning, starting decision, stopping decision, seeking alternatives.</div>
  <div id="singBars" class="bar-rows"></div>
</div>

<!-- Chart 6b: Singulair Discourse -->
<div class="card">
  <h2>Singulair Discourse Categories</h2>
  <div id="singDisc" class="bar-rows"></div>
</div>

<!-- Chart 2: ED Discourse -->
<div class="card">
  <h2>ED/Hospital Decision-Making <button class="methods-btn" onclick="toggleMethods('m-ed')">Methods</button></h2>
  <div class="methods-box" id="m-ed">Detects 4 categories of ED discourse: decision uncertainty ("should I go to the ER"), post-visit experience, return/repeat visits, and barriers to access (cost, insurance, wait times). Maps to 72-hour ED return research.</div>
  <div id="edBars" class="bar-rows"></div>
</div>

<!-- Chart 2b: ED Daily -->
<div class="card">
  <h2>ED Discourse Daily Trend</h2>
  <div class="chart-wrap"><canvas id="edChart"></canvas></div>
</div>

<!-- Chart 3: Treatment Beliefs -->
<div class="card full">
  <h2>Treatment Beliefs &amp; Misconceptions <button class="methods-btn" onclick="toggleMethods('m-beliefs')">Methods</button></h2>
  <div class="methods-box" id="m-beliefs">Detects 10 guideline-discordant beliefs with stance classification: <strong>concordant</strong> (aligns with NAEPP/GINA guidelines, i.e. debunking the myth), <strong>discordant</strong> (against guidelines), <strong>uncertain</strong> (questioning), <strong>unclear</strong>. Proximity-bounded regex patterns prevent false positives.</div>
  <div id="beliefBars" class="bar-rows"></div>
</div>

<!-- Chart 4: Triggers -->
<div class="card">
  <h2>Triggers &amp; Environmental Narratives <button class="methods-btn" onclick="toggleMethods('m-trig')">Methods</button></h2>
  <div class="methods-box" id="m-trig">Tracks 19 triggers in 3 categories: <strong>Environmental</strong> (mold, pollution, smoke, pets, pollen, dust mites, weather, cold air), <strong>Viral</strong> (RSV, cold, flu, COVID, respiratory infection, croup), <strong>Non-evidence-based</strong> (vaccines, diet/toxins, chemicals, mold toxicity, EMF).</div>
  <div id="trigBars" class="bar-rows"></div>
</div>

<!-- Chart 4b: Trigger Daily -->
<div class="card">
  <h2>Trigger Daily Trend</h2>
  <div class="chart-wrap"><canvas id="trigChart"></canvas></div>
</div>

<!-- Chart 5: Caregiver Emotional State -->
<div class="card">
  <h2>Caregiver Emotional State <button class="methods-btn" onclick="toggleMethods('m-care')">Methods</button></h2>
  <div class="methods-box" id="m-care">Classifies caregiver emotional state into 5 categories: trust in providers, frustration with system, feeling dismissed, anxiety/fear, and empowerment/advocacy.</div>
  <div id="careBars" class="bar-rows"></div>
</div>

<!-- Chart 5b: Caregiver Daily -->
<div class="card">
  <h2>Caregiver Sentiment Daily</h2>
  <div class="chart-wrap"><canvas id="careChart"></canvas></div>
</div>

<!-- Chart 7: Corticosteroid Side Effects -->
<div class="card">
  <h2>Corticosteroid Side Effects <button class="methods-btn" onclick="toggleMethods('m-cortico')">Methods</button></h2>
  <div class="methods-box" id="m-cortico">Tracks 8 corticosteroid-related side effects (roid rage, mood swings, sleep disturbances, appetite/weight, glucose issues, growth concerns, adrenal suppression, bone density) in posts/comments mentioning corticosteroids. Strict proximity matching: effect must appear within ~40 characters of steroid mention.</div>
  <div id="corticoBars" class="bar-rows"></div>
</div>

<!-- Chart 7b: Corticosteroid Daily -->
<div class="card">
  <h2>Corticosteroid Effects Trend</h2>
  <div class="chart-wrap"><canvas id="corticoChart"></canvas></div>
</div>

<!-- Chart 8: Functional Impact -->
<div class="card">
  <h2>Functional Impact <button class="methods-btn" onclick="toggleMethods('m-func')">Methods</button></h2>
  <div class="methods-box" id="m-func">Tracks functional burden of pediatric asthma: missed school, missed parental work, activity limitations, and sports impact. Captures discourse around school absences, work absences, and activity restrictions due to asthma.</div>
  <div id="funcBars" class="bar-rows"></div>
</div>

<!-- Chart 8b: Functional Impact Daily -->
<div class="card">
  <h2>Functional Impact Trend</h2>
  <div class="chart-wrap"><canvas id="funcChart"></canvas></div>
</div>

<!-- Chart 9: Inhaler Confusion -->
<div class="card">
  <h2>Inhaler Confusion &amp; Technique <button class="methods-btn" onclick="toggleMethods('m-inhconf')">Methods</button></h2>
  <div class="methods-box" id="m-inhconf">Detects 4 categories of inhaler confusion: type confusion (controller vs rescue), technique issues (improper use), timing confusion (when/how often to use), and device confusion (MDI vs nebulizer).</div>
  <div id="inhConfBars" class="bar-rows"></div>
</div>

<!-- Chart 9b: Inhaler Confusion Daily -->
<div class="card">
  <h2>Inhaler Confusion Trend</h2>
  <div class="chart-wrap"><canvas id="inhConfChart"></canvas></div>
</div>

<!-- Chart 10: ED Post-Visit Subcategories -->
<div class="card">
  <h2>Post-Visit Experience Breakdown <button class="methods-btn" onclick="toggleMethods('m-edsubcat')">Methods</button></h2>
  <div class="methods-box" id="m-edsubcat">Breaks down post-visit ED discourse into 5 subcategories: new medication prescribed, diagnosis given, discharge instructions, satisfaction with care, and ongoing worry despite visit.</div>
  <div id="edSubBars" class="bar-rows"></div>
</div>

<!-- Chart 11: Post-ED Outcome -->
<div class="card">
  <h2>Post-ED Outcome Sentiment <button class="methods-btn" onclick="toggleMethods('m-edoutcome')">Methods</button></h2>
  <div class="methods-box" id="m-edoutcome">Captures parent-reported outcomes after ED visits: improvement (child better after), no improvement (still struggling), and temporary relief (symptoms returned). Only analyzed in posts with ED discourse.</div>
  <div id="edOutBars" class="bar-rows"></div>
</div>

<!-- Chart 12: Caregiver Emotional State (ED-linked) -->
<div class="card">
  <h2>Caregiver Emotions — ED Context <button class="methods-btn" onclick="toggleMethods('m-careed')">Methods</button></h2>
  <div class="methods-box" id="m-careed">Shows caregiver emotional state specifically in posts/comments that also contain ED/hospital discourse. Compare with the general caregiver chart above to see how ED visits affect emotional state.</div>
  <div id="careEdBars" class="bar-rows"></div>
</div>

<!-- Post Explorer -->
<div class="card full" id="explorerCard">
  <h2>Post Explorer</h2>
  <div class="explorer-controls">
    <select id="explorerType"><option value="">Select medication...</option></select>
    <button onclick="loadPosts()">Load Posts</button>
  </div>
  <div id="explorerTable"><div class="empty">Select a medication and click Load Posts</div></div>
</div>

</div><!-- grid -->
</div><!-- tabDashboard -->

<!-- ==================== VALIDATE TAB ==================== -->
<div class="tab-content" id="tabValidate">
  <div style="margin-bottom:12px">
    <label style="color:var(--muted);font-size:13px">Validator name:
      <input id="validatorName" style="background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:4px 8px;font-size:13px;width:120px" onchange="localStorage.setItem('asthma_validator',this.value)">
    </label>
  </div>
  <div class="facet-bar">
    <button class="facet-btn active" onclick="switchFacet('beliefs',this)">Beliefs</button>
    <button class="facet-btn" onclick="switchFacet('effects',this)">Side Effects</button>
    <button class="facet-btn" onclick="switchFacet('sentiment',this)">Sentiment</button>
  </div>
  <div id="facetBeliefs"><div id="valStats"></div><div id="valBatch"></div><button class="scrape-btn" style="margin-top:8px" onclick="loadValBatch()">Load New Batch</button><button class="scrape-btn" style="margin-top:8px;margin-left:8px;background:var(--green)" onclick="submitValVotes()">Submit Votes</button></div>
  <div id="facetEffects" style="display:none"><div id="seStats"></div><div id="seBatch"></div><button class="scrape-btn" style="margin-top:8px" onclick="loadSEBatch()">Load New Batch</button><button class="scrape-btn" style="margin-top:8px;margin-left:8px;background:var(--green)" onclick="submitSEVotes()">Submit Votes</button></div>
  <div id="facetSentiment" style="display:none"><div id="sentStats"></div><div id="sentBatch"></div><button class="scrape-btn" style="margin-top:8px" onclick="loadSentBatch()">Load New Batch</button><button class="scrape-btn" style="margin-top:8px;margin-left:8px;background:var(--green)" onclick="submitSentVotes()">Submit Votes</button></div>
</div>

<!-- ==================== FEEDBACK TAB ==================== -->
<div class="tab-content" id="tabFeedback">
  <div class="feedback-wrap">
    <div class="feedback-compose">
      <h2 style="margin-bottom:12px;font-size:16px">Share Your Feedback</h2>
      <textarea id="feedbackText" placeholder="Feature request, data question, or suggestion..." oninput="document.getElementById('charCount').textContent=this.value.length+' / 500'"></textarea>
      <div class="char-counter" id="charCount">0 / 500</div>
      <button class="feedback-submit" onclick="submitFeedback()">Submit</button>
      <div class="feedback-error" id="feedbackError"></div>
    </div>
    <div id="feedbackList"></div>
  </div>
</div>

</main>

<script>
// ==================== STATE ====================
const EFFECTS_LIST = ['Growth concerns','Oral thrush','Hoarseness','Jitteriness','Rapid heartbeat','Mood changes','Hyperactivity','Sleep issues','Nightmares','Behavioral changes','Nausea','Headaches','Weight gain','Decreased appetite','Fatigue','Anxiety','Depression','Aggression','Adrenal suppression','Bone density'];
let charts = {};

function qp(){
  const f = document.getElementById('dateFrom').value;
  const t = document.getElementById('dateTo').value;
  const s = document.getElementById('subFilter').value;
  let q = '';
  if(f) q += 'from='+f+'&';
  if(t) q += 'to='+t+'&';
  if(s) q += 'sub='+s+'&';
  return q;
}

function saveState(){
  const f = document.getElementById('dateFrom').value;
  const t = document.getElementById('dateTo').value;
  const s = document.getElementById('subFilter').value;
  window.location.hash = `from=${f}&to=${t}&sub=${s}`;
}

function loadState(){
  const h = window.location.hash.slice(1);
  if(!h) return false;
  const p = new URLSearchParams(h);
  if(p.get('from')) document.getElementById('dateFrom').value = p.get('from');
  if(p.get('to')) document.getElementById('dateTo').value = p.get('to');
  if(p.get('sub')) document.getElementById('subFilter').value = p.get('sub');
  return true;
}

function setPreset(days){
  if(days === 0){
    document.getElementById('dateFrom').value = '';
    document.getElementById('dateTo').value = '';
  } else {
    const to = new Date();
    const from = new Date(to);
    from.setDate(from.getDate() - days);
    document.getElementById('dateFrom').value = from.toISOString().split('T')[0];
    document.getElementById('dateTo').value = to.toISOString().split('T')[0];
  }
  document.getElementById('customDateWrap').style.display = 'none';
  saveState();
  loadAll();
}

function switchTab(tab, btn){
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(c=>c.classList.remove('active'));
  btn.classList.add('active');
  if(tab==='dashboard'){document.getElementById('tabDashboard').classList.add('active')}
  else if(tab==='validate'){document.getElementById('tabValidate').classList.add('active');loadValStats();loadSEStats();loadSentStats()}
  else if(tab==='feedback'){document.getElementById('tabFeedback').classList.add('active');loadFeedback()}
}

function switchFacet(facet, btn){
  document.querySelectorAll('.facet-btn').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('facetBeliefs').style.display = facet==='beliefs'?'block':'none';
  document.getElementById('facetEffects').style.display = facet==='effects'?'block':'none';
  document.getElementById('facetSentiment').style.display = facet==='sentiment'?'block':'none';
}

function toggleMethods(id){
  const el = document.getElementById(id);
  el.classList.toggle('show');
}

function esc(s){return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;')}

const MED_CLASS_COLORS = {
  'ICS':'ics','Oral corticosteroids':'oral','Bronchodilators':'broncho',
  'Biologics':'bio','Leukotriene modifiers':'leuko','Combination inhalers':'combo',
  'Devices':'device'
};
const MED_CLASSES = {
  'Flovent':'ICS','QVAR':'ICS','Pulmicort':'ICS','Alvesco':'ICS','Asmanex':'ICS','Arnuity':'ICS','budesonide':'ICS','fluticasone':'ICS','beclomethasone':'ICS',
  'prednisone':'Oral corticosteroids','prednisolone':'Oral corticosteroids','dexamethasone':'Oral corticosteroids','oral steroids':'Oral corticosteroids',
  'albuterol':'Bronchodilators','ProAir':'Bronchodilators','Ventolin':'Bronchodilators','Proventil':'Bronchodilators','levalbuterol':'Bronchodilators','Xopenex':'Bronchodilators','rescue inhaler':'Bronchodilators',
  'Dupixent':'Biologics','Xolair':'Biologics','Nucala':'Biologics','Fasenra':'Biologics','Tezspire':'Biologics',
  'Singulair':'Leukotriene modifiers','Accolate':'Leukotriene modifiers',
  'Advair':'Combination inhalers','Symbicort':'Combination inhalers','Dulera':'Combination inhalers','Breo':'Combination inhalers','AirDuo':'Combination inhalers','SMART therapy':'Combination inhalers',
  'nebulizer':'Devices','spacer':'Devices','peak flow meter':'Devices','pulse oximeter':'Devices'
};
const TRIG_CATS = {'Mold':'env','Air pollution':'env','Smoke':'env','Pets':'env','Pollen/seasonal':'env','Dust mites':'env','Weather changes':'env','Cold air':'env','RSV':'viral','Common cold':'viral','Flu':'viral','COVID':'viral','Respiratory infection':'viral','Croup':'viral','Vaccines':'neb','Diet/toxins':'neb','Chemicals':'neb','Mold toxicity':'neb','EMF':'neb'};
const CARE_COLORS = {'trust':'var(--green)','frustration':'var(--red)','dismissed':'var(--yellow)','anxiety':'var(--fear)','empowerment':'var(--accent)'};

// ==================== RENDER HELPERS ====================
function renderBars(container, items, maxVal, colorFn){
  if(!items.length){container.innerHTML='<div class="empty">No data</div>';return}
  const mx = maxVal || Math.max(...items.map(i=>i[1]));
  container.innerHTML = items.map(([label, count, cls]) => {
    const pct = mx ? (count/mx*100) : 0;
    const c = cls || colorFn?.(label) || 'default';
    return `<div class="bar-row"><div class="bar-label" title="${esc(label)}">${esc(label)}</div><div class="bar-track"><div class="bar-fill ${c}" style="width:${pct}%"></div></div><div class="bar-count">${count}</div></div>`;
  }).join('');
}

function renderStanceBars(container, data){
  const beliefs = Object.keys(data);
  if(!beliefs.length){container.innerHTML='<div class="empty">No data</div>';return}
  const maxVal = Math.max(...beliefs.map(b => Object.values(data[b]).reduce((a,c)=>a+c,0)));
  container.innerHTML = beliefs.map(b => {
    const total = Object.values(data[b]).reduce((a,c)=>a+c,0);
    const segments = ['concordant','discordant','uncertain','unclear'].map(s => {
      const cnt = data[b][s] || 0;
      const pct = total ? (cnt/maxVal*100) : 0;
      return pct > 0 ? `<div class="bar-fill ${s}" style="width:${pct}%;display:inline-block" title="${s}: ${cnt}"></div>` : '';
    }).join('');
    return `<div class="bar-row"><div class="bar-label" title="${esc(b)}">${esc(b)}</div><div class="bar-track" style="display:flex">${segments}</div><div class="bar-count">${total}</div></div>`;
  }).join('');
}

function destroyChart(key){if(charts[key]){charts[key].destroy();delete charts[key]}}

const seasonalPlugin = {
  id:'seasonShading',
  beforeDraw(chart, args, opts){
    if(!opts || !opts.enabled) return;
    const {ctx, chartArea:{left,right,top,bottom}, scales:{x}} = chart;
    if(!x) return;
    const labels = chart.data.labels;
    if(!labels || !labels.length) return;
    ctx.save();
    // Aug-Oct (back-to-school), Mar-Apr (spring pollen)
    const seasons = [
      {start:8,end:10,color:'rgba(240,160,64,0.06)'},
      {start:3,end:4,color:'rgba(74,222,128,0.06)'}
    ];
    for(const s of seasons){
      let inSeason=false, sx=left;
      for(let i=0;i<labels.length;i++){
        const m=parseInt(labels[i].split('-')[1],10);
        const px=x.getPixelForValue(i);
        if(m>=s.start&&m<=s.end){
          if(!inSeason){sx=px;inSeason=true}
        } else if(inSeason){
          ctx.fillStyle=s.color;
          ctx.fillRect(sx,top,px-sx,bottom-top);
          inSeason=false;
        }
      }
      if(inSeason){ctx.fillStyle=s.color;ctx.fillRect(sx,top,right-sx,bottom-top)}
    }
    ctx.restore();
  }
};
Chart.register(seasonalPlugin);

function renderLineChart(canvasId, dailyData, key, showSeasons){
  destroyChart(key);
  const dates = Object.keys(dailyData).sort();
  if(!dates.length) return;
  const allKeys = {};
  dates.forEach(d => Object.keys(dailyData[d]).forEach(k => allKeys[k] = (allKeys[k]||0) + dailyData[d][k]));
  const top5 = Object.entries(allKeys).sort((a,b)=>b[1]-a[1]).slice(0,5).map(e=>e[0]);
  const colors = ['#7c6ef0','#e06090','#40d0d0','#f0c040','#4ade80'];
  const datasets = top5.map((t,i) => ({
    label: t,
    data: dates.map(d => dailyData[d][t] || 0),
    borderColor: colors[i%5],
    fill: false,
    tension: 0.2,
    pointRadius: 1,
  }));
  const ctx = document.getElementById(canvasId).getContext('2d');
  charts[key] = new Chart(ctx, {
    type: 'line',
    data: {labels: dates, datasets},
    options: {
      responsive: true, maintainAspectRatio: false,
      scales: {x:{ticks:{maxTicksLimit:10,color:'#8b90a0'}},y:{beginAtZero:true,ticks:{color:'#8b90a0'}}},
      plugins:{legend:{labels:{color:'#e1e4ed',boxWidth:12}},seasonShading:{enabled:!!showSeasons}}
    }
  });
}

// ==================== LOAD ALL ====================
async function loadAll(){
  const q = qp();
  try{
    const [meds, medSent, medDaily, edCounts, edDaily, beliefs, beliefStance, trigs, trigDaily, care, careDaily, singEffects, singDaily, singDisc, corticoEffects, corticoDaily, funcImpact, funcDaily, inhConf, inhConfDaily, edSubcats, careEd, edOutcome, stats] = await Promise.all([
      fetch('/api/medications?'+q).then(r=>r.json()),
      fetch('/api/medication-sentiment?'+q).then(r=>r.json()),
      fetch('/api/medication-daily?'+q).then(r=>r.json()),
      fetch('/api/ed-discourse?'+q).then(r=>r.json()),
      fetch('/api/ed-discourse-daily?'+q).then(r=>r.json()),
      fetch('/api/beliefs?'+q).then(r=>r.json()),
      fetch('/api/beliefs-stance?'+q).then(r=>r.json()),
      fetch('/api/triggers?'+q).then(r=>r.json()),
      fetch('/api/triggers-daily?'+q).then(r=>r.json()),
      fetch('/api/caregiver?'+q).then(r=>r.json()),
      fetch('/api/caregiver-daily?'+q).then(r=>r.json()),
      fetch('/api/singulair?'+q).then(r=>r.json()),
      fetch('/api/singulair-daily?'+q).then(r=>r.json()),
      fetch('/api/singulair-discourse?'+q).then(r=>r.json()),
      fetch('/api/corticosteroid-effects?'+q).then(r=>r.json()),
      fetch('/api/corticosteroid-daily?'+q).then(r=>r.json()),
      fetch('/api/functional-impact?'+q).then(r=>r.json()),
      fetch('/api/functional-impact-daily?'+q).then(r=>r.json()),
      fetch('/api/inhaler-confusion?'+q).then(r=>r.json()),
      fetch('/api/inhaler-confusion-daily?'+q).then(r=>r.json()),
      fetch('/api/ed-subcategories?'+q).then(r=>r.json()),
      fetch('/api/caregiver-ed?'+q).then(r=>r.json()),
      fetch('/api/post-ed-outcome?'+q).then(r=>r.json()),
      fetch('/api/status').then(r=>r.json()),
    ]);

    // Stats row
    const st = meds._stats || {};
    document.getElementById('statsRow').innerHTML = [
      ['Posts', st.total_posts||0, ''],
      ['Comments', st.total_comments||0, ''],
      ['Medications', st.total_mentions||0, ''],
      ['Avg Sentiment', st.avg_sentiment!=null?st.avg_sentiment.toFixed(3):'—', st.avg_sentiment>0?'green':st.avg_sentiment<0?'red':''],
      ['Avg Fear', st.avg_fear!=null?st.avg_fear.toFixed(3):'—', 'fear'],
      ['Peds Definite', st.peds_definite||0, ''],
      ['Peds Likely', st.peds_likely||0, ''],
      ['Errors 24h', st.error_count_24h||0, (st.error_count_24h||0)>0?'red':'green'],
    ].map(([label, val, cls]) => `<div class="stat"><div class="stat-val ${cls}">${val}</div><div class="stat-label">${label}</div></div>`).join('');

    // Scheduler status
    const dot = document.getElementById('schedDot');
    dot.className = 'sched-dot ' + (stats.running ? 'running' : 'idle');
    document.getElementById('schedText').textContent = stats.running ? 'Scraping...' : `Last: ${stats.last_run||'never'} | Next: ${stats.next_run||'—'}`;

    // Chart 1: Medication mentions
    const medItems = (meds.medications||[]).map(([name,cnt]) => [name, cnt, MED_CLASS_COLORS[MED_CLASSES[name]]||'default']);
    renderBars(document.getElementById('medBars'), medItems, 0, null);

    // Chart 1b: Sentiment+Fear chart
    destroyChart('sent');
    const sentData = meds.medication_sentiment || medSent || [];
    if(sentData.length){
      const labels = sentData.map(r=>r[0]);
      const sentVals = sentData.map(r=>r[1]);
      const fearVals = sentData.map(r=>r[2]);
      const ctx = document.getElementById('sentChart').getContext('2d');
      charts['sent'] = new Chart(ctx, {
        type:'bar', data:{labels, datasets:[
          {label:'Sentiment',data:sentVals,backgroundColor:sentVals.map(v=>v>0.1?'#4ade80':v<-0.1?'#f06060':'#8b90a0')},
          {label:'Fear',data:fearVals,backgroundColor:'#f0a040'}
        ]},
        options:{responsive:true,maintainAspectRatio:false,scales:{y:{beginAtZero:true,ticks:{color:'#8b90a0'}},x:{ticks:{color:'#8b90a0',maxRotation:45}}},plugins:{legend:{labels:{color:'#e1e4ed'}}}}
      });
    }

    // Chart 1c: Daily trend (with seasonal shading)
    renderLineChart('trendChart', meds.medication_daily || medDaily || {}, 'trend', true);

    // Chart 6: Singulair effects
    const singItems = (singEffects||[]).map(([name,cnt]) => [name, cnt, 'leuko']);
    renderBars(document.getElementById('singBars'), singItems, 0, null);

    // Chart 6b: Singulair discourse
    const discItems = (singDisc||[]).map(([name,cnt]) => [name, cnt, 'bio']);
    renderBars(document.getElementById('singDisc'), discItems, 0, null);

    // Chart 2: ED discourse
    const edLabels = {'decision_uncertainty':'Decision Uncertainty','post_visit':'Post-Visit Experience','return_visits':'Return Visits','barriers':'Barriers to Access'};
    const edItems = (edCounts||[]).map(([name,cnt]) => [edLabels[name]||name, cnt, 'oral']);
    renderBars(document.getElementById('edBars'), edItems, 0, null);

    // Chart 2b: ED daily
    renderLineChart('edChart', edDaily || {}, 'edTrend');

    // Chart 3: Beliefs with stance
    renderStanceBars(document.getElementById('beliefBars'), beliefStance || {});

    // Chart 4: Triggers
    const trigItems = (trigs||[]).map(([name,cat,cnt]) => [name, cnt, TRIG_CATS[name]||'default']);
    renderBars(document.getElementById('trigBars'), trigItems, 0, null);

    // Chart 4b: Trigger daily (with seasonal shading)
    renderLineChart('trigChart', trigDaily || {}, 'trigTrend', true);

    // Chart 5: Caregiver
    const careItems = (care||[]).map(([name,cnt]) => [name, cnt, 'default']);
    renderBars(document.getElementById('careBars'), careItems, 0, n => {
      const m = {'trust':'concordant','frustration':'discordant','dismissed':'uncertain','anxiety':'oral','empowerment':'ics'};
      return m[n]||'default';
    });

    // Chart 5b: Caregiver daily
    renderLineChart('careChart', careDaily || {}, 'careTrend');

    // Chart 7: Corticosteroid effects
    const corticoLabels = {'roid_rage':'Roid Rage','mood_swings':'Mood Swings','sleep_disturbances':'Sleep Disturbances','appetite_weight':'Appetite/Weight','glucose_issues':'Glucose Issues','growth_concerns':'Growth Concerns','adrenal_suppression':'Adrenal Suppression','bone_density':'Bone Density'};
    const corticoItems = (corticoEffects||[]).map(([name,cnt]) => [corticoLabels[name]||name, cnt, 'oral']);
    renderBars(document.getElementById('corticoBars'), corticoItems, 0, null);

    // Chart 7b: Corticosteroid daily
    renderLineChart('corticoChart', corticoDaily || {}, 'corticoTrend');

    // Chart 8: Functional impact
    const funcLabels = {'missed_school':'Missed School','missed_work':'Missed Work (Parent)','activity_limitation':'Activity Limitation','sports_impact':'Sports Impact'};
    const funcItems = (funcImpact||[]).map(([name,cnt]) => [funcLabels[name]||name, cnt, 'default']);
    renderBars(document.getElementById('funcBars'), funcItems, 0, n => {
      const m = {'Missed School':'discordant','Missed Work (Parent)':'oral','Activity Limitation':'uncertain','Sports Impact':'leuko'};
      return m[n]||'default';
    });

    // Chart 8b: Functional impact daily
    renderLineChart('funcChart', funcDaily || {}, 'funcTrend');

    // Chart 9: Inhaler confusion
    const inhLabels = {'type_confusion':'Type Confusion','technique_issues':'Technique Issues','timing_confusion':'Timing Confusion','device_confusion':'Device Confusion'};
    const inhItems = (inhConf||[]).map(([name,cnt]) => [inhLabels[name]||name, cnt, 'broncho']);
    renderBars(document.getElementById('inhConfBars'), inhItems, 0, null);

    // Chart 9b: Inhaler confusion daily
    renderLineChart('inhConfChart', inhConfDaily || {}, 'inhConfTrend');

    // Chart 10: ED subcategories
    const edSubLabels = {'prescribed_new_med':'New Medication Prescribed','diagnosis_given':'Diagnosis Given','discharge_instructions':'Discharge Instructions','satisfaction':'Care Satisfaction','still_worried':'Still Worried'};
    const edSubItems = (edSubcats||[]).map(([name,cnt]) => [edSubLabels[name]||name, cnt, 'combo']);
    renderBars(document.getElementById('edSubBars'), edSubItems, 0, null);

    // Chart 11: Post-ED outcome
    const edOutLabels = {'improvement':'Improvement','no_improvement':'No Improvement','temporary_relief':'Temporary Relief'};
    const edOutItems = (edOutcome||[]).map(([name,cnt]) => [edOutLabels[name]||name, cnt, 'default']);
    renderBars(document.getElementById('edOutBars'), edOutItems, 0, n => {
      const m = {'Improvement':'concordant','No Improvement':'discordant','Temporary Relief':'uncertain'};
      return m[n]||'default';
    });

    // Chart 12: Caregiver ED-linked
    const careEdItems = (careEd||[]).map(([name,cnt]) => [name, cnt, 'default']);
    renderBars(document.getElementById('careEdBars'), careEdItems, 0, n => {
      const m = {'trust':'concordant','frustration':'discordant','dismissed':'uncertain','anxiety':'oral','empowerment':'ics'};
      return m[n]||'default';
    });

    // Populate explorer dropdown
    const sel = document.getElementById('explorerType');
    const curVal = sel.value;
    sel.innerHTML = '<option value="">Select medication...</option>' + (meds.medications||[]).map(([n]) => `<option value="${esc(n)}">${esc(n)}</option>`).join('');
    if(curVal) sel.value = curVal;

  }catch(e){console.error('loadAll failed:',e)}
}

// ==================== POST EXPLORER ====================
async function loadPosts(){
  const type = document.getElementById('explorerType').value;
  if(!type){return}
  const q = qp() + 'medication=' + encodeURIComponent(type) + '&limit=20';
  const posts = await fetch('/api/posts?'+q).then(r=>r.json());
  if(!posts.length){document.getElementById('explorerTable').innerHTML='<div class="empty">No posts found</div>';return}
  let html = '<table class="explorer"><thead><tr><th>Post</th><th>Sub</th><th>Sent</th><th>Fear</th><th>Peds</th><th>Eng</th></tr></thead><tbody>';
  for(const p of posts){
    const sc = p.sentiment!=null ? (p.sentiment>0.1?'pos':p.sentiment<-0.1?'neg':'neu') : 'neu';
    const fc = p.fear_score!=null && p.fear_score>0.3 ? 'fear' : 'neu';
    const pc = p.pediatric_confidence==='definite'?'definite':p.pediatric_confidence==='likely'?'likely':'neu';
    html += `<tr onclick="toggleExp(this,'${p.id}')" style="cursor:pointer"><td>${esc(p.title).slice(0,80)}</td><td><span class="pill">${p.subreddit||''}</span></td><td><span class="pill ${sc}">${p.sentiment!=null?p.sentiment.toFixed(2):'—'}</span></td><td><span class="pill ${fc}">${p.fear_score!=null?p.fear_score.toFixed(2):'—'}</span></td><td><span class="pill ${pc}">${p.pediatric_confidence||'—'}</span></td><td>${(p.engagement_score||0).toFixed(1)}</td></tr>`;
    html += `<tr class="expanded-row" style="display:none" id="exp-${p.id}"><td colspan="6"><div class="post-text">${esc(p.selftext||'').slice(0,500)}</div><div id="comments-${p.id}"></div></td></tr>`;
  }
  html += '</tbody></table>';
  document.getElementById('explorerTable').innerHTML = html;
}

async function toggleExp(row, postId){
  const exp = document.getElementById('exp-'+postId);
  if(exp.style.display==='none'){
    exp.style.display='table-row';
    const el = document.getElementById('comments-'+postId);
    if(!el.innerHTML){
      el.innerHTML='Loading comments...';
      const comments = await fetch('/api/comments?post_id='+postId).then(r=>r.json());
      el.innerHTML = comments.length ? comments.map(c=>`<div class="comment-item"><div class="comment-meta">${esc(c.author)} · ${c.score} pts · sent:${c.sentiment!=null?c.sentiment.toFixed(2):'—'} · fear:${c.fear_score!=null?c.fear_score.toFixed(2):'—'}</div><div>${esc(c.body).slice(0,200)}</div></div>`).join('') : '<div class="empty">No comments</div>';
    }
  } else {exp.style.display='none'}
}

// ==================== SCRAPE ====================
async function triggerScrape(){
  const btn = document.getElementById('scrapeBtn');
  btn.disabled=true; btn.textContent='Scraping...';
  try{
    const r = await fetch('/api/scrape',{method:'POST'}).then(r=>r.json());
    if(r.ok){loadAll()}
    btn.textContent = r.ok ? 'Done!' : r.error||'Error';
  }catch(e){btn.textContent='Error'}
  setTimeout(()=>{btn.disabled=false;btn.textContent='Scrape Now'},3000);
}

// ==================== VALIDATION ====================
let valBatch=[], seBatch=[], sentBatchData=[];

async function loadValStats(){
  const r = await fetch('/api/validation/stats').then(r=>r.json());
  if(!r.total_votes){document.getElementById('valStats').innerHTML='<div class="empty">No validation data yet</div>';return}
  document.getElementById('valStats').innerHTML = `<div class="val-stats">${[
    ['Votes',r.total_votes],['Agreement',r.agreement_rate+'%'],['Precision',r.precision],['Recall',r.recall],['F1',r.f1],["Scott's Pi",r.scotts_pi]
  ].map(([l,v])=>`<div class="val-stat"><div class="val">${v}</div><div class="lbl">${l}</div></div>`).join('')}</div>`;
}

async function loadValBatch(){
  const v = document.getElementById('validatorName').value || 'anon';
  valBatch = await fetch('/api/validation/batch?validator='+encodeURIComponent(v)).then(r=>r.json());
  document.getElementById('valBatch').innerHTML = valBatch.map((p,i) => `<div class="val-card"><div class="val-title">${esc(p.title)} <span class="pill">${p.subreddit}</span></div><div class="val-text">${esc(p.selftext)}</div><div class="val-actions"><button class="vote-btn yes" onclick="this.classList.toggle('selected');document.querySelector('#vc-${i} .no')?.classList.remove('selected')" id="vc-${i}-yes">Yes — belief detected</button><button class="vote-btn no" onclick="this.classList.toggle('selected');document.querySelector('#vc-${i}-yes')?.classList.remove('selected')" id="vc-${i}">No</button></div></div>`).join('');
}

async function submitValVotes(){
  const v = document.getElementById('validatorName').value || 'anon';
  const votes = valBatch.map((p,i) => {
    const yes = document.getElementById('vc-'+i+'-yes')?.classList.contains('selected');
    return {post_id:p.id, human_flagged:yes?1:0};
  });
  await fetch('/api/validation/votes',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({validator:v,votes})});
  loadValStats(); loadValBatch();
}

async function loadSEStats(){
  const r = await fetch('/api/validation/side-effects/stats').then(r=>r.json());
  if(!r.total_votes){document.getElementById('seStats').innerHTML='<div class="empty">No side-effect validation data yet</div>';return}
  document.getElementById('seStats').innerHTML = `<div class="val-stats">${[['Votes',r.total_votes],['Avg Jaccard',r.avg_jaccard]].map(([l,v])=>`<div class="val-stat"><div class="val">${v}</div><div class="lbl">${l}</div></div>`).join('')}</div>`;
}

async function loadSEBatch(){
  const v = document.getElementById('validatorName').value || 'anon';
  seBatch = await fetch('/api/validation/side-effects/batch?validator='+encodeURIComponent(v)).then(r=>r.json());
  document.getElementById('seBatch').innerHTML = seBatch.map((c,i) => `<div class="val-card"><div class="val-title">${esc(c.post_title)} <span class="pill">${c.subreddit}</span></div><div class="val-text">${esc(c.body)}</div><div class="checkbox-grid" id="se-checks-${i}">${EFFECTS_LIST.map(e=>`<label><input type="checkbox" value="${e}"> ${e}</label>`).join('')}</div><input placeholder="Something else..." style="background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:4px 8px;font-size:12px;width:200px;margin-top:4px" id="se-other-${i}"></div>`).join('');
}

async function submitSEVotes(){
  const v = document.getElementById('validatorName').value || 'anon';
  const votes = seBatch.map((c,i) => {
    const checks = [...document.querySelectorAll('#se-checks-'+i+' input:checked')].map(cb=>cb.value);
    const other = document.getElementById('se-other-'+i)?.value||'';
    return {comment_id:c.comment_id, human_effects:checks, other_effect:other};
  });
  await fetch('/api/validation/side-effects/votes',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({validator:v,votes})});
  loadSEStats(); loadSEBatch();
}

async function loadSentStats(){
  const r = await fetch('/api/validation/sentiment/stats').then(r=>r.json());
  if(!r.total_votes){document.getElementById('sentStats').innerHTML='<div class="empty">No sentiment validation data yet</div>';return}
  document.getElementById('sentStats').innerHTML = `<div class="val-stats">${[['Votes',r.total_votes],['Pearson r',r.pearson_r],['MAE',r.mae]].map(([l,v])=>`<div class="val-stat"><div class="val">${v}</div><div class="lbl">${l}</div></div>`).join('')}</div>`;
}

async function loadSentBatch(){
  const v = document.getElementById('validatorName').value || 'anon';
  sentBatchData = await fetch('/api/validation/sentiment/batch?validator='+encodeURIComponent(v)).then(r=>r.json());
  document.getElementById('sentBatch').innerHTML = sentBatchData.map((c,i) => `<div class="val-card"><div class="val-title">${esc(c.post_title)} <span class="pill">${c.subreddit}</span></div><div class="val-text">${esc(c.body)}</div><div class="val-actions" id="sent-btns-${i}">${[[-1,'V.Neg'],[-0.5,'Neg'],[0,'Neutral'],[0.5,'Pos'],[1,'V.Pos']].map(([s,l])=>`<button class="vote-btn likert-btn" data-score="${s}" onclick="selectLikert(${i},${s},this)">${l}</button>`).join('')}</div></div>`).join('');
}

function selectLikert(idx, score, btn){
  document.querySelectorAll('#sent-btns-'+idx+' .likert-btn').forEach(b=>b.classList.remove('selected'));
  btn.classList.add('selected');
  sentBatchData[idx]._human_score = score;
}

async function submitSentVotes(){
  const v = document.getElementById('validatorName').value || 'anon';
  const votes = sentBatchData.filter(c=>c._human_score!==undefined).map(c=>({comment_id:c.comment_id, human_score:c._human_score}));
  if(!votes.length) return;
  await fetch('/api/validation/sentiment/votes',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({validator:v,votes})});
  loadSentStats(); loadSentBatch();
}

// ==================== FEEDBACK ====================
function getVoterId(){
  let id=localStorage.getItem('asthma_voter_id');
  if(!id){id='v_'+Math.random().toString(36).slice(2)+Date.now().toString(36);localStorage.setItem('asthma_voter_id',id)}
  return id;
}

async function loadFeedback(){
  const r = await fetch('/api/feedback?voter='+getVoterId()).then(r=>r.json());
  const items = r.suggestions || r || [];
  if(!items.length){document.getElementById('feedbackList').innerHTML='<div class="empty">No suggestions yet</div>';return}
  document.getElementById('feedbackList').innerHTML = items.map(f=>`<div class="fb-item"><div class="fb-vote"><button class="${f.voted_by_me?'voted':''}" onclick="voteFeedback(${f.id})">&#9650;</button><div class="count">${f.vote_count}</div></div><div class="fb-body">${esc(f.suggestion)}<div class="fb-time">${f.created_at}</div></div></div>`).join('');
}

async function submitFeedback(){
  const text = document.getElementById('feedbackText').value.trim();
  const err = document.getElementById('feedbackError');
  err.textContent='';
  if(!text){err.textContent='Cannot be empty';return}
  if(text.length>500){err.textContent='Max 500 characters';return}
  try{
    const r = await fetch('/api/feedback',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({voter_id:getVoterId(),suggestion:text})}).then(r=>r.json());
    if(r.error){err.textContent=r.error;return}
    document.getElementById('feedbackText').value='';
    document.getElementById('charCount').textContent='0 / 500';
    loadFeedback();
  }catch(e){err.textContent='Error submitting'}
}

async function voteFeedback(id){
  await fetch('/api/feedback/vote',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({feedback_id:id,voter_id:getVoterId()})});
  loadFeedback();
}

// ==================== INIT ====================
window.addEventListener('hashchange', ()=>{loadState();loadAll()});
document.addEventListener('DOMContentLoaded', ()=>{
  const v = localStorage.getItem('asthma_validator');
  if(v) document.getElementById('validatorName').value = v;
  if(!loadState()) setPreset(0);
  else loadAll();
});
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# HTTP Request Handler
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):

    def _parse_dates(self, params):
        df = dt = None
        f = params.get("from", [None])[0]
        t = params.get("to", [None])[0]
        if f:
            try:
                df = datetime.strptime(f, "%Y-%m-%d").timestamp()
            except ValueError:
                pass
        if t:
            try:
                dt = datetime.strptime(t, "%Y-%m-%d").timestamp() + 86400
            except ValueError:
                pass
        return df, dt

    def _json(self, data):
        body = json.dumps(data, default=str).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _html(self, html):
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # Suppress default HTTP logging

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)
        df, dt = self._parse_dates(params)
        sv = params.get("sub", [None])[0] or None

        if path == "/":
            self._html(HTML_PAGE)

        elif path == "/api/medications":
            conn = tracker.get_db()
            meds = tracker.query_medication_counts(conn, date_from=df, date_to=dt, subreddit=sv)
            med_sent = tracker.query_medication_sentiment(conn, date_from=df, date_to=dt, subreddit=sv)
            med_daily = tracker.query_daily_medication_counts(conn, date_from=df, date_to=dt, subreddit=sv)
            stats = tracker.query_db_stats(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json({"medications": meds, "medication_sentiment": med_sent, "medication_daily": med_daily, "_stats": stats})

        elif path == "/api/medication-sentiment":
            conn = tracker.get_db()
            data = tracker.query_medication_sentiment(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/medication-daily":
            conn = tracker.get_db()
            data = tracker.query_daily_medication_counts(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/ed-discourse":
            conn = tracker.get_db()
            data = tracker.query_ed_discourse_counts(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/ed-discourse-daily":
            conn = tracker.get_db()
            data = tracker.query_ed_discourse_daily(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/beliefs":
            conn = tracker.get_db()
            data = tracker.query_belief_counts(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/beliefs-stance":
            conn = tracker.get_db()
            data = tracker.query_belief_stance_counts(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/triggers":
            conn = tracker.get_db()
            data = tracker.query_trigger_counts(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/triggers-daily":
            conn = tracker.get_db()
            data = tracker.query_trigger_daily(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/caregiver":
            conn = tracker.get_db()
            data = tracker.query_caregiver_counts(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/caregiver-daily":
            conn = tracker.get_db()
            data = tracker.query_caregiver_daily(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/singulair":
            conn = tracker.get_db()
            data = tracker.query_singulair_effect_counts(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/singulair-daily":
            conn = tracker.get_db()
            data = tracker.query_singulair_daily(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/singulair-discourse":
            conn = tracker.get_db()
            data = tracker.query_singulair_discourse_counts(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/corticosteroid-effects":
            conn = tracker.get_db()
            data = tracker.query_corticosteroid_effect_counts(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/corticosteroid-daily":
            conn = tracker.get_db()
            data = tracker.query_corticosteroid_daily(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/functional-impact":
            conn = tracker.get_db()
            data = tracker.query_functional_impact_counts(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/functional-impact-daily":
            conn = tracker.get_db()
            data = tracker.query_functional_impact_daily(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/inhaler-confusion":
            conn = tracker.get_db()
            data = tracker.query_inhaler_confusion_counts(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/inhaler-confusion-daily":
            conn = tracker.get_db()
            data = tracker.query_inhaler_confusion_daily(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/ed-subcategories":
            conn = tracker.get_db()
            data = tracker.query_ed_subcategory_counts(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/caregiver-ed":
            conn = tracker.get_db()
            data = tracker.query_caregiver_ed_linked(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/post-ed-outcome":
            conn = tracker.get_db()
            data = tracker.query_post_ed_outcome_counts(conn, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/posts":
            med = params.get("medication", [None])[0]
            limit = int(params.get("limit", [20])[0])
            if not med:
                self._json([])
                return
            conn = tracker.get_db()
            data = tracker.query_top_posts(conn, med, limit=limit, date_from=df, date_to=dt, subreddit=sv)
            conn.close()
            self._json(data)

        elif path == "/api/comments":
            post_id = params.get("post_id", [None])[0]
            if not post_id:
                self._json([])
                return
            conn = tracker.get_db()
            data = tracker.query_comments_for_post(conn, post_id)
            conn.close()
            self._json(data)

        elif path == "/api/post-effects":
            post_id = params.get("id", [None])[0]
            if not post_id:
                self._json([])
                return
            conn = tracker.get_db()
            rows = conn.execute(
                "SELECT effect FROM side_effects WHERE source_type = 'post' AND source_id = ?",
                (post_id,)).fetchall()
            conn.close()
            self._json([r[0] for r in rows])

        elif path == "/api/status":
            self._json(scheduler.status())

        elif path == "/api/errors":
            limit = int(params.get("limit", [50])[0])
            conn = tracker.get_db()
            data = tracker.query_recent_errors(conn, limit=limit)
            conn.close()
            self._json(data)

        elif path == "/api/scrape-log":
            limit = int(params.get("limit", [20])[0])
            conn = tracker.get_db()
            data = tracker.query_scrape_log(conn, limit=limit)
            conn.close()
            self._json(data)

        elif path == "/api/validation/batch":
            v = params.get("validator", ["anon"])[0]
            conn = tracker.get_db()
            data = tracker.get_validation_batch(conn, v)
            for item in data:
                item.pop("system_claims", None)
            conn.close()
            self._json(data)

        elif path == "/api/validation/stats":
            conn = tracker.get_db()
            data = tracker.query_validation_stats(conn)
            conn.close()
            self._json(data)

        elif path == "/api/validation/side-effects/batch":
            v = params.get("validator", ["anon"])[0]
            conn = tracker.get_db()
            data = tracker.get_side_effect_batch(conn, v)
            for item in data:
                item.pop("system_effects", None)
            conn.close()
            self._json(data)

        elif path == "/api/validation/side-effects/stats":
            conn = tracker.get_db()
            data = tracker.query_side_effect_validation_stats(conn)
            conn.close()
            self._json(data)

        elif path == "/api/validation/sentiment/batch":
            v = params.get("validator", ["anon"])[0]
            conn = tracker.get_db()
            data = tracker.get_sentiment_batch(conn, v)
            for item in data:
                item.pop("system_score", None)
            conn.close()
            self._json(data)

        elif path == "/api/validation/sentiment/stats":
            conn = tracker.get_db()
            data = tracker.query_sentiment_validation_stats(conn)
            conn.close()
            self._json(data)

        elif path == "/api/feedback":
            vid = params.get("voter", [None])[0]
            conn = tracker.get_db()
            data = tracker.query_feedback(conn, voter_id=vid)
            conn.close()
            self._json({"suggestions": data})

        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode() if content_length else "{}"

        if path == "/api/scrape":
            self._json(scheduler.run_now())

        elif path == "/api/validation/votes":
            data = json.loads(body)
            conn = tracker.get_db()
            tracker.save_validation_votes(conn, data.get("validator", "anon"), data.get("votes", []))
            conn.close()
            self._json({"saved": len(data.get("votes", []))})

        elif path == "/api/validation/side-effects/votes":
            data = json.loads(body)
            conn = tracker.get_db()
            tracker.save_side_effect_votes(conn, data.get("validator", "anon"), data.get("votes", []))
            conn.close()
            self._json({"saved": len(data.get("votes", []))})

        elif path == "/api/validation/sentiment/votes":
            data = json.loads(body)
            conn = tracker.get_db()
            tracker.save_sentiment_votes(conn, data.get("validator", "anon"), data.get("votes", []))
            conn.close()
            self._json({"saved": len(data.get("votes", []))})

        elif path == "/api/feedback":
            data = json.loads(body)
            conn = tracker.get_db()
            try:
                fid = tracker.save_feedback(conn, data.get("voter_id", ""), data.get("suggestion", ""))
                conn.close()
                self._json({"id": fid})
            except ValueError as e:
                conn.close()
                self._json({"error": str(e)})

        elif path == "/api/feedback/vote":
            data = json.loads(body)
            conn = tracker.get_db()
            count = tracker.toggle_feedback_vote(conn, data.get("feedback_id", 0), data.get("voter_id", ""))
            conn.close()
            self._json({"vote_count": count})

        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    conn = tracker.get_db()
    tracker.backfill_sentiment_and_effects(conn)
    conn.close()

    scheduler.start()
    print(f"Pediatric Asthma Tracker dashboard: http://localhost:{PORT}")
    print(f"Auto-scrape every {SCRAPE_INTERVAL // 3600} hours. Press Ctrl+C to stop.")

    try:
        server = HTTPServer(("0.0.0.0", PORT), Handler)
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        scheduler.stop()


if __name__ == "__main__":
    main()
