# Peds-Asthma-Tracker Implementation Plan

## Context
New Reddit tracker for pediatric asthma discourse, modeled on the existing bc-tracker at `/Users/hanl/bc-tracker/`. Purpose: track caregiver attitudes toward asthma medications, ED utilization discourse, treatment misconceptions, environmental triggers, and healthcare system trust — all from a pediatric perspective. Data supports a research paper on improving patient education and reducing ED utilization. Deployed to VPS + GitHub Pages for multi-computer access.

## Architecture (mirrors bc-tracker exactly)
- **3 Python files**: `asthma_tracker.py` (core), `asthma_tracker_web.py` (web), `generate_site.py` (static)
- **Zero external deps** — Python stdlib only
- **SQLite WAL mode**, Reddit public JSON API, 1.5s rate limiting, residential proxy
- **Port 8053**, domain `asthma.hanlabnw.com`, VPS path `/opt/peds-asthma-tracker`
- **GitHub repo**: `leolhan1425/peds-asthma-tracker`

## File Structure
```
~/peds-asthma-tracker/
  asthma_tracker.py              # Core: scraping, regex, pediatric gate, sentiment, SQLite, CLI
  asthma_tracker_web.py          # Web server + Chart.js dashboard (auto-scrapes every 6h)
  generate_site.py               # Static site generator -> docs/index.html
  CLAUDE.md                      # Project reference
  .gitignore                     # Exclude data dirs, __pycache__, .env, *.log
  deploy.sh                      # rsync + systemctl restart
  vps-setup.sh                   # One-time VPS provisioning
  peds-asthma-tracker.service    # systemd unit file
  docs/index.html                # Auto-generated static dashboard
  asthma_tracker_data/tracker.db # SQLite database
```

## Two-Stage Content Gate
1. **Asthma gate**: Post must mention asthma/inhaler/nebulizer/medication terms → skip if no match
2. **Pediatric gate**: Classify `pediatric_confidence` as `definite` (parenting subs or strong keywords like "my child", "pediatrician", age 1-17), `likely` (weaker keywords like "kid", "LO"), or `none`. Only `definite`/`likely` posts get full analysis.

## Target Subreddits (10)
r/Asthma (200), r/Parenting (200), r/beyondthebump (200, auto-peds), r/Mommit (100, auto-peds), r/daddit (100, auto-peds), r/AskDocs (200), r/Allergies (100), r/medical (100), r/Pediatrics (100, auto-peds), r/NewParents (100, auto-peds)

## 6 Dashboard Modules

### Chart 1: Medication Sentiment Tracker
~35 medications in 8 classes: ICS (Flovent, QVAR, Pulmicort, Alvesco, Asmanex, Arnuity, budesonide, fluticasone, beclomethasone), Oral corticosteroids (prednisone, prednisolone, dexamethasone, oral steroids), Bronchodilators (albuterol, ProAir, Ventolin, Proventil, levalbuterol, Xopenex, rescue inhaler), Biologics (Dupixent, Xolair, Nucala, Fasenra, Tezspire), Leukotriene modifiers (Singulair, Accolate), Combination inhalers (Advair, Symbicort, Dulera, Breo, AirDuo), Devices (nebulizer, spacer, peak flow meter, pulse oximeter). Each gets sentiment + fear score, time-series with volume.

### Chart 2: ED/Hospital Decision-Making Discourse
4 categories: decision uncertainty ("should I go to the ER"), post-visit experience, return visits ("back again"), barriers to access (cost, insurance, wait times). Time-series with volume + sentiment.

### Chart 3: Treatment Beliefs and Misconceptions
10 guideline-discordant beliefs with stance detection (concordant/discordant/uncertain/unclear):
- Albuterol-only reliance, nebulizer superiority myth, alternative medicine cures, steroid growth stunting fear, outgrow asthma belief, inhalers are addictive, natural remedies are better, steroids are dangerous long-term, asthma is psychological, only need medicine during attacks. Each with NAEPP/GINA source citations.

### Chart 4: Trigger and Environmental Narrative Tracker
19 triggers in 3 categories: Environmental (mold, pollution, smoke, pets, pollen, dust mites, weather, cold air), Viral (RSV, cold, flu, COVID, respiratory infection, croup), Non-evidence-based (vaccines, diet/toxins, chemicals, mold toxicity, EMF). Seasonal pattern tracking.

### Chart 5: Caregiver Emotional State and System Trust
5 categories: trust in providers, frustration with system, feeling dismissed, anxiety/fear, empowerment.

### Chart 6: Singulair/Montelukast Deep Dive (dedicated section)
9 behavioral effects (nightmares, aggression, mood changes, suicidal ideation, sleep disturbances, anxiety, depression, personality changes, hyperactivity) + 4 discourse categories (black box warning, starting decision, stopping decision, seeking alternatives). Volume + fear sentiment time-series.

## Database Schema (17 tables)
- **posts** — id, title, selftext, created_utc, score, num_comments, permalink, first_seen, sentiment, fear_score, comments_scraped, subreddit, engagement_score, sort_source, crosspost_parent, pediatric_confidence, child_age_mentioned
- **comments** — id, post_id, body, score, created_utc, author, sentiment, fear_score, first_seen
- **medication_mentions** — (post_id, medication) PK, med_class
- **side_effects** — (source_type, source_id, effect) PK
- **treatment_beliefs** — (source_type, source_id, belief) PK, stance
- **ed_discourse** — (source_type, source_id, category) PK
- **triggers** — (source_type, source_id, trigger_name) PK, trigger_category
- **caregiver_sentiment** — (source_type, source_id, category) PK
- **singulair_effects** — (source_type, source_id, effect) PK
- **scrape_runs**, **scrape_errors** — same as bc-tracker
- **validation_votes** — beliefs validation (precision/recall/F1/Scott's Pi)
- **side_effect_votes** — per-effect validation (Jaccard/F1)
- **sentiment_votes** — sentiment validation (Pearson r/MAE/Cohen's kappa)
- **feedback**, **feedback_votes** — suggestion box

## Sentiment: Standard + Fear Dimension
- Standard sentiment (-1.0 to +1.0): bc-tracker's keyword scorer with negation windows + confidence damping
- Fear score (0.0 to 1.0): separate dimension for medical anxiety (scared, terrified, can't breathe, emergency, 911, gasping, etc.)

## Phased Implementation

### Phase 1: Project Scaffold + Core Tracker (`asthma_tracker.py`)
Create `~/peds-asthma-tracker/` directory and `asthma_tracker.py` with:
- All regex dicts (asthma gate, pediatric gate, medications, side effects, beliefs, ED discourse, triggers, caregiver sentiment, Singulair effects)
- Sentiment + fear scoring functions
- All find_*/explain_* analysis functions
- Full DB schema (17 tables + indexes, WAL mode, idempotent migrations)
- All query functions with date/subreddit filtering
- Validation system (3 facets)
- Reddit scraping (same as bc-tracker: fetch_json, scrape_subreddit, scrape_comments_batch)
- Backup to `~/Library/CloudStorage/Dropbox-Personal/backups/peds-asthma-tracker/`
- CLI: `scrape --all`, `report --days N`, `report --csv`
- **Reference**: `/Users/hanl/bc-tracker/bc_tracker.py`
- **Verify**: `python3 asthma_tracker.py scrape --all` completes; `sqlite3 tracker.db ".tables"` shows 17 tables

### Phase 2: Web Server + Dashboard (`asthma_tracker_web.py`)
Create web server with embedded HTML dashboard:
- Scheduler (6h interval), port 8053
- All API endpoints (~30 GET + 6 POST)
- Dark-theme Chart.js dashboard: Stats row, Chart 1 (medication mentions/sentiment/trend), Chart 6 (Singulair deep dive), Charts 2-5, heatmaps, post explorer
- Validate tab (3 facets) + Feedback tab
- **Reference**: `/Users/hanl/bc-tracker/bc_tracker_web.py`
- **Verify**: Dashboard loads at localhost:8053; all charts render; scrape button works

### Phase 3: Static Site + Deploy Scripts + GitHub
- `generate_site.py` — bake all data into self-contained HTML
- `deploy.sh`, `vps-setup.sh`, `peds-asthma-tracker.service`
- `.gitignore`, `CLAUDE.md`
- Git init + push to `https://github.com/leolhan1425/peds-asthma-tracker.git`
- GitHub Pages: `docs/` folder
- **Verify**: `python3 generate_site.py` creates `docs/index.html`; GitHub Pages live

### Phase 4: VPS Deployment
- Create `/opt/peds-asthma-tracker/` on VPS
- Deploy via `deploy.sh`
- Install systemd service, enable + start
- Add Caddy config for `asthma.hanlabnw.com`
- Verify DNS + SSL
- **Verify**: `https://asthma.hanlabnw.com` loads with SSL; first scrape completes

## Key Design Decisions
1. **Asthma gate before pediatric gate** — prevents processing every parenting sub post
2. **Fear dimension** — separate from sentiment, captures medical anxiety specific to asthma caregiving
3. **Singulair dedicated section** — FDA black box controversy warrants standalone analysis
4. **Post-level mentions only** — same as bc-tracker, comment text analyzed for effects/sentiment but not medication mentions
5. **Stance terminology**: concordant/discordant/uncertain/unclear (clinical guideline alignment, not misinformation framing)
6. **CLAUDE.md maintained from day 1** — full record-keeping for paper methodology section
