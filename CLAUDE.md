# Pediatric Asthma Reddit Tracker

## What this project does
Scrapes **10 subreddits** for pediatric asthma discussions, identifies mentions of ~35 asthma medications (in 8 classes), ~20 side effects, ~10 treatment beliefs/misconceptions (with stance classification against NAEPP/GINA guidelines), ED/hospital decision-making discourse (4 categories), ~19 environmental/viral/non-evidence-based triggers, caregiver emotional state (5 categories), and Singulair/montelukast behavioral effects (9 effects + 4 discourse categories). Uses two-stage content gating (asthma gate + pediatric gate), keyword-based sentiment with a fear/anxiety dimension, stores everything in SQLite, and serves a web dashboard with charts, heatmaps, and a post/comment explorer. Medication mentions counted at **post level only**.

## Project status
**Phase 1 COMPLETE** — `asthma_tracker.py` built (2332 lines). All regex dicts, two-stage content gate, sentiment + fear scoring, 17-table DB schema, query functions, validation system, Reddit scraping, backup, CLI.
**Phase 2 NEXT** — `asthma_tracker_web.py` (web server + dashboard).

## Architecture (mirrors bc-tracker at ~/bc-tracker/)
- **3 Python files**: `asthma_tracker.py` (core), `asthma_tracker_web.py` (web server), `generate_site.py` (static site)
- **Zero external deps** — Python 3.9 stdlib only
- **SQLite WAL mode**, Reddit public JSON API, 1.5s rate limiting, residential proxy
- **Port 8053**, domain `asthma.hanlabnw.com`, VPS path `/opt/peds-asthma-tracker`

## Subreddits scraped
| Subreddit | Posts per cycle | Auto-pediatric |
|-----------|----------------|----------------|
| r/Asthma | 200 /new + 50 /hot | No |
| r/Parenting | 200 /new + 50 /hot | No |
| r/beyondthebump | 200 /new + 50 /hot | Yes |
| r/Mommit | 100 /new + 50 /hot | Yes |
| r/daddit | 100 /new + 50 /hot | Yes |
| r/AskDocs | 200 /new + 50 /hot | No |
| r/Allergies | 100 /new + 50 /hot | No |
| r/medical | 100 /new + 50 /hot | No |
| r/Pediatrics | 100 /new + 50 /hot | Yes |
| r/NewParents | 100 /new + 50 /hot | Yes |

## Two-stage content gate
1. **Asthma gate** — post must mention asthma/inhaler/nebulizer/medication terms. Skip if no match.
2. **Pediatric gate** — classify `pediatric_confidence` as:
   - `definite` — auto-pediatric subreddit OR strong keywords ("my child", "my kid", "pediatrician", age 1-17)
   - `likely` — weaker keywords ("children", "kids", "teen", "LO")
   - `none` — no pediatric signal
   Only `definite`/`likely` posts get full analysis.

## Project structure
```
peds-asthma-tracker/
  asthma_tracker.py              # Core: scraping, regex, pediatric gate, sentiment, SQLite, CLI
  asthma_tracker_web.py          # Web server + dashboard (auto-scrapes every 6 hours)
  generate_site.py               # Static site generator -> docs/index.html for GitHub Pages
  CLAUDE.md                      # This file — project reference for Claude
  .gitignore                     # Data dirs, __pycache__, .env, *.log
  deploy.sh                      # rsync + systemctl restart
  vps-setup.sh                   # One-time VPS provisioning
  peds-asthma-tracker.service    # systemd unit file
  docs/
    index.html                   # Auto-generated interactive dashboard (GitHub Pages)
  asthma_tracker_data/
    tracker.db                   # SQLite database
    scrape_errors.log            # Error log (WARNING+)
```

Backups go to: `~/Library/CloudStorage/Dropbox-Personal/backups/peds-asthma-tracker/`

## Database schema (17 tables)

- **posts** — id (TEXT PK), title, selftext, created_utc (REAL), score, num_comments, permalink, first_seen, sentiment (REAL), fear_score (REAL), comments_scraped (0/1), subreddit, engagement_score, sort_source ('new'/'hot'), crosspost_parent, pediatric_confidence ('definite'/'likely'/'none'), child_age_mentioned (TEXT)
- **comments** — id (TEXT PK), post_id (FK), body, score, created_utc, author, sentiment, fear_score, first_seen
- **medication_mentions** — post_id, medication, med_class (composite PK: post_id, medication). Post-level only.
- **side_effects** — source_type ('post'/'comment'), source_id, effect (composite PK)
- **treatment_beliefs** — source_type, source_id, belief, stance ('concordant'/'discordant'/'uncertain'/'unclear') (composite PK: source_type, source_id, belief)
- **ed_discourse** — source_type, source_id, category ('decision_uncertainty'/'post_visit'/'return_visits'/'barriers') (composite PK)
- **triggers** — source_type, source_id, trigger_name, trigger_category ('environmental'/'viral'/'non_evidence_based') (composite PK: source_type, source_id, trigger_name)
- **caregiver_sentiment** — source_type, source_id, category ('trust'/'frustration'/'dismissed'/'anxiety'/'empowerment') (composite PK)
- **singulair_effects** — source_type, source_id, effect (composite PK)
- **scrape_runs** — id, scraped_at, post_count, subreddit, error_count
- **scrape_errors** — id, timestamp, subreddit, error_type, message, source_id, source_type
- **validation_votes** — id, post_id, validator, system_flagged, human_flagged, human_stance, reason, system_claims (JSON), voted_at. UNIQUE(post_id, validator).
- **side_effect_votes** — id, comment_id, validator, system_effects (JSON), human_effects (JSON), other_effect, voted_at. UNIQUE(comment_id, validator).
- **sentiment_votes** — id, comment_id, validator, system_score, human_score, voted_at. UNIQUE(comment_id, validator).
- **feedback** — id, voter_id, suggestion, created_at, status
- **feedback_votes** — id, feedback_id, voter_id, voted_at. UNIQUE(feedback_id, voter_id).

## 6 Dashboard modules

### Chart 1: Medication Sentiment Tracker
~35 medications in 8 classes: ICS (Flovent, QVAR, Pulmicort, Alvesco, Asmanex, Arnuity, budesonide, fluticasone, beclomethasone), Oral corticosteroids (prednisone, prednisolone, dexamethasone, oral steroids), Bronchodilators (albuterol, ProAir, Ventolin, Proventil, levalbuterol, Xopenex, rescue inhaler), Biologics (Dupixent, Xolair, Nucala, Fasenra, Tezspire), Leukotriene modifiers (Singulair, Accolate), Combination inhalers (Advair, Symbicort, Dulera, Breo, AirDuo), Devices (nebulizer, spacer, peak flow meter, pulse oximeter). Sentiment + fear score per medication, time-series with volume.

### Chart 2: ED/Hospital Decision-Making Discourse
4 categories: decision uncertainty, post-visit experience, return visits, barriers to access. Time-series with volume + sentiment. Maps to 72-hour ED return research.

### Chart 3: Treatment Beliefs and Misconceptions
10 guideline-discordant beliefs with stance detection (concordant/discordant/uncertain/unclear):
1. Albuterol-only reliance
2. Nebulizer superiority myth
3. Alternative medicine cures
4. Steroid growth stunting fear
5. Outgrow asthma belief
6. Inhalers are addictive
7. Natural remedies are better
8. Steroids are dangerous long-term
9. Asthma is psychological
10. Only need medicine during attacks

Each with NAEPP/GINA source citations. Proximity-bounded regex patterns.

### Chart 4: Trigger and Environmental Narrative Tracker
19 triggers in 3 categories:
- Environmental (8): Mold, Air pollution, Smoke, Pets, Pollen/seasonal, Dust mites, Weather changes, Cold air
- Viral (6): RSV, Common cold, Flu, COVID, Respiratory infection, Croup
- Non-evidence-based (5): Vaccines, Diet/toxins, Chemicals, Mold toxicity, EMF
Seasonal pattern tracking (viral spikes fall/winter, environmental summer).

### Chart 5: Caregiver Emotional State and System Trust
5 categories: trust in providers, frustration with system, feeling dismissed, anxiety/fear, empowerment.

### Chart 6: Singulair/Montelukast Deep Dive (dedicated section)
9 behavioral effects: nightmares, aggression, mood changes, suicidal ideation, sleep disturbances, anxiety, depression, personality changes, hyperactivity.
4 discourse categories: black box warning, starting decision, stopping decision, seeking alternatives.
Volume + fear sentiment time-series.

## Sentiment analysis
- **Standard sentiment** (-1.0 to +1.0): keyword-based scorer with negation windows + confidence dampening (same as bc-tracker)
- **Fear score** (0.0 to 1.0): separate dimension for medical anxiety (scared, terrified, can't breathe, emergency, 911, gasping, etc.)

## Human validation framework (from day 1)
Three facets:
- **Treatment Beliefs**: 10 random posts (5 flagged + 5 unflagged), concordant/discordant/uncertain voting, confusion matrix, per-belief accuracy, Scott's Pi
- **Side Effects**: 5 random comments, checkbox multi-select + write-in, per-effect precision/recall/F1, Jaccard similarity
- **Sentiment**: 5 random comments, 5-point Likert scale, Pearson r, MAE, Cohen's kappa

## Key design decisions
1. **Asthma gate before pediatric gate** — prevents processing every parenting sub post
2. **Fear dimension** — separate from sentiment, captures medical anxiety specific to asthma caregiving
3. **Singulair dedicated section** — FDA black box controversy warrants standalone analysis
4. **Post-level mentions only** — comment text analyzed for effects/sentiment but not medication mentions
5. **Stance terminology**: concordant/discordant/uncertain/unclear (clinical guideline alignment)
6. **Engagement scoring**: `log2(max(upvotes, 1)) + log2(max(comments, 1)) * 1.5`

## Web API endpoints
All data endpoints accept `?from=YYYY-MM-DD&to=YYYY-MM-DD&sub=Asthma` for filtering.

### Core
- `GET /` — dashboard
- `GET /api/data` — medication counts, daily breakdown, DB stats
- `GET /api/status` — scheduler status
- `POST /api/scrape` — trigger immediate scrape
- `GET /api/errors` — recent scrape errors

### Chart 1: Medications
- `GET /api/medications` — medication mention counts by class
- `GET /api/medication-sentiment` — avg sentiment + fear per medication
- `GET /api/medication-daily` — daily counts (time-series)

### Chart 2: ED Discourse
- `GET /api/ed-discourse` — counts per category
- `GET /api/ed-discourse-daily` — daily time-series

### Chart 3: Beliefs
- `GET /api/beliefs` — belief counts with stance
- `GET /api/beliefs-heatmap` — {medication: {belief: count}}
- `GET /api/beliefs-stance` — {belief: {stance: count}}

### Chart 4: Triggers
- `GET /api/triggers` — trigger counts by category
- `GET /api/triggers-daily` — daily time-series (seasonal)
- `GET /api/triggers-heatmap` — trigger matrix

### Chart 5: Caregiver
- `GET /api/caregiver` — caregiver sentiment counts
- `GET /api/caregiver-daily` — daily time-series

### Chart 6: Singulair
- `GET /api/singulair` — behavioral effect counts
- `GET /api/singulair-daily` — daily time-series
- `GET /api/singulair-discourse` — discourse categories

### Post Explorer
- `GET /api/posts?medication=Albuterol&limit=20` — top posts
- `GET /api/comments?post_id=abc123` — comments
- `GET /api/post-effects?id=abc123` — side effects

### Validation
- `GET /api/validation/batch` — posts for beliefs validation
- `POST /api/validation/votes` — submit belief votes
- `GET /api/validation/stats` — precision/recall/F1/Scott's Pi
- `GET /api/validation/side-effects/batch` — comments for SE validation
- `POST /api/validation/side-effects/votes` — submit SE votes
- `GET /api/validation/side-effects/stats` — per-effect P/R/F1, Jaccard
- `GET /api/validation/sentiment/batch` — comments for sentiment validation
- `POST /api/validation/sentiment/votes` — submit sentiment votes
- `GET /api/validation/sentiment/stats` — Pearson r, MAE, Cohen's kappa

### Feedback
- `GET /api/feedback` — suggestions
- `POST /api/feedback` — submit suggestion
- `POST /api/feedback/vote` — toggle upvote

## Reference project
This project mirrors the architecture of `~/bc-tracker/` (Reddit Contraceptive Tracker). Key reference files:
- `~/bc-tracker/bc_tracker.py` (3498 lines) — core tracker patterns
- `~/bc-tracker/bc_tracker_web.py` (2773 lines) — web dashboard patterns
- `~/bc-tracker/generate_site.py` (1527 lines) — static site patterns

## Implementation phases
### Phase 1: Core Tracker (`asthma_tracker.py`) — COMPLETE
All regex dicts, sentiment + fear scoring, DB schema, query functions, validation system, Reddit scraping, backup, CLI.

### Phase 2: Web Dashboard (`asthma_tracker_web.py`) — COMPLETE
Scheduler (6h auto-scrape), ~30 GET + 6 POST API endpoints, dark-theme Chart.js dashboard with all 6 modules, post explorer, validate tab (3 facets), feedback tab. 1208 lines.

### Phase 3: Static Site + Deploy Scripts — COMPLETE
`generate_site.py` (894 lines): bakes raw data into self-contained `docs/index.html` with client-side filtering (date range + subreddit), all 6 chart modules, post explorer, validation stats, CSV export. `deploy.sh`, `vps-setup.sh`, `peds-asthma-tracker.service` for VPS deployment.

### Phase 4: VPS Deployment
`/opt/peds-asthma-tracker/`, systemd service, `asthma.hanlabnw.com`. Ready to deploy.

## How to run (once built)
```bash
cd ~/peds-asthma-tracker
python3 asthma_tracker.py scrape --all     # Scrape all subs
python3 asthma_tracker.py report --days 30 # Report
python3 asthma_tracker_web.py              # Dashboard at :8053
python3 generate_site.py --push            # Static site + GitHub Pages
```

## How to continue building
Tell Claude: "Continue building peds-asthma-tracker. Read CLAUDE.md and the plan file, then build asthma_tracker.py (Phase 1). Reference ~/bc-tracker/bc_tracker.py for the scraping, sentiment, DB, query, and validation patterns."

## History
- **2026-02-25** — Project created. Plan finalized with 6 dashboard modules, 17 DB tables, 10 subreddits, two-stage content gate, fear dimension, Singulair deep dive, human validation framework. Scaffold + CLAUDE.md committed.
- **2026-02-26** — Phase 1 complete: `asthma_tracker.py` (2332 lines). All regex patterns (35 medications, 20 side effects, 10 treatment beliefs, 4 ED categories, 19 triggers, 5 caregiver categories, 9 Singulair effects, 4 Singulair discourse). Two-stage content gate (asthma gate + pediatric gate). Sentiment + fear scoring. 17-table SQLite DB. All query functions with date/subreddit filtering. 3-facet validation system (beliefs, side effects, sentiment). Reddit scraping with crosspost dedup. Backup + CLI.
- **2026-02-26** — Phase 2 complete: `asthma_tracker_web.py` (1208 lines). Scheduler, ~30 API endpoints, embedded HTML/CSS/JS dashboard with 6 chart modules, post explorer, validate tab, feedback tab. Port 8053.
- **2026-02-26** — Phase 3 complete: `generate_site.py` (894 lines) bakes raw data into self-contained `docs/index.html` with client-side filtering. `deploy.sh`, `vps-setup.sh`, `peds-asthma-tracker.service` for VPS deployment.
