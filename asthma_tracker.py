#!/usr/bin/env python3
"""
Pediatric Asthma Reddit Tracker

Scrapes 10 subreddits for pediatric asthma discussions, identifies mentions
of ~35 asthma medications (in 8 classes), ~20 side effects, ~10 treatment
beliefs/misconceptions (with stance classification), ED/hospital discourse
(4 categories), ~19 triggers, caregiver emotional state (5 categories),
and Singulair/montelukast behavioral effects. Uses two-stage content gating
(asthma gate + pediatric gate), keyword-based sentiment with a fear/anxiety
dimension, stores everything in SQLite, and serves data for a web dashboard.

Usage:
    python asthma_tracker.py scrape          # Scrape today's posts + comments
    python asthma_tracker.py scrape --all    # Include all fetched posts
    python asthma_tracker.py report          # Print summary report
    python asthma_tracker.py report --days 7 # Report for last 7 days
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import shutil
import sqlite3
import sys
import time
import urllib.request
import urllib.error
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

DATA_DIR = Path(__file__).parent / "asthma_tracker_data"
DB_FILE = DATA_DIR / "tracker.db"
_backup_env = os.environ.get("ASTHMA_TRACKER_BACKUP_DIR")
BACKUP_DIR = Path(_backup_env) if _backup_env else Path.home() / "Library" / "CloudStorage" / "Dropbox-Personal" / "backups" / "peds-asthma-tracker"
LOG_FILE = DATA_DIR / "scrape_errors.log"

USER_AGENT = "PedsAsthmaTracker/1.0 (research; educational)"

# Reddit OAuth (optional — falls back to public JSON API)
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET", "")
_oauth_token: Optional[str] = None
_oauth_expires: float = 0.0

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("asthma_tracker")
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                       datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(ch)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(str(LOG_FILE))
    fh.setLevel(logging.WARNING)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                       datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(fh)
    return logger

log = _setup_logging()

# ---------------------------------------------------------------------------
# Subreddit configuration
# ---------------------------------------------------------------------------

# auto_pediatric=True means all posts from that sub are treated as
# pediatric_confidence='definite' without needing keyword matches.
SUBREDDITS = [
    {"name": "Asthma",        "limit": 200, "auto_pediatric": False},
    {"name": "Parenting",     "limit": 200, "auto_pediatric": False},
    {"name": "beyondthebump", "limit": 200, "auto_pediatric": True},
    {"name": "Mommit",        "limit": 100, "auto_pediatric": True},
    {"name": "daddit",        "limit": 100, "auto_pediatric": True},
    {"name": "AskDocs",       "limit": 200, "auto_pediatric": False},
    {"name": "Allergies",     "limit": 100, "auto_pediatric": False},
    {"name": "medical",       "limit": 100, "auto_pediatric": False},
    {"name": "Pediatrics",    "limit": 100, "auto_pediatric": True},
    {"name": "NewParents",    "limit": 100, "auto_pediatric": True},
]

# Lookup for auto-pediatric subreddits
_AUTO_PEDIATRIC_SUBS = {s["name"].lower() for s in SUBREDDITS if s.get("auto_pediatric")}

# ---------------------------------------------------------------------------
# Bot author filter (insert-time + query-time exclusion)
# ---------------------------------------------------------------------------

_KNOWN_BOT_AUTHORS = {
    "automoderator", "sneakpeekbot", "wikitextbot", "remindmebot",
    "limbretrieval-bot", "b0trank", "b0trankbot", "botdefs",
    "goodbot_badbot", "converter-bot", "repostsleuthbot",
    "sub_doesnt_exist_bot", "image_linker_bot", "properu",
    "vredditdownloader", "substitute-bot", "gifreversingbot",
    "anti-gif-bot", "transcribot", "haikubot-1911",
}

# Heuristic: name ends with 'bot', '-bot', or '_bot', OR contains 'bot'
# and ends with digits. Case-insensitive.
_BOT_HEURISTIC_RE = re.compile(
    r"(?:^|[-_])bot$|^.*bot\d+$|^.*bot[-_]\d+$",
    re.IGNORECASE,
)


def is_bot_author(author):
    """Return 'known_bot', 'heuristic_bot', or None."""
    if not author:
        return None
    a = author.strip().lower()
    if not a or a in ("[deleted]", "[removed]"):
        return None
    if a in _KNOWN_BOT_AUTHORS:
        return "known_bot"
    if _BOT_HEURISTIC_RE.search(a):
        return "heuristic_bot"
    return None


# ---------------------------------------------------------------------------
# Location extraction (US / non-US classification)
# ---------------------------------------------------------------------------

_US_STATES = (
    r"alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|"
    r"florida|georgia|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|"
    r"maine|maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|"
    r"nebraska|nevada|new\s+hampshire|new\s+jersey|new\s+mexico|new\s+york|"
    r"north\s+carolina|north\s+dakota|ohio|oklahoma|oregon|pennsylvania|"
    r"rhode\s+island|south\s+carolina|south\s+dakota|tennessee|texas|utah|"
    r"vermont|virginia|washington|west\s+virginia|wisconsin|wyoming"
)

# Case-sensitive state abbreviations — exclude ambiguous: IN, OR, ME, OK, HI, OH, AL
_SAFE_STATE_ABBREVS = (
    r"AK|AZ|AR|CA|CO|CT|DE|FL|GA|ID|IL|IA|KS|KY|LA|MD|MA|MI|MN|MS|MO|MT|"
    r"NE|NV|NH|NJ|NM|NY|NC|ND|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY"
)

_NON_US_COUNTRIES = (
    r"the\s+UK|United\s+Kingdom|England|Scotland|Wales|Ireland|Canada|Australia|"
    r"New\s+Zealand|India|Germany|France|Spain|Italy|Netherlands|Belgium|Sweden|"
    r"Norway|Denmark|Finland|Switzerland|Austria|Portugal|Brazil|Mexico|Japan|"
    r"South\s+Korea|Philippines|Singapore|Malaysia|South\s+Africa|Nigeria|"
    r"Poland|Czech\s+Republic|Turkey|Israel|Egypt"
)

_NATIONALITIES = (
    r"British|Canadian|Australian|Indian|German|French|Spanish|Italian|Dutch|"
    r"Swedish|Norwegian|Danish|Finnish|Swiss|Brazilian|Mexican|Japanese|Korean|"
    r"Filipino|Irish|Scottish|Welsh|Kiwi|South\s+African|Nigerian|Polish|Turkish|Israeli"
)

_FIRST_PERSON_LOC = (
    r"(?:I'?m|I\s+am|I\s+live|I'?m\s+from|I\s+live\s+in|I'?m\s+in"
    r"|I'?m\s+based\s+in|I\s+am\s+from|I\s+am\s+in)"
)

_LOCATION_PATTERNS = [
    re.compile(rf"{_FIRST_PERSON_LOC}\s+(?:in\s+)?(?:the\s+)?(?:US|USA|United\s+States|America)\b", re.IGNORECASE),
    re.compile(rf"\bI'?m\s+American\b", re.IGNORECASE),
    re.compile(rf"{_FIRST_PERSON_LOC}\s+(?:in\s+)?(?:{_US_STATES})\b", re.IGNORECASE),
    re.compile(rf"{_FIRST_PERSON_LOC}\s+(?:in\s+)?(?:{_SAFE_STATE_ABBREVS})\b"),
    re.compile(rf"{_FIRST_PERSON_LOC}\s+(?:in\s+)?(?:{_NON_US_COUNTRIES})\b", re.IGNORECASE),
    re.compile(rf"\bI'?m\s+(?:{_NATIONALITIES})\b", re.IGNORECASE),
    re.compile(r"\bmy\s+(?:GP|NHS)\b", re.IGNORECASE),
]

_LOC_PATTERN_IS_US = [True, True, True, True, False, False, False]

_THIRD_PERSON_LOC_RE = re.compile(
    r"\b(?:my\s+(?:husband|wife|partner|boyfriend|girlfriend|mom|dad|mother|father"
    r"|sister|brother|friend|colleague|coworker|neighbor|neighbour|cousin|aunt|uncle))\s",
    re.IGNORECASE,
)


def _is_third_person(snippet: str, match_start: int) -> bool:
    """True if the location match is preceded by a third-person reference within 30 chars."""
    prefix = snippet[max(0, match_start - 30): match_start]
    return bool(_THIRD_PERSON_LOC_RE.search(prefix))


def extract_location(text: str) -> Optional[str]:
    """Extract US/non-US location from first 500 chars of post text.
    Returns 'us', 'non_us', or None."""
    if not text:
        return None
    snippet = text[:500]
    for idx, pat in enumerate(_LOCATION_PATTERNS):
        m = pat.search(snippet)
        if m and not _is_third_person(snippet, m.start()):
            return 'us' if _LOC_PATTERN_IS_US[idx] else 'non_us'
    return None


# ---------------------------------------------------------------------------
# Two-stage content gate
# ---------------------------------------------------------------------------

# Stage 1: Asthma gate — post must mention asthma-related terms
ASTHMA_GATE_PATTERN = re.compile(
    r'\basthma(?:tic)?\b|\binhaler[s]?\b|\bnebulizer[s]?\b|\bnebuliser[s]?\b'
    r'|\bwheezing\b|\bwheeze[sd]?\b|\bbronchospasm\b|\bbronchodilat\w*\b'
    r'|\balbuterol\b|\bsalbutamol\b|\bventolin\b|\bproair\b|\bflovent\b|\bpulmicort\b'
    r'|\bqvar\b|\bsingulair\b|\bmontelukast\b|\bdupixent\b|\bxolair\b'
    r'|\badvair\b|\bsymbicort\b|\bbreo\b|\bbudesonide\b|\bfluticasone\b'
    r'|\barmonair\b|\bwixela\b|\bairsupra\b|\bbreyna\b|\bpediapred\b'
    r'|\bxopenex\b|\blevalbuterol\b|\bnucala\b|\bfasenra\b|\btezspire\b'
    r'|\bpeak\s*flow\b|\bpulse\s*ox\w*\b|\brescue\s*inhaler\b'
    r'|\bcontroller\s*(?:med|medication|inhaler)\b|\bsteroid\s*inhaler\b'
    r'|\bspacer\b|\brespiratory\s*(?:distress|infection|issue|problem)\b'
    r'|\bsmart\s+therapy\b|\bmart\s+therapy\b',
    re.IGNORECASE
)

# Stage 2: Pediatric gate — classify pediatric confidence
_PEDIATRIC_DEFINITE_PATTERN = re.compile(
    r'\bmy\s+(?:child|kid|son|daughter|baby|toddler|infant|little\s+one|LO)\b'
    r'|\bmy\s+\d+\s*(?:year|yr|month|mo)\s*(?:\s*old)?\b'
    r'|\bpediatrician\b|\bpeds?\b|\bchild(?:ren)?\'?s?\s+(?:doctor|hospital|er|ed)\b'
    r'|\b(?:[1-9]|1[0-7])\s*(?:year|yr)\s*(?:\s*old)\b'
    r'|\b(?:he|she)\s+is\s+(?:[1-9]|1[0-7])\b'
    r'|\bmy\s+(?:3|4|5|6|7|8|9|10|11|12|13|14|15|16|17)\s*(?:yo|y/?o|year\s*old)\b'
    r'|\bnewborn\b|\binfant\b|\bpreemie\b|\bnicu\b'
    r'|\b(?:his|her)\s+(?:inhaler|nebulizer|spacer|pulmicort|flovent|albuterol)\b'
    r'|\bI\s+(?:gave|give|administered)\s+(?:him|her)\s+(?:the\s+)?(?:albuterol|pulmicort|flovent|budesonide|prednis\w*|inhaler|nebulizer|neb)\b'
    r'|\b(?:brought|took|rushed)\s+(?:him|her)\s+to\s+(?:the\s+)?(?:er|ed|emergency|hospital|pediatrician|urgent\s+care)\b'
    r'|\bschool\s+nurse\b',
    re.IGNORECASE
)

_PEDIATRIC_LIKELY_PATTERN = re.compile(
    r'\bchildren\b|\bkids\b|\bteen(?:ager)?s?\b|\byoung(?:ster)?\b'
    r'|\bLO\b|\blittle\s+one\b|\bkiddo\b|\bminor\b|\bjuvenile\b'
    r'|\bschool\s*age\b|\bpreschool\w*\b|\belementary\b'
    r'|\bmiddle\s*school\b|\bhigh\s*school\b',
    re.IGNORECASE
)


def passes_asthma_gate(text: str) -> bool:
    """Stage 1: Does the text mention asthma/inhaler/medication terms?"""
    return bool(ASTHMA_GATE_PATTERN.search(text))


def classify_pediatric_confidence(text: str, subreddit: str = "") -> str:
    """Stage 2: Classify pediatric confidence as definite/likely/none."""
    if subreddit.lower() in _AUTO_PEDIATRIC_SUBS:
        return "definite"
    if _PEDIATRIC_DEFINITE_PATTERN.search(text):
        return "definite"
    likely_matches = _PEDIATRIC_LIKELY_PATTERN.findall(text)
    if likely_matches:
        # Promote to definite if 2+ distinct likely signals found
        if len(set(m.lower().strip() for m in likely_matches)) >= 2:
            return "definite"
        return "likely"
    return "none"


def extract_child_age(text: str) -> Optional[str]:
    """Extract mentioned child age from text, e.g. '3 year old', '8mo'."""
    patterns = [
        re.compile(r'\b(\d{1,2})\s*(?:year|yr|y/?o)\s*(?:old)?\b', re.IGNORECASE),
        re.compile(r'\b(\d{1,2})\s*(?:month|mo)\s*(?:old|s)?\b', re.IGNORECASE),
        re.compile(r'\bage\s*(\d{1,2})\b', re.IGNORECASE),
    ]
    for pat in patterns:
        m = pat.search(text[:500])
        if m:
            return m.group(0).strip()
    return None


# ---------------------------------------------------------------------------
# Medication patterns (~35 medications in 8 classes)
# ---------------------------------------------------------------------------

MEDICATIONS = {
    # ICS (Inhaled Corticosteroids)
    "Flovent": r"\bflovent\b|\bfluticasone\s*propionate\b|\barmonair\b",
    "QVAR": r"\bqvar\b",
    "Pulmicort": r"\bpulmicort\b|\bpulmicort\s*(?:respules|flexhaler)\b",
    "Alvesco": r"\balvesco\b|\bciclesonide\b",
    "Asmanex": r"\basmanex\b|\bmometasone\b(?!\s*(?:\/|and)\s*formoterol)",
    "Arnuity": r"\barnuity\b|\barnuity\s*ellipta\b|\bfluticasone\s*furoate\b",
    "budesonide": r"\bbudesonide\b(?!\s*(?:\/|and)\s*formoterol)",
    "fluticasone": r"\bfluticasone\b(?!\s*(?:\/|and|propionate|furoate)\b)",
    "beclomethasone": r"\bbeclomethasone\b|\bbeclovent\b",
    # Oral corticosteroids
    "prednisone": r"\bprednisone\b|\bpred\b(?!\s*(?:forte|mild))",
    "prednisolone": r"\bprednisolone\b|\borapred\b|\bprelone\b|\bpediapred\b",
    "dexamethasone": r"\bdexamethasone\b|\bdecadron\b|\bdex\b(?=\s+(?:dose|taper|burst|steroid|for|to\s+treat))",
    "oral steroids": r"\boral\s+steroid[s]?\b|\bsteroid\s+(?:burst|course|taper|pack)\b",
    # Bronchodilators
    "albuterol": r"\balbuterol\b|\bsalbutamol\b|\balbuterol\s*(?:hfa|sulfate)\b",
    "ProAir": r"\bproair\b|\bpro[\s-]?air\b|\bproair\s*(?:hfa|respiclick|digihaler)\b",
    "Ventolin": r"\bventolin\b|\bventolin\s*hfa\b",
    "Proventil": r"\bproventil\b|\bproventil\s*hfa\b",
    "levalbuterol": r"\blevalbuterol\b|\bxopenex\b",
    "rescue inhaler": r"\brescue\s*inhaler[s]?\b|\bemergency\s*inhaler\b|\brelief\s*inhaler\b|\bhfa\s*inhaler\b",
    # Biologics
    "Dupixent": r"\bdupixent\b|\bdupilumab\b",
    "Xolair": r"\bxolair\b|\bomalizumab\b",
    "Nucala": r"\bnucala\b|\bmepolizumab\b",
    "Fasenra": r"\bfasenra\b|\bbenralizumab\b",
    "Tezspire": r"\btezspire\b|\btezepelumab\b",
    # Leukotriene modifiers
    "Singulair": r"\bsingulair\b|\bsingular\b|\bmontelukast\b",
    "Accolate": r"\baccolate\b|\bzafirlukast\b",
    # Combination inhalers
    "Advair": r"\badvair\b|\bwixela\b|\bfluticasone\s*(?:\/|and)\s*salmeterol\b",
    "Symbicort": r"\bsymbicort\b|\bbudesonide\s*(?:\/|and)\s*formoterol\b|\bbreyna\b",
    "Dulera": r"\bdulera\b|\bmometasone\s*(?:\/|and)\s*formoterol\b",
    "Breo": r"\bbreo\b|\bbreo\s*ellipta\b|\bfluticasone\s*(?:\/|and)\s*vilanterol\b",
    "AirDuo": r"\bairduo\b|\bair\s*duo\b|\bairduo\s*(?:respiclick|digihaler)\b",
    "AirSupra": r"\bairsupra\b|\bair\s*supra\b|\balbuterol\s*(?:\/|and)\s*budesonide\b",
    "SMART therapy": r"\bsmart\s+(?:therapy|approach|protocol|regimen|inhaler)\b|\bsingle\s+maintenance\s+and\s+reliever\b|\bmaintenance\s+and\s+reliever\s+therapy\b|\bmart\s+(?:therapy|approach|protocol)\b",
    # Devices
    "nebulizer": r"\bnebulizer[s]?\b|\bnebuliser[s]?\b|\bneb\s+(?:treatment|machine|mask)\b|\bneb\b",
    "spacer": r"\bspacer[s]?\b|\baero\s*chamber\b",
    "peak flow meter": r"\bpeak\s*flow\s*(?:meter|reading|number|test)\b",
    "pulse oximeter": r"\bpulse\s*ox(?:imeter|imetry)?\b|\bsp[oO]2\b|\boxygen\s*(?:level|sat\w*|reading)\b",
}

MEDICATION_CLASSES = {
    "Flovent": "ICS", "QVAR": "ICS", "Pulmicort": "ICS", "Alvesco": "ICS",
    "Asmanex": "ICS", "Arnuity": "ICS", "budesonide": "ICS",
    "fluticasone": "ICS", "beclomethasone": "ICS",
    "prednisone": "Oral corticosteroids", "prednisolone": "Oral corticosteroids",
    "dexamethasone": "Oral corticosteroids", "oral steroids": "Oral corticosteroids",
    "albuterol": "Bronchodilators", "ProAir": "Bronchodilators",
    "Ventolin": "Bronchodilators", "Proventil": "Bronchodilators",
    "levalbuterol": "Bronchodilators",
    "rescue inhaler": "Bronchodilators",
    "Dupixent": "Biologics", "Xolair": "Biologics", "Nucala": "Biologics",
    "Fasenra": "Biologics", "Tezspire": "Biologics",
    "Singulair": "Leukotriene modifiers", "Accolate": "Leukotriene modifiers",
    "Advair": "Combination inhalers", "Symbicort": "Combination inhalers",
    "Dulera": "Combination inhalers", "Breo": "Combination inhalers",
    "AirDuo": "Combination inhalers", "AirSupra": "Combination inhalers",
    "SMART therapy": "Combination inhalers",
    "nebulizer": "Devices", "spacer": "Devices",
    "peak flow meter": "Devices", "pulse oximeter": "Devices",
}

_COMPILED_MEDS = {name: re.compile(pat, re.IGNORECASE) for name, pat in MEDICATIONS.items()}

# ---------------------------------------------------------------------------
# Side-effect patterns (~20 categories)
# ---------------------------------------------------------------------------

SIDE_EFFECTS = {
    "Growth concerns": r"\bgrowth\s+(?:stunt|slow|delay|suppress|affect|impact|concern|issue)\w*\b|\bstunted\s+growth\b|\bheight\s+(?:concern|issue|affect)\w*\b",
    "Oral thrush": r"\bthrush\b|\boral\s+(?:yeast|candid\w*)\b|\bwhite\s+(?:patches|spots)\s+(?:in|on)\s+mouth\b",
    "Hoarseness": r"\bhoarse(?:ness)?\b|\braspy\s+voice\b|\bvoice\s+change\w*\b",
    "Jitteriness": r"\bjitter(?:y|iness|s)\b|\bshak(?:y|ing|iness)\b|\btremor[s]?\b",
    "Rapid heartbeat": r"\brapid\s+heart\w*\b|\bheart\s*(?:racing|pounding|fast)\b|\btachycardia\b|\bpalpitat\w*\b",
    "Mood changes": r"\bmood\s+(?:swing|change|shift)\w*\b|\bemotional\b|\birritab(?:le|ility)\b",
    "Hyperactivity": r"\bhyperactiv\w*\b|\bhyper\b|\bcan'?t\s+(?:sit\s+)?still\b|\bwired\b|\bbouncing\s+off\b",
    "Sleep issues": r"\bsleep\s+(?:issue|problem|disturb|difficult|trouble)\w*\b|\binsomnia\b|\bcan'?t\s+sleep\b|\bup\s+all\s+night\b",
    "Nightmares": r"\bnightmare[s]?\b|\bnight\s*terror[s]?\b|\bbad\s+dream[s]?\b|\bscream(?:ing|s)?\s+(?:at|in)\s+(?:night|sleep)\b",
    "Behavioral changes": r"\bbehavio(?:r|ur)\s+(?:change|issue|problem|concern)\w*\b|\bacting\s+(?:out|up|different)\b",
    "Nausea": r"\bnause(?:a|ous|ated)\b|\bvomit(?:ing)?\b|\bthrew\s+up\b|\bthrow(?:ing)?\s+up\b|\bstomach\s+(?:upset|ache|pain|sick)\b",
    "Headaches": r"\bheadache[s]?\b|\bmigraine[s]?\b|\bhead\s+(?:hurt|pain|ache)\w*\b",
    "Weight gain": r"\bweight\s+gain\b|\bgained\s+weight\b|\bgaining\s+weight\b|\bappetite\s+increas\w*\b",
    "Decreased appetite": r"\bappetite\s+(?:decreas|loss|reduc|suppress)\w*\b|\bnot\s+eat(?:ing)?\b|\bwon'?t\s+eat\b|\brefus(?:e[sd]?|ing)\s+(?:to\s+)?eat\b",
    "Fatigue": r"\bfatigue[d]?\b|\bexhaust(?:ed|ion)\b|\btired(?:ness)?\b|\blethargi?c\b|\bno\s+energy\b|\bsluggish\b",
    "Anxiety": r"\banxi(?:ety|ous)\b|\bpanic\s+attack[s]?\b|\bnervous(?:ness)?\b|\bworr(?:y|ied|ying)\b",
    "Depression": r"\bdepress(?:ed|ion|ing)?\b|\bmental\s+health\b|\bsad(?:ness)?\b|\bwithdr(?:awn|ew|awal)\b",
    "Aggression": r"\baggress(?:ion|ive|iveness)\b|\bviolent\b|\banger\s+(?:issue|outburst)\w*\b|\brage\b|\bhitting\b|\bbiting\b",
    "Adrenal suppression": r"\badrenal\s+(?:suppress|insufficien|crisis)\w*\b|\badrenal\b",
    "Bone density": r"\bbone\s+(?:densit|loss|thin)\w*\b|\bosteoporo\w*\b",
}

_COMPILED_EFFECTS = {name: re.compile(pat, re.IGNORECASE) for name, pat in SIDE_EFFECTS.items()}

# ---------------------------------------------------------------------------
# Treatment belief patterns (10 guideline-discordant beliefs)
# ---------------------------------------------------------------------------

TREATMENT_BELIEFS = {
    "Albuterol-only reliance":
        r"\b(?:only|just)\s+(?:need|use|give|take)\w*\s+(?:\w+\s+){0,2}(?:albuterol|rescue\s*inhaler|ventolin|proair)\b"
        r"|\b(?:albuterol|rescue\s*inhaler)\b.{0,40}\b(?:(?:is\s+)?enough|all\s+(?:we|they|you|he|she)\s+need|don'?t\s+need\s+(?:a\s+)?(?:controller|daily|maintenance))\b"
        r"|\bjust\s+use\s+\w+\s+(?:albuterol|rescue\s*inhaler|ventolin|proair)\b.{0,30}\b(?:when|during|if)\b",
    "Nebulizer superiority myth":
        r"\bnebulizer\w*\b.{0,50}\b(?:work\w*\s+better|better\s+than|stronger\s+than|more\s+effective)\b"
        r"|\bnebulizer\w*\b.{0,40}\b(?:superior|more\s+powerful|stronger)\b"
        r"|\binhaler\w*\b.{0,40}\b(?:don'?t|doesn'?t|not)\b.{0,20}\b(?:work\s+as\s+well|as\s+(?:good|effective))\b",
    "Alternative medicine cures":
        r"\b(?:essential\s+oil\w*|homeopath\w*|chiropractic|acupunctur\w*|herbal|naturopath\w*)\b.{0,60}\b(?:cure[sd]?|treat\w*|fix\w*|heal\w*|help\w*)\b.{0,30}\basthma\b"
        r"|\basthma\b.{0,60}\b(?:cure[sd]?|treat\w*|fix\w*|heal\w*)\b.{0,30}\b(?:essential\s+oil|homeopath|chiropractic|herbal|natural|holistic)\b"
        r"|\b(?:essential\s+oil\w*|homeopath\w*|chiropractic)\b.{0,30}\b(?:cure[sd]?|heal\w*|fix\w*)\b.{0,30}\b(?:his|her|their|our|my)\b.{0,15}\basthma\b",
    "Steroid growth stunting fear":
        r"\b(?:steroid\w*|inhaler\w*|flovent|pulmicort|budesonide|fluticasone|ics)\b.{0,60}\b(?:stunt|slow|affect|stop|harm|delay|suppress)\w*\b.{0,20}\b(?:his|her|their|the)?\s*(?:growth|height|growing)\b"
        r"|\bgrowth\b.{0,40}\b(?:stunt|slow|suppress|delay|affect)\w*\b.{0,40}\b(?:steroid|inhaler|flovent|pulmicort|budesonide)\b"
        r"|\b(?:only|just|half)\s+(?:give|use|do)\w*\s+(?:the\s+)?half\s+(?:the\s+)?dose\b"
        r"|\bhalf\s+(?:the\s+)?dose\b.{0,30}\b(?:steroid|inhaler|flovent|pulmicort)\b"
        r"|\bdon'?t\s+(?:give|use)\s+(?:the\s+)?full\s+dose\b",
    "Outgrow asthma belief":
        r"\b(?:outgrow|grow\s+out\s+of|grow\s+out)\b.{0,30}\b(?:asthma|it)\b"
        r"|\basthma\b.{0,40}\b(?:goes?\s+away|disappear|outgrow|grow\s+out)\b"
        r"|\b(?:he|she|they|child|kid)\b.{0,20}\b(?:will|going\s+to|gonna)\s+(?:just\s+)?(?:outgrow|grow\s+out)\b",
    "Inhalers are addictive":
        r"\b(?:inhaler\w*|albuterol|steroid\s+inhaler)\b.{0,40}\b(?:addict\w*|depend\w*|habit[\s-]*form\w*|hooked|reliant)\b"
        r"|\b(?:addict\w*|depend\w*|hooked|reliant)\b.{0,40}\b(?:inhaler|albuterol)\b",
    "Natural remedies are better":
        r"\b(?:natural|holistic|organic|alternative)\b.{0,50}\b(?:better|safer|prefer|instead\s+of)\b.{0,30}\b(?:medicine|medication|inhaler|steroid|drug)\b"
        r"|\bdon'?t\s+(?:want|like|believe\s+in)\s+(?:giving\s+)?(?:medication|medicine|drugs?|chemicals?)\b.{0,30}\basthma\b",
    "Steroids are dangerous long-term":
        r"\b(?:steroid\w*|inhaler\w*|ics|corticosteroid\w*)\b.{0,60}\b(?:dangerous|harmful|bad|unsafe|toxic|poison|damag)\w*\b.{0,30}\b(?:long[\s-]*term|forever|years|lifetime|prolonged)\b"
        r"|\b(?:dangerous|harmful|bad|unsafe)\b.{0,40}\b(?:long[\s-]*term)\b.{0,40}\b(?:steroid|inhaler)\b"
        r"|\bdon'?t\s+want\s+(?:him|her|them|my\s+\w+)\s+on\s+steroid\w*\b",
    "Asthma is psychological":
        r"\basthma\b.{0,50}\b(?:(?:all\s+)?in\s+(?:their|your|his|her)\s+head|psycholog\w*|psychosomatic|mental|imagin\w*|anxiety[\s-]*(?:based|caused|driven)|just\s+(?:stress|anxiety|panic))\b",
    "Only need medicine during attacks":
        r"\b(?:only|just)\s+(?:need|use|give|take)\w*\s+(?:\w+\s+){0,2}(?:medicine|medication|inhaler|treatment)\b.{0,40}\b(?:during|when|if)\s+(?:\w+\s+){0,2}(?:attack|flare|episode|symptom)\b"
        r"|\bdon'?t\s+need\b.{0,40}\b(?:daily|every\s*day|controller|maintenance|preventive)\b"
        r"|\b(?:only|just)\s+(?:when|during|if)\s+(?:he|she|they)\s+(?:ha(?:s|ve)|is\s+having)\s+(?:an?\s+)?(?:attack|flare|episode)\b",
}

BELIEF_SOURCES = {
    "Albuterol-only reliance": "NAEPP EPR-3 Stepwise Therapy; GINA 2023 Step 2+",
    "Nebulizer superiority myth": "NAEPP EPR-3; Cochrane Review (MDI+spacer = nebulizer efficacy)",
    "Alternative medicine cures": "NAEPP EPR-3; GINA 2023 (no evidence for CAM)",
    "Steroid growth stunting fear": "CAMP Trial (NEJM 2000); GINA 2023 (0.5-1cm, not progressive)",
    "Outgrow asthma belief": "NAEPP EPR-3; GINA 2023 (remission ≠ cure, may recur)",
    "Inhalers are addictive": "GINA 2023; NAEPP EPR-3 (ICS are not addictive)",
    "Natural remedies are better": "NAEPP EPR-3 (insufficient evidence for CAM)",
    "Steroids are dangerous long-term": "GINA 2023 (ICS benefit-risk ratio favorable)",
    "Asthma is psychological": "NAEPP EPR-3; GINA 2023 (asthma is inflammatory, not psychological)",
    "Only need medicine during attacks": "GINA 2023 Step 2+ (daily controller for persistent asthma)",
}

_COMPILED_BELIEFS = {name: re.compile(pat, re.IGNORECASE) for name, pat in TREATMENT_BELIEFS.items()}

# ---------------------------------------------------------------------------
# Stance detection (concordant/discordant/uncertain/unclear)
# ---------------------------------------------------------------------------

_CONCORDANT_PATTERN = re.compile(
    r'\b(?:myth|not\s+true|false|actually|misconception|incorrect|'
    r'wrong|untrue|no\s+evidence|fact\s+is|in\s+reality|debunk\w*|disproven|'
    r'unfounded|doesn\'t\s+actually|don\'t\s+actually|won\'t\s+actually|'
    r'that\'s\s+(?:not|a\s+myth)|this\s+is\s+(?:not|false|a\s+myth)|'
    r'contrary\s+to|no[,.]?\s+(?:it|they|that)\s+(?:do(?:es)?n\'t|can\'t|won\'t)|'
    r'has\s+(?:been\s+)?(?:debunked|disproven)|there\'s\s+no\s+(?:evidence|proof|link)|'
    r'research\s+shows|studies?\s+show|evidence\s+(?:says|shows|suggests)|'
    r'(?:doctor|pediatrician|pulmonologist)\s+(?:said|told|explained|assured))\b',
    re.IGNORECASE
)

_UNCERTAIN_PATTERN = re.compile(
    r'\?\s*$|'
    r'\b(?:is\s+it\s+true|does\s+(?:it|this|that)|can\s+(?:it|this|that)|'
    r'I\s+heard|someone\s+(?:told|said)|is\s+(?:it|this|that)\s+(?:true|real|possible)|'
    r'worried\s+(?:that|about)|scared\s+(?:that|about)|afraid\s+(?:that|about)|'
    r'should\s+I\s+(?:be\s+)?(?:worried|concerned)|not\s+sure\s+(?:if|whether|about))\b',
    re.IGNORECASE
)


def classify_stance(text: str, match_start: int, match_end: int) -> str:
    """
    Classify the stance of a treatment belief match.
    Returns: 'concordant', 'discordant', 'uncertain', or 'unclear'.
    concordant = aligns with guidelines (debunking the myth)
    discordant = against guidelines (asserting the myth)
    """
    window_start = max(0, match_start - 150)
    window_end = min(len(text), match_end + 150)
    window = text[window_start:window_end]

    sent_start = text.rfind('.', 0, match_start)
    sent_start = 0 if sent_start == -1 else sent_start + 1
    sent_end = text.find('.', match_end)
    sent_end = len(text) if sent_end == -1 else sent_end
    sentence = text[sent_start:sent_end]

    combined = f"{window} {sentence}"

    if _CONCORDANT_PATTERN.search(combined):
        return 'concordant'
    if _UNCERTAIN_PATTERN.search(combined):
        return 'uncertain'

    # Check for discordant signals — assertive language
    discordant_pattern = re.compile(
        r'\b(?:definitely|obviously|trust\s+me|I\s+(?:know|believe|think)|'
        r'causes?|makes?\s+(?:them|you|kids?|children)|will\s+(?:stunt|harm|damage)|'
        r'ruins?\s+(?:their|your)|dangerous|harmful|never\s+(?:give|use)|'
        r'refuse[sd]?\s+to|stopped?\s+(?:giving|using)|we\s+(?:don\'t|stopped|quit))\b',
        re.IGNORECASE
    )
    if discordant_pattern.search(combined):
        return 'discordant'

    return 'unclear'


# ---------------------------------------------------------------------------
# ED/Hospital discourse patterns (4 categories)
# ---------------------------------------------------------------------------

ED_DISCOURSE = {
    "decision_uncertainty":
        r"\bshould\s+(?:I|we)\s+(?:go|take)\s+(?:to\s+)?(?:the\s+)?(?:er|e\.?r\.?|ed|emergency|hospital|urgent\s+care)\b"
        r"|\bwhen\s+(?:to|should)\s+(?:go|take|bring|call)\b.{0,30}\b(?:er|e\.?r\.?|ed|emergency|hospital|911|doctor)\b"
        r"|\b(?:debating|considering|thinking\s+about)\s+(?:going|taking)\s+(?:to\s+)?(?:the\s+)?(?:er|emergency|hospital)\b"
        r"|\b(?:is\s+this|does\s+this)\s+(?:warrant|require|need)\s+(?:an?\s+)?(?:er|emergency|hospital)\s+(?:visit|trip)\b",
    "post_visit":
        r"\b(?:just\s+)?(?:got\s+back|came\s+back|returned|discharged|back\s+from|home\s+from|left)\s+(?:from\s+)?(?:the\s+)?(?:er|e\.?r\.?|ed|emergency|hospital|urgent\s+care)\b"
        r"|\b(?:they|er|ed|doctor|hospital)\s+(?:gave|prescribed|sent\s+(?:us|him|her)\s+home|discharged)\b"
        r"|\b(?:er|emergency|hospital)\s+(?:visit|trip|stay)\b.{0,40}\b(?:they\s+said|told\s+us|prescribed|gave)\b",
    "return_visits":
        r"\b(?:back\s+(?:to|in|at)\s+(?:the\s+)?(?:er|ed|emergency|hospital))\b"
        r"|\b(?:again|another|second|third|4th|5th)\s+(?:er|ed|emergency|hospital)\s+(?:visit|trip)\b"
        r"|\b(?:keep|keeps?|kept)\s+(?:going|ending\s+up|landing)\s+(?:in|at)\s+(?:the\s+)?(?:er|ed|emergency)\b"
        r"|\b(?:frequent\s+flyer|repeat\s+visit|readmi(?:tted|ssion))\b",
    "barriers":
        r"\b(?:can'?t|couldn'?t)\s+afford\b.{0,40}\b(?:er|emergency|hospital|doctor|medication|inhaler|treatment)\b"
        r"|\b(?:no\s+)?insurance\b.{0,40}\b(?:er|emergency|hospital|inhaler|medication)\b"
        r"|\b(?:wait|waited|waiting)\s+(?:for\s+)?(?:hours?|forever|so\s+long)\s+(?:in|at)\s+(?:the\s+)?(?:er|ed|emergency)\b"
        r"|\b(?:cost|expensive|price|afford)\b.{0,40}\b(?:inhaler|albuterol|medication|treatment|nebulizer)\b",
}

_COMPILED_ED = {name: re.compile(pat, re.IGNORECASE) for name, pat in ED_DISCOURSE.items()}

# ---------------------------------------------------------------------------
# Trigger patterns (19 triggers in 3 categories)
# ---------------------------------------------------------------------------

TRIGGERS = {
    # Environmental (8)
    "Mold": r"\bmold\b|\bmould\b|\bmildew\b",
    "Air pollution": r"\bair\s*(?:quality|pollution|index)\b|\baqi\b|\bsmog\b|\bpollut\w*\b",
    "Smoke": r"\bsmoke\b|\bsmoking\b|\bsecond[\s-]*hand\s*smoke\b|\bwild\s*fire\s*smoke\b|\bcigarette\b|\bvape\b|\bvaping\b",
    "Pets": r"\b(?:pet|cat|dog|animal)\s+(?:allerg|dander)\w*\b|\bdander\b|\bfur\s+(?:allerg|trigger)\w*\b",
    "Pollen/seasonal": r"\bpollen\b|\ballerg(?:y|ies)\s+season\b|\bseasonal\s+allerg\w*\b|\bhay\s*fever\b|\bspring\s+allerg\w*\b|\bfall\s+allerg\w*\b|\bragweed\b|\btree\s+pollen\b",
    "Dust mites": r"\bdust\s*mite[s]?\b|\bdust\s*(?:allerg|trigger)\w*\b|\bdusty\b",
    "Weather changes": r"\bweather\s+(?:change|shift|front)\w*\b|\bbarometric\b|\bhumid(?:ity)?\b|\bstorm\b",
    "Cold air": r"\bcold\s+(?:air|weather|wind|outside|temperature)\b|\bfreezing\s+(?:air|cold)\b",
    # Viral (6)
    "RSV": r"\brsv\b|\brespiratory\s+syncytial\b",
    "Common cold": r"\bcold\b(?=.{0,20}\b(?:trigger|flare|attack|worse|asthma|cough))\b|\bcaught\s+(?:a\s+)?cold\b|\bhead\s*cold\b|\brunny\s+nose\b.{0,30}\basthma\b",
    "Flu": r"\bflu\b|\binfluenza\b",
    "COVID": r"\bcovid\b|\bcovid[\s-]*19\b|\bcoronavirus\b|\bsars[\s-]*cov\b",
    "Respiratory infection": r"\brespiratory\s+infection\b|\bupper\s+respiratory\b|\blower\s+respiratory\b|\buri\b|\blrti\b|\bbronchit\w*\b|\bpneumonia\b",
    "Croup": r"\bcroup\b|\bbarking\s+cough\b",
    # Non-evidence-based (5)
    "Vaccines": r"\bvaccin\w*\b.{0,40}\b(?:cause[sd]?\s+asthma|trigger\w*\s+asthma|gave\s+(?:them\s+)?asthma|asthma\s+(?:from|after|since))\b|\basthma\b.{0,40}\b(?:from|caused?\s+by|after|since)\s+(?:the\s+)?vaccin\w*\b",
    "Diet/toxins": r"\b(?:diet|food|sugar|dairy|gluten|preservative|additive|processed|toxin|detox)\b.{0,40}\b(?:cause[sd]?\s+asthma|trigger\w*\s+asthma|asthma\s+(?:trigger|cause|cure))\b",
    "Chemicals": r"\bchemical[s]?\b.{0,40}\b(?:cause[sd]?\s+asthma|trigger\w*\s+asthma)\b|\b(?:cleaning\s+product|household\s+chemical)\w*\b.{0,30}\basthma\b",
    "Mold toxicity": r"\bmold\s+toxic\w*\b|\btoxic\s+mold\b|\bmycotoxin\w*\b|\bblack\s+mold\b",
    "EMF": r"\bemf\b.{0,30}\basthma\b|\belectromagnetic\b.{0,30}\basthma\b|\b5g\b.{0,30}\basthma\b",
}

TRIGGER_CATEGORIES = {
    "Mold": "environmental", "Air pollution": "environmental", "Smoke": "environmental",
    "Pets": "environmental", "Pollen/seasonal": "environmental", "Dust mites": "environmental",
    "Weather changes": "environmental", "Cold air": "environmental",
    "RSV": "viral", "Common cold": "viral", "Flu": "viral", "COVID": "viral",
    "Respiratory infection": "viral", "Croup": "viral",
    "Vaccines": "non_evidence_based", "Diet/toxins": "non_evidence_based",
    "Chemicals": "non_evidence_based", "Mold toxicity": "non_evidence_based",
    "EMF": "non_evidence_based",
}

_COMPILED_TRIGGERS = {name: re.compile(pat, re.IGNORECASE) for name, pat in TRIGGERS.items()}

# ---------------------------------------------------------------------------
# Caregiver emotional state patterns (5 categories)
# ---------------------------------------------------------------------------

CAREGIVER_SENTIMENT = {
    "trust":
        r"\b(?:trust|faith|confidence)\b.{0,40}\b(?:doctor|pediatrician|pulmonologist|provider|specialist|hospital)\b"
        r"|\b(?:doctor|pediatrician|provider)\b.{0,40}\b(?:great|wonderful|amazing|helpful|trust|listens?|understands?)\b"
        r"|\b(?:finally\s+)?found\s+a\s+(?:good|great)\s+(?:doctor|pediatrician|specialist)\b",
    "frustration":
        r"\b(?:frustrat\w*|anger|angry|furious|fed\s+up|sick\s+(?:of|and\s+tired))\b.{0,40}\b(?:doctor|pediatrician|provider|hospital|system|insurance|er|specialist)\b"
        r"|\b(?:doctor|pediatrician|provider|er)\b.{0,40}\b(?:frustrat\w*|useless|incompetent|terrible|awful|horrible|worst)\b"
        r"|\bno\s+one\s+(?:listens?|cares?|helps?)\b",
    "dismissed":
        r"\b(?:dismiss\w*|brush\w*\s+off|ignore[sd]?|not\s+taken?\s+seriously?|blown?\s+off|gaslight\w*)\b.{0,40}\b(?:doctor|pediatrician|provider|er|hospital)\b"
        r"|\b(?:doctor|pediatrician|provider)\b.{0,40}\b(?:dismiss\w*|brush\w*\s+off|ignore[sd]?|didn'?t\s+(?:listen|care|take\s+(?:it|us)\s+seriously))\b"
        r"|\bjust\s+(?:a\s+)?(?:cold|virus|cough)\b.{0,30}\b(?:they\s+said|told\s+(?:us|me))\b",
    "anxiety":
        r"\b(?:scared|terrified|petrified|panick\w*|afraid|fear(?:ful)?|anxious|anxiety|dread|worried\s+sick)\b.{0,40}\b(?:asthma|attack|breathing|inhaler|er|emergency|hospital|can'?t\s+breathe|911)\b"
        r"|\bcan'?t\s+(?:breathe|stop\s+worry|sleep\s+(?:at\s+night|because))\b"
        r"|\b(?:worst|scariest)\s+(?:night|moment|experience|thing)\b.{0,30}\b(?:asthma|breathing|hospital)\b"
        r"|\bPTSD\b|\btrauma(?:tiz|tic)\w*\b.{0,30}\b(?:asthma|hospital|er)\b",
    "empowerment":
        r"\b(?:finally|learned|figured\s+out|understand|know\s+(?:how|what|when))\b.{0,40}\b(?:manage|control|handle|treat|help|action\s+plan)\b"
        r"|\basthma\s+action\s+plan\b|\bunder\s+control\b|\bwell[\s-]*controlled\b"
        r"|\b(?:advocate|advocating|fought\s+for|pushed\s+for|demanded)\b.{0,30}\b(?:my\s+(?:child|kid|son|daughter)|referral|specialist|treatment)\b"
        r"|\b(?:game[\s-]*changer|life[\s-]*saver|turning\s+point)\b",
}

_COMPILED_CAREGIVER = {name: re.compile(pat, re.IGNORECASE) for name, pat in CAREGIVER_SENTIMENT.items()}

# ---------------------------------------------------------------------------
# Singulair/Montelukast deep dive
# ---------------------------------------------------------------------------

SINGULAIR_EFFECTS = {
    "nightmares": r"\bnightmare[s]?\b|\bnight\s*terror[s]?\b|\bbad\s+dream[s]?\b|\bvivid\s+dream[s]?\b|\bscream(?:ing)?\s+(?:at|in)\s+(?:night|sleep)\b",
    "aggression": r"\baggress(?:ion|ive|iveness)\b|\bviolent\b|\brage\b|\banger\b|\bhitting\b|\bbiting\b|\blash(?:ing)?\s+out\b",
    "mood changes": r"\bmood\s+(?:swing|change|shift)\w*\b|\bemotional(?:ly\s+(?:unstable|volatile))?\b",
    "suicidal ideation": r"\bsuicid\w*\b|\bself[\s-]*harm\b|\bkill\s+(?:my|him|her)self\b|\bwant(?:ed|ing|s)?\s+to\s+die\b|\bend\s+(?:it|their\s+life)\b",
    "sleep disturbances": r"\bsleep\s+(?:issue|problem|disturb|difficult|trouble)\w*\b|\binsomnia\b|\bcan'?t\s+sleep\b|\bwake[s]?\s+up\s+(?:crying|screaming|multiple)\b",
    "anxiety": r"\banxi(?:ety|ous)\b|\bpanic\b|\bnervous(?:ness)?\b|\bworr(?:y|ied|ying)\b.{0,20}\b(?:since|after|start\w*)\b.{0,20}\bsingulair\b",
    "depression": r"\bdepress(?:ed|ion|ing)?\b|\bsad(?:ness)?\b|\bwithdr(?:awn|ew|awal)\b|\bno\s+(?:interest|motivation|joy)\b",
    "personality changes": r"\bpersonality\s+change\w*\b|\bdifferent\s+(?:child|kid|person)\b|\bnot\s+(?:the\s+same|my|himself|herself)\b|\bcompletely\s+changed\b",
    "hyperactivity": r"\bhyperactiv\w*\b|\bhyper\b|\bcan'?t\s+(?:sit\s+)?still\b|\bwired\b|\bbouncing\s+off\b|\bADHD[\s-]*like\b",
}

SINGULAIR_DISCOURSE = {
    "black_box_warning": r"\bblack\s*box\b|\bboxed\s*warning\b|\bfda\s+warning\b|\bfda\s+(?:black|boxed)\b",
    "starting_decision": r"\b(?:start|starting|prescrib\w*|put\s+(?:on|him|her))\b.{0,30}\b(?:singulair|montelukast|singular)\b"
        r"|\b(?:singulair|montelukast)\b.{0,30}\b(?:start|prescrib\w*|try|worth\s+(?:it|trying))\b",
    "stopping_decision": r"\b(?:stop\w*|quit|discontinu\w*|took\s+(?:off|him|her)\s+off|wean\w*)\b.{0,30}\b(?:singulair|montelukast|singular)\b"
        r"|\b(?:singulair|montelukast)\b.{0,30}\b(?:stop\w*|quit|discontinu\w*|off\s+(?:of\s+)?it)\b",
    "seeking_alternatives": r"\b(?:alternative|instead\s+of|replace|substitut)\w*\b.{0,30}\b(?:singulair|montelukast)\b"
        r"|\b(?:singulair|montelukast)\b.{0,30}\b(?:alternative|instead|replace|substitut|what\s+else)\w*\b",
}

_COMPILED_SINGULAIR_EFFECTS = {name: re.compile(pat, re.IGNORECASE) for name, pat in SINGULAIR_EFFECTS.items()}
_COMPILED_SINGULAIR_DISCOURSE = {name: re.compile(pat, re.IGNORECASE) for name, pat in SINGULAIR_DISCOURSE.items()}

# ---------------------------------------------------------------------------
# Corticosteroid side effects (strict proximity: effect near steroid mention)
# ---------------------------------------------------------------------------

_CORTICOSTEROID_PATTERN = re.compile(
    r'\b(?:steroid|corticosteroid|prednisone|prednisolone|dexamethasone|decadron|budesonide|fluticasone|'
    r'pulmicort|flovent|qvar|alvesco|asmanex|arnuity|beclomethasone|oral\s+steroid)\b',
    re.IGNORECASE
)

CORTICOSTEROID_EFFECTS = {
    "roid_rage":
        r"\broid\s*rage\b"
        r"|\b(?:aggress\w*|rage|violent|anger|angry)\b.{0,40}\b(?:steroid|prednisone|prednisolone|dex\w*|corticosteroid|pred)\b"
        r"|\b(?:steroid|prednisone|prednisolone|dex\w*|pred)\b.{0,40}\b(?:aggress\w*|rage|violent|anger|angry)\b",
    "mood_swings":
        r"\b(?:mood\s+(?:swing|change|shift)\w*|irritab\w*|emotional(?:ly)?)\b.{0,40}\b(?:steroid|pred\w*|dex\w*|corticosteroid|budesonide|fluticasone)\b"
        r"|\b(?:steroid|pred\w*|dex\w*|corticosteroid)\b.{0,40}\b(?:mood\s+(?:swing|change|shift)|irritab\w*)\b",
    "sleep_disturbances":
        r"\b(?:sleep\s+(?:issue|problem|disturb)\w*|insomnia|can'?t\s+sleep|up\s+all\s+night|wired)\b.{0,40}\b(?:steroid|pred\w*|dex\w*|budesonide|fluticasone|corticosteroid)\b"
        r"|\b(?:steroid|pred\w*|dex\w*|corticosteroid)\b.{0,40}\b(?:sleep\s+(?:issue|problem)|insomnia|can'?t\s+sleep|wired)\b",
    "appetite_weight":
        r"\b(?:appetite\s+increas\w*|weight\s+gain|gained\s+weight|always\s+hungry|eating\s+everything|ravenous)\b.{0,40}\b(?:steroid|pred\w*|dex\w*|corticosteroid)\b"
        r"|\b(?:steroid|pred\w*|dex\w*|corticosteroid)\b.{0,40}\b(?:appetite|weight\s+gain|hungry|eating\s+everything)\b",
    "glucose_issues":
        r"\b(?:blood\s+sugar|glucose|hyperglycemi\w*|sugar\s+levels?|diabetes)\b.{0,40}\b(?:steroid|pred\w*|dex\w*|corticosteroid)\b"
        r"|\b(?:steroid|pred\w*|dex\w*|corticosteroid)\b.{0,40}\b(?:blood\s+sugar|glucose|hyperglycemi\w*|sugar\s+level)\b",
    "growth_concerns":
        r"\b(?:growth\s+(?:stunt|slow|delay|suppress|concern|affect)\w*|short\s+stature|height\s+(?:concern|issue|affect)\w*|not\s+growing)\b.{0,40}\b(?:steroid|ics|inhaler|flovent|pulmicort|budesonide|fluticasone|corticosteroid)\b"
        r"|\b(?:steroid|ics|flovent|pulmicort|budesonide|fluticasone)\b.{0,40}\b(?:growth\s+(?:stunt|slow|delay|suppress|concern)|short\s+stature|height\s+concern|not\s+growing)\b",
    "adrenal_suppression":
        r"\b(?:adrenal\s+(?:suppress\w*|insufficien\w*|crisis|fatigue)|cortisol\s+(?:low|suppress|level))\b.{0,40}\b(?:steroid|pred\w*|flovent|pulmicort|budesonide|corticosteroid)\b"
        r"|\b(?:steroid|pred\w*|corticosteroid)\b.{0,40}\b(?:adrenal\s+(?:suppress|insufficien|crisis)|cortisol)\b",
    "bone_density":
        r"\b(?:bone\s+(?:densit|loss|thin)\w*|osteoporo\w*)\b.{0,40}\b(?:steroid|pred\w*|ics|corticosteroid|long[\s-]*term)\b"
        r"|\b(?:steroid|pred\w*|corticosteroid)\b.{0,40}\b(?:bone\s+(?:densit|loss|thin)|osteoporo)\b",
}

_COMPILED_CORTICOSTEROID_EFFECTS = {name: re.compile(pat, re.IGNORECASE) for name, pat in CORTICOSTEROID_EFFECTS.items()}

# ---------------------------------------------------------------------------
# Functional impact (missed school/work/activities)
# ---------------------------------------------------------------------------

FUNCTIONAL_IMPACT = {
    "missed_school":
        r"\bmissed\s+(?:school|class(?:es)?)\b|\bcan'?t\s+go\s+to\s+school\b|\babsent\s+from\s+school\b"
        r"|\bhome\s+from\s+school\b|\bschool\s+absence\b|\bstay(?:ed|ing)?\s+home\s+from\s+school\b"
        r"|\bout\s+of\s+school\b|\bmissing\s+school\b|\bdays?\s+(?:off|out)\s+(?:of\s+)?school\b",
    "missed_work":
        r"\bmissed\s+work\b|\bhad\s+to\s+call\s+(?:off|in|out)\b|\bcan'?t\s+(?:go\s+to\s+)?work\b"
        r"|\bstay(?:ed|ing)?\s+home\s+from\s+work\b|\bfmla\b|\btook\s+(?:off|time\s+off)\s+(?:from\s+)?work\b"
        r"|\bcall(?:ed|ing)?\s+(?:out|off)\s+(?:of\s+)?work\b|\bmissing\s+work\b",
    "activity_limitation":
        r"\bcan'?t\s+(?:play|run|exercise|participate)\b|\bsitting\s+out\b|\blimited\s+activit\w*\b"
        r"|\bno\s+(?:sports|running|exercise)\b|\bmissed\s+(?:practice|game|recital|recess)\b"
        r"|\bcan'?t\s+exercise\b|\bstopped?\s+playing\b|\bcan'?t\s+keep\s+up\b"
        r"|\bsit\s+out\b|\brestrict(?:ed|ing)?\s+activit\w*\b",
    "sports_impact":
        r"\bquit\s+(?:the\s+)?(?:team|sport)\b|\bcan'?t\s+play\s+(?:soccer|baseball|basketball|football|hockey|lacrosse)\b"
        r"|\bsideline[sd]?\b|\bbenched\b|\bcan'?t\s+(?:do\s+)?(?:pe|gym\s+class|recess)\b"
        r"|\bpulled\s+(?:from|off)\s+(?:the\s+)?(?:team|field|game)\b",
}

_COMPILED_FUNCTIONAL_IMPACT = {name: re.compile(pat, re.IGNORECASE) for name, pat in FUNCTIONAL_IMPACT.items()}

# ---------------------------------------------------------------------------
# Inhaler confusion / technique issues
# ---------------------------------------------------------------------------

INHALER_CONFUSION = {
    "type_confusion":
        r"\bwhich\s+inhaler\b|\bwrong\s+inhaler\b|\bconfused\s+about\s+(?:the\s+)?inhaler\b"
        r"|\bdon'?t\s+know\s+which\s+(?:inhaler|one)\b|\bcontroller\s+vs\.?\s+rescue\b"
        r"|\bdifference\s+between\b.{0,20}\b(?:inhaler|controller|rescue|preventer|reliever)\b"
        r"|\bwhich\s+(?:one\s+)?(?:is\s+)?(?:the\s+)?(?:rescue|controller|daily|maintenance)\b.{0,15}\b(?:inhaler|one)\b",
    "technique_issues":
        r"\busing\s+(?:it|the\s+inhaler)\s+wrong\b|\bnot\s+using\s+(?:it\s+)?correctly\b"
        r"|\binhaler\s+technique\b|\bspacer\s+technique\b|\bhow\s+to\s+use\s+(?:the\s+)?(?:inhaler|spacer|neb)\b"
        r"|\bnot\s+working\s+right\b|\bbad\s+technique\b|\bproper\s+technique\b"
        r"|\bcan'?t\s+(?:get\s+)?(?:him|her|them)\s+to\s+use\s+(?:the\s+)?(?:inhaler|spacer)\b",
    "timing_confusion":
        r"\bwhen\s+to\s+use\b.{0,20}\b(?:inhaler|rescue|controller|albuterol)\b"
        r"|\bhow\s+often\b.{0,15}\b(?:inhaler|albuterol|rescue|nebulizer)\b"
        r"|\bbefore\s+or\s+after\b.{0,15}\b(?:inhaler|exercise|sports)\b"
        r"|\bas\s+needed\s+vs\.?\s+daily\b|\bprn\b.{0,20}\b(?:vs\.?|versus|or)\b.{0,10}\b(?:daily|scheduled)\b"
        r"|\bscheduled\s+vs\.?\s+(?:rescue|as\s+needed)\b",
    "device_confusion":
        r"\bmdi\s+vs\.?\s+(?:nebulizer|neb)\b|\bdry\s+powder\b.{0,15}\b(?:vs\.?|or|confused)\b"
        r"|\bwhich\s+device\b|\bhow\s+to\s+use\s+(?:the\s+)?spacer\b"
        r"|\bdon'?t\s+know\s+how\s+to\s+use\b.{0,15}\b(?:inhaler|spacer|neb|nebulizer)\b"
        r"|\b(?:metered\s+dose|dry\s+powder|diskus|respiclick)\b.{0,20}\b(?:confused|don'?t\s+understand|which)\b",
}

_COMPILED_INHALER_CONFUSION = {name: re.compile(pat, re.IGNORECASE) for name, pat in INHALER_CONFUSION.items()}

# ---------------------------------------------------------------------------
# Post-visit subcategories (for ED discourse post_visit)
# ---------------------------------------------------------------------------

POST_VISIT_SUBCATEGORIES = {
    "prescribed_new_med":
        r"\b(?:they|er|ed|doctor|hospital)\s+(?:prescribed|gave\s+(?:us|him|her))\s+(?:\w+\s+){0,3}(?:steroid|prednisone|prednisolone|albuterol|inhaler|nebulizer|medication|antibiotic)\b"
        r"|\bstarted?\s+(?:on|him|her)\s+(?:on\s+)?(?:a\s+)?(?:new\s+)?(?:medication|inhaler|steroid|controller)\b",
    "diagnosis_given":
        r"\b(?:diagnosed\s+(?:with|as)|told\s+(?:us|me)\s+(?:it\s+was|(?:he|she)\s+has))\b.{0,30}\b(?:asthma|reactive\s+airway|bronchitis|croup|pneumonia|rsv|rav)\b"
        r"|\bfirst\s+(?:asthma\s+)?diagnosis\b|\bfinally\s+diagnosed\b",
    "discharge_instructions":
        r"\bsent?\s+(?:us|him|her)\s+home\s+with\b|\basthma\s+action\s+plan\b"
        r"|\btold\s+(?:us|me)\s+to\s+follow\s+up\b|\bdischarge\s+instructions?\b"
        r"|\bfollow[\s-]*up\s+(?:with|in|appointment)\b",
    "satisfaction":
        r"\b(?:good|bad|great|terrible|amazing|awful|wonderful|horrible|helpful|unhelpful)\s+(?:experience|staff|doctor|nurse|visit)\b"
        r"|\b(?:the\s+)?(?:doctor|nurse|staff)\s+(?:was|were)\s+(?:great|terrible|helpful|rude|amazing|awful|wonderful|kind)\b",
    "still_worried":
        r"\bstill\s+(?:concerned|worried|scared|anxious)\b.{0,20}\b(?:despite|after|even\s+after)\b"
        r"|\bsymptoms?\s+(?:persist|continu|came\s+back|still|haven'?t\s+improved)\b.{0,20}\b(?:after|since)\s+(?:the\s+)?(?:er|ed|visit|hospital)\b"
        r"|\bstill\s+(?:wheezing|coughing|struggling)\s+(?:after|since)\b",
}

_COMPILED_POST_VISIT_SUBCATEGORIES = {name: re.compile(pat, re.IGNORECASE) for name, pat in POST_VISIT_SUBCATEGORIES.items()}

# ---------------------------------------------------------------------------
# Post-ED outcome sentiment
# ---------------------------------------------------------------------------

POST_ED_OUTCOME = {
    "improvement":
        r"\b(?:better|improved|improving)\s+(?:after|since|following)\b.{0,30}\b(?:er|ed|emergency|hospital|visit|discharge)\b"
        r"|\b(?:er|ed|emergency|hospital)\b.{0,30}\b(?:helped|worked|resolved|better|improved)\b"
        r"|\bbreathing\s+(?:much\s+)?better\b.{0,20}\b(?:after|since|now)\b"
        r"|\bback\s+to\s+normal\b|\bcleared?\s+up\b|\bfeeling\s+better\s+(?:since|after|now)\b",
    "no_improvement":
        r"\bstill\s+struggling\b.{0,20}\b(?:after|since)\s+(?:the\s+)?(?:er|ed|visit|hospital)\b"
        r"|\bdidn'?t\s+help\b.{0,20}\b(?:er|ed|visit|hospital|trip)\b"
        r"|\bgot\s+worse\b.{0,20}\b(?:after|since)\s+(?:the\s+)?(?:er|ed|visit)\b"
        r"|\bback\s+(?:to|in|at)\s+(?:the\s+)?(?:er|ed)\s+(?:again|already|next\s+day)\b"
        r"|\bno\s+improvement\b|\bnot\s+(?:any\s+)?better\b.{0,20}\b(?:after|since)\b",
    "temporary_relief":
        r"\bbetter\s+for\s+(?:a\s+)?(?:while|few\s+(?:hours?|days?))\b"
        r"|\b(?:symptoms?|wheezing|cough)\s+came\s+back\b"
        r"|\bwore\s+off\b.{0,20}\b(?:after|hours?|days?)\b"
        r"|\btemporary\b.{0,15}\b(?:relief|better|improvement)\b"
        r"|\bonly\s+lasted\b|\b(?:symptoms?\s+)?return(?:ed|ing)\b.{0,20}\b(?:after|hours?|days?|next)\b",
}

_COMPILED_POST_ED_OUTCOME = {name: re.compile(pat, re.IGNORECASE) for name, pat in POST_ED_OUTCOME.items()}

# ---------------------------------------------------------------------------
# Sentiment analysis (standard + fear dimension)
# ---------------------------------------------------------------------------

_POSITIVE_WORDS = {
    "love", "loved", "loving", "great", "amazing", "wonderful", "fantastic",
    "happy", "happier", "recommend", "recommended", "perfect", "relief",
    "comfortable", "easy", "easier", "helped", "helping", "helpful", "works", "worked",
    "effective", "glad", "satisfied", "awesome", "excellent", "best", "better",
    "worth", "grateful", "thankful", "thrilled", "pleased", "enjoy", "enjoying",
    "improvement", "improved", "freedom", "convenient", "reliable", "safe",
    "success", "successful", "smooth", "positive", "hopeful", "reassuring",
    "welcome", "nice", "fine", "support", "supportive", "hope", "calm",
    "reassured", "trust", "confident", "normal", "healthy", "controlled",
    "manageable", "stable", "responded", "breakthrough",
}

_NEGATIVE_WORDS = {
    "hate", "hated", "hating", "terrible", "awful", "horrible", "worst",
    "pain", "painful", "suffer", "suffered", "suffering", "miserable",
    "nightmare", "regret", "regretted", "angry", "frustrated", "frustrating",
    "unbearable", "ruined", "scared", "scary", "fear", "worried", "worry",
    "worrying", "concerned", "bad", "worse", "sucks", "sucked", "annoying",
    "annoyed", "disappointing", "disappointed", "uncomfortable", "difficult",
    "hard", "struggle", "struggling", "failed", "failure", "problem",
    "problems", "issue", "issues", "wrong", "severe", "seriously", "misery",
    "cry", "crying", "cried", "upset", "distressed", "hurt", "hurts",
    "wtf", "unfair", "ridiculous", "gross", "mad", "ugh", "crazy",
    "stupid", "disgusting", "horrifying", "dreadful", "desperate",
    "dangerous", "toxic", "terrifying", "exhausting", "helpless",
}

_FEAR_WORDS = {
    "scared", "terrified", "petrified", "panicked", "panicking", "panic",
    "afraid", "fear", "fearful", "frightened", "frightening", "dread",
    "dreading", "horrified", "horrifying", "alarming", "alarmed",
    "emergency", "911", "ambulance", "gasping", "suffocating",
    "can't breathe", "couldn't breathe", "struggling to breathe",
    "blue", "turning blue", "lips blue", "retracting", "retractions",
    "oxygen", "intubat", "icu", "life threatening", "life-threatening",
    "dying", "die", "death", "hospital", "er", "ptsd", "trauma",
    "worst night", "scariest", "helpless", "powerless",
}

_INTENSIFIERS = {"very", "really", "extremely", "so", "incredibly", "super", "absolutely", "totally"}
_NEGATORS = {"not", "no", "never", "don't", "didn't", "doesn't", "wasn't", "weren't",
             "isn't", "aren't", "won't", "can't", "couldn't", "shouldn't", "hardly", "barely"}
_NEGATION_WINDOW = 3


def score_sentiment(text: str) -> Optional[int]:
    """Score text sentiment on a 1-5 Likert scale.
    Returns None if no sentiment words found.
    Scale: 1=Very Negative, 2=Negative, 3=Neutral, 4=Positive, 5=Very Positive.
    Internally computes -1.0 to +1.0 then maps: round(raw * 2 + 3).
    """
    if not text:
        return None
    words = re.findall(r"[a-z']+", text.lower())
    if not words:
        return None

    pos_score = 0.0
    neg_score = 0.0
    negate_remaining = 0
    intensify = 1.0
    word_count = 0

    for word in words:
        if word in _NEGATORS:
            negate_remaining = _NEGATION_WINDOW
            continue
        if word in _INTENSIFIERS:
            intensify = 1.5
            continue
        if word in _POSITIVE_WORDS:
            word_count += 1
            if negate_remaining > 0:
                neg_score += intensify
            else:
                pos_score += intensify
            negate_remaining = 0
            intensify = 1.0
        elif word in _NEGATIVE_WORDS:
            word_count += 1
            if negate_remaining > 0:
                pos_score += intensify
            else:
                neg_score += intensify
            negate_remaining = 0
            intensify = 1.0
        else:
            if negate_remaining > 0:
                negate_remaining -= 1
            else:
                intensify = 1.0

    total = pos_score + neg_score
    if total == 0:
        return None
    raw = (pos_score - neg_score) / total
    confidence = min(1.0, 1.0 - 1.0 / (1.0 + word_count * 0.7))
    dampened = raw * confidence
    clamped = max(-1.0, min(1.0, dampened))
    return round(clamped * 2 + 3)  # 1-5 Likert: -1→1, 0→3, 1→5


def _sentiment_bucket(score: int) -> str:
    """Map a 1-5 Likert score to a named bucket for Cohen's kappa."""
    if score is None:
        return 'Neutral'
    if score <= 1:
        return 'Very Negative'
    if score == 2:
        return 'Negative'
    if score == 3:
        return 'Neutral'
    if score == 4:
        return 'Positive'
    return 'Very Positive'


def score_fear(text: str) -> Optional[float]:
    """Score fear/anxiety from 0.0 to 1.0. Returns None if no fear words found."""
    if not text:
        return None
    t = text.lower()
    fear_count = 0
    for fw in _FEAR_WORDS:
        if fw in t:
            fear_count += 1
    if fear_count == 0:
        return None
    # Scale: 1 word ~ 0.2, 2 ~ 0.35, 3 ~ 0.5, 5+ ~ 0.7+, 8+ ~ 0.85+
    raw = 1.0 - 1.0 / (1.0 + fear_count * 0.4)
    return max(0.0, min(1.0, round(raw, 4)))


# ---------------------------------------------------------------------------
# Analysis helpers (find_* functions)
# ---------------------------------------------------------------------------

def find_medications(text: str) -> list:
    """Return list of medication names found in text."""
    return [name for name, pat in _COMPILED_MEDS.items() if pat.search(text)]


def find_side_effects(text: str) -> list:
    """Return list of side effect names found in text."""
    return [name for name, pat in _COMPILED_EFFECTS.items() if pat.search(text)]


def find_treatment_beliefs(text: str) -> list:
    """Return list of treatment belief names found in text."""
    return [name for name, pat in _COMPILED_BELIEFS.items() if pat.search(text)]


def find_treatment_beliefs_with_stance(text: str) -> list:
    """Return [(belief_name, stance)] with stance classification."""
    results = []
    for name, pat in _COMPILED_BELIEFS.items():
        m = pat.search(text)
        if m:
            stance = classify_stance(text, m.start(), m.end())
            results.append((name, stance))
    return results


def find_ed_discourse(text: str) -> list:
    """Return list of ED discourse categories found in text."""
    return [name for name, pat in _COMPILED_ED.items() if pat.search(text)]


def find_triggers(text: str) -> list:
    """Return list of trigger names found in text."""
    return [name for name, pat in _COMPILED_TRIGGERS.items() if pat.search(text)]


def find_caregiver_sentiment(text: str) -> list:
    """Return list of caregiver sentiment categories found in text."""
    return [name for name, pat in _COMPILED_CAREGIVER.items() if pat.search(text)]


def find_singulair_effects(text: str) -> list:
    """Return list of Singulair behavioral effect names found in text."""
    return [name for name, pat in _COMPILED_SINGULAIR_EFFECTS.items() if pat.search(text)]


def find_singulair_discourse(text: str) -> list:
    """Return list of Singulair discourse categories found in text."""
    return [name for name, pat in _COMPILED_SINGULAIR_DISCOURSE.items() if pat.search(text)]


def find_corticosteroid_effects(text: str) -> list:
    """Return list of corticosteroid side effects found in text."""
    return [name for name, pat in _COMPILED_CORTICOSTEROID_EFFECTS.items() if pat.search(text)]


def find_functional_impact(text: str) -> list:
    """Return list of functional impact categories found in text."""
    return [name for name, pat in _COMPILED_FUNCTIONAL_IMPACT.items() if pat.search(text)]


def find_inhaler_confusion(text: str) -> list:
    """Return list of inhaler confusion categories found in text."""
    return [name for name, pat in _COMPILED_INHALER_CONFUSION.items() if pat.search(text)]


def find_post_visit_subcategories(text: str) -> list:
    """Return list of post-visit subcategories found in text."""
    return [name for name, pat in _COMPILED_POST_VISIT_SUBCATEGORIES.items() if pat.search(text)]


def find_post_ed_outcome(text: str) -> list:
    """Return list of post-ED outcomes found in text."""
    return [name for name, pat in _COMPILED_POST_ED_OUTCOME.items() if pat.search(text)]


def compute_engagement(score: int, num_comments: int) -> float:
    """Engagement score: weights discussion (comments) higher than upvotes."""
    return math.log2(max(score, 1)) + math.log2(max(num_comments, 1)) * 1.5


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_db(path: Optional[Path] = None) -> sqlite3.Connection:
    """Open (and initialize if needed) the SQLite database."""
    p = path or DB_FILE
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            title TEXT,
            selftext TEXT,
            created_utc REAL,
            score INTEGER,
            num_comments INTEGER,
            permalink TEXT,
            first_seen TEXT NOT NULL,
            sentiment REAL,
            fear_score REAL,
            comments_scraped INTEGER DEFAULT 0,
            subreddit TEXT,
            engagement_score REAL DEFAULT 0,
            sort_source TEXT DEFAULT 'new',
            crosspost_parent TEXT,
            pediatric_confidence TEXT DEFAULT 'none',
            child_age_mentioned TEXT
        );
        CREATE TABLE IF NOT EXISTS comments (
            id TEXT PRIMARY KEY,
            post_id TEXT REFERENCES posts(id),
            body TEXT,
            score INTEGER,
            created_utc REAL,
            author TEXT,
            sentiment REAL,
            fear_score REAL,
            first_seen TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS medication_mentions (
            post_id TEXT REFERENCES posts(id),
            medication TEXT NOT NULL,
            med_class TEXT,
            PRIMARY KEY (post_id, medication)
        );
        CREATE TABLE IF NOT EXISTS side_effects (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            effect TEXT NOT NULL,
            PRIMARY KEY (source_type, source_id, effect)
        );
        CREATE TABLE IF NOT EXISTS treatment_beliefs (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            belief TEXT NOT NULL,
            stance TEXT DEFAULT 'unclear',
            PRIMARY KEY (source_type, source_id, belief)
        );
        CREATE TABLE IF NOT EXISTS ed_discourse (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            category TEXT NOT NULL,
            PRIMARY KEY (source_type, source_id, category)
        );
        CREATE TABLE IF NOT EXISTS triggers (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            trigger_name TEXT NOT NULL,
            trigger_category TEXT,
            PRIMARY KEY (source_type, source_id, trigger_name)
        );
        CREATE TABLE IF NOT EXISTS caregiver_sentiment (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            category TEXT NOT NULL,
            PRIMARY KEY (source_type, source_id, category)
        );
        CREATE TABLE IF NOT EXISTS singulair_effects (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            effect TEXT NOT NULL,
            PRIMARY KEY (source_type, source_id, effect)
        );
        CREATE TABLE IF NOT EXISTS corticosteroid_effects (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            effect TEXT NOT NULL,
            PRIMARY KEY (source_type, source_id, effect)
        );
        CREATE TABLE IF NOT EXISTS functional_impact (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            category TEXT NOT NULL,
            PRIMARY KEY (source_type, source_id, category)
        );
        CREATE TABLE IF NOT EXISTS inhaler_confusion (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            category TEXT NOT NULL,
            PRIMARY KEY (source_type, source_id, category)
        );
        CREATE TABLE IF NOT EXISTS ed_subcategories (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            subcategory TEXT NOT NULL,
            PRIMARY KEY (source_type, source_id, subcategory)
        );
        CREATE TABLE IF NOT EXISTS post_ed_outcome (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            outcome TEXT NOT NULL,
            PRIMARY KEY (source_type, source_id, outcome)
        );
        CREATE TABLE IF NOT EXISTS scrape_runs (
            id INTEGER PRIMARY KEY,
            scraped_at TEXT NOT NULL,
            post_count INTEGER NOT NULL,
            subreddit TEXT,
            error_count INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS scrape_errors (
            id INTEGER PRIMARY KEY,
            timestamp TEXT NOT NULL,
            subreddit TEXT,
            error_type TEXT NOT NULL,
            message TEXT,
            source_id TEXT,
            source_type TEXT
        );
        CREATE TABLE IF NOT EXISTS validation_votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id TEXT NOT NULL REFERENCES posts(id),
            validator TEXT NOT NULL,
            system_flagged INTEGER NOT NULL,
            human_flagged INTEGER NOT NULL,
            human_stance TEXT,
            reason TEXT,
            system_claims TEXT,
            voted_at TEXT NOT NULL,
            UNIQUE(post_id, validator)
        );
        CREATE TABLE IF NOT EXISTS side_effect_votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            comment_id TEXT NOT NULL REFERENCES comments(id),
            validator TEXT NOT NULL,
            system_effects TEXT,
            human_effects TEXT,
            other_effect TEXT,
            voted_at TEXT NOT NULL,
            UNIQUE(comment_id, validator)
        );
        CREATE TABLE IF NOT EXISTS sentiment_votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            comment_id TEXT NOT NULL REFERENCES comments(id),
            validator TEXT NOT NULL,
            system_score REAL,
            human_score REAL NOT NULL,
            voted_at TEXT NOT NULL,
            UNIQUE(comment_id, validator)
        );
        CREATE TABLE IF NOT EXISTS feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            voter_id TEXT NOT NULL,
            suggestion TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            status TEXT NOT NULL DEFAULT 'open'
        );
        CREATE TABLE IF NOT EXISTS feedback_votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            feedback_id INTEGER NOT NULL REFERENCES feedback(id),
            voter_id TEXT NOT NULL,
            voted_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(feedback_id, voter_id)
        );
        -- Indexes
        CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_utc);
        CREATE INDEX IF NOT EXISTS idx_posts_subreddit ON posts(subreddit);
        CREATE INDEX IF NOT EXISTS idx_posts_peds ON posts(pediatric_confidence);
        CREATE INDEX IF NOT EXISTS idx_comments_post ON comments(post_id);
        CREATE INDEX IF NOT EXISTS idx_med_mentions_med ON medication_mentions(medication);
        CREATE INDEX IF NOT EXISTS idx_side_effects_effect ON side_effects(effect);
        CREATE INDEX IF NOT EXISTS idx_beliefs_belief ON treatment_beliefs(belief);
        CREATE INDEX IF NOT EXISTS idx_ed_cat ON ed_discourse(category);
        CREATE INDEX IF NOT EXISTS idx_triggers_name ON triggers(trigger_name);
        CREATE INDEX IF NOT EXISTS idx_caregiver_cat ON caregiver_sentiment(category);
        CREATE INDEX IF NOT EXISTS idx_singulair_effect ON singulair_effects(effect);
        CREATE INDEX IF NOT EXISTS idx_cortico_effect ON corticosteroid_effects(effect);
        CREATE INDEX IF NOT EXISTS idx_functional_cat ON functional_impact(category);
        CREATE INDEX IF NOT EXISTS idx_inhaler_conf_cat ON inhaler_confusion(category);
        CREATE INDEX IF NOT EXISTS idx_ed_subcat ON ed_subcategories(subcategory);
        CREATE INDEX IF NOT EXISTS idx_post_ed_outcome ON post_ed_outcome(outcome);
        CREATE INDEX IF NOT EXISTS idx_errors_ts ON scrape_errors(timestamp);
        CREATE INDEX IF NOT EXISTS idx_vv_validator ON validation_votes(validator);
        CREATE INDEX IF NOT EXISTS idx_vv_post ON validation_votes(post_id);
        CREATE INDEX IF NOT EXISTS idx_sev_validator ON side_effect_votes(validator);
        CREATE INDEX IF NOT EXISTS idx_sev_comment ON side_effect_votes(comment_id);
        CREATE INDEX IF NOT EXISTS idx_sentv_validator ON sentiment_votes(validator);
        CREATE INDEX IF NOT EXISTS idx_sentv_comment ON sentiment_votes(comment_id);
        CREATE INDEX IF NOT EXISTS idx_feedback_status ON feedback(status);
        CREATE INDEX IF NOT EXISTS idx_feedback_votes_fid ON feedback_votes(feedback_id);

        CREATE TABLE IF NOT EXISTS bot_filter_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_at TEXT NOT NULL DEFAULT (datetime('now')),
            source_id TEXT NOT NULL,
            source_type TEXT NOT NULL,
            author TEXT,
            subreddit TEXT,
            reason TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_bot_filter_reason ON bot_filter_log(reason);
        CREATE INDEX IF NOT EXISTS idx_bot_filter_author ON bot_filter_log(author);

        CREATE TABLE IF NOT EXISTS validation_core_items (
            facet TEXT NOT NULL,
            item_id TEXT NOT NULL,
            item_type TEXT NOT NULL,
            PRIMARY KEY (facet, item_id)
        );
    """)
    # Add ed_related column to caregiver_sentiment if not present
    try:
        conn.execute("ALTER TABLE caregiver_sentiment ADD COLUMN ed_related INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # Column already exists
    # Add location column to posts if not present
    try:
        conn.execute("ALTER TABLE posts ADD COLUMN location TEXT")
    except sqlite3.OperationalError:
        pass
    conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_location ON posts(location)")
    # --- Sentiment scale migration: float (-1/+1) → 1-5 Likert integer ---
    # Guard: only convert values still in [-1, 1] range (skip if already 1-5)
    for tbl in ('posts', 'comments'):
        conn.execute(f"""
            UPDATE {tbl} SET sentiment = ROUND(sentiment * 2 + 3)
            WHERE sentiment IS NOT NULL AND sentiment >= -1.0 AND sentiment < 1.0
        """)
    conn.commit()
    conn.row_factory = sqlite3.Row
    _ensure_core_items_selected(conn)
    return conn


# ---------------------------------------------------------------------------
# Error logging helpers
# ---------------------------------------------------------------------------

def save_error_to_db(conn: sqlite3.Connection, subreddit: str, error_type: str,
                     message: str, source_id: Optional[str] = None,
                     source_type: Optional[str] = None) -> None:
    conn.execute(
        """INSERT INTO scrape_errors (timestamp, subreddit, error_type, message, source_id, source_type)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (datetime.utcnow().isoformat(), subreddit, error_type, message, source_id, source_type),
    )
    conn.commit()


def query_recent_errors(conn: sqlite3.Connection, limit: int = 50) -> list:
    rows = conn.execute(
        "SELECT * FROM scrape_errors ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def query_error_count(conn: sqlite3.Connection, hours: int = 24) -> int:
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    return conn.execute(
        "SELECT COUNT(*) FROM scrape_errors WHERE timestamp >= ?", (cutoff,)
    ).fetchone()[0]


# ---------------------------------------------------------------------------
# Feedback / Suggestion box
# ---------------------------------------------------------------------------

def query_feedback(conn: sqlite3.Connection, voter_id: str = None) -> list:
    rows = conn.execute("""
        SELECT f.id, f.suggestion, f.created_at, f.voter_id AS author_id,
               COUNT(fv.id) AS vote_count
        FROM feedback f
        LEFT JOIN feedback_votes fv ON fv.feedback_id = f.id
        WHERE f.status = 'open'
        GROUP BY f.id
        ORDER BY vote_count DESC, f.created_at DESC
    """).fetchall()
    voted_set = set()
    if voter_id:
        voted_rows = conn.execute(
            "SELECT feedback_id FROM feedback_votes WHERE voter_id = ?",
            (voter_id,)).fetchall()
        voted_set = {r[0] for r in voted_rows}
    return [
        {"id": r["id"], "suggestion": r["suggestion"], "vote_count": r["vote_count"],
         "created_at": r["created_at"], "voted_by_me": r["id"] in voted_set}
        for r in rows
    ]


def save_feedback(conn: sqlite3.Connection, voter_id: str, suggestion: str) -> int:
    suggestion = suggestion.strip()
    if not suggestion:
        raise ValueError("Suggestion cannot be empty")
    if len(suggestion) > 500:
        raise ValueError("Suggestion must be 500 characters or fewer")
    recent = conn.execute(
        "SELECT COUNT(*) FROM feedback WHERE voter_id = ? AND created_at > datetime('now', '-1 hour')",
        (voter_id,)).fetchone()[0]
    if recent > 0:
        raise ValueError("Rate limit: you can submit one suggestion per hour")
    cur = conn.execute(
        "INSERT INTO feedback (voter_id, suggestion) VALUES (?, ?)",
        (voter_id, suggestion))
    feedback_id = cur.lastrowid
    conn.execute(
        "INSERT INTO feedback_votes (feedback_id, voter_id) VALUES (?, ?)",
        (feedback_id, voter_id))
    conn.commit()
    return feedback_id


def toggle_feedback_vote(conn: sqlite3.Connection, feedback_id: int, voter_id: str) -> int:
    existing = conn.execute(
        "SELECT id FROM feedback_votes WHERE feedback_id = ? AND voter_id = ?",
        (feedback_id, voter_id)).fetchone()
    if existing:
        conn.execute("DELETE FROM feedback_votes WHERE id = ?", (existing[0],))
    else:
        conn.execute(
            "INSERT INTO feedback_votes (feedback_id, voter_id) VALUES (?, ?)",
            (feedback_id, voter_id))
    conn.commit()
    count = conn.execute(
        "SELECT COUNT(*) FROM feedback_votes WHERE feedback_id = ?",
        (feedback_id,)).fetchone()[0]
    return count


# ---------------------------------------------------------------------------
# Save posts and comments to DB
# ---------------------------------------------------------------------------

def save_posts_to_db(conn: sqlite3.Connection, posts: list,
                     mention_map: dict, subreddit: str = "") -> int:
    """Upsert posts and all analysis tables. Returns number of new posts."""
    now = datetime.utcnow().isoformat()
    new_count = 0
    for post in posts:
        text = f"{post['title']} {post['selftext']}"
        sent = score_sentiment(text)
        fear = score_fear(text)
        eng = compute_engagement(post["score"], post["num_comments"])
        xpost = post.get("crosspost_parent")
        sort_src = post.get("sort_source", "new")
        sub = post.get("subreddit", subreddit)
        peds_conf = classify_pediatric_confidence(text, sub)
        child_age = extract_child_age(text)
        location = extract_location(text)

        cur = conn.execute(
            """INSERT INTO posts (id, title, selftext, created_utc, score,
                                  num_comments, permalink, first_seen, sentiment,
                                  fear_score, subreddit, engagement_score, sort_source,
                                  crosspost_parent, pediatric_confidence, child_age_mentioned,
                                  location)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                   score = excluded.score,
                   num_comments = excluded.num_comments,
                   sentiment = excluded.sentiment,
                   fear_score = excluded.fear_score,
                   engagement_score = MAX(posts.engagement_score, excluded.engagement_score),
                   sort_source = CASE WHEN excluded.sort_source = 'hot' THEN 'hot'
                                      ELSE posts.sort_source END""",
            (post["id"], post["title"], post["selftext"],
             post["created_utc"], post["score"], post["num_comments"],
             post["permalink"], now, sent, fear,
             sub, eng, sort_src, xpost, peds_conf, child_age, location),
        )
        if cur.lastrowid:
            new_count += 1

        # Skip analysis for cross-posts and non-pediatric posts
        if xpost:
            continue
        if peds_conf == "none":
            continue

        # Medication mentions (post-level only)
        for med in mention_map.get(post["id"], []):
            med_class = MEDICATION_CLASSES.get(med, "")
            conn.execute(
                "INSERT OR IGNORE INTO medication_mentions (post_id, medication, med_class) VALUES (?, ?, ?)",
                (post["id"], med, med_class),
            )

        # Side effects
        for effect in find_side_effects(text):
            conn.execute(
                "INSERT OR IGNORE INTO side_effects (source_type, source_id, effect) VALUES ('post', ?, ?)",
                (post["id"], effect),
            )

        # Treatment beliefs with stance
        for belief, stance in find_treatment_beliefs_with_stance(text):
            conn.execute(
                """INSERT INTO treatment_beliefs (source_type, source_id, belief, stance)
                   VALUES ('post', ?, ?, ?)
                   ON CONFLICT(source_type, source_id, belief) DO UPDATE SET stance = excluded.stance""",
                (post["id"], belief, stance),
            )

        # ED discourse
        ed_categories = find_ed_discourse(text)
        has_ed = bool(ed_categories)
        for cat in ed_categories:
            conn.execute(
                "INSERT OR IGNORE INTO ed_discourse (source_type, source_id, category) VALUES ('post', ?, ?)",
                (post["id"], cat),
            )

        # Triggers
        for trig in find_triggers(text):
            trig_cat = TRIGGER_CATEGORIES.get(trig, "")
            conn.execute(
                "INSERT OR IGNORE INTO triggers (source_type, source_id, trigger_name, trigger_category) VALUES ('post', ?, ?, ?)",
                (post["id"], trig, trig_cat),
            )

        # Caregiver sentiment (with ED linkage)
        for cat in find_caregiver_sentiment(text):
            conn.execute(
                """INSERT INTO caregiver_sentiment (source_type, source_id, category, ed_related)
                   VALUES ('post', ?, ?, ?)
                   ON CONFLICT(source_type, source_id, category)
                   DO UPDATE SET ed_related = excluded.ed_related""",
                (post["id"], cat, 1 if has_ed else 0),
            )

        # Singulair effects (only if post mentions Singulair/montelukast)
        if _COMPILED_MEDS["Singulair"].search(text):
            for eff in find_singulair_effects(text):
                conn.execute(
                    "INSERT OR IGNORE INTO singulair_effects (source_type, source_id, effect) VALUES ('post', ?, ?)",
                    (post["id"], eff),
                )

        # Corticosteroid effects (only if post mentions a corticosteroid)
        if _CORTICOSTEROID_PATTERN.search(text):
            for eff in find_corticosteroid_effects(text):
                conn.execute(
                    "INSERT OR IGNORE INTO corticosteroid_effects (source_type, source_id, effect) VALUES ('post', ?, ?)",
                    (post["id"], eff),
                )

        # Functional impact
        for cat in find_functional_impact(text):
            conn.execute(
                "INSERT OR IGNORE INTO functional_impact (source_type, source_id, category) VALUES ('post', ?, ?)",
                (post["id"], cat),
            )

        # Inhaler confusion
        for cat in find_inhaler_confusion(text):
            conn.execute(
                "INSERT OR IGNORE INTO inhaler_confusion (source_type, source_id, category) VALUES ('post', ?, ?)",
                (post["id"], cat),
            )

        # Post-visit subcategories (only if post_visit ED discourse matched)
        if "post_visit" in ed_categories:
            for subcat in find_post_visit_subcategories(text):
                conn.execute(
                    "INSERT OR IGNORE INTO ed_subcategories (source_type, source_id, subcategory) VALUES ('post', ?, ?)",
                    (post["id"], subcat),
                )

        # Post-ED outcome (only if any ED discourse matched)
        if has_ed:
            for outcome in find_post_ed_outcome(text):
                conn.execute(
                    "INSERT OR IGNORE INTO post_ed_outcome (source_type, source_id, outcome) VALUES ('post', ?, ?)",
                    (post["id"], outcome),
                )

    conn.execute(
        "INSERT INTO scrape_runs (scraped_at, post_count, subreddit) VALUES (?, ?, ?)",
        (now, len(posts), subreddit),
    )
    conn.commit()
    return new_count


def save_comments_to_db(conn: sqlite3.Connection, post_id: str,
                        comments: list) -> int:
    """Save scraped comments with sentiment, fear, and analysis."""
    now = datetime.utcnow().isoformat()
    new_count = 0
    parent_sub_row = conn.execute(
        "SELECT subreddit FROM posts WHERE id = ?", (post_id,)
    ).fetchone()
    parent_sub = parent_sub_row[0] if parent_sub_row else None
    for c in comments:
        # Bot filter: skip and log known/heuristic bot authors
        bot_reason = is_bot_author(c.get("author", ""))
        if bot_reason:
            conn.execute(
                """INSERT INTO bot_filter_log
                   (source_id, source_type, author, subreddit, reason)
                   VALUES (?, 'comment', ?, ?, ?)""",
                (c["id"], c.get("author", ""), parent_sub, bot_reason),
            )
            continue

        body = c.get("body", "")
        sent = score_sentiment(body)
        fear = score_fear(body)

        cur = conn.execute(
            """INSERT INTO comments (id, post_id, body, score, created_utc, author, sentiment, fear_score, first_seen)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET score = excluded.score, sentiment = excluded.sentiment, fear_score = excluded.fear_score""",
            (c["id"], post_id, body, c.get("score", 0),
             c.get("created_utc", 0), c.get("author", ""), sent, fear, now),
        )
        if cur.lastrowid:
            new_count += 1

        # Side effects from comments
        for effect in find_side_effects(body):
            conn.execute(
                "INSERT OR IGNORE INTO side_effects (source_type, source_id, effect) VALUES ('comment', ?, ?)",
                (c["id"], effect),
            )

        # Treatment beliefs from comments
        for belief, stance in find_treatment_beliefs_with_stance(body):
            conn.execute(
                """INSERT INTO treatment_beliefs (source_type, source_id, belief, stance)
                   VALUES ('comment', ?, ?, ?)
                   ON CONFLICT(source_type, source_id, belief) DO UPDATE SET stance = excluded.stance""",
                (c["id"], belief, stance),
            )

        # ED discourse from comments
        comment_ed_categories = find_ed_discourse(body)
        comment_has_ed = bool(comment_ed_categories)
        for cat in comment_ed_categories:
            conn.execute(
                "INSERT OR IGNORE INTO ed_discourse (source_type, source_id, category) VALUES ('comment', ?, ?)",
                (c["id"], cat),
            )

        # Triggers from comments
        for trig in find_triggers(body):
            trig_cat = TRIGGER_CATEGORIES.get(trig, "")
            conn.execute(
                "INSERT OR IGNORE INTO triggers (source_type, source_id, trigger_name, trigger_category) VALUES ('comment', ?, ?, ?)",
                (c["id"], trig, trig_cat),
            )

        # Caregiver sentiment from comments (with ED linkage)
        for cat in find_caregiver_sentiment(body):
            conn.execute(
                """INSERT INTO caregiver_sentiment (source_type, source_id, category, ed_related)
                   VALUES ('comment', ?, ?, ?)
                   ON CONFLICT(source_type, source_id, category)
                   DO UPDATE SET ed_related = excluded.ed_related""",
                (c["id"], cat, 1 if comment_has_ed else 0),
            )

        # Singulair effects from comments
        if _COMPILED_MEDS["Singulair"].search(body):
            for eff in find_singulair_effects(body):
                conn.execute(
                    "INSERT OR IGNORE INTO singulair_effects (source_type, source_id, effect) VALUES ('comment', ?, ?)",
                    (c["id"], eff),
                )

        # Corticosteroid effects from comments
        if _CORTICOSTEROID_PATTERN.search(body):
            for eff in find_corticosteroid_effects(body):
                conn.execute(
                    "INSERT OR IGNORE INTO corticosteroid_effects (source_type, source_id, effect) VALUES ('comment', ?, ?)",
                    (c["id"], eff),
                )

        # Functional impact from comments
        for cat in find_functional_impact(body):
            conn.execute(
                "INSERT OR IGNORE INTO functional_impact (source_type, source_id, category) VALUES ('comment', ?, ?)",
                (c["id"], cat),
            )

        # Inhaler confusion from comments
        for cat in find_inhaler_confusion(body):
            conn.execute(
                "INSERT OR IGNORE INTO inhaler_confusion (source_type, source_id, category) VALUES ('comment', ?, ?)",
                (c["id"], cat),
            )

        # Post-visit subcategories from comments
        if "post_visit" in comment_ed_categories:
            for subcat in find_post_visit_subcategories(body):
                conn.execute(
                    "INSERT OR IGNORE INTO ed_subcategories (source_type, source_id, subcategory) VALUES ('comment', ?, ?)",
                    (c["id"], subcat),
                )

        # Post-ED outcome from comments
        if comment_has_ed:
            for outcome in find_post_ed_outcome(body):
                conn.execute(
                    "INSERT OR IGNORE INTO post_ed_outcome (source_type, source_id, outcome) VALUES ('comment', ?, ?)",
                    (c["id"], outcome),
                )

    conn.execute(
        "UPDATE posts SET comments_scraped = 1 WHERE id = ?", (post_id,))
    conn.commit()
    return new_count


# ---------------------------------------------------------------------------
# Query functions
# ---------------------------------------------------------------------------

def _add_date_filter(sql: str, params: list, date_from: Optional[float],
                     date_to: Optional[float], col: str = "p.created_utc") -> str:
    if date_from is not None:
        sql += f" AND {col} >= ?"
        params.append(date_from)
    if date_to is not None:
        sql += f" AND {col} <= ?"
        params.append(date_to)
    return sql


def query_medication_counts(conn: sqlite3.Connection,
                            date_from: Optional[float] = None,
                            date_to: Optional[float] = None,
                            subreddit: Optional[str] = None) -> list:
    """Return [(medication, count)] sorted by count desc."""
    sql = "SELECT mm.medication, COUNT(*) as cnt FROM medication_mentions mm"
    wheres, params = [], []
    need_join = date_from is not None or date_to is not None or subreddit is not None
    if need_join:
        sql += " JOIN posts p ON p.id = mm.post_id"
    if date_from is not None:
        wheres.append("p.created_utc >= ?"); params.append(date_from)
    if date_to is not None:
        wheres.append("p.created_utc <= ?"); params.append(date_to)
    if subreddit is not None:
        wheres.append("p.subreddit = ?"); params.append(subreddit)
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " GROUP BY mm.medication ORDER BY cnt DESC"
    return [(r[0], r[1]) for r in conn.execute(sql, params).fetchall()]


def query_daily_medication_counts(conn: sqlite3.Connection,
                                  date_from: Optional[float] = None,
                                  date_to: Optional[float] = None,
                                  subreddit: Optional[str] = None) -> dict:
    """Return {date: {medication: count}}."""
    sql = """SELECT date(p.created_utc, 'unixepoch') as day,
                    mm.medication, COUNT(*) as cnt
             FROM medication_mentions mm JOIN posts p ON p.id = mm.post_id
             WHERE p.created_utc > 0"""
    params = []
    sql = _add_date_filter(sql, params, date_from, date_to)
    if subreddit is not None:
        sql += " AND p.subreddit = ?"; params.append(subreddit)
    sql += " GROUP BY day, mm.medication ORDER BY day"
    result = {}
    for row in conn.execute(sql, params).fetchall():
        result.setdefault(row[0], {})[row[1]] = row[2]
    return result


def query_medication_sentiment(conn: sqlite3.Connection,
                               date_from: Optional[float] = None,
                               date_to: Optional[float] = None,
                               subreddit: Optional[str] = None) -> list:
    """Return [(medication, avg_sentiment, avg_fear, count)]."""
    sql = """SELECT mm.medication, AVG(p.sentiment) as avg_s, AVG(p.fear_score) as avg_f, COUNT(*) as cnt
             FROM medication_mentions mm JOIN posts p ON p.id = mm.post_id
             WHERE p.sentiment IS NOT NULL"""
    params = []
    sql = _add_date_filter(sql, params, date_from, date_to)
    if subreddit is not None:
        sql += " AND p.subreddit = ?"; params.append(subreddit)
    sql += " GROUP BY mm.medication HAVING cnt >= 2 ORDER BY avg_s DESC"
    return [(r[0], round(r[1], 3) if r[1] else 0, round(r[2], 3) if r[2] else 0, r[3])
            for r in conn.execute(sql, params).fetchall()]


def query_db_stats(conn: sqlite3.Connection,
                   date_from: Optional[float] = None,
                   date_to: Optional[float] = None,
                   subreddit: Optional[str] = None) -> dict:
    total_posts = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    total_comments = conn.execute("SELECT COUNT(*) FROM comments").fetchone()[0]
    total_mentions = conn.execute("SELECT COUNT(*) FROM medication_mentions").fetchone()[0]
    total_scrapes = conn.execute("SELECT COUNT(*) FROM scrape_runs").fetchone()[0]
    last_scrape = conn.execute(
        "SELECT scraped_at FROM scrape_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    avg_sent = conn.execute(
        "SELECT AVG(sentiment) FROM posts WHERE sentiment IS NOT NULL"
    ).fetchone()[0]
    avg_fear = conn.execute(
        "SELECT AVG(fear_score) FROM posts WHERE fear_score IS NOT NULL"
    ).fetchone()[0]
    subreddit_count = conn.execute(
        "SELECT COUNT(DISTINCT subreddit) FROM posts WHERE subreddit IS NOT NULL"
    ).fetchone()[0]
    peds_definite = conn.execute(
        "SELECT COUNT(*) FROM posts WHERE pediatric_confidence = 'definite'"
    ).fetchone()[0]
    peds_likely = conn.execute(
        "SELECT COUNT(*) FROM posts WHERE pediatric_confidence = 'likely'"
    ).fetchone()[0]
    error_count_24h = query_error_count(conn, hours=24)
    return {
        "total_posts": total_posts,
        "total_comments": total_comments,
        "total_mentions": total_mentions,
        "total_scrapes": total_scrapes,
        "last_scrape": last_scrape[0] if last_scrape else None,
        "avg_sentiment": round(avg_sent, 3) if avg_sent is not None else None,
        "avg_fear": round(avg_fear, 3) if avg_fear is not None else None,
        "subreddit_count": subreddit_count or 0,
        "peds_definite": peds_definite,
        "peds_likely": peds_likely,
        "error_count_24h": error_count_24h,
    }


def query_top_posts(conn: sqlite3.Connection, medication: str,
                    limit: int = 30, offset: int = 0,
                    date_from: Optional[float] = None,
                    date_to: Optional[float] = None,
                    subreddit: Optional[str] = None) -> list:
    sql = """SELECT p.id, p.title, p.selftext, p.created_utc, p.score,
                    p.num_comments, p.permalink, p.sentiment, p.fear_score,
                    p.subreddit, p.engagement_score, p.pediatric_confidence
             FROM posts p JOIN medication_mentions mm ON mm.post_id = p.id
             WHERE mm.medication = ?"""
    params = [medication]
    sql = _add_date_filter(sql, params, date_from, date_to)
    if subreddit is not None:
        sql += " AND p.subreddit = ?"; params.append(subreddit)
    sql += " ORDER BY p.engagement_score DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def query_comments_for_post(conn: sqlite3.Connection, post_id: str) -> list:
    rows = conn.execute("""
        SELECT id, body, score, created_utc, author, sentiment, fear_score
        FROM comments WHERE post_id = ? ORDER BY score DESC
    """, (post_id,)).fetchall()
    return [dict(r) for r in rows]


def query_side_effect_counts(conn: sqlite3.Connection,
                             date_from: Optional[float] = None,
                             date_to: Optional[float] = None,
                             subreddit: Optional[str] = None) -> list:
    sql = "SELECT se.effect, COUNT(DISTINCT se.source_id) as cnt FROM side_effects se"
    wheres, params = [], []
    need_filter = date_from is not None or date_to is not None or subreddit is not None
    if need_filter:
        sql += """ LEFT JOIN posts p ON (se.source_type = 'post' AND se.source_id = p.id)
                   LEFT JOIN comments c ON (se.source_type = 'comment' AND se.source_id = c.id)"""
        if date_from is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) >= ?)"); params.append(date_from)
        if date_to is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) <= ?)"); params.append(date_to)
        if subreddit is not None:
            wheres.append("COALESCE(p.subreddit, (SELECT p2.subreddit FROM posts p2 WHERE p2.id = c.post_id)) = ?")
            params.append(subreddit)
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " GROUP BY se.effect ORDER BY cnt DESC"
    return [(r[0], r[1]) for r in conn.execute(sql, params).fetchall()]


def query_belief_counts(conn: sqlite3.Connection,
                        date_from: Optional[float] = None,
                        date_to: Optional[float] = None,
                        subreddit: Optional[str] = None) -> list:
    sql = "SELECT tb.belief, COUNT(DISTINCT tb.source_id) as cnt FROM treatment_beliefs tb"
    wheres, params = [], []
    need_filter = date_from is not None or date_to is not None or subreddit is not None
    if need_filter:
        sql += """ LEFT JOIN posts p ON (tb.source_type = 'post' AND tb.source_id = p.id)
                   LEFT JOIN comments c ON (tb.source_type = 'comment' AND tb.source_id = c.id)"""
        if date_from is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) >= ?)"); params.append(date_from)
        if date_to is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) <= ?)"); params.append(date_to)
        if subreddit is not None:
            wheres.append("COALESCE(p.subreddit, (SELECT p2.subreddit FROM posts p2 WHERE p2.id = c.post_id)) = ?")
            params.append(subreddit)
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " GROUP BY tb.belief ORDER BY cnt DESC"
    return [(r[0], r[1]) for r in conn.execute(sql, params).fetchall()]


def query_belief_stance_counts(conn: sqlite3.Connection,
                               date_from: Optional[float] = None,
                               date_to: Optional[float] = None,
                               subreddit: Optional[str] = None) -> dict:
    """Return {belief: {stance: count}}."""
    sql = "SELECT tb.belief, tb.stance, COUNT(DISTINCT tb.source_id) as cnt FROM treatment_beliefs tb"
    wheres, params = [], []
    need_filter = date_from is not None or date_to is not None or subreddit is not None
    if need_filter:
        sql += """ LEFT JOIN posts p ON (tb.source_type = 'post' AND tb.source_id = p.id)
                   LEFT JOIN comments c ON (tb.source_type = 'comment' AND tb.source_id = c.id)"""
        if date_from is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) >= ?)"); params.append(date_from)
        if date_to is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) <= ?)"); params.append(date_to)
        if subreddit is not None:
            wheres.append("COALESCE(p.subreddit, (SELECT p2.subreddit FROM posts p2 WHERE p2.id = c.post_id)) = ?")
            params.append(subreddit)
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " GROUP BY tb.belief, tb.stance ORDER BY tb.belief"
    result = {}
    for row in conn.execute(sql, params).fetchall():
        result.setdefault(row[0], {})[row[1] or 'unclear'] = row[2]
    return result


def query_ed_discourse_counts(conn: sqlite3.Connection,
                              date_from: Optional[float] = None,
                              date_to: Optional[float] = None,
                              subreddit: Optional[str] = None) -> list:
    sql = "SELECT ed.category, COUNT(DISTINCT ed.source_id) as cnt FROM ed_discourse ed"
    wheres, params = [], []
    need_filter = date_from is not None or date_to is not None or subreddit is not None
    if need_filter:
        sql += """ LEFT JOIN posts p ON (ed.source_type = 'post' AND ed.source_id = p.id)
                   LEFT JOIN comments c ON (ed.source_type = 'comment' AND ed.source_id = c.id)"""
        if date_from is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) >= ?)"); params.append(date_from)
        if date_to is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) <= ?)"); params.append(date_to)
        if subreddit is not None:
            wheres.append("COALESCE(p.subreddit, (SELECT p2.subreddit FROM posts p2 WHERE p2.id = c.post_id)) = ?")
            params.append(subreddit)
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " GROUP BY ed.category ORDER BY cnt DESC"
    return [(r[0], r[1]) for r in conn.execute(sql, params).fetchall()]


def query_ed_discourse_daily(conn: sqlite3.Connection,
                             date_from: Optional[float] = None,
                             date_to: Optional[float] = None,
                             subreddit: Optional[str] = None) -> dict:
    """Return {date: {category: count}}."""
    sql = """SELECT date(p.created_utc, 'unixepoch') as day, ed.category, COUNT(*) as cnt
             FROM ed_discourse ed
             JOIN posts p ON (ed.source_type = 'post' AND ed.source_id = p.id)
             WHERE p.created_utc > 0"""
    params = []
    sql = _add_date_filter(sql, params, date_from, date_to)
    if subreddit is not None:
        sql += " AND p.subreddit = ?"; params.append(subreddit)
    sql += " GROUP BY day, ed.category ORDER BY day"
    result = {}
    for row in conn.execute(sql, params).fetchall():
        result.setdefault(row[0], {})[row[1]] = row[2]
    return result


def query_trigger_counts(conn: sqlite3.Connection,
                         date_from: Optional[float] = None,
                         date_to: Optional[float] = None,
                         subreddit: Optional[str] = None) -> list:
    sql = "SELECT t.trigger_name, t.trigger_category, COUNT(DISTINCT t.source_id) as cnt FROM triggers t"
    wheres, params = [], []
    need_filter = date_from is not None or date_to is not None or subreddit is not None
    if need_filter:
        sql += """ LEFT JOIN posts p ON (t.source_type = 'post' AND t.source_id = p.id)
                   LEFT JOIN comments c ON (t.source_type = 'comment' AND t.source_id = c.id)"""
        if date_from is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) >= ?)"); params.append(date_from)
        if date_to is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) <= ?)"); params.append(date_to)
        if subreddit is not None:
            wheres.append("COALESCE(p.subreddit, (SELECT p2.subreddit FROM posts p2 WHERE p2.id = c.post_id)) = ?")
            params.append(subreddit)
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " GROUP BY t.trigger_name ORDER BY cnt DESC"
    return [(r[0], r[1], r[2]) for r in conn.execute(sql, params).fetchall()]


def query_trigger_daily(conn: sqlite3.Connection,
                        date_from: Optional[float] = None,
                        date_to: Optional[float] = None,
                        subreddit: Optional[str] = None) -> dict:
    """Return {date: {trigger_name: count}}."""
    sql = """SELECT date(p.created_utc, 'unixepoch') as day, t.trigger_name, COUNT(*) as cnt
             FROM triggers t
             JOIN posts p ON (t.source_type = 'post' AND t.source_id = p.id)
             WHERE p.created_utc > 0"""
    params = []
    sql = _add_date_filter(sql, params, date_from, date_to)
    if subreddit is not None:
        sql += " AND p.subreddit = ?"; params.append(subreddit)
    sql += " GROUP BY day, t.trigger_name ORDER BY day"
    result = {}
    for row in conn.execute(sql, params).fetchall():
        result.setdefault(row[0], {})[row[1]] = row[2]
    return result


def query_caregiver_counts(conn: sqlite3.Connection,
                           date_from: Optional[float] = None,
                           date_to: Optional[float] = None,
                           subreddit: Optional[str] = None) -> list:
    sql = "SELECT cs.category, COUNT(DISTINCT cs.source_id) as cnt FROM caregiver_sentiment cs"
    wheres, params = [], []
    need_filter = date_from is not None or date_to is not None or subreddit is not None
    if need_filter:
        sql += """ LEFT JOIN posts p ON (cs.source_type = 'post' AND cs.source_id = p.id)
                   LEFT JOIN comments c ON (cs.source_type = 'comment' AND cs.source_id = c.id)"""
        if date_from is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) >= ?)"); params.append(date_from)
        if date_to is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) <= ?)"); params.append(date_to)
        if subreddit is not None:
            wheres.append("COALESCE(p.subreddit, (SELECT p2.subreddit FROM posts p2 WHERE p2.id = c.post_id)) = ?")
            params.append(subreddit)
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " GROUP BY cs.category ORDER BY cnt DESC"
    return [(r[0], r[1]) for r in conn.execute(sql, params).fetchall()]


def query_caregiver_daily(conn: sqlite3.Connection,
                          date_from: Optional[float] = None,
                          date_to: Optional[float] = None,
                          subreddit: Optional[str] = None) -> dict:
    """Return {date: {category: count}}."""
    sql = """SELECT date(p.created_utc, 'unixepoch') as day, cs.category, COUNT(*) as cnt
             FROM caregiver_sentiment cs
             JOIN posts p ON (cs.source_type = 'post' AND cs.source_id = p.id)
             WHERE p.created_utc > 0"""
    params = []
    sql = _add_date_filter(sql, params, date_from, date_to)
    if subreddit is not None:
        sql += " AND p.subreddit = ?"; params.append(subreddit)
    sql += " GROUP BY day, cs.category ORDER BY day"
    result = {}
    for row in conn.execute(sql, params).fetchall():
        result.setdefault(row[0], {})[row[1]] = row[2]
    return result


def query_singulair_effect_counts(conn: sqlite3.Connection,
                                  date_from: Optional[float] = None,
                                  date_to: Optional[float] = None,
                                  subreddit: Optional[str] = None) -> list:
    sql = "SELECT se.effect, COUNT(DISTINCT se.source_id) as cnt FROM singulair_effects se"
    wheres, params = [], []
    need_filter = date_from is not None or date_to is not None or subreddit is not None
    if need_filter:
        sql += """ LEFT JOIN posts p ON (se.source_type = 'post' AND se.source_id = p.id)
                   LEFT JOIN comments c ON (se.source_type = 'comment' AND se.source_id = c.id)"""
        if date_from is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) >= ?)"); params.append(date_from)
        if date_to is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) <= ?)"); params.append(date_to)
        if subreddit is not None:
            wheres.append("COALESCE(p.subreddit, (SELECT p2.subreddit FROM posts p2 WHERE p2.id = c.post_id)) = ?")
            params.append(subreddit)
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " GROUP BY se.effect ORDER BY cnt DESC"
    return [(r[0], r[1]) for r in conn.execute(sql, params).fetchall()]


def query_singulair_daily(conn: sqlite3.Connection,
                          date_from: Optional[float] = None,
                          date_to: Optional[float] = None,
                          subreddit: Optional[str] = None) -> dict:
    """Return {date: {effect: count}}."""
    sql = """SELECT date(p.created_utc, 'unixepoch') as day, se.effect, COUNT(*) as cnt
             FROM singulair_effects se
             JOIN posts p ON (se.source_type = 'post' AND se.source_id = p.id)
             WHERE p.created_utc > 0"""
    params = []
    sql = _add_date_filter(sql, params, date_from, date_to)
    if subreddit is not None:
        sql += " AND p.subreddit = ?"; params.append(subreddit)
    sql += " GROUP BY day, se.effect ORDER BY day"
    result = {}
    for row in conn.execute(sql, params).fetchall():
        result.setdefault(row[0], {})[row[1]] = row[2]
    return result


def query_singulair_discourse_counts(conn: sqlite3.Connection,
                                     date_from: Optional[float] = None,
                                     date_to: Optional[float] = None,
                                     subreddit: Optional[str] = None) -> list:
    """Count Singulair discourse categories from posts that mention Singulair."""
    sql = """SELECT 'placeholder' as cat, 0 as cnt"""
    # We need to scan post text for discourse patterns since they're not stored in a table
    # Instead, query posts with Singulair mentions and scan text
    rows = conn.execute("""
        SELECT p.id, p.title, p.selftext
        FROM posts p JOIN medication_mentions mm ON mm.post_id = p.id
        WHERE mm.medication = 'Singulair'
    """).fetchall()
    counts = Counter()
    for r in rows:
        text = f"{r['title']} {r['selftext']}"
        for cat in find_singulair_discourse(text):
            counts[cat] += 1
    return [(cat, cnt) for cat, cnt in counts.most_common()]


def query_corticosteroid_effect_counts(conn: sqlite3.Connection,
                                        date_from: Optional[float] = None,
                                        date_to: Optional[float] = None,
                                        subreddit: Optional[str] = None) -> list:
    sql = "SELECT ce.effect, COUNT(DISTINCT ce.source_id) as cnt FROM corticosteroid_effects ce"
    wheres, params = [], []
    need_filter = date_from is not None or date_to is not None or subreddit is not None
    if need_filter:
        sql += """ LEFT JOIN posts p ON (ce.source_type = 'post' AND ce.source_id = p.id)
                   LEFT JOIN comments c ON (ce.source_type = 'comment' AND ce.source_id = c.id)"""
        if date_from is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) >= ?)"); params.append(date_from)
        if date_to is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) <= ?)"); params.append(date_to)
        if subreddit is not None:
            wheres.append("COALESCE(p.subreddit, (SELECT p2.subreddit FROM posts p2 WHERE p2.id = c.post_id)) = ?")
            params.append(subreddit)
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " GROUP BY ce.effect ORDER BY cnt DESC"
    return [(r[0], r[1]) for r in conn.execute(sql, params).fetchall()]


def query_corticosteroid_daily(conn: sqlite3.Connection,
                                date_from: Optional[float] = None,
                                date_to: Optional[float] = None,
                                subreddit: Optional[str] = None) -> dict:
    """Return {date: {effect: count}}."""
    sql = """SELECT date(p.created_utc, 'unixepoch') as day, ce.effect, COUNT(*) as cnt
             FROM corticosteroid_effects ce
             JOIN posts p ON (ce.source_type = 'post' AND ce.source_id = p.id)
             WHERE p.created_utc > 0"""
    params = []
    sql = _add_date_filter(sql, params, date_from, date_to)
    if subreddit is not None:
        sql += " AND p.subreddit = ?"; params.append(subreddit)
    sql += " GROUP BY day, ce.effect ORDER BY day"
    result = {}
    for row in conn.execute(sql, params).fetchall():
        result.setdefault(row[0], {})[row[1]] = row[2]
    return result


def query_functional_impact_counts(conn: sqlite3.Connection,
                                    date_from: Optional[float] = None,
                                    date_to: Optional[float] = None,
                                    subreddit: Optional[str] = None) -> list:
    sql = "SELECT fi.category, COUNT(DISTINCT fi.source_id) as cnt FROM functional_impact fi"
    wheres, params = [], []
    need_filter = date_from is not None or date_to is not None or subreddit is not None
    if need_filter:
        sql += """ LEFT JOIN posts p ON (fi.source_type = 'post' AND fi.source_id = p.id)
                   LEFT JOIN comments c ON (fi.source_type = 'comment' AND fi.source_id = c.id)"""
        if date_from is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) >= ?)"); params.append(date_from)
        if date_to is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) <= ?)"); params.append(date_to)
        if subreddit is not None:
            wheres.append("COALESCE(p.subreddit, (SELECT p2.subreddit FROM posts p2 WHERE p2.id = c.post_id)) = ?")
            params.append(subreddit)
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " GROUP BY fi.category ORDER BY cnt DESC"
    return [(r[0], r[1]) for r in conn.execute(sql, params).fetchall()]


def query_functional_impact_daily(conn: sqlite3.Connection,
                                   date_from: Optional[float] = None,
                                   date_to: Optional[float] = None,
                                   subreddit: Optional[str] = None) -> dict:
    """Return {date: {category: count}}."""
    sql = """SELECT date(p.created_utc, 'unixepoch') as day, fi.category, COUNT(*) as cnt
             FROM functional_impact fi
             JOIN posts p ON (fi.source_type = 'post' AND fi.source_id = p.id)
             WHERE p.created_utc > 0"""
    params = []
    sql = _add_date_filter(sql, params, date_from, date_to)
    if subreddit is not None:
        sql += " AND p.subreddit = ?"; params.append(subreddit)
    sql += " GROUP BY day, fi.category ORDER BY day"
    result = {}
    for row in conn.execute(sql, params).fetchall():
        result.setdefault(row[0], {})[row[1]] = row[2]
    return result


def query_inhaler_confusion_counts(conn: sqlite3.Connection,
                                    date_from: Optional[float] = None,
                                    date_to: Optional[float] = None,
                                    subreddit: Optional[str] = None) -> list:
    sql = "SELECT ic.category, COUNT(DISTINCT ic.source_id) as cnt FROM inhaler_confusion ic"
    wheres, params = [], []
    need_filter = date_from is not None or date_to is not None or subreddit is not None
    if need_filter:
        sql += """ LEFT JOIN posts p ON (ic.source_type = 'post' AND ic.source_id = p.id)
                   LEFT JOIN comments c ON (ic.source_type = 'comment' AND ic.source_id = c.id)"""
        if date_from is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) >= ?)"); params.append(date_from)
        if date_to is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) <= ?)"); params.append(date_to)
        if subreddit is not None:
            wheres.append("COALESCE(p.subreddit, (SELECT p2.subreddit FROM posts p2 WHERE p2.id = c.post_id)) = ?")
            params.append(subreddit)
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " GROUP BY ic.category ORDER BY cnt DESC"
    return [(r[0], r[1]) for r in conn.execute(sql, params).fetchall()]


def query_inhaler_confusion_daily(conn: sqlite3.Connection,
                                   date_from: Optional[float] = None,
                                   date_to: Optional[float] = None,
                                   subreddit: Optional[str] = None) -> dict:
    """Return {date: {category: count}}."""
    sql = """SELECT date(p.created_utc, 'unixepoch') as day, ic.category, COUNT(*) as cnt
             FROM inhaler_confusion ic
             JOIN posts p ON (ic.source_type = 'post' AND ic.source_id = p.id)
             WHERE p.created_utc > 0"""
    params = []
    sql = _add_date_filter(sql, params, date_from, date_to)
    if subreddit is not None:
        sql += " AND p.subreddit = ?"; params.append(subreddit)
    sql += " GROUP BY day, ic.category ORDER BY day"
    result = {}
    for row in conn.execute(sql, params).fetchall():
        result.setdefault(row[0], {})[row[1]] = row[2]
    return result


def query_ed_subcategory_counts(conn: sqlite3.Connection,
                                 date_from: Optional[float] = None,
                                 date_to: Optional[float] = None,
                                 subreddit: Optional[str] = None) -> list:
    sql = "SELECT es.subcategory, COUNT(DISTINCT es.source_id) as cnt FROM ed_subcategories es"
    wheres, params = [], []
    need_filter = date_from is not None or date_to is not None or subreddit is not None
    if need_filter:
        sql += """ LEFT JOIN posts p ON (es.source_type = 'post' AND es.source_id = p.id)
                   LEFT JOIN comments c ON (es.source_type = 'comment' AND es.source_id = c.id)"""
        if date_from is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) >= ?)"); params.append(date_from)
        if date_to is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) <= ?)"); params.append(date_to)
        if subreddit is not None:
            wheres.append("COALESCE(p.subreddit, (SELECT p2.subreddit FROM posts p2 WHERE p2.id = c.post_id)) = ?")
            params.append(subreddit)
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " GROUP BY es.subcategory ORDER BY cnt DESC"
    return [(r[0], r[1]) for r in conn.execute(sql, params).fetchall()]


def query_caregiver_ed_linked(conn: sqlite3.Connection,
                               date_from: Optional[float] = None,
                               date_to: Optional[float] = None,
                               subreddit: Optional[str] = None) -> list:
    """Return [(category, count)] for caregiver sentiments co-occurring with ED discourse."""
    sql = "SELECT cs.category, COUNT(DISTINCT cs.source_id) as cnt FROM caregiver_sentiment cs"
    wheres, params = ["cs.ed_related = 1"], []
    need_filter = date_from is not None or date_to is not None or subreddit is not None
    if need_filter:
        sql += """ LEFT JOIN posts p ON (cs.source_type = 'post' AND cs.source_id = p.id)
                   LEFT JOIN comments c ON (cs.source_type = 'comment' AND cs.source_id = c.id)"""
        if date_from is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) >= ?)"); params.append(date_from)
        if date_to is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) <= ?)"); params.append(date_to)
        if subreddit is not None:
            wheres.append("COALESCE(p.subreddit, (SELECT p2.subreddit FROM posts p2 WHERE p2.id = c.post_id)) = ?")
            params.append(subreddit)
    sql += " WHERE " + " AND ".join(wheres)
    sql += " GROUP BY cs.category ORDER BY cnt DESC"
    return [(r[0], r[1]) for r in conn.execute(sql, params).fetchall()]


def query_post_ed_outcome_counts(conn: sqlite3.Connection,
                                  date_from: Optional[float] = None,
                                  date_to: Optional[float] = None,
                                  subreddit: Optional[str] = None) -> list:
    sql = "SELECT peo.outcome, COUNT(DISTINCT peo.source_id) as cnt FROM post_ed_outcome peo"
    wheres, params = [], []
    need_filter = date_from is not None or date_to is not None or subreddit is not None
    if need_filter:
        sql += """ LEFT JOIN posts p ON (peo.source_type = 'post' AND peo.source_id = p.id)
                   LEFT JOIN comments c ON (peo.source_type = 'comment' AND peo.source_id = c.id)"""
        if date_from is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) >= ?)"); params.append(date_from)
        if date_to is not None:
            wheres.append("(COALESCE(p.created_utc, c.created_utc) <= ?)"); params.append(date_to)
        if subreddit is not None:
            wheres.append("COALESCE(p.subreddit, (SELECT p2.subreddit FROM posts p2 WHERE p2.id = c.post_id)) = ?")
            params.append(subreddit)
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " GROUP BY peo.outcome ORDER BY cnt DESC"
    return [(r[0], r[1]) for r in conn.execute(sql, params).fetchall()]


def query_scrape_log(conn: sqlite3.Connection, limit: int = 20) -> list:
    rows = conn.execute("""
        SELECT strftime('%Y-%m-%dT%H:', scraped_at) ||
               CAST((CAST(strftime('%M', scraped_at) AS INTEGER) / 10) * 10 AS TEXT) AS cycle,
               MIN(scraped_at) AS started,
               MAX(scraped_at) AS ended,
               COUNT(*) AS sub_count,
               SUM(error_count) AS errors
        FROM scrape_runs
        GROUP BY cycle
        ORDER BY started DESC
        LIMIT ?
    """, (limit,)).fetchall()
    results = []
    for r in rows:
        new_posts = conn.execute(
            "SELECT COUNT(*) FROM posts WHERE first_seen BETWEEN ? AND ?",
            (r["started"], r["ended"])).fetchone()[0]
        new_comments = conn.execute(
            "SELECT COUNT(*) FROM comments WHERE first_seen BETWEEN ? AND strftime('%Y-%m-%dT%H:%M:%f', ?, '+10 minutes')",
            (r["started"], r["ended"])).fetchone()[0]
        results.append({
            "scraped_at": r["started"], "subreddits": r["sub_count"],
            "new_posts": new_posts, "new_comments": new_comments,
            "errors": r["errors"] or 0,
        })
    return results


# ---------------------------------------------------------------------------
# Validation system (3 facets: beliefs, side effects, sentiment)
# ---------------------------------------------------------------------------

import random as _random


def _time_stratified_sample(rows: list, count: int) -> list:
    """Sample rows with time diversity: split into 3 terciles by created_utc,
    sample ~equal from each. Fallback: just return all rows if not enough."""
    if len(rows) <= count:
        return list(rows)
    sorted_rows = sorted(rows, key=lambda r: (r['created_utc'] or 0))
    n = len(sorted_rows)
    terciles = [sorted_rows[:n // 3], sorted_rows[n // 3:2 * n // 3], sorted_rows[2 * n // 3:]]
    per_tercile = count // 3
    remainder = count - per_tercile * 3
    result = []
    shortfall = 0
    for i, t in enumerate(terciles):
        want = per_tercile + (1 if i < remainder else 0)
        if len(t) >= want:
            result.extend(_random.sample(t, want))
        else:
            result.extend(t)
            shortfall += want - len(t)
    if shortfall > 0:
        used_ids = {id(r) for r in result}
        pool = [r for r in sorted_rows if id(r) not in used_ids]
        result.extend(_random.sample(pool, min(shortfall, len(pool))))
    return result


def _ensure_core_items_selected(conn: sqlite3.Connection):
    """One-time selection of core validation items for the tiered pool strategy.

    Core items are shown to EVERY validator (for inter-rater reliability).
    Exploration items fill the rest of each batch (for coverage).
    Idempotent — only populates if validation_core_items is empty.
    """
    existing = conn.execute(
        "SELECT COUNT(*) FROM validation_core_items"
    ).fetchone()[0]
    if existing > 0:
        return  # Already populated

    log.info("Selecting core validation items (one-time)...")

    bot_exclude = """AND (c.author IS NULL OR (
          LOWER(c.author) NOT IN ({known})
          AND c.author NOT GLOB '*[Bb]ot'
          AND c.author NOT GLOB '*-[Bb]ot'
          AND c.author NOT GLOB '*_[Bb]ot'
        ))""".format(known=",".join(f"'{a}'" for a in _KNOWN_BOT_AUTHORS))

    # --- Treatment beliefs: 80 posts, balanced flagged/unflagged ---
    tb_flagged = conn.execute("""
        SELECT DISTINCT p.id, p.created_utc
        FROM posts p
        JOIN treatment_beliefs tb ON tb.source_type = 'post' AND tb.source_id = p.id
        JOIN medication_mentions mm ON mm.post_id = p.id
        WHERE length(p.selftext) > 30
        LIMIT 300
    """).fetchall()
    tb_unflagged = conn.execute("""
        SELECT DISTINCT p.id, p.created_utc
        FROM posts p
        JOIN medication_mentions mm ON mm.post_id = p.id
        WHERE p.id NOT IN (SELECT source_id FROM treatment_beliefs WHERE source_type = 'post')
          AND length(p.selftext) > 30
        LIMIT 300
    """).fetchall()
    n_flagged = min(40, len(tb_flagged))
    n_unflagged = min(80 - n_flagged, len(tb_unflagged))
    if n_flagged + n_unflagged < 80 and len(tb_flagged) > n_flagged:
        n_flagged = min(80 - n_unflagged, len(tb_flagged))
    tb_f = _time_stratified_sample(tb_flagged, n_flagged)
    tb_u = _time_stratified_sample(tb_unflagged, n_unflagged)
    for r in list(tb_f) + list(tb_u):
        conn.execute(
            "INSERT OR IGNORE INTO validation_core_items (facet, item_id, item_type) VALUES ('beliefs', ?, 'post')",
            (r['id'],))
    log.info(f"  Beliefs core: {len(tb_f) + len(tb_u)} posts ({len(tb_f)} flagged, {len(tb_u)} unflagged)")

    # --- Side effects: 80 comments, 48 with effects + 32 without ---
    se_with_pool = conn.execute(f"""
        SELECT DISTINCT c.id, c.created_utc
        FROM comments c
        JOIN side_effects se ON se.source_type = 'comment' AND se.source_id = c.id
        WHERE length(c.body) > 20 {bot_exclude}
        LIMIT 500
    """).fetchall()
    se_without_pool = conn.execute(f"""
        SELECT c.id, c.created_utc
        FROM comments c
        WHERE c.id NOT IN (SELECT source_id FROM side_effects WHERE source_type = 'comment')
          AND length(c.body) > 20 {bot_exclude}
        LIMIT 500
    """).fetchall()
    se_with = _time_stratified_sample(se_with_pool, min(48, len(se_with_pool)))
    se_without = _time_stratified_sample(se_without_pool, min(32, len(se_without_pool)))
    for r in list(se_with) + list(se_without):
        conn.execute(
            "INSERT OR IGNORE INTO validation_core_items (facet, item_id, item_type) VALUES ('side_effects', ?, 'comment')",
            (r['id'],))
    log.info(f"  Side effects core: {len(se_with) + len(se_without)} comments ({len(se_with)} with, {len(se_without)} without)")

    # --- Sentiment: 80 comments, 16 neg + 16 neutral + 16 pos + 32 random ---
    sent_base = f"""
        SELECT DISTINCT c.id, c.created_utc, c.sentiment
        FROM comments c
        JOIN posts p ON p.id = c.post_id
        JOIN medication_mentions m ON m.post_id = p.id
        WHERE c.sentiment IS NOT NULL AND length(c.body) > 20
        {bot_exclude}
    """
    neg_pool = conn.execute(sent_base + " AND c.sentiment <= 2 LIMIT 200").fetchall()
    neut_pool = conn.execute(sent_base + " AND c.sentiment = 3 LIMIT 200").fetchall()
    pos_pool = conn.execute(sent_base + " AND c.sentiment >= 4 LIMIT 200").fetchall()
    all_pool = conn.execute(sent_base + " LIMIT 500").fetchall()

    neg = _time_stratified_sample(neg_pool, min(16, len(neg_pool)))
    neut = _time_stratified_sample(neut_pool, min(16, len(neut_pool)))
    pos = _time_stratified_sample(pos_pool, min(16, len(pos_pool)))
    got_ids = {r['id'] for r in list(neg) + list(neut) + list(pos)}
    remaining_pool = [r for r in all_pool if r['id'] not in got_ids]
    rand = _time_stratified_sample(remaining_pool, min(32, len(remaining_pool)))
    for r in list(neg) + list(neut) + list(pos) + list(rand):
        conn.execute(
            "INSERT OR IGNORE INTO validation_core_items (facet, item_id, item_type) VALUES ('sentiment', ?, 'comment')",
            (r['id'],))
    log.info(f"  Sentiment core: {len(neg) + len(neut) + len(pos) + len(rand)} comments")

    conn.commit()
    log.info("Core validation items selected.")


def get_validation_batch(conn: sqlite3.Connection, validator: str,
                         count: int = 10) -> list:
    """Get posts for treatment beliefs validation using tiered pool strategy.

    Tier 1 (core): pre-selected items shown to ALL validators for inter-rater
    reliability. Up to 3 core items per batch, no retirement cap.
    Tier 2 (exploration): remaining pool, retired after 2 votes.
    """
    # --- Tier 1: Core items (no retirement cap) ---
    core_pool = conn.execute("""
        SELECT DISTINCT p.id, p.title, substr(p.selftext, 1, 2000) as selftext,
               p.subreddit, p.created_utc
        FROM posts p
        JOIN validation_core_items vci ON vci.item_id = p.id AND vci.facet = 'beliefs'
        WHERE p.id NOT IN (SELECT post_id FROM validation_votes WHERE validator = ?)
          AND length(p.selftext) > 30
    """, (validator,)).fetchall()
    core_sample = _time_stratified_sample(core_pool, min(3, len(core_pool)))

    # --- Tier 2: Exploration items (retire after 2 votes) ---
    remaining = count - len(core_sample)
    explore_cap = "AND (SELECT COUNT(*) FROM validation_votes WHERE post_id = p.id) < 2"
    core_exclude = "AND p.id NOT IN (SELECT item_id FROM validation_core_items WHERE facet = 'beliefs')"
    half = remaining // 2
    other_half = remaining - half

    flagged_pool = conn.execute(f"""
        SELECT DISTINCT p.id, p.title, substr(p.selftext, 1, 2000) as selftext,
               p.subreddit, p.created_utc
        FROM posts p
        JOIN treatment_beliefs tb ON tb.source_type = 'post' AND tb.source_id = p.id
        JOIN medication_mentions mm ON mm.post_id = p.id
        WHERE p.id NOT IN (SELECT post_id FROM validation_votes WHERE validator = ?)
          AND length(p.selftext) > 30
          {explore_cap}
          {core_exclude}
        LIMIT 300
    """, (validator,)).fetchall()
    flagged = _time_stratified_sample(flagged_pool, half)

    unflagged_pool = conn.execute(f"""
        SELECT DISTINCT p.id, p.title, substr(p.selftext, 1, 2000) as selftext,
               p.subreddit, p.created_utc
        FROM posts p
        JOIN medication_mentions mm ON mm.post_id = p.id
        WHERE p.id NOT IN (SELECT source_id FROM treatment_beliefs WHERE source_type = 'post')
          AND p.id NOT IN (SELECT post_id FROM validation_votes WHERE validator = ?)
          AND length(p.selftext) > 30
          {explore_cap}
          {core_exclude}
        LIMIT 300
    """, (validator,)).fetchall()
    unflagged = _time_stratified_sample(unflagged_pool, other_half)

    # Backfill if one side is short
    if len(flagged) < half and len(unflagged_pool) > len(unflagged):
        extra_pool = [r for r in unflagged_pool if r['id'] not in {x['id'] for x in unflagged}]
        unflagged = list(unflagged) + list(_time_stratified_sample(extra_pool, half - len(flagged)))
    elif len(unflagged) < other_half and len(flagged_pool) > len(flagged):
        extra_pool = [r for r in flagged_pool if r['id'] not in {x['id'] for x in flagged}]
        flagged = list(flagged) + list(_time_stratified_sample(extra_pool, other_half - len(unflagged)))

    result = []
    for r in list(core_sample) + list(flagged) + list(unflagged):
        claims = conn.execute(
            "SELECT belief, stance FROM treatment_beliefs WHERE source_type = 'post' AND source_id = ?",
            (r["id"],)).fetchall()
        system_flagged = 1 if claims else 0
        system_claims = json.dumps([{"belief": c["belief"], "stance": c["stance"]} for c in claims])
        result.append({
            "id": r["id"], "title": r["title"], "selftext": r["selftext"],
            "subreddit": r["subreddit"], "system_flagged": system_flagged,
            "system_claims": system_claims,
        })
    _random.shuffle(result)
    return result


def save_validation_votes(conn: sqlite3.Connection, validator: str, votes: list) -> None:
    now = datetime.utcnow().isoformat()
    for v in votes:
        claims = conn.execute(
            "SELECT belief FROM treatment_beliefs WHERE source_type = 'post' AND source_id = ?",
            (v["post_id"],)).fetchall()
        system_flagged = 1 if claims else 0
        system_claims = json.dumps([c["belief"] for c in claims])
        conn.execute(
            """INSERT INTO validation_votes (post_id, validator, system_flagged, human_flagged, human_stance, reason, system_claims, voted_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(post_id, validator) DO UPDATE SET
                   human_flagged = excluded.human_flagged, human_stance = excluded.human_stance,
                   reason = excluded.reason, voted_at = excluded.voted_at""",
            (v["post_id"], validator, system_flagged, v.get("human_flagged", 0),
             v.get("human_stance", ""), v.get("reason", ""), system_claims, now),
        )
    conn.commit()


def query_validation_stats(conn: sqlite3.Connection) -> dict:
    """Compute validation statistics: precision/recall/F1 and Scott's Pi.

    Scott's Pi is computed over posts rated by 2+ validators (multi-rater
    inter-rater reliability). Unlike the marginal approach, this counts observed
    agreement per post pair and accounts for marginal chance agreement.
    """
    rows = conn.execute(
        "SELECT system_flagged, human_flagged, system_claims, post_id, validator FROM validation_votes"
    ).fetchall()
    if not rows:
        empty = {'agreement_rate': None, 'precision': None, 'recall': None, 'f1': None,
                 'confusion': {'tp': 0, 'fp': 0, 'fn': 0, 'tn': 0}, 'n': 0}
        return {
            'total_votes': 0, 'total_validators': 0,
            'agreement_rate': None, 'precision': None, 'recall': None, 'f1': None,
            'confusion_matrix': {'tp': 0, 'fp': 0, 'fn': 0, 'tn': 0},
            'per_claim': [], 'scotts_pi': None, 'recent_disagreements': [],
        }

    total = len(rows)
    tp = sum(1 for r in rows if r['system_flagged'] == 1 and r['human_flagged'] == 1)
    fp = sum(1 for r in rows if r['system_flagged'] == 1 and r['human_flagged'] == 0)
    fn = sum(1 for r in rows if r['system_flagged'] == 0 and r['human_flagged'] == 1)
    tn = sum(1 for r in rows if r['system_flagged'] == 0 and r['human_flagged'] == 0)
    agree = tp + tn
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
    agreement_rate = agree / total if total else 0

    # Per-claim accuracy
    claim_stats: dict = {}
    for r in rows:
        if r['system_claims']:
            try:
                claims_data = json.loads(r['system_claims'])
            except (json.JSONDecodeError, TypeError):
                claims_data = []
            for item in claims_data:
                belief = item.get('belief') if isinstance(item, dict) else str(item)
                if belief:
                    if belief not in claim_stats:
                        claim_stats[belief] = {'agreed': 0, 'total': 0}
                    claim_stats[belief]['total'] += 1
                    if r['human_flagged'] == 1:
                        claim_stats[belief]['agreed'] += 1
    per_claim = [
        {'claim': c, 'agreed': s['agreed'], 'total': s['total'],
         'accuracy': round(s['agreed'] / s['total'], 3) if s['total'] else 0}
        for c, s in sorted(claim_stats.items(), key=lambda x: -x[1]['total'])
    ]

    # Scott's Pi — only posts rated by 2+ validators (multi-rater reliability)
    post_votes: dict = {}
    for r in rows:
        pid = r['post_id']
        if pid not in post_votes:
            post_votes[pid] = []
        post_votes[pid].append(r['human_flagged'])

    multi_rated = {pid: votes for pid, votes in post_votes.items() if len(votes) >= 2}
    scotts_pi = None
    if multi_rated:
        n_pairs = 0
        observed_agree = 0
        total_yes = 0
        total_no = 0
        total_judgments = 0
        for pid, votes in multi_rated.items():
            for i in range(len(votes)):
                for j in range(i + 1, len(votes)):
                    n_pairs += 1
                    if votes[i] == votes[j]:
                        observed_agree += 1
            for v in votes:
                total_judgments += 1
                if v == 1:
                    total_yes += 1
                else:
                    total_no += 1
        if n_pairs > 0 and total_judgments > 0:
            p_observed = observed_agree / n_pairs
            p_yes = total_yes / total_judgments
            p_no = total_no / total_judgments
            p_expected = p_yes ** 2 + p_no ** 2
            if p_expected < 1:
                scotts_pi = round((p_observed - p_expected) / (1 - p_expected), 3)

    # Recent disagreements
    disagreements = conn.execute("""
        SELECT vv.post_id, vv.validator, vv.system_flagged, vv.human_flagged,
               vv.reason, vv.system_claims, vv.voted_at, p.title
        FROM validation_votes vv
        JOIN posts p ON p.id = vv.post_id
        WHERE vv.system_flagged != vv.human_flagged
        ORDER BY vv.voted_at DESC LIMIT 10
    """).fetchall()

    total_validators = conn.execute(
        "SELECT COUNT(DISTINCT validator) FROM validation_votes"
    ).fetchone()[0]

    return {
        'total_votes': total,
        'total_validators': total_validators,
        'agreement_rate': round(agreement_rate * 100, 1),
        'precision': round(precision, 3),
        'recall': round(recall, 3),
        'f1': round(f1, 3),
        'confusion_matrix': {'tp': tp, 'fp': fp, 'fn': fn, 'tn': tn},
        'per_claim': per_claim,
        'scotts_pi': scotts_pi,
        'recent_disagreements': [
            {'post_id': d['post_id'], 'validator': d['validator'],
             'system_flagged': d['system_flagged'], 'human_flagged': d['human_flagged'],
             'reason': d['reason'], 'system_claims': d['system_claims'],
             'voted_at': d['voted_at'], 'title': d['title']}
            for d in disagreements
        ],
    }


def get_side_effect_batch(conn: sqlite3.Connection, validator: str,
                          count: int = 5) -> list:
    """Get a batch of comments for side-effect validation using tiered pool.

    Tier 1 (core): up to 3 pre-selected items per batch, no retirement cap.
    Tier 2 (exploration): fills rest with 60/40 with/without effects, retires at 2.
    """
    bot_exclude = """AND (c.author IS NULL OR (
          LOWER(c.author) NOT IN ({known})
          AND c.author NOT GLOB '*[Bb]ot'
          AND c.author NOT GLOB '*-[Bb]ot'
          AND c.author NOT GLOB '*_[Bb]ot'
        ))""".format(known=",".join(f"'{a}'" for a in _KNOWN_BOT_AUTHORS))

    # --- Tier 1: Core items (no retirement cap) ---
    core_pool = conn.execute(f"""
        SELECT DISTINCT c.id as comment_id, substr(c.body, 1, 2000) as body, c.post_id,
               p.title as post_title, p.subreddit, c.created_utc
        FROM comments c
        JOIN validation_core_items vci ON vci.item_id = c.id AND vci.facet = 'side_effects'
        JOIN posts p ON p.id = c.post_id
        WHERE c.id NOT IN (SELECT comment_id FROM side_effect_votes WHERE validator = ?)
          AND length(c.body) > 20
          {bot_exclude}
    """, (validator,)).fetchall()
    core_sample = _time_stratified_sample(core_pool, min(3, len(core_pool)))

    # --- Tier 2: Exploration items (retire after 2 votes) ---
    remaining = count - len(core_sample)
    explore_cap = "AND (SELECT COUNT(*) FROM side_effect_votes WHERE comment_id = c.id) < 2"
    core_exclude = "AND c.id NOT IN (SELECT item_id FROM validation_core_items WHERE facet = 'side_effects')"
    with_count = (remaining * 3 + 4) // 5
    without_count = remaining - with_count

    with_pool = conn.execute(f"""
        SELECT DISTINCT c.id as comment_id, substr(c.body, 1, 2000) as body, c.post_id,
               p.title as post_title, p.subreddit, c.created_utc
        FROM comments c
        JOIN side_effects se ON se.source_type = 'comment' AND se.source_id = c.id
        JOIN posts p ON p.id = c.post_id
        WHERE c.id NOT IN (SELECT comment_id FROM side_effect_votes WHERE validator = ?)
          AND length(c.body) > 20
          {explore_cap}
          {core_exclude}
          {bot_exclude}
        LIMIT 300
    """, (validator,)).fetchall()
    with_effects = _time_stratified_sample(with_pool, with_count)

    without_pool = conn.execute(f"""
        SELECT c.id as comment_id, substr(c.body, 1, 2000) as body, c.post_id,
               p.title as post_title, p.subreddit, c.created_utc
        FROM comments c
        JOIN posts p ON p.id = c.post_id
        WHERE c.id NOT IN (SELECT source_id FROM side_effects WHERE source_type = 'comment')
          AND c.id NOT IN (SELECT comment_id FROM side_effect_votes WHERE validator = ?)
          AND length(c.body) > 20
          {explore_cap}
          {core_exclude}
          {bot_exclude}
        LIMIT 300
    """, (validator,)).fetchall()
    without_effects = _time_stratified_sample(without_pool, without_count)

    # Backfill if either type runs short
    if len(with_effects) < with_count and len(without_pool) > len(without_effects):
        extra = [r for r in without_pool if r['comment_id'] not in {x['comment_id'] for x in without_effects}]
        without_effects = list(without_effects) + list(_time_stratified_sample(extra, with_count - len(with_effects)))
    elif len(without_effects) < without_count and len(with_pool) > len(with_effects):
        extra = [r for r in with_pool if r['comment_id'] not in {x['comment_id'] for x in with_effects}]
        with_effects = list(with_effects) + list(_time_stratified_sample(extra, without_count - len(without_effects)))

    result = []
    for r in list(core_sample) + list(with_effects) + list(without_effects):
        sys_effects = conn.execute(
            "SELECT effect FROM side_effects WHERE source_type = 'comment' AND source_id = ?",
            (r["comment_id"],)).fetchall()
        result.append({
            "comment_id": r["comment_id"], "body": r["body"],
            "post_id": r["post_id"], "post_title": r["post_title"],
            "subreddit": r["subreddit"],
            "system_effects": json.dumps([e["effect"] for e in sys_effects]),
        })
    _random.shuffle(result)
    return result


def save_side_effect_votes(conn: sqlite3.Connection, validator: str, votes: list) -> None:
    now = datetime.utcnow().isoformat()
    for v in votes:
        sys_effects = conn.execute(
            "SELECT effect FROM side_effects WHERE source_type = 'comment' AND source_id = ?",
            (v["comment_id"],)).fetchall()
        conn.execute(
            """INSERT INTO side_effect_votes (comment_id, validator, system_effects, human_effects, other_effect, voted_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(comment_id, validator) DO UPDATE SET
                   human_effects = excluded.human_effects, other_effect = excluded.other_effect, voted_at = excluded.voted_at""",
            (v["comment_id"], validator, json.dumps([e["effect"] for e in sys_effects]),
             json.dumps(v.get("human_effects", [])), v.get("other_effect", ""), now),
        )
    conn.commit()


def query_side_effect_validation_stats(conn: sqlite3.Connection) -> dict:
    rows = conn.execute("SELECT * FROM side_effect_votes").fetchall()
    if not rows:
        return {"total_votes": 0}
    jaccard_sum = 0.0
    per_effect = {}
    for r in rows:
        sys_set = set(json.loads(r["system_effects"] or "[]"))
        hum_set = set(json.loads(r["human_effects"] or "[]"))
        union = sys_set | hum_set
        inter = sys_set & hum_set
        if union:
            jaccard_sum += len(inter) / len(union)
        for eff in sys_set | hum_set:
            if eff not in per_effect:
                per_effect[eff] = {"tp": 0, "fp": 0, "fn": 0}
            if eff in inter:
                per_effect[eff]["tp"] += 1
            elif eff in sys_set:
                per_effect[eff]["fp"] += 1
            else:
                per_effect[eff]["fn"] += 1
    stats = {}
    for eff, c in per_effect.items():
        p = c["tp"] / (c["tp"] + c["fp"]) if (c["tp"] + c["fp"]) > 0 else 0
        r = c["tp"] / (c["tp"] + c["fn"]) if (c["tp"] + c["fn"]) > 0 else 0
        f = 2 * p * r / (p + r) if (p + r) > 0 else 0
        stats[eff] = {"precision": round(p, 3), "recall": round(r, 3), "f1": round(f, 3)}
    return {
        "total_votes": len(rows),
        "avg_jaccard": round(jaccard_sum / len(rows), 3),
        "per_effect_stats": stats,
    }


def get_sentiment_batch(conn: sqlite3.Connection, validator: str,
                        count: int = 5) -> list:
    """Get a batch of comments for sentiment validation using tiered pool.

    Tier 1 (core): up to 3 pre-selected items per batch, no retirement cap.
    Tier 2 (exploration): sentiment-balanced (neg/neutral/pos), retires at 2.
    """
    bot_exclude = """AND (c.author IS NULL OR (
          LOWER(c.author) NOT IN ({known})
          AND c.author NOT GLOB '*[Bb]ot'
          AND c.author NOT GLOB '*-[Bb]ot'
          AND c.author NOT GLOB '*_[Bb]ot'
        ))""".format(known=",".join(f"'{a}'" for a in _KNOWN_BOT_AUTHORS))

    # --- Tier 1: Core items (no retirement cap) ---
    core_pool = conn.execute(f"""
        SELECT DISTINCT c.id as comment_id, substr(c.body, 1, 2000) as body, c.post_id,
               p.title as post_title, p.subreddit, c.sentiment as system_score, c.created_utc
        FROM comments c
        JOIN validation_core_items vci ON vci.item_id = c.id AND vci.facet = 'sentiment'
        JOIN posts p ON p.id = c.post_id
        JOIN medication_mentions m ON m.post_id = p.id
        WHERE c.id NOT IN (SELECT comment_id FROM sentiment_votes WHERE validator = ?)
          AND c.sentiment IS NOT NULL
          AND length(c.body) > 20
          {bot_exclude}
    """, (validator,)).fetchall()
    core_sample = _time_stratified_sample(core_pool, min(3, len(core_pool)))

    # --- Tier 2: Exploration items (retire after 2 votes, sentiment-balanced) ---
    remaining = count - len(core_sample)
    explore_cap = "AND (SELECT COUNT(*) FROM sentiment_votes WHERE comment_id = c.id) < 2"
    core_exclude = "AND c.id NOT IN (SELECT item_id FROM validation_core_items WHERE facet = 'sentiment')"
    base = f"""
        SELECT DISTINCT c.id as comment_id, substr(c.body, 1, 2000) as body, c.post_id,
               p.title as post_title, p.subreddit, c.sentiment as system_score, c.created_utc
        FROM comments c JOIN posts p ON p.id = c.post_id
        JOIN medication_mentions m ON m.post_id = p.id
        WHERE c.id NOT IN (SELECT comment_id FROM sentiment_votes WHERE validator = ?)
          AND c.sentiment IS NOT NULL
          AND length(c.body) > 20
          {explore_cap}
          {core_exclude}
          {bot_exclude}
    """

    # Sentiment-balanced: 1 neg (1-2) + 1 neutral (3) + 1 pos (4-5) + rest random
    neg_pool = conn.execute(base + " AND c.sentiment <= 2 LIMIT 50", (validator,)).fetchall()
    neut_pool = conn.execute(base + " AND c.sentiment = 3 LIMIT 50", (validator,)).fetchall()
    pos_pool = conn.execute(base + " AND c.sentiment >= 4 LIMIT 50", (validator,)).fetchall()

    negative = _time_stratified_sample(neg_pool, 1)
    neutral = _time_stratified_sample(neut_pool, 1)
    positive = _time_stratified_sample(pos_pool, 1)

    got = list(negative) + list(neutral) + list(positive)
    got_ids = {r['comment_id'] for r in got}
    explore_remaining = remaining - len(got)

    if explore_remaining > 0:
        exclude = ','.join('?' * len(got_ids)) if got_ids else "''"
        extra_sql = base + (f" AND c.id NOT IN ({exclude})" if got_ids else "") + " LIMIT 300"
        extra_params = (validator, *list(got_ids)) if got_ids else (validator,)
        extra_pool = conn.execute(extra_sql, extra_params).fetchall()
        got = got + _time_stratified_sample(extra_pool, explore_remaining)

    result = list(core_sample) + got
    _random.shuffle(result)
    return [
        {
            "comment_id": r["comment_id"], "body": r["body"],
            "post_id": r["post_id"], "post_title": r["post_title"],
            "subreddit": r["subreddit"], "system_score": r["system_score"],
        }
        for r in result
    ]


def save_sentiment_votes(conn: sqlite3.Connection, validator: str, votes: list) -> None:
    now = datetime.utcnow().isoformat()
    for v in votes:
        sys_score = conn.execute(
            "SELECT sentiment FROM comments WHERE id = ?",
            (v["comment_id"],)).fetchone()
        conn.execute(
            """INSERT INTO sentiment_votes (comment_id, validator, system_score, human_score, voted_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(comment_id, validator) DO UPDATE SET
                   human_score = excluded.human_score, voted_at = excluded.voted_at""",
            (v["comment_id"], validator, sys_score[0] if sys_score else None,
             v["human_score"], now),
        )
    conn.commit()


def query_sentiment_validation_stats(conn: sqlite3.Connection) -> dict:
    rows = conn.execute(
        "SELECT system_score, human_score FROM sentiment_votes WHERE system_score IS NOT NULL"
    ).fetchall()
    if not rows:
        return {"total_votes": 0}
    sys_scores = [r["system_score"] for r in rows]
    hum_scores = [r["human_score"] for r in rows]
    n = len(rows)
    mae = sum(abs(s - h) for s, h in zip(sys_scores, hum_scores)) / n
    s_mean = sum(sys_scores) / n
    h_mean = sum(hum_scores) / n
    num = sum((s - s_mean) * (h - h_mean) for s, h in zip(sys_scores, hum_scores))
    den_s = sum((s - s_mean) ** 2 for s in sys_scores) ** 0.5
    den_h = sum((h - h_mean) ** 2 for h in hum_scores) ** 0.5
    pearson_r = num / (den_s * den_h) if den_s * den_h > 0 else 0
    return {
        "total_votes": n,
        "pearson_r": round(pearson_r, 3),
        "mae": round(mae, 3),
    }


# ---------------------------------------------------------------------------
# Data export (for static site generation)
# ---------------------------------------------------------------------------

def export_all_data(conn: sqlite3.Connection) -> dict:
    return {
        "medications": query_medication_counts(conn),
        "medication_sentiment": query_medication_sentiment(conn),
        "medication_daily": query_daily_medication_counts(conn),
        "side_effects": query_side_effect_counts(conn),
        "beliefs": query_belief_counts(conn),
        "belief_stance": query_belief_stance_counts(conn),
        "ed_discourse": query_ed_discourse_counts(conn),
        "ed_discourse_daily": query_ed_discourse_daily(conn),
        "triggers": query_trigger_counts(conn),
        "trigger_daily": query_trigger_daily(conn),
        "caregiver": query_caregiver_counts(conn),
        "caregiver_daily": query_caregiver_daily(conn),
        "singulair_effects": query_singulair_effect_counts(conn),
        "singulair_daily": query_singulair_daily(conn),
        "singulair_discourse": query_singulair_discourse_counts(conn),
        "corticosteroid_effects": query_corticosteroid_effect_counts(conn),
        "corticosteroid_daily": query_corticosteroid_daily(conn),
        "functional_impact": query_functional_impact_counts(conn),
        "functional_impact_daily": query_functional_impact_daily(conn),
        "inhaler_confusion": query_inhaler_confusion_counts(conn),
        "inhaler_confusion_daily": query_inhaler_confusion_daily(conn),
        "ed_subcategories": query_ed_subcategory_counts(conn),
        "caregiver_ed": query_caregiver_ed_linked(conn),
        "post_ed_outcome": query_post_ed_outcome_counts(conn),
        "stats": query_db_stats(conn),
        "validation": query_validation_stats(conn),
    }


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------

def backfill_sentiment_and_effects(conn: sqlite3.Connection) -> None:
    """Backfill posts missing sentiment, fear, engagement, or analysis."""
    # Sentiment + fear backfill
    rows = conn.execute(
        "SELECT id, title, selftext FROM posts WHERE sentiment IS NULL AND selftext != ''"
    ).fetchall()
    if rows:
        log.info(f"Backfilling sentiment/fear for {len(rows)} posts...")
        for r in rows:
            text = f"{r['title']} {r['selftext']}"
            sent = score_sentiment(text)
            fear = score_fear(text)
            conn.execute("UPDATE posts SET sentiment = ?, fear_score = ? WHERE id = ?",
                         (sent, fear, r["id"]))
        conn.commit()

    # Engagement backfill
    rows = conn.execute(
        "SELECT id, score, num_comments FROM posts WHERE engagement_score = 0 OR engagement_score IS NULL"
    ).fetchall()
    if rows:
        log.info(f"Backfilling engagement for {len(rows)} posts...")
        for r in rows:
            eng = compute_engagement(r["score"] or 0, r["num_comments"] or 0)
            conn.execute("UPDATE posts SET engagement_score = ? WHERE id = ?", (eng, r["id"]))
        conn.commit()

    # Re-run analysis for expanded keywords + new modules
    rows = conn.execute(
        "SELECT id, title, selftext, subreddit FROM posts WHERE selftext != '' AND crosspost_parent IS NULL"
    ).fetchall()
    new_meds = 0
    new_beliefs = 0
    new_peds_promoted = 0
    new_cortico = 0
    new_functional = 0
    new_inhaler = 0
    new_ed_subcat = 0
    new_ed_outcome = 0
    new_caregiver_ed = 0
    for r in rows:
        text = f"{r['title']} {r['selftext']}"
        peds_conf = classify_pediatric_confidence(text, r["subreddit"] or "")

        # Update pediatric confidence if changed
        conn.execute(
            "UPDATE posts SET pediatric_confidence = ? WHERE id = ? AND pediatric_confidence != ?",
            (peds_conf, r["id"], peds_conf))
        if conn.execute("SELECT changes()").fetchone()[0]:
            new_peds_promoted += 1

        if peds_conf == "none":
            continue

        for med in find_medications(text):
            med_class = MEDICATION_CLASSES.get(med, "")
            cur = conn.execute(
                "INSERT OR IGNORE INTO medication_mentions (post_id, medication, med_class) VALUES (?, ?, ?)",
                (r["id"], med, med_class))
            if cur.rowcount:
                new_meds += 1
        for belief, stance in find_treatment_beliefs_with_stance(text):
            cur = conn.execute(
                """INSERT INTO treatment_beliefs (source_type, source_id, belief, stance)
                   VALUES ('post', ?, ?, ?)
                   ON CONFLICT(source_type, source_id, belief) DO UPDATE SET stance = excluded.stance""",
                (r["id"], belief, stance))
            if cur.rowcount:
                new_beliefs += 1

        # Corticosteroid effects backfill
        if _CORTICOSTEROID_PATTERN.search(text):
            for eff in find_corticosteroid_effects(text):
                cur = conn.execute(
                    "INSERT OR IGNORE INTO corticosteroid_effects (source_type, source_id, effect) VALUES ('post', ?, ?)",
                    (r["id"], eff))
                if cur.rowcount:
                    new_cortico += 1

        # Functional impact backfill
        for cat in find_functional_impact(text):
            cur = conn.execute(
                "INSERT OR IGNORE INTO functional_impact (source_type, source_id, category) VALUES ('post', ?, ?)",
                (r["id"], cat))
            if cur.rowcount:
                new_functional += 1

        # Inhaler confusion backfill
        for cat in find_inhaler_confusion(text):
            cur = conn.execute(
                "INSERT OR IGNORE INTO inhaler_confusion (source_type, source_id, category) VALUES ('post', ?, ?)",
                (r["id"], cat))
            if cur.rowcount:
                new_inhaler += 1

        # ED-related backfill (subcategories, caregiver linkage, post-ED outcome)
        ed_cats = find_ed_discourse(text)
        has_ed = bool(ed_cats)

        # Update caregiver ed_related flag
        if has_ed:
            cur = conn.execute(
                "UPDATE caregiver_sentiment SET ed_related = 1 WHERE source_type = 'post' AND source_id = ? AND ed_related = 0",
                (r["id"],))
            if cur.rowcount:
                new_caregiver_ed += cur.rowcount

        # Post-visit subcategories
        if "post_visit" in ed_cats:
            for subcat in find_post_visit_subcategories(text):
                cur = conn.execute(
                    "INSERT OR IGNORE INTO ed_subcategories (source_type, source_id, subcategory) VALUES ('post', ?, ?)",
                    (r["id"], subcat))
                if cur.rowcount:
                    new_ed_subcat += 1

        # Post-ED outcome
        if has_ed:
            for outcome in find_post_ed_outcome(text):
                cur = conn.execute(
                    "INSERT OR IGNORE INTO post_ed_outcome (source_type, source_id, outcome) VALUES ('post', ?, ?)",
                    (r["id"], outcome))
                if cur.rowcount:
                    new_ed_outcome += 1

    conn.commit()
    if new_meds:
        log.info(f"  Found {new_meds} new medication mentions from backfill.")
    if new_beliefs:
        log.info(f"  Found {new_beliefs} new treatment beliefs from backfill.")
    if new_peds_promoted:
        log.info(f"  Reclassified {new_peds_promoted} posts (pediatric confidence).")
    if new_cortico:
        log.info(f"  Found {new_cortico} new corticosteroid effects from backfill.")
    if new_functional:
        log.info(f"  Found {new_functional} new functional impact entries from backfill.")
    if new_inhaler:
        log.info(f"  Found {new_inhaler} new inhaler confusion entries from backfill.")
    if new_ed_subcat:
        log.info(f"  Found {new_ed_subcat} new post-visit subcategories from backfill.")
    if new_ed_outcome:
        log.info(f"  Found {new_ed_outcome} new post-ED outcomes from backfill.")
    if new_caregiver_ed:
        log.info(f"  Updated {new_caregiver_ed} caregiver entries with ED linkage.")


# ---------------------------------------------------------------------------
# Backup
# ---------------------------------------------------------------------------

def backup_db() -> Optional[str]:
    if not _backup_env:
        dropbox_root = BACKUP_DIR.parent.parent
        if not dropbox_root.exists():
            return None
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    dest = BACKUP_DIR / f"tracker-{today}.db"
    src_conn = sqlite3.connect(str(DB_FILE))
    dst_conn = sqlite3.connect(str(dest))
    src_conn.backup(dst_conn)
    dst_conn.close()
    src_conn.close()
    project_dir = Path(__file__).parent
    for fname in ["asthma_tracker.py", "asthma_tracker_web.py", "CLAUDE.md"]:
        src = project_dir / fname
        if src.exists():
            shutil.copy2(str(src), str(BACKUP_DIR / fname))
    backups = sorted(BACKUP_DIR.glob("tracker-*.db"), reverse=True)
    for old in backups[7:]:
        old.unlink()
    return str(dest)


# ---------------------------------------------------------------------------
# Reddit scraping
# ---------------------------------------------------------------------------

def _get_oauth_token() -> Optional[str]:
    global _oauth_token, _oauth_expires
    if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
        return None
    if _oauth_token and time.time() < _oauth_expires - 60:
        return _oauth_token
    try:
        import base64
        url = "https://www.reddit.com/api/v1/access_token"
        data = "grant_type=client_credentials".encode()
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("User-Agent", USER_AGENT)
        credentials = base64.b64encode(
            f"{REDDIT_CLIENT_ID}:{REDDIT_CLIENT_SECRET}".encode()
        ).decode()
        req.add_header("Authorization", f"Basic {credentials}")
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read().decode())
        _oauth_token = result.get("access_token")
        expires_in = result.get("expires_in", 3600)
        _oauth_expires = time.time() + expires_in
        log.info(f"Reddit OAuth token acquired (expires in {expires_in}s)")
        return _oauth_token
    except Exception as e:
        log.error(f"Failed to get Reddit OAuth token: {e}")
        return None


def fetch_json(url: str) -> dict:
    token = _get_oauth_token()
    if token:
        url = url.replace("https://www.reddit.com", "https://oauth.reddit.com")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def scrape_subreddit(subreddit: str, limit: int = 200,
                     sort: str = "new") -> list:
    posts = []
    base = f"https://www.reddit.com/r/{subreddit}/{sort}.json"
    after = None
    seen_ids = set()
    while len(posts) < limit:
        url = f"{base}?limit=100&raw_json=1"
        if after:
            url += f"&after={after}"
        try:
            data = fetch_json(url)
        except urllib.error.HTTPError as e:
            log.error(f"HTTP {e.code} scraping r/{subreddit}/{sort}: {e.reason}")
            break
        except urllib.error.URLError as e:
            log.error(f"Network error scraping r/{subreddit}/{sort}: {e.reason}")
            break
        children = data.get("data", {}).get("children", [])
        if not children:
            break
        for child in children:
            d = child["data"]
            if d["id"] in seen_ids:
                continue
            seen_ids.add(d["id"])
            xpost_list = d.get("crosspost_parent_list", [])
            xpost_parent = xpost_list[0].get("id") if xpost_list else None
            posts.append({
                "id": d["id"], "title": d.get("title", ""),
                "selftext": d.get("selftext", ""),
                "created_utc": d.get("created_utc", 0),
                "score": d.get("score", 0),
                "num_comments": d.get("num_comments", 0),
                "permalink": d.get("permalink", ""),
                "subreddit": subreddit,
                "sort_source": sort,
                "crosspost_parent": xpost_parent,
            })
        after = data["data"].get("after")
        if not after:
            break
        time.sleep(1.5)
    return posts


def scrape_comments_for_post(post_id: str, permalink: str) -> list:
    url = f"https://www.reddit.com{permalink}.json?raw_json=1&limit=200"
    try:
        data = fetch_json(url)
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        log.error(f"Comment fetch failed for {post_id}: {e}")
        return []

    comments = []
    if len(data) < 2:
        return comments

    def walk_tree(node):
        if isinstance(node, dict):
            kind = node.get("kind")
            if kind == "t1":
                d = node.get("data", {})
                if d.get("body") and d.get("body") != "[deleted]":
                    comments.append({
                        "id": d["id"],
                        "body": d.get("body", ""),
                        "score": d.get("score", 0),
                        "created_utc": d.get("created_utc", 0),
                        "author": d.get("author", ""),
                    })
                replies = d.get("replies")
                if isinstance(replies, dict):
                    walk_tree(replies)
            elif kind == "Listing":
                for child in node.get("data", {}).get("children", []):
                    walk_tree(child)

    walk_tree(data[1])
    return comments


def scrape_comments_batch(conn: sqlite3.Connection, limit: int = 50) -> int:
    rows = conn.execute("""
        SELECT DISTINCT p.id, p.permalink
        FROM posts p JOIN medication_mentions mm ON mm.post_id = p.id
        WHERE p.comments_scraped = 0 AND p.permalink != ''
        ORDER BY p.created_utc DESC LIMIT ?
    """, (limit,)).fetchall()

    if not rows:
        return 0

    total_new = 0
    print(f"Scraping comments for {len(rows)} posts...")
    for i, row in enumerate(rows):
        comments = scrape_comments_for_post(row["id"], row["permalink"])
        if comments:
            n = save_comments_to_db(conn, row["id"], comments)
            total_new += n
            print(f"  [{i+1}/{len(rows)}] {row['id']}: {len(comments)} comments ({n} new)")
        else:
            conn.execute("UPDATE posts SET comments_scraped = 1 WHERE id = ?", (row["id"],))
            conn.commit()
        time.sleep(1.5)

    return total_new


# ---------------------------------------------------------------------------
# Post analysis
# ---------------------------------------------------------------------------

def analyze_posts(posts: list) -> dict:
    """Return {post_id: [medication_names]} for posts that pass both gates."""
    result = {}
    for post in posts:
        combined = f"{post['title']} {post['selftext']}"
        if not passes_asthma_gate(combined):
            continue
        meds = find_medications(combined)
        if meds:
            result[post["id"]] = meds
    return result


# ---------------------------------------------------------------------------
# Public scrape function
# ---------------------------------------------------------------------------

def run_scrape(limit: int = 200, filter_today: bool = False) -> dict:
    now = datetime.utcnow()
    log.info(f"Starting peds-asthma scrape ({now.strftime('%Y-%m-%d %H:%M UTC')})...")

    conn = get_db()
    backfill_sentiment_and_effects(conn)

    total_fetched = 0
    total_new = 0
    all_mention_counts = Counter()
    error_count = 0

    for sub_config in SUBREDDITS:
        sub_name = sub_config["name"]
        sub_limit = sub_config["limit"]
        log.info(f"  Scraping r/{sub_name} ({sub_limit} /new + 50 /hot)...")

        try:
            posts_new = scrape_subreddit(subreddit=sub_name, limit=sub_limit, sort="new")
            posts_hot = scrape_subreddit(subreddit=sub_name, limit=min(sub_limit, 50), sort="hot")
            posts = posts_new + posts_hot
        except Exception as e:
            log.error(f"Failed to scrape r/{sub_name}: {e}")
            save_error_to_db(conn, sub_name, "scrape_failure", str(e))
            error_count += 1
            continue

        if not posts:
            log.info(f"    No posts from r/{sub_name}")
            continue

        if filter_today:
            day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            day_ts = day_start.timestamp()
            posts = [p for p in posts if p["created_utc"] >= day_ts]

        mention_map = analyze_posts(posts)
        new_count = save_posts_to_db(conn, posts, mention_map, subreddit=sub_name)
        total_fetched += len(posts)
        total_new += new_count

        for meds in mention_map.values():
            for m in meds:
                all_mention_counts[m] += 1

        log.info(f"    r/{sub_name}: {len(posts)} fetched, {new_count} new, {len(mention_map)} with mentions")

    # Scrape comments for posts with mentions
    new_comments = scrape_comments_batch(conn, limit=50)
    log.info(f"  {new_comments} new comments saved.")

    log.info(f"\nMedication mentions found:")
    for name, count in all_mention_counts.most_common():
        log.info(f"  {name:25s} {count}")

    stats = query_db_stats(conn)
    conn.close()

    log.info(f"Totals: {stats['total_posts']} posts, {stats['total_comments']} comments, {error_count} errors")

    bk = backup_db()
    if bk:
        log.info(f"Backup saved to {bk}")

    return {
        "ok": True,
        "posts_fetched": total_fetched,
        "new_posts": total_new,
        "new_comments": new_comments,
        "mention_counts": dict(all_mention_counts),
        "db_total_posts": stats["total_posts"],
        "db_total_comments": stats["total_comments"],
        "error_count": error_count,
    }


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------

def cmd_scrape(args: argparse.Namespace) -> None:
    run_scrape(limit=args.limit, filter_today=not args.all)


def cmd_report(args: argparse.Namespace) -> None:
    conn = get_db()
    backfill_sentiment_and_effects(conn)

    stats = query_db_stats(conn)
    if stats["total_posts"] == 0:
        print("No data yet. Run `python asthma_tracker.py scrape --all` first.")
        conn.close()
        return

    df = (datetime.utcnow() - timedelta(days=args.days)).timestamp() if args.days else None
    med_counts = query_medication_counts(conn, date_from=df)
    med_sent = query_medication_sentiment(conn, date_from=df)
    effects = query_side_effect_counts(conn, date_from=df)
    beliefs = query_belief_counts(conn, date_from=df)
    triggers = query_trigger_counts(conn, date_from=df)
    conn.close()

    print("=" * 70)
    print("  Pediatric Asthma Reddit Tracker — Report")
    print("=" * 70)
    print(f"  DB posts      : {stats['total_posts']}")
    print(f"  DB comments   : {stats['total_comments']}")
    print(f"  Peds definite : {stats['peds_definite']}")
    print(f"  Peds likely   : {stats['peds_likely']}")
    print(f"  Avg sentiment : {stats['avg_sentiment']}")
    print(f"  Avg fear      : {stats['avg_fear']}")
    print("-" * 70)

    if med_counts:
        max_count = med_counts[0][1]
        print(f"\n  {'Medication':<25s} {'Count':>5s}  Distribution")
        print(f"  {'-'*25} {'-'*5}  {'-'*30}")
        for name, count in med_counts[:20]:
            bar_len = int((count / max_count) * 30) if max_count else 0
            print(f"  {name:<25s} {count:>5d}  {'#' * bar_len}")

    if med_sent:
        print(f"\n  {'Medication':<25s} {'Sentiment':>9s}  {'Fear':>5s}  {'Posts':>5s}")
        print(f"  {'-'*25} {'-'*9}  {'-'*5}  {'-'*5}")
        for name, avg_s, avg_f, cnt in med_sent[:15]:
            ind = "+" if avg_s > 0 else ""
            print(f"  {name:<25s} {ind}{avg_s:>8.3f}  {avg_f:>5.3f}  {cnt:>5d}")

    if effects:
        print(f"\n  Top Side Effects:")
        print(f"  {'-'*40}")
        for effect, cnt in effects[:10]:
            print(f"  {effect:<30s} {cnt:>5d}")

    if beliefs:
        print(f"\n  Treatment Beliefs Detected:")
        print(f"  {'-'*40}")
        for belief, cnt in beliefs[:10]:
            print(f"  {belief:<40s} {cnt:>5d}")

    if triggers:
        print(f"\n  Top Triggers:")
        print(f"  {'-'*40}")
        for trig, cat, cnt in triggers[:10]:
            print(f"  {trig:<25s} ({cat:<15s}) {cnt:>5d}")

    print("=" * 70)

    if args.csv:
        csv_path = DATA_DIR / "report.csv"
        with open(csv_path, "w") as f:
            f.write("medication,mentions,avg_sentiment,avg_fear\n")
            for name, avg_s, avg_f, cnt in med_sent:
                f.write(f"{name},{cnt},{avg_s},{avg_f}\n")
        print(f"  CSV exported to {csv_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Track pediatric asthma discussions across subreddits"
    )
    sub = parser.add_subparsers(dest="command")

    sp_scrape = sub.add_parser("scrape", help="Scrape subreddits and save data")
    sp_scrape.add_argument("--limit", type=int, default=200)
    sp_scrape.add_argument("--all", action="store_true",
                           help="Include all fetched posts, not just today's")
    sp_scrape.set_defaults(func=cmd_scrape)

    sp_report = sub.add_parser("report", help="Generate summary report")
    sp_report.add_argument("--days", type=int, default=None)
    sp_report.add_argument("--csv", action="store_true")
    sp_report.set_defaults(func=cmd_report)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
