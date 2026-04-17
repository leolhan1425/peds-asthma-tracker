#!/usr/bin/env python3
"""
AI Pipeline for peds-asthma-tracker.

Runs a single combined Claude Haiku call per item (post or comment) extracting
all classification dimensions at once: medications, side effects, treatment beliefs
(with stance), ED discourse, triggers, caregiver emotional state, Singulair effects,
functional impact, inhaler confusion, sentiment + fear score, and asthma health topics.

CRITICAL: Text is CAREGIVER VOICE. The poster is a parent/guardian discussing their
child's asthma. Prompt instructs the model to extract the caregiver's perspective
on the child, not assume the child is the poster.

Results stored in existing analysis tables plus:
  - ai_raw_responses   (audit trail + prompt versioning)
  - ai_cost_log        (billing tracking)
  - asthma_health_topics (12 approved health topic themes)

Haiku pricing: $0.80/M input tokens, $4.00/M output tokens.

Usage:
    python3 ai_pipeline.py pilot --core          # Process 240 core validation items
    python3 ai_pipeline.py pilot --sample N      # Process N random items
    python3 ai_pipeline.py backfill              # Full corpus
    python3 ai_pipeline.py test-golden           # Run golden regression tests
    python3 ai_pipeline.py costs                 # Cost report

Requirements:
    pip3 install anthropic
    ANTHROPIC_API_KEY in .env or environment variable
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# .env loader (stdlib only — no python-dotenv needed)
# ---------------------------------------------------------------------------

_ENV_FILE = Path(__file__).parent / ".env"


def _load_dotenv() -> None:
    if not _ENV_FILE.is_file():
        return
    with open(_ENV_FILE) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip("'\"")
            if key and key not in os.environ:
                os.environ[key] = value


_load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ai_pipeline")

# ---------------------------------------------------------------------------
# Import asthma_tracker for MEDICATIONS dict and DB access
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).parent))
import asthma_tracker as tracker

# ---------------------------------------------------------------------------
# Model + pricing
# ---------------------------------------------------------------------------

MODEL = "claude-haiku-4-5-20251001"
_INPUT_COST_PER_TOKEN = 0.80 / 1_000_000   # $0.80 per 1M input tokens
_OUTPUT_COST_PER_TOKEN = 4.00 / 1_000_000  # $4.00 per 1M output tokens

# ---------------------------------------------------------------------------
# The combined system prompt (CAREGIVER VOICE — child is the patient)
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """You are analyzing a Reddit post or comment from a CAREGIVER (parent or guardian) about their CHILD'S asthma.

CRITICAL FRAMING: The person writing is the CAREGIVER (parent/guardian). The CHILD is the patient with asthma. Extract what the caregiver reports about their child's condition — not the caregiver's own health. When the text says "my son uses albuterol," that means the child uses albuterol.

Extract ALL of the following as a single JSON object:

1. "medications": Asthma medications mentioned (the child's medications).
   Use canonical names. Include informal references:
   "rescue inhaler"/"puffer" = albuterol class, "controller"/"preventer" = ICS or combination,
   "steroid inhaler" = ICS, "nebulizer treatments" = nebulizer, "puffer" = inhaler.
   Each entry: {"name": "canonical_name", "med_class": "ICS|Oral corticosteroids|Bronchodilators|Biologics|Leukotriene modifiers|Combination inhalers|Devices|null"}
   Canonical names include: Flovent, QVAR, Pulmicort, Alvesco, Asmanex, Arnuity, budesonide,
   fluticasone, beclomethasone, prednisone, prednisolone, dexamethasone, oral steroids,
   albuterol, ProAir, Ventolin, Proventil, levalbuterol, rescue inhaler, Dupixent, Xolair,
   Nucala, Fasenra, Tezspire, Singulair, Accolate, Advair, Symbicort, Dulera, Breo, AirDuo,
   AirSupra, SMART therapy, nebulizer, spacer, peak flow meter, pulse oximeter.

2. "side_effects": Side effects the caregiver attributes to a medication (reported/observed, not hypothetical).
   Each: {"effect": "...", "medication": "canonical_name_or_null"}
   Known effects: Growth concerns, Oral thrush, Hoarseness, Jitteriness, Rapid heartbeat,
   Mood changes, Hyperactivity, Sleep issues, Nightmares, Behavioral changes, Nausea,
   Headaches, Weight gain, Decreased appetite, Fatigue, Anxiety, Depression, Aggression,
   Adrenal suppression, Bone density.

3. "treatment_beliefs": NAEPP/GINA-discordant beliefs mentioned (stated, questioned, OR debunked).
   Each: {"belief": "...", "stance": "concordant|discordant|uncertain|unclear"}
   - concordant = caregiver aligns with guidelines (e.g., debunking a myth, following guideline advice)
   - discordant = caregiver holds a guideline-discordant belief (asserting the myth as true)
   - uncertain = questioning, unsure whether the belief is true
   - unclear = cannot determine stance
   Known beliefs (use exact names):
   "Albuterol-only reliance" — relies only on rescue inhaler, avoids controller
   "Nebulizer superiority myth" — believes nebulizer is stronger/better than MDI+spacer
   "Alternative medicine cures" — believes CAM cures or treats asthma
   "Steroid growth stunting fear" — fears ICS will stunt child's growth
   "Outgrow asthma belief" — believes child will simply outgrow asthma
   "Inhalers are addictive" — believes inhalers/ICS are habit-forming or addictive
   "Natural remedies are better" — prefers natural remedies over medications
   "Steroids are dangerous long-term" — believes long-term ICS are dangerous/harmful
   "Asthma is psychological" — believes asthma is all in the child's head or anxiety-driven
   "Only need medicine during attacks" — believes medication only needed during acute episodes

4. "ed_discourse": ED/hospital decision-making categories present (list of strings):
   "decision_uncertainty" — debating whether to go to ER
   "post_visit" — describing a recent ER/hospital visit
   "return_visits" — multiple or repeat ER/hospital visits
   "barriers" — cost, insurance, wait time barriers to ER/care

5. "triggers": Asthma triggers identified for the child.
   Each: {"trigger": "...", "category": "environmental|viral|non_evidence_based"}
   Environmental: Mold, Air pollution, Smoke, Pets, Pollen/seasonal, Dust mites, Weather changes, Cold air
   Viral: RSV, Common cold, Flu, COVID, Respiratory infection, Croup
   Non-evidence-based: Vaccines, Diet/toxins, Chemicals, Mold toxicity, EMF

6. "caregiver_emotional_state": Emotional categories present in the caregiver's voice (list):
   "trust" — trust/confidence in doctors/providers
   "frustration" — frustration with providers, system, or insurance
   "dismissed" — feeling dismissed, ignored, or not taken seriously by providers
   "anxiety" — fear/anxiety about child's asthma, attacks, or hospitalization
   "empowerment" — feeling informed, in control, advocating effectively

7. "singulair_effects": Behavioral effects the caregiver attributes to Singulair/montelukast (list):
   "nightmares", "aggression", "mood changes", "suicidal ideation",
   "sleep disturbances", "anxiety", "depression", "personality changes", "hyperactivity"

8. "functional_impact": Impact on the child's or family's functioning (list):
   "missed_school" — child missed school days
   "missed_work" — parent/caregiver missed work
   "activity_limitation" — child limited in activities (sports, play, recess)
   "sports_impact" — specifically quit sports, benched, or pulled from team

9. "inhaler_confusion": Confusion or technique issues present (list):
   "type_confusion" — caregiver confused about which inhaler to use (rescue vs. controller)
   "technique_issues" — incorrect inhaler technique mentioned
   "timing_confusion" — confused about when or how often to use inhaler
   "device_confusion" — confused about device type (MDI vs. DPI vs. nebulizer)

10. "sentiment": Overall caregiver sentiment toward their child's asthma situation and care.
    Integer 1-5 Likert scale: 1=very negative, 2=negative, 3=neutral, 4=positive, 5=very positive.
    null if no clear sentiment.
    "fear_score": Caregiver fear and anxiety level specifically about the child's safety and breathing.
    Float 0.0-1.0: 0.0=no fear, 0.3=mild concern, 0.5=moderate fear, 0.7=significant fear, 1.0=extreme panic/terror.
    null if no fear/anxiety expressed.
    "sentiment_emotion": Dominant emotion (anxiety/fear/frustration/sadness/relief/hope/empowerment/neutral/mixed).

11. "health_topics": Asthma health topics present (list — choose all that apply):
    "School Impact" — child missing school, school nurse, 504 plan
    "Sleep Disruption" — nighttime symptoms, waking up, sleep problems
    "ER/Hospital" — emergency visits, hospitalization, urgent care
    "Trigger Management" — identifying or managing asthma triggers
    "Growth/Development" — concerns about growth, development, steroid effects on growth
    "Sports/Activity" — sports participation, exercise-induced asthma, PE class
    "Parental Burden" — impact on caregiver (work, stress, finances, FMLA)
    "Medication Adherence" — challenges with or discussions of medication adherence
    "Diagnosis Journey" — getting a diagnosis, diagnostic process, initial diagnosis
    "Seasonal Patterns" — seasonal exacerbations, back-to-school, spring/fall patterns
    "Caregiver/Sibling Impact" — impact on siblings, family dynamics
    "Psychosocial" — mental health, behavioral effects of medications, child's emotional wellbeing

12. "additional_observations": Anything notable not captured above (child's age, severity level,
    key treatment decisions, access/cost issues, important provider relationships).
    Free text, 1-3 sentences. Empty string if nothing notable.

Return ONLY valid JSON. No markdown code fences, no explanation, no prefix text."""

# Prompt version = first 8 chars of SHA-256 of the system prompt text
PROMPT_VERSION = hashlib.sha256(_SYSTEM_PROMPT.encode()).hexdigest()[:8]

# ---------------------------------------------------------------------------
# Valid values for normalization / validation
# ---------------------------------------------------------------------------

_VALID_MED_CLASSES = {
    "ICS", "Oral corticosteroids", "Bronchodilators", "Biologics",
    "Leukotriene modifiers", "Combination inhalers", "Devices",
}

_VALID_STANCES = {"concordant", "discordant", "uncertain", "unclear"}

_VALID_TREATMENT_BELIEFS = {
    "Albuterol-only reliance",
    "Nebulizer superiority myth",
    "Alternative medicine cures",
    "Steroid growth stunting fear",
    "Outgrow asthma belief",
    "Inhalers are addictive",
    "Natural remedies are better",
    "Steroids are dangerous long-term",
    "Asthma is psychological",
    "Only need medicine during attacks",
}

_VALID_ED_DISCOURSE = {"decision_uncertainty", "post_visit", "return_visits", "barriers"}

_VALID_TRIGGER_CATEGORIES = {"environmental", "viral", "non_evidence_based"}

_VALID_CAREGIVER_STATES = {"trust", "frustration", "dismissed", "anxiety", "empowerment"}

_VALID_SINGULAIR_EFFECTS = {
    "nightmares", "aggression", "mood changes", "suicidal ideation",
    "sleep disturbances", "anxiety", "depression", "personality changes", "hyperactivity",
}

_VALID_FUNCTIONAL_IMPACT = {"missed_school", "missed_work", "activity_limitation", "sports_impact"}

_VALID_INHALER_CONFUSION = {"type_confusion", "technique_issues", "timing_confusion", "device_confusion"}

_VALID_EMOTIONS = {
    "anxiety", "fear", "frustration", "sadness", "relief",
    "hope", "empowerment", "neutral", "mixed",
}

_VALID_HEALTH_TOPICS = {
    "School Impact", "Sleep Disruption", "ER/Hospital", "Trigger Management",
    "Growth/Development", "Sports/Activity", "Parental Burden", "Medication Adherence",
    "Diagnosis Journey", "Seasonal Patterns", "Caregiver/Sibling Impact", "Psychosocial",
}

# ---------------------------------------------------------------------------
# Claude API wrapper
# ---------------------------------------------------------------------------

_client = None


def _get_client():
    global _client
    if _client is None:
        try:
            import anthropic
            key = os.environ.get("ANTHROPIC_API_KEY", "")
            if not key:
                raise RuntimeError(
                    "ANTHROPIC_API_KEY not set. Add it to .env or environment."
                )
            _client = anthropic.Anthropic(api_key=key)
        except ImportError:
            log.error("anthropic SDK not installed. Run: pip3 install anthropic")
            raise
    return _client


def _call_claude_raw(user_content: str, max_tokens: int = 1200) -> tuple[str, dict]:
    """
    Call Claude Haiku and return (raw_text, usage_dict).
    Higher max_tokens than bc-tracker because peds-asthma extracts more dimensions.
    """
    client = _get_client()
    response = client.messages.create(
        model=MODEL,
        max_tokens=max_tokens,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_content}],
    )
    raw = response.content[0].text.strip()
    usage = {
        "input_tokens": response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
        "estimated_usd": (
            response.usage.input_tokens * _INPUT_COST_PER_TOKEN
            + response.usage.output_tokens * _OUTPUT_COST_PER_TOKEN
        ),
    }
    return raw, usage


# ---------------------------------------------------------------------------
# Medication normalization
# ---------------------------------------------------------------------------

def normalize_medication(raw: str) -> Optional[str]:
    """
    Map an AI-returned medication name to a canonical MEDICATIONS key.

    Strategy:
    1. Exact case-insensitive match against canonical names
    2. Run each compiled regex pattern against the raw string
    3. Return None for novel mentions not matching any pattern
    """
    if not raw:
        return None
    raw_lower = raw.lower().strip()

    # 1. Exact match
    for name in tracker.MEDICATIONS:
        if name.lower() == raw_lower:
            return name

    # 2. Run each compiled regex against the raw string
    for name, pat in tracker._COMPILED_MEDS.items():
        if pat.search(raw):
            return name

    return None  # Novel mention


# ---------------------------------------------------------------------------
# Single combined classification call
# ---------------------------------------------------------------------------

def classify_item(text: str, source_type: str) -> tuple[dict, str, dict]:
    """
    Run a single combined Claude Haiku call on text.

    Args:
        text: The full post/comment text (title + selftext for posts)
        source_type: 'post' or 'comment'

    Returns:
        (structured_result, raw_response_text, usage_dict)
    """
    # Truncate to ~3500 chars to keep cost reasonable
    if len(text) > 3500:
        text = text[:3500] + "...[truncated]"

    user_prompt = f"[{source_type.upper()}]\n\n{text}"

    try:
        raw, usage = _call_claude_raw(user_prompt)
    except Exception as e:
        log.error(f"Claude API call failed: {e}")
        raise

    # Strip markdown code fences if present
    clean = raw
    if clean.startswith("```"):
        lines = clean.split("\n", 1)
        clean = lines[1] if len(lines) > 1 else clean[3:]
        if "```" in clean:
            clean = clean[:clean.rindex("```")].strip()

    try:
        result = json.loads(clean)
    except json.JSONDecodeError:
        log.warning(f"Non-JSON response (first 200 chars): {raw[:200]}")
        result = {}

    structured = _normalize_result(result, source_type)
    return structured, raw, usage


def _normalize_result(raw_result: dict, source_type: str) -> dict:
    """Validate and coerce the AI output into a clean structured dict."""

    # --- medications: list of {name, med_class} ---
    raw_meds = raw_result.get("medications", [])
    if not isinstance(raw_meds, list):
        raw_meds = []
    medications = []
    for item in raw_meds:
        if isinstance(item, dict):
            name = str(item.get("name", "")).strip()
            med_class = str(item.get("med_class", "")).strip()
            if med_class not in _VALID_MED_CLASSES:
                med_class = None
            if name:
                canonical = normalize_medication(name)
                store_name = canonical if canonical else name
                # Look up class from MEDICATION_CLASSES if AI didn't provide one
                if not med_class and canonical:
                    med_class = tracker.MEDICATION_CLASSES.get(canonical)
                medications.append({"name": store_name, "med_class": med_class, "novel": 0 if canonical else 1})
        elif isinstance(item, str):
            name = item.strip()
            if name:
                canonical = normalize_medication(name)
                store_name = canonical if canonical else name
                med_class = tracker.MEDICATION_CLASSES.get(canonical) if canonical else None
                medications.append({"name": store_name, "med_class": med_class, "novel": 0 if canonical else 1})

    # --- side_effects: list of {effect, medication} ---
    raw_se = raw_result.get("side_effects", [])
    if not isinstance(raw_se, list):
        raw_se = []
    side_effects = []
    for item in raw_se:
        if isinstance(item, dict):
            effect = str(item.get("effect", "")).strip()
            med = item.get("medication")
            if isinstance(med, str):
                med = med.strip() or None
            if effect:
                side_effects.append({"effect": effect, "medication": med})
        elif isinstance(item, str):
            effect = item.strip()
            if effect:
                side_effects.append({"effect": effect, "medication": None})

    # --- treatment_beliefs: list of {belief, stance} ---
    raw_beliefs = raw_result.get("treatment_beliefs", [])
    if not isinstance(raw_beliefs, list):
        raw_beliefs = []
    treatment_beliefs = []
    for item in raw_beliefs:
        if isinstance(item, dict):
            belief = str(item.get("belief", "")).strip()
            stance = str(item.get("stance", "")).lower().strip()
            if stance not in _VALID_STANCES:
                stance = "unclear"
            if belief:
                treatment_beliefs.append({"belief": belief, "stance": stance})

    # --- ed_discourse: list of str ---
    raw_ed = raw_result.get("ed_discourse", [])
    if not isinstance(raw_ed, list):
        raw_ed = []
    ed_discourse = [
        s for s in (str(x).strip().lower() for x in raw_ed)
        if s in _VALID_ED_DISCOURSE
    ]

    # --- triggers: list of {trigger, category} ---
    raw_triggers = raw_result.get("triggers", [])
    if not isinstance(raw_triggers, list):
        raw_triggers = []
    triggers = []
    for item in raw_triggers:
        if isinstance(item, dict):
            trig = str(item.get("trigger", "")).strip()
            cat = str(item.get("category", "")).strip().lower()
            if cat not in _VALID_TRIGGER_CATEGORIES:
                cat = None
            if trig:
                # Try to match to canonical trigger name
                canonical_trig = _normalize_trigger(trig)
                store_trig = canonical_trig if canonical_trig else trig
                if not cat and canonical_trig:
                    cat = tracker.TRIGGER_CATEGORIES.get(canonical_trig)
                triggers.append({"trigger": store_trig, "category": cat})
        elif isinstance(item, str):
            trig = item.strip()
            if trig:
                canonical_trig = _normalize_trigger(trig)
                store_trig = canonical_trig if canonical_trig else trig
                cat = tracker.TRIGGER_CATEGORIES.get(canonical_trig) if canonical_trig else None
                triggers.append({"trigger": store_trig, "category": cat})

    # --- caregiver_emotional_state: list of str ---
    raw_ces = raw_result.get("caregiver_emotional_state", [])
    if not isinstance(raw_ces, list):
        raw_ces = []
    caregiver_emotional_state = [
        s for s in (str(x).strip().lower() for x in raw_ces)
        if s in _VALID_CAREGIVER_STATES
    ]

    # --- singulair_effects: list of str ---
    raw_sing = raw_result.get("singulair_effects", [])
    if not isinstance(raw_sing, list):
        raw_sing = []
    singulair_effects = [
        s for s in (str(x).strip().lower() for x in raw_sing)
        if s in _VALID_SINGULAIR_EFFECTS
    ]

    # --- functional_impact: list of str ---
    raw_fi = raw_result.get("functional_impact", [])
    if not isinstance(raw_fi, list):
        raw_fi = []
    functional_impact = [
        s for s in (str(x).strip().lower() for x in raw_fi)
        if s in _VALID_FUNCTIONAL_IMPACT
    ]

    # --- inhaler_confusion: list of str ---
    raw_ic = raw_result.get("inhaler_confusion", [])
    if not isinstance(raw_ic, list):
        raw_ic = []
    inhaler_confusion = [
        s for s in (str(x).strip().lower() for x in raw_ic)
        if s in _VALID_INHALER_CONFUSION
    ]

    # --- sentiment (1-5 Likert) ---
    sentiment = raw_result.get("sentiment")
    if sentiment is not None:
        try:
            sentiment = round(float(sentiment))
            sentiment = max(1, min(5, sentiment))
        except (TypeError, ValueError):
            sentiment = None

    # --- fear_score (0.0-1.0) ---
    fear_score = raw_result.get("fear_score")
    if fear_score is not None:
        try:
            fear_score = float(fear_score)
            fear_score = max(0.0, min(1.0, fear_score))
            fear_score = round(fear_score, 3)
        except (TypeError, ValueError):
            fear_score = None

    # --- sentiment_emotion ---
    emotion = str(raw_result.get("sentiment_emotion", "")).lower().strip()
    if emotion not in _VALID_EMOTIONS:
        emotion = "neutral" if emotion else None

    # --- health_topics: list of str ---
    raw_ht = raw_result.get("health_topics", [])
    if not isinstance(raw_ht, list):
        raw_ht = []
    health_topics = [
        s for s in (str(x).strip() for x in raw_ht)
        if s in _VALID_HEALTH_TOPICS
    ]

    # --- additional_observations ---
    additional_obs = str(raw_result.get("additional_observations", "")).strip()
    if len(additional_obs) > 1000:
        additional_obs = additional_obs[:1000]

    return {
        "medications": medications,
        "side_effects": side_effects,
        "treatment_beliefs": treatment_beliefs,
        "ed_discourse": ed_discourse,
        "triggers": triggers,
        "caregiver_emotional_state": caregiver_emotional_state,
        "singulair_effects": singulair_effects,
        "functional_impact": functional_impact,
        "inhaler_confusion": inhaler_confusion,
        "sentiment": sentiment,
        "fear_score": fear_score,
        "sentiment_emotion": emotion,
        "health_topics": health_topics,
        "additional_observations": additional_obs,
    }


def _normalize_trigger(raw: str) -> Optional[str]:
    """Map an AI-returned trigger name to a canonical TRIGGERS key."""
    if not raw:
        return None
    raw_lower = raw.lower().strip()
    for name in tracker.TRIGGERS:
        if name.lower() == raw_lower:
            return name
    # Fuzzy: check if canonical name words appear in raw
    for name in tracker.TRIGGERS:
        name_words = set(name.lower().split("/"))
        for w in name_words:
            if w and w in raw_lower:
                return name
    return None


# ---------------------------------------------------------------------------
# Save results to DB
# ---------------------------------------------------------------------------

def save_ai_results(
    conn,
    source_type: str,
    source_id: str,
    results: dict,
    raw_response: str,
    model: str,
    tokens: dict,
) -> None:
    """
    Store extracted data in AI tables and ai_raw_responses.

    Saves to:
      ai_raw_responses — audit trail
      ai_medication_mentions — medications (posts only)
      ai_side_effects — side effects
      ai_treatment_beliefs — beliefs with stance
      ai_ed_discourse — ED discourse categories
      ai_triggers — triggers
      ai_caregiver_emotional_state — caregiver emotions
      ai_singulair_effects — Singulair behavioral effects
      ai_functional_impact — functional impact
      ai_inhaler_confusion — inhaler confusion
      asthma_health_topics — health topic themes
      posts/comments — updates sentiment, fear_score, ai_analyzed
    """
    now = datetime.utcnow().isoformat()

    # --- ai_raw_responses ---
    conn.execute(
        """INSERT OR REPLACE INTO ai_raw_responses
           (source_type, source_id, prompt_version, raw_response, model,
            input_tokens, output_tokens, estimated_usd, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            source_type, source_id, PROMPT_VERSION, raw_response, model,
            tokens.get("input_tokens", 0),
            tokens.get("output_tokens", 0),
            tokens.get("estimated_usd", 0.0),
            now,
        ),
    )

    # --- Medications (posts only — post-level extraction) ---
    if source_type == "post":
        for med in results.get("medications", []):
            name = med["name"]
            med_class = med.get("med_class")
            novel = med.get("novel", 0)
            conn.execute(
                """INSERT OR IGNORE INTO ai_medication_mentions
                   (post_id, medication, med_class, novel)
                   VALUES (?, ?, ?, ?)""",
                (source_id, name, med_class, novel),
            )

    # --- Side effects ---
    for se in results.get("side_effects", []):
        effect = se["effect"]
        medication = se.get("medication")
        conn.execute(
            """INSERT OR REPLACE INTO ai_side_effects
               (source_type, source_id, effect, medication)
               VALUES (?, ?, ?, ?)""",
            (source_type, source_id, effect, medication),
        )

    # --- Treatment beliefs ---
    for belief_item in results.get("treatment_beliefs", []):
        belief = belief_item["belief"]
        stance = belief_item["stance"]
        conn.execute(
            """INSERT OR REPLACE INTO ai_treatment_beliefs
               (source_type, source_id, belief, stance)
               VALUES (?, ?, ?, ?)""",
            (source_type, source_id, belief, stance),
        )

    # --- ED discourse ---
    for cat in results.get("ed_discourse", []):
        conn.execute(
            """INSERT OR IGNORE INTO ai_ed_discourse
               (source_type, source_id, category)
               VALUES (?, ?, ?)""",
            (source_type, source_id, cat),
        )

    # --- Triggers ---
    for trig_item in results.get("triggers", []):
        trig_name = trig_item["trigger"]
        trig_cat = trig_item.get("category")
        conn.execute(
            """INSERT OR IGNORE INTO ai_triggers
               (source_type, source_id, trigger_name, trigger_category)
               VALUES (?, ?, ?, ?)""",
            (source_type, source_id, trig_name, trig_cat),
        )

    # --- Caregiver emotional state ---
    for cat in results.get("caregiver_emotional_state", []):
        conn.execute(
            """INSERT OR IGNORE INTO ai_caregiver_emotional_state
               (source_type, source_id, category)
               VALUES (?, ?, ?)""",
            (source_type, source_id, cat),
        )

    # --- Singulair effects ---
    for effect in results.get("singulair_effects", []):
        conn.execute(
            """INSERT OR IGNORE INTO ai_singulair_effects
               (source_type, source_id, effect)
               VALUES (?, ?, ?)""",
            (source_type, source_id, effect),
        )

    # --- Functional impact ---
    for cat in results.get("functional_impact", []):
        conn.execute(
            """INSERT OR IGNORE INTO ai_functional_impact
               (source_type, source_id, category)
               VALUES (?, ?, ?)""",
            (source_type, source_id, cat),
        )

    # --- Inhaler confusion ---
    for cat in results.get("inhaler_confusion", []):
        conn.execute(
            """INSERT OR IGNORE INTO ai_inhaler_confusion
               (source_type, source_id, category)
               VALUES (?, ?, ?)""",
            (source_type, source_id, cat),
        )

    # --- Health topics ---
    for topic in results.get("health_topics", []):
        conn.execute(
            """INSERT OR IGNORE INTO asthma_health_topics
               (source_type, source_id, topic)
               VALUES (?, ?, ?)""",
            (source_type, source_id, topic),
        )

    # --- Sentiment + fear_score ---
    sentiment = results.get("sentiment")
    fear_score = results.get("fear_score")
    emotion = results.get("sentiment_emotion")
    if sentiment is not None or fear_score is not None:
        if source_type == "post":
            if sentiment is not None:
                conn.execute(
                    "UPDATE posts SET ai_sentiment = ? WHERE id = ?",
                    (sentiment, source_id),
                )
            if fear_score is not None:
                conn.execute(
                    "UPDATE posts SET ai_fear_score = ? WHERE id = ?",
                    (fear_score, source_id),
                )
        elif source_type == "comment":
            if sentiment is not None:
                conn.execute(
                    "UPDATE comments SET ai_sentiment = ? WHERE id = ?",
                    (sentiment, source_id),
                )
            if fear_score is not None:
                conn.execute(
                    "UPDATE comments SET ai_fear_score = ? WHERE id = ?",
                    (fear_score, source_id),
                )

    # --- Mark as AI-analyzed ---
    if source_type == "post":
        conn.execute(
            "UPDATE posts SET ai_analyzed = 1 WHERE id = ?",
            (source_id,),
        )
    elif source_type == "comment":
        conn.execute(
            "UPDATE comments SET ai_analyzed = 1 WHERE id = ?",
            (source_id,),
        )

    conn.commit()


# ---------------------------------------------------------------------------
# Pilot runner
# ---------------------------------------------------------------------------

def run_ai_pilot(conn, mode: str = "sample", sample_size: int = 500) -> dict:
    """
    Process items through the AI pipeline.

    mode='sample' : random sample of size sample_size
    mode='full'   : entire corpus

    Incremental — skips already-processed items (checks ai_raw_responses).
    Returns stats dict.
    """
    stats = {
        "processed": 0,
        "skipped_already_done": 0,
        "errors": 0,
        "total_input_tokens": 0,
        "total_output_tokens": 0,
        "total_usd": 0.0,
    }

    items = _get_pilot_items(conn, mode, sample_size)
    log.info(f"AI pilot ({mode}): {len(items)} items to process")

    if not items:
        log.info("No items found for this mode. Is the DB populated?")
        return stats

    for i, (item_id, item_type) in enumerate(items):
        # Check if already processed with current prompt version
        existing = conn.execute(
            """SELECT id FROM ai_raw_responses
               WHERE source_type = ? AND source_id = ? AND prompt_version = ?
               LIMIT 1""",
            (item_type, item_id, PROMPT_VERSION),
        ).fetchone()
        if existing:
            stats["skipped_already_done"] += 1
            continue

        # Get text
        text = _get_item_text(conn, item_id, item_type)
        if not text or not text.strip():
            log.debug(f"Skipping {item_type} {item_id}: empty text")
            stats["errors"] += 1
            continue

        # Call AI
        try:
            result, raw, usage = classify_item(text, item_type)
        except Exception as e:
            log.error(f"Failed to classify {item_type} {item_id}: {e}")
            stats["errors"] += 1
            time.sleep(1)
            continue

        # Save results
        try:
            save_ai_results(conn, item_type, item_id, result, raw, MODEL, usage)
        except Exception as e:
            log.error(f"Failed to save results for {item_type} {item_id}: {e}")
            stats["errors"] += 1
            continue

        stats["processed"] += 1
        stats["total_input_tokens"] += usage.get("input_tokens", 0)
        stats["total_output_tokens"] += usage.get("output_tokens", 0)
        stats["total_usd"] += usage.get("estimated_usd", 0.0)

        # Progress log every 25 items
        if (i + 1) % 25 == 0:
            log.info(
                f"  Progress: {i+1}/{len(items)} | "
                f"Processed: {stats['processed']} | "
                f"Cost so far: ${stats['total_usd']:.4f}"
            )

        # Polite delay to avoid rate limiting
        time.sleep(0.15)

    # Save run cost to ai_cost_log
    if stats["total_usd"] > 0:
        try:
            conn.execute(
                """INSERT INTO ai_cost_log
                   (timestamp, input_tokens, output_tokens, api_calls,
                    estimated_usd, context)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    datetime.utcnow().isoformat(),
                    stats["total_input_tokens"],
                    stats["total_output_tokens"],
                    stats["processed"],
                    stats["total_usd"],
                    f"pilot_{mode}",
                ),
            )
            conn.commit()
        except Exception as e:
            log.warning(f"Failed to save cost log: {e}")

    log.info(
        f"\nAI pilot complete: {stats['processed']} processed, "
        f"{stats['skipped_already_done']} skipped (already done), "
        f"{stats['errors']} errors. "
        f"Cost: ${stats['total_usd']:.4f} "
        f"({stats['total_input_tokens']} in + {stats['total_output_tokens']} out tokens)"
    )
    return stats


def _get_pilot_items(conn, mode: str, sample_size: int) -> list[tuple[str, str]]:
    """Return list of (item_id, item_type) for the given mode."""
    if mode == "sample":
        posts = conn.execute(
            """SELECT id FROM posts
               WHERE selftext != '' AND selftext IS NOT NULL
               AND pediatric_confidence IN ('definite', 'likely')
               ORDER BY RANDOM() LIMIT ?""",
            (sample_size // 2,),
        ).fetchall()
        comments = conn.execute(
            """SELECT id FROM comments
               WHERE body IS NOT NULL AND length(body) > 30
               ORDER BY RANDOM() LIMIT ?""",
            (sample_size - len(posts),),
        ).fetchall()
        items = [(r["id"], "post") for r in posts] + [(r["id"], "comment") for r in comments]
        random.shuffle(items)
        return items

    elif mode == "full":
        posts = conn.execute(
            """SELECT id FROM posts
               WHERE selftext != '' AND selftext IS NOT NULL
               AND pediatric_confidence IN ('definite', 'likely')""",
        ).fetchall()
        comments = conn.execute(
            "SELECT id FROM comments WHERE body IS NOT NULL AND length(body) > 30",
        ).fetchall()
        items = [(r["id"], "post") for r in posts] + [(r["id"], "comment") for r in comments]
        random.shuffle(items)
        return items

    else:
        raise ValueError(f"Unknown mode: {mode}")


def _get_item_text(conn, item_id: str, item_type: str) -> Optional[str]:
    """Fetch the full text for a post or comment."""
    if item_type == "post":
        row = conn.execute(
            "SELECT title, selftext FROM posts WHERE id = ?", (item_id,)
        ).fetchone()
        if not row:
            return None
        return f"{row['title'] or ''} {row['selftext'] or ''}".strip()
    elif item_type == "comment":
        row = conn.execute(
            "SELECT body FROM comments WHERE id = ?", (item_id,)
        ).fetchone()
        return row["body"] if row else None
    return None


# ---------------------------------------------------------------------------
# Golden test suite
# ---------------------------------------------------------------------------

_GOLDEN_FILE = Path(__file__).parent / "tests" / "golden_items.json"

# Tolerances
_SENTIMENT_TOLERANCE = 1          # AI sentiment within ±1 of expected (1-5 Likert)
_FEAR_TOLERANCE = 0.2             # AI fear_score within ±0.2 of expected (0.0-1.0)
_MENTION_RECALL_THRESHOLD = 0.5   # ≥50% of expected medications found
_BELIEF_KEYWORD_OVERLAP = 0.5     # >50% keyword overlap for belief matching


def run_golden_tests(verbose: bool = False) -> bool:
    """
    Run AI on 10 golden items and compare to expected outputs.
    Returns True if all tests pass within tolerances.

    Tolerances:
      - sentiment: ±1 Likert point
      - fear_score: ±0.2
      - medication recall: ≥50% of expected meds found
      - belief keyword overlap: >50% for each expected belief
    """
    if not _GOLDEN_FILE.exists():
        log.error(f"Golden items file not found: {_GOLDEN_FILE}")
        return False

    with open(_GOLDEN_FILE) as f:
        golden = json.load(f)

    if not golden:
        log.error("Golden items file is empty")
        return False

    log.info(f"Running {len(golden)} golden tests with prompt version {PROMPT_VERSION}...")
    passed = 0
    failed = 0
    total_usd = 0.0

    for i, item in enumerate(golden):
        item_id = item.get("id", f"golden_{i}")
        source_type = item.get("source_type", "post")
        text = item.get("text", "")
        expected = item.get("expected", {})

        if not text:
            log.warning(f"  [{i+1}] {item_id}: Empty text, skipping")
            continue

        log.info(f"  [{i+1}/{len(golden)}] Testing {source_type} '{item_id}'...")

        try:
            result, raw, usage = classify_item(text, source_type)
            total_usd += usage.get("estimated_usd", 0.0)
        except Exception as e:
            log.error(f"  [{i+1}] FAIL: API error: {e}")
            failed += 1
            continue

        failures = []

        # --- Check medication recall (≥50%) ---
        expected_meds = [m.lower() for m in expected.get("medications", [])]
        if expected_meds:
            ai_meds = set()
            for m in result.get("medications", []):
                name = m["name"] if isinstance(m, dict) else str(m)
                canonical = normalize_medication(name)
                ai_meds.add((canonical or name).lower())
            found = sum(1 for m in expected_meds if m in ai_meds)
            recall = found / len(expected_meds)
            if recall < _MENTION_RECALL_THRESHOLD:
                failures.append(
                    f"Medication recall {recall:.0%} < {_MENTION_RECALL_THRESHOLD:.0%} "
                    f"(expected {expected_meds}, got {sorted(ai_meds)})"
                )

        # --- Check sentiment (within ±1) ---
        exp_sentiment = expected.get("sentiment")
        ai_sentiment = result.get("sentiment")
        if exp_sentiment is not None and ai_sentiment is not None:
            diff = abs(ai_sentiment - exp_sentiment)
            if diff > _SENTIMENT_TOLERANCE:
                failures.append(
                    f"Sentiment {ai_sentiment} too far from expected {exp_sentiment} "
                    f"(diff={diff} > tolerance={_SENTIMENT_TOLERANCE})"
                )

        # --- Check fear_score (within ±0.2) ---
        exp_fear = expected.get("fear_score")
        ai_fear = result.get("fear_score")
        if exp_fear is not None:
            if ai_fear is None:
                # If expected fear is very low (≤0.15), None is acceptable
                if exp_fear > 0.15:
                    failures.append(
                        f"fear_score is None but expected {exp_fear}"
                    )
            else:
                diff = abs(ai_fear - exp_fear)
                if diff > _FEAR_TOLERANCE:
                    failures.append(
                        f"fear_score {ai_fear:.3f} too far from expected {exp_fear:.3f} "
                        f"(diff={diff:.3f} > tolerance={_FEAR_TOLERANCE})"
                    )

        # --- Check treatment_beliefs (keyword overlap >50%) ---
        expected_beliefs = expected.get("treatment_beliefs", [])
        if expected_beliefs:
            ai_beliefs = [
                b["belief"].lower() if isinstance(b, dict) else str(b).lower()
                for b in result.get("treatment_beliefs", [])
            ]
            for exp_belief in expected_beliefs:
                exp_text = (
                    exp_belief.get("belief") if isinstance(exp_belief, dict) else exp_belief
                ).lower()
                exp_words = set(exp_text.split())
                matched = any(
                    len(exp_words & set(ab.split())) >= len(exp_words) * _BELIEF_KEYWORD_OVERLAP
                    for ab in ai_beliefs
                )
                if not matched:
                    failures.append(f"Missing treatment belief: '{exp_text}'")

        # --- Check singulair_effects recall (≥50%) ---
        expected_sing = [s.lower() for s in expected.get("singulair_effects", [])]
        if expected_sing:
            ai_sing = [s.lower() for s in result.get("singulair_effects", [])]
            found = sum(1 for s in expected_sing if s in ai_sing)
            recall = found / len(expected_sing)
            if recall < _MENTION_RECALL_THRESHOLD:
                failures.append(
                    f"Singulair effect recall {recall:.0%} < {_MENTION_RECALL_THRESHOLD:.0%} "
                    f"(expected {expected_sing}, got {ai_sing})"
                )

        # --- Check functional_impact recall (≥50%) ---
        expected_fi = [s.lower() for s in expected.get("functional_impact", [])]
        if expected_fi:
            ai_fi = [s.lower() for s in result.get("functional_impact", [])]
            found = sum(1 for s in expected_fi if s in ai_fi)
            recall = found / len(expected_fi)
            if recall < _MENTION_RECALL_THRESHOLD:
                failures.append(
                    f"Functional impact recall {recall:.0%} < {_MENTION_RECALL_THRESHOLD:.0%} "
                    f"(expected {expected_fi}, got {ai_fi})"
                )

        # --- Check health_topics recall (≥50%) ---
        expected_ht = expected.get("health_topics", [])
        if expected_ht:
            ai_ht = result.get("health_topics", [])
            found = sum(1 for t in expected_ht if t in ai_ht)
            recall = found / len(expected_ht)
            if recall < _MENTION_RECALL_THRESHOLD:
                failures.append(
                    f"Health topics recall {recall:.0%} < {_MENTION_RECALL_THRESHOLD:.0%} "
                    f"(expected {expected_ht}, got {ai_ht})"
                )

        if failures:
            log.warning(f"  [{i+1}] FAIL: {item_id}")
            for fail in failures:
                log.warning(f"         - {fail}")
            if verbose:
                log.info(f"         AI result:\n{json.dumps(result, indent=2)}")
            failed += 1
        else:
            log.info(f"  [{i+1}] PASS: {item_id}")
            passed += 1

        time.sleep(0.2)

    log.info(
        f"\nGolden tests: {passed}/{len(golden)} passed, {failed} failed. "
        f"Cost: ${total_usd:.4f}"
    )
    return failed == 0


# ---------------------------------------------------------------------------
# Cost report
# ---------------------------------------------------------------------------

def print_cost_report(conn, days: int = 30) -> None:
    """Print a cost summary from ai_cost_log."""
    rows = conn.execute(
        """SELECT timestamp, input_tokens, output_tokens, api_calls,
                  estimated_usd, context
           FROM ai_cost_log
           WHERE timestamp >= datetime('now', ?)
           ORDER BY timestamp DESC""",
        (f"-{days} days",),
    ).fetchall()

    if not rows:
        print(f"No AI cost entries in the last {days} days.")
        return

    total_usd = sum(r["estimated_usd"] for r in rows)
    total_calls = sum(r["api_calls"] for r in rows)
    total_input = sum(r["input_tokens"] for r in rows)
    total_output = sum(r["output_tokens"] for r in rows)

    print(f"\nAI Cost Report (last {days} days):")
    print(f"  Model: {MODEL}")
    print(f"  Pricing: ${_INPUT_COST_PER_TOKEN * 1_000_000:.2f}/M input, ${_OUTPUT_COST_PER_TOKEN * 1_000_000:.2f}/M output")
    print(f"  Total: ${total_usd:.4f} across {total_calls} API calls")
    print(f"  Tokens: {total_input:,} in + {total_output:,} out")
    print(f"  Entries:")
    for r in rows[:10]:
        print(
            f"    {r['timestamp'][:19]}  ${r['estimated_usd']:.4f}  "
            f"{r['api_calls']} calls  [{r['context']}]"
        )
    if len(rows) > 10:
        print(f"    ... and {len(rows) - 10} more")

    # ai_raw_responses stats
    rr = conn.execute(
        "SELECT COUNT(*), COUNT(DISTINCT source_type || ':' || source_id) FROM ai_raw_responses"
    ).fetchone()
    if rr:
        print(f"\n  ai_raw_responses: {rr[0]} rows, {rr[1]} distinct items processed")

    pv_rows = conn.execute(
        "SELECT prompt_version, COUNT(*) as cnt FROM ai_raw_responses GROUP BY prompt_version ORDER BY cnt DESC"
    ).fetchall()
    if pv_rows:
        print("  Prompt versions:")
        for pv in pv_rows:
            print(f"    {pv['prompt_version']}: {pv['cnt']} items")


# ---------------------------------------------------------------------------
# Schema helper — creates AI pipeline tables
# ---------------------------------------------------------------------------

def ensure_schema(conn) -> None:
    """
    Create all AI pipeline tables and add AI columns to posts/comments.
    Safe to call multiple times (uses CREATE IF NOT EXISTS + ALTER TABLE guards).
    """
    conn.executescript("""
        -- Audit trail for all AI calls
        CREATE TABLE IF NOT EXISTS ai_raw_responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            prompt_version TEXT NOT NULL,
            raw_response TEXT NOT NULL,
            model TEXT NOT NULL,
            input_tokens INTEGER,
            output_tokens INTEGER,
            estimated_usd REAL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_air_source
            ON ai_raw_responses(source_type, source_id);
        CREATE INDEX IF NOT EXISTS idx_air_prompt
            ON ai_raw_responses(prompt_version);

        -- Billing / cost tracking
        CREATE TABLE IF NOT EXISTS ai_cost_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            input_tokens INTEGER,
            output_tokens INTEGER,
            api_calls INTEGER,
            estimated_usd REAL,
            context TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_cost_log_ts ON ai_cost_log(timestamp);

        -- AI-extracted medication mentions (posts only)
        CREATE TABLE IF NOT EXISTS ai_medication_mentions (
            post_id TEXT NOT NULL,
            medication TEXT NOT NULL,
            med_class TEXT,
            novel INTEGER DEFAULT 0,
            PRIMARY KEY (post_id, medication)
        );
        CREATE INDEX IF NOT EXISTS idx_ai_med_post ON ai_medication_mentions(post_id);
        CREATE INDEX IF NOT EXISTS idx_ai_med_name ON ai_medication_mentions(medication);

        -- AI-extracted side effects
        CREATE TABLE IF NOT EXISTS ai_side_effects (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            effect TEXT NOT NULL,
            medication TEXT,
            PRIMARY KEY (source_type, source_id, effect)
        );

        -- AI-extracted treatment beliefs with stance
        CREATE TABLE IF NOT EXISTS ai_treatment_beliefs (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            belief TEXT NOT NULL,
            stance TEXT DEFAULT 'unclear',
            PRIMARY KEY (source_type, source_id, belief)
        );

        -- AI-extracted ED discourse categories
        CREATE TABLE IF NOT EXISTS ai_ed_discourse (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            category TEXT NOT NULL,
            PRIMARY KEY (source_type, source_id, category)
        );

        -- AI-extracted triggers
        CREATE TABLE IF NOT EXISTS ai_triggers (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            trigger_name TEXT NOT NULL,
            trigger_category TEXT,
            PRIMARY KEY (source_type, source_id, trigger_name)
        );

        -- AI-extracted caregiver emotional state
        CREATE TABLE IF NOT EXISTS ai_caregiver_emotional_state (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            category TEXT NOT NULL,
            PRIMARY KEY (source_type, source_id, category)
        );

        -- AI-extracted Singulair behavioral effects
        CREATE TABLE IF NOT EXISTS ai_singulair_effects (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            effect TEXT NOT NULL,
            PRIMARY KEY (source_type, source_id, effect)
        );

        -- AI-extracted functional impact
        CREATE TABLE IF NOT EXISTS ai_functional_impact (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            category TEXT NOT NULL,
            PRIMARY KEY (source_type, source_id, category)
        );

        -- AI-extracted inhaler confusion
        CREATE TABLE IF NOT EXISTS ai_inhaler_confusion (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            category TEXT NOT NULL,
            PRIMARY KEY (source_type, source_id, category)
        );

        -- 12 user-approved asthma health topics
        CREATE TABLE IF NOT EXISTS asthma_health_topics (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            topic TEXT NOT NULL,
            PRIMARY KEY (source_type, source_id, topic)
        );
        CREATE INDEX IF NOT EXISTS idx_health_topics_topic
            ON asthma_health_topics(topic);
        CREATE INDEX IF NOT EXISTS idx_health_topics_source
            ON asthma_health_topics(source_type, source_id);
    """)

    # Add AI columns to posts and comments tables
    for col, tbl in [
        ("ai_analyzed", "posts"),
        ("ai_sentiment", "posts"),
        ("ai_fear_score", "posts"),
        ("ai_analyzed", "comments"),
        ("ai_sentiment", "comments"),
        ("ai_fear_score", "comments"),
    ]:
        try:
            conn.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} INTEGER DEFAULT 0"
                         if col == "ai_analyzed"
                         else f"ALTER TABLE {tbl} ADD COLUMN {col} REAL")
        except Exception:
            pass  # Column already exists

    conn.commit()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="AI pipeline for peds-asthma-tracker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Commands:
  pilot --core          Process 240 core validation items (120 posts + 120 comments)
  pilot --sample N      Process N random items (~${0.80 / 1_000_000 * 800 * 1000:.2f} per 1K posts)
  backfill              Process full corpus
  test-golden           Run golden regression test suite (10 items)
  costs                 Print AI cost report

Model: {MODEL}
Pricing: ${_INPUT_COST_PER_TOKEN * 1_000_000:.2f}/M input, ${_OUTPUT_COST_PER_TOKEN * 1_000_000:.2f}/M output
Prompt version: {PROMPT_VERSION}
        """,
    )
    subparsers = parser.add_subparsers(dest="command")

    # pilot
    pilot_p = subparsers.add_parser("pilot", help="Run AI on subset of items")
    pilot_group = pilot_p.add_mutually_exclusive_group(required=True)
    pilot_group.add_argument("--core", action="store_true",
                              help="Process 240 core validation items (120 posts + 120 comments)")
    pilot_group.add_argument("--sample", type=int, metavar="N",
                              help="Process N random items")

    # backfill
    subparsers.add_parser("backfill", help="Process full corpus")

    # test-golden
    tg_p = subparsers.add_parser("test-golden", help="Run golden regression tests")
    tg_p.add_argument("--verbose", "-v", action="store_true",
                       help="Show full AI output for failing tests")

    # costs
    costs_p = subparsers.add_parser("costs", help="Print AI cost report")
    costs_p.add_argument("--days", type=int, default=30,
                          help="Days to look back (default: 30)")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "test-golden":
        verbose = getattr(args, "verbose", False)
        passed = run_golden_tests(verbose=verbose)
        sys.exit(0 if passed else 1)

    # All other commands need DB access
    conn = tracker.get_db()
    ensure_schema(conn)

    if args.command == "pilot":
        if getattr(args, "core", False):
            run_ai_pilot(conn, mode="sample", sample_size=240)
        else:
            run_ai_pilot(conn, mode="sample", sample_size=args.sample)

    elif args.command == "backfill":
        run_ai_pilot(conn, mode="full")

    elif args.command == "costs":
        days = getattr(args, "days", 30)
        print_cost_report(conn, days=days)

    conn.close()


if __name__ == "__main__":
    main()
