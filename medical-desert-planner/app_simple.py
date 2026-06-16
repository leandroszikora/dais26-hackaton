#!/usr/bin/env python
"""Medical Desert Planner - Production · DAIS 2026 Hackathon Track 2

Gap risk = trust-weighted supply coverage × NFHS-5 district demand.
Data: 10K real facilities · NFHS-5 district health indicators · India POST PIN directory.
"""

import json, math, time, io, csv
from datetime import datetime
from flask import Flask, render_template_string, request, jsonify, Response
from databricks.sdk import WorkspaceClient

app = Flask(__name__)
w   = WorkspaceClient()

FAC_TABLE  = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities"
NFHS_TABLE = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.nfhs_5_district_health_indicators"
PIN_TABLE  = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.india_post_pincode_directory"
SCENARIO_TABLE = "workspace.medical_desert_app.scenarios"

INDIA_LAT = (6.0, 37.5)
INDIA_LON = (67.0, 97.5)

# Structured specialty taxonomy → capability (highest-signal match)
SPECIALTY_CAPS = {
    "ICU":        {"criticalCareMedicine","pulmonologyCriticalCare","anesthesiaCriticalCare"},
    "Maternity":  {"gynecologyAndObstetrics","maternalAndFetalMedicine",
                   "reproductiveEndocrinologyAndInfertility","neonatologyPerinatalMedicine"},
    "Emergency":  {"emergencyMedicine","pediatricEmergencyMedicine","traumaSurgery"},
    "Dialysis":   {"nephrology","pediatricNephrology"},
    "Oncology":   {"oncology","radiationOncology","surgicalOncology","hematologyOncology",
                   "pediatricHematologyOncology","gynecologicOncology"},
    "Cardiology": {"cardiology","interventionalCardiology","pediatricCardiology"},
    "Pediatrics": {"pediatrics","neonatologyPerinatalMedicine","pediatricSurgery",
                   "pediatricOrthopedicSurgery","pediatricEmergencyMedicine",
                   "pediatricNephrology","pediatricHematologyOncology","pediatricCardiology"},
    "Surgery":    {"generalSurgery","traumaSurgery","neurosurgery","orthopedicSurgery",
                   "plasticSurgery","urology","spineNeurosurgery","jointReconstructionSurgery"},
}

# Text keywords (supplement to specialty - catches description/procedure/equipment)
KEYWORDS = {
    "ICU":        ["icu","intensive care","critical care","ventilator","icu bed"],
    "Maternity":  ["maternity","obstetric","gynecol","delivery","labour","labor",
                   "prenatal","antenatal","postnatal","birth"],
    "Emergency":  ["emergency","trauma","ambulance","casualty","accident"],
    "Dialysis":   ["dialysis","hemodialysis","renal","nephrology"],
    "Oncology":   ["oncology","cancer","chemotherapy","radiation therapy","radiotherapy","tumor"],
    "Cardiology": ["cardiology","cardiac","coronary","angioplasty","echocardiography"],
    "Pediatrics": ["pediatric","paediatric","nicu","neonatal","children","child health"],
    "Surgery":    ["surgery","surgical","operation theater","laparoscopic","general surgery"],
}

# NFHS-5 demand indicator per capability:
# (column, label, invert, description)
# invert=True  → low indicator value = high need  (e.g. few institutional births = need maternity)
# invert=False → high indicator value = high need  (e.g. high BP prevalence = need cardiology)
DEMAND = {
    "Maternity":  ("institutional_birth_5y_pct",
                   "Institutional births %", True,
                   "Districts with fewer institutional births need maternity care most urgently"),
    "Emergency":  ("hh_member_covered_health_insurance_pct",
                   "Health insurance %", True,
                   "Low insurance coverage = high out-of-pocket burden for emergency care"),
    "ICU":        ("hh_member_covered_health_insurance_pct",
                   "Health insurance %", True,
                   "Low insurance = high financial barrier to ICU access"),
    "Dialysis":   ("all_w15_49_who_are_anaemic_pct",
                   "Women anaemia %", False,
                   "Anaemia prevalence reflects chronic disease / kidney disease burden"),
    "Oncology":   ("hh_member_covered_health_insurance_pct",
                   "Health insurance %", True,
                   "Low insurance = high financial barrier to cancer treatment"),
    "Cardiology": ("w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct",
                   "High BP prevalence %", False,
                   "High BP prevalence signals elevated cardiovascular disease burden"),
    "Pediatrics": ("prev_diarrhoea_2wk_child_u5_pct",
                   "Child diarrhoea %", False,
                   "Child diarrhoea prevalence indicates unmet pediatric health burden"),
    "Surgery":    ("births_delivered_by_csection_5y_pct",
                   "C-section births %", False,
                   "C-section rate is a proxy for existing surgical access and utilisation"),
}

CAPABILITIES    = list(KEYWORDS.keys())
PRIORITY_ORDER  = ["CRITICAL GAP","HIGH PRIORITY","MODERATE","DATA-POOR","ADEQUATE"]
COLORS = {"CRITICAL GAP":"#D32F2F","HIGH PRIORITY":"#F57C00",
          "MODERATE":"#FBC02D","DATA-POOR":"#9E9E9E","ADEQUATE":"#388E3C"}

_cache:     dict = {"fac": None, "nfhs": None, "pin": None, "ts": 0.0}
_dir_cache: dict = {"data": None, "ts": 0.0}

# ── Data layer ────────────────────────────────────────────────────────────────

def _wh():
    whs = list(w.warehouses.list())
    if not whs: raise RuntimeError("No SQL warehouse available.")
    return whs[0].id

def _query(sql, params=None):
    from databricks.sdk.service.sql import StatementState
    TERMINAL = {StatementState.SUCCEEDED, StatementState.FAILED,
                StatementState.CANCELED, StatementState.CLOSED}
    r = w.statement_execution.execute_statement(
        warehouse_id=_wh(), statement=sql, wait_timeout="50s", parameters=params)
    while r.status.state not in TERMINAL:
        time.sleep(3)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"Query failed: {r.status.error}")
    cols = [c.name for c in r.manifest.schema.columns]
    rows = []
    chunk = r.result
    while chunk:
        if chunk.data_array:
            rows.extend(dict(zip(cols, row)) for row in chunk.data_array)
        if chunk.next_chunk_index is None:
            break
        chunk = w.statement_execution.get_statement_result_chunk_n(
            r.statement_id, chunk.next_chunk_index)
    return rows

import re as _re
_PIN_SUFFIX = _re.compile(
    r'\s+(B\.?O\.?|H\.?O\.?|S\.?O\.?|G\.?P\.?O\.?|P\.?O\.?|'
    r'Branch\s*Office|Head\s*Office|Sub\s*Office|'
    r'Sub\s*P\.?O\.?|Head\s*P\.?O\.?|B/O|H/O|S/O)\s*$',
    _re.IGNORECASE)

def load_data():
    if _cache["fac"] is not None and time.time() - _cache["ts"] < 3600:
        return

    fac = _query(f"""
        SELECT name, facilityTypeId,
               address_city, address_stateOrRegion, address_zipOrPostcode,
               latitude, longitude,
               SUBSTRING(COALESCE(description,            ''), 1, 200)  AS description,
               SUBSTRING(COALESCE(CAST(capability  AS STRING), ''), 1, 600)  AS capability,
               SUBSTRING(COALESCE(CAST(procedure   AS STRING), ''), 1, 300)  AS procedure,
               SUBSTRING(COALESCE(CAST(equipment   AS STRING), ''), 1, 150)  AS equipment,
               SUBSTRING(COALESCE(CAST(specialties AS STRING), ''), 1, 1200) AS specialties,
               numberDoctors, capacity,
               distinct_social_media_presence_count,
               affiliated_staff_presence, custom_logo_presence,
               SUBSTRING(COALESCE(CAST(source_types AS STRING), ''), 1, 150) AS source_types
        FROM {FAC_TABLE} LIMIT 10000
    """)

    nfhs_rows = _query(f"""
        SELECT district_name, state_ut,
               institutional_birth_5y_pct,
               hh_member_covered_health_insurance_pct,
               all_w15_49_who_are_anaemic_pct,
               w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct,
               m15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct,
               w15_plus_with_high_or_very_high_gt_140_mg_dl_blood_sugar_or_pct,
               women_age_15_49_years_who_are_overweight_obese_bmi_gte_25_0_pct,
               prev_diarrhoea_2wk_child_u5_pct,
               children_prev_symptoms_of_acute_respiratory_infection_ari_2_pct,
               women_age_15_49_who_are_literate_pct,
               hh_use_improved_sanitation_pct,
               hh_improved_water_pct,
               hh_electricity_pct,
               births_delivered_by_csection_5y_pct,
               births_attended_by_skilled_hp_5y_10_pct
        FROM {NFHS_TABLE}
    """)

    pin_rows = _query(f"""
        SELECT CAST(pincode AS STRING) AS pincode, district, statename
        FROM {PIN_TABLE}
        GROUP BY pincode, district, statename
    """)

    # Officename → district index for city-name fallback
    # Strip post-office suffixes (B.O, H.O etc.) to get bare place names
    office_rows = _query(f"""
        SELECT DISTINCT officename, district, LOWER(statename) AS state
        FROM {PIN_TABLE}
        WHERE officename IS NOT NULL AND officename != '' AND district IS NOT NULL
    """)

    # Index NFHS by (district.lower(), state.lower()) and by state.lower()
    by_dist, by_state = {}, {}
    for r in nfhs_rows:
        d = (r.get("district_name") or "").strip().lower()
        s = (r.get("state_ut")      or "").strip().lower()
        by_dist[(d, s)] = r
        by_state.setdefault(s, []).append(r)

    # Index PIN: pincode → {district, state}
    pin_idx = {}
    for r in pin_rows:
        p = str(r.get("pincode") or "").strip()
        if p:
            pin_idx[p] = {"district": (r.get("district") or "").strip(),
                          "state":    (r.get("statename") or "").strip()}

    # Tier-2 index: officename (cleaned) + state → district
    office_idx = {}
    for r in office_rows:
        raw  = (r.get("officename") or "").strip()
        clean = _PIN_SUFFIX.sub("", raw).strip().lower()
        state = (r.get("state") or "").strip()
        dist  = (r.get("district") or "").strip()
        if clean and dist and state:
            office_idx.setdefault((clean, state), dist)

    # Tier-3 index: city + state → district, derived from facilities that DO
    # have a valid PIN.  Built after pin_idx so we can resolve immediately.
    city_dist = {}
    for f in fac:
        pin  = str(f.get("address_zipOrPostcode") or "").strip()
        city = (f.get("address_city") or "").strip()
        st   = (f.get("address_stateOrRegion") or "").strip()
        info = pin_idx.get(pin)
        if info and city and st:
            city_dist.setdefault((city.lower(), st.lower()), info["district"])

    _cache["fac"]       = fac
    _cache["nfhs"]      = {"dist": by_dist, "state": by_state}
    _cache["pin"]       = pin_idx
    _cache["office_idx"]= office_idx
    _cache["city_dist"] = city_dist
    _cache["ts"]        = time.time()

# ── Helpers ───────────────────────────────────────────────────────────────────

def _arr(v):
    if not v or str(v) in ("None","nan","null","[]",""): return []
    if isinstance(v, list): return [str(x) for x in v if x]
    try:
        r = json.loads(v)
        return [str(x) for x in r] if isinstance(r, list) else [str(r)]
    except:
        return [str(v)]

def _f(v):
    try: return float(v)
    except: return None

def valid_coords(lat, lon):
    try:
        return INDIA_LAT[0]<=float(lat)<=INDIA_LAT[1] and INDIA_LON[0]<=float(lon)<=INDIA_LON[1]
    except: return False

def haversine(la1, lo1, la2, lo2):
    R = 6371; f1,f2 = math.radians(la1), math.radians(la2)
    a = math.sin((f2-f1)/2)**2 + math.cos(f1)*math.cos(f2)*math.sin(math.radians(lo2-lo1)/2)**2
    return round(2*R*math.asin(math.sqrt(a)), 1)

def _avg_nfhs(rows):
    cols = ["institutional_birth_5y_pct","hh_member_covered_health_insurance_pct",
            "all_w15_49_who_are_anaemic_pct",
            "w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct",
            "prev_diarrhoea_2wk_child_u5_pct","births_delivered_by_csection_5y_pct",
            "births_attended_by_skilled_hp_5y_10_pct"]
    out = {}
    for c in cols:
        vals = [_f(r.get(c)) for r in rows if _f(r.get(c)) is not None]
        out[c] = round(sum(vals)/len(vals),1) if vals else None
    return out

def _district_of(row):
    """Return (district, state) via 3-tier lookup: PIN → city-name → officename."""
    pin  = str(row.get("address_zipOrPostcode") or "").strip()
    city = (row.get("address_city") or "").strip()
    state= (row.get("address_stateOrRegion") or "").strip()

    # Tier 1: direct PIN match
    info = _cache["pin"].get(pin)
    if info:
        return info["district"], info["state"]

    if city and state:
        sl = state.lower()
        # Tier 2: same city appears in a facility that has a known PIN
        dist = _cache.get("city_dist", {}).get((city.lower(), sl))
        if dist:
            return dist, state
        # Tier 3: city name matches a post office name in PIN directory
        dist = _cache.get("office_idx", {}).get((city.lower(), sl))
        if dist:
            return dist, state

    return None, None

def get_nfhs(row):
    dist, dstate = _district_of(row)
    if dist:
        s = (dstate or row.get("address_stateOrRegion") or "").strip().lower()
        rec = _cache["nfhs"]["dist"].get((dist.lower(), s))
        if rec: return rec, dist
    state_key = (row.get("address_stateOrRegion") or "").strip().lower()
    rows = _cache["nfhs"]["state"].get(state_key, [])
    return (_avg_nfhs(rows) if rows else None), None

# ── Facility scoring ──────────────────────────────────────────────────────────

def score_facility(row, cap):
    # 1. Structured specialty match - 0-6 pts
    # Search for each specialty ID as a quoted token in the raw JSON string.
    # This is robust to truncation (parsed JSON would fail on a cut-off array).
    spec_raw  = str(row.get("specialties") or "")
    spec_hits = {s for s in SPECIALTY_CAPS.get(cap, set())
                 if f'"{s}"' in spec_raw or f"'{s}'" in spec_raw}
    ss        = min(len(spec_hits), 3) * 2.0

    # 2. Text keyword evidence - 0-4 pts
    kws   = KEYWORDS.get(cap, [])
    texts = {
        "capability":  " ".join(_arr(row.get("capability"))),
        "procedure":   " ".join(_arr(row.get("procedure"))),
        "equipment":   " ".join(_arr(row.get("equipment"))),
        "description": str(row.get("description") or ""),
    }
    evidence, fhits = [], {}
    for fld, txt in texts.items():
        tl = txt.lower()
        for kw in kws:
            if kw in tl:
                i = tl.find(kw); s2,e = max(0,i-55), min(len(tl),i+len(kw)+55)
                evidence.append({"field":fld,"text":f"…{tl[s2:e].strip()}…","kw":kw})
                fhits[fld] = True; break
    ts = min(len(fhits)*1.5, 4.0)

    # 3. Source & data quality - 0-2 pts
    q, qsig = 0.0, []
    if str(row.get("affiliated_staff_presence")).lower() in ("true","1"):
        q+=0.5;  qsig.append("staff profiles")
    if str(row.get("custom_logo_presence")).lower() in ("true","1"):
        q+=0.25; qsig.append("verified logo")
    try:
        sm = int(row.get("distinct_social_media_presence_count") or 0)
        if sm>=2: q+=0.5;  qsig.append(f"{sm} social channels")
        elif sm==1: q+=0.2; qsig.append("1 social channel")
    except: pass
    if len(set(_arr(row.get("source_types")))) >= 2:
        q+=0.25; qsig.append("multi-source confirmed")
    try:
        d = int(row.get("numberDoctors") or 0)
        if d>0: q+=0.5; qsig.append(f"{d} doctors")
    except: pass
    try:
        c = int(row.get("capacity") or 0)
        if c>0: q+=0.25; qsig.append(f"{c} beds")
    except: pass

    total = ss + ts + q

    if ss>=4 or (ss>=2 and len(fhits)>=2):               level="STRONG"
    elif ss>=2 or len(fhits)>=2 or (ss>0 and len(fhits)): level="PARTIAL"
    elif ss>0 or len(fhits)>=1:                            level="WEAK"
    else:                                                   level="NO CLAIM"

    return {"score":round(total,2),"level":level,
            "spec_hits":sorted(spec_hits),"evidence":evidence[:5],"quality":qsig,
            "spec_score":ss,"text_score":round(ts,2),"quality_score":round(q,2)}

# ── Geography ─────────────────────────────────────────────────────────────────

def region_key(row, geo):
    city  = row.get("address_city") or "Unknown"
    state = row.get("address_stateOrRegion") or "Unknown"
    pin   = str(row.get("address_zipOrPostcode") or "").strip()
    if geo=="state":    return state
    if geo=="city":     return f"{city} ({state})"
    if geo=="district":
        dist, dstate = _district_of(row)
        return f"{dist}, {dstate or state}" if dist else f"Unknown District ({state})"
    return f"PIN {pin} · {city}" if pin else f"{city} ({state})"

def coverage_pct(strong, partial, weak, n):
    if n==0: return 0.0
    return round((strong + partial*0.5 + weak*0.25)/n*100, 1)

def demand_score(nfhs_rec, cap):
    if not nfhs_rec: return None,None,None,None
    cfg = DEMAND.get(cap)
    if not cfg:     return None,None,None,None
    col,label,invert,desc = cfg
    val = _f(nfhs_rec.get(col))
    if val is None: return None,label,"%",desc
    need = round((100-val) if invert else val, 1)
    return need,label,"%",desc

def priority_label(cov, n, has_ev, need=None):
    risk = round((1-cov/100)*(need/100 if need is not None else 0.5)*100,1) if need is not None else None
    if n<3 and not has_ev:  return "DATA-POOR","Too few records - cannot distinguish real gap from data absence",risk
    if cov<20 and n>=3:     return "CRITICAL GAP","High confidence - many facilities, very low capability evidence",risk
    if cov<40:              return "HIGH PRIORITY","Likely gap - capability evidence weak across most facilities",risk
    if cov<65:              return "MODERATE","Partial coverage - some facilities show evidence",risk
    return "ADEQUATE","Most facilities show evidence of this capability",risk

def confidence_pct(n, strong, partial, has_district):
    # Sample size (0-40 pts): 20+ facilities = full score
    n_pts  = min(n / 20.0, 1.0) * 40
    # Evidence quality (0-40 pts): weight strong evidence more than partial
    ev_pts = ((strong + partial * 0.5) / n * 40) if n > 0 else 0
    # Geographic data resolution (8 or 20 pts): district NFHS > state average
    geo_pts = 20 if has_district else 8
    return int(min(round(n_pts + ev_pts + geo_pts), 100))

def margin_of_error(n):
    if n == 0: return 50
    # Approximation: ±50/sqrt(n), floored at 5%
    return max(5, int(round(50 / math.sqrt(n))))

# ── CGR score (adapted from india-health-map methodology) ─────────────────────
# Composite Care Gap Risk 0-100
#   40% Geographic Access deficit  (inverse of our coverage%)
#   25% Health Burden               (NFHS-5: anaemia, BP, blood sugar, obesity, diarrhoea, ARI)
#   25% Social Vulnerability        (inverse of literacy, sanitation, insurance, water, electricity, inst.birth)
#   10% Capability Density Gap      (from evidence mix strong/partial ratios)
#
# Risk tiers: CRITICAL >= 57 · HIGH 47-57 · ELEVATED 35-47 · MODERATE 22-35 · LOW < 22

_CGR_TIER_COLORS = {
    "CRITICAL": "#B71C1C", "HIGH": "#E64A19",
    "ELEVATED": "#F57C00", "MODERATE": "#FBC02D", "LOW": "#388E3C"
}

def cgr_score(nfhs_row, coverage, strong, partial, weak, n):
    def _p(k): return float(nfhs_row.get(k) or 0) if nfhs_row else 0

    # GA: piecewise inverse of our coverage% (0=fully covered, 95=zero coverage)
    ga = (1.0 - min(coverage or 0, 100) / 100.0) * 95.0

    # HB: average of 6 burden indicators
    if nfhs_row:
        hb = min(100, max(0, (
            _p("all_w15_49_who_are_anaemic_pct") +
            (_p("w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct") +
             _p("m15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct")) / 2.0 +
            _p("w15_plus_with_high_or_very_high_gt_140_mg_dl_blood_sugar_or_pct") +
            _p("women_age_15_49_years_who_are_overweight_obese_bmi_gte_25_0_pct") +
            _p("prev_diarrhoea_2wk_child_u5_pct") +
            _p("children_prev_symptoms_of_acute_respiratory_infection_ari_2_pct")
        ) / 6.0))
        # SV: inverse of 6 protective indicators (lower coverage = more vulnerable)
        def _sv(k, dflt=50): return 100 - (_p(k) or dflt)
        sv = min(100, max(0, (
            _sv("women_age_15_49_who_are_literate_pct") +
            _sv("hh_use_improved_sanitation_pct") +
            _sv("hh_member_covered_health_insurance_pct") +
            _sv("hh_improved_water_pct") +
            _sv("hh_electricity_pct") +
            _sv("institutional_birth_5y_pct")
        ) / 6.0))
    else:
        hb, sv = 30.0, 45.0  # national average fallbacks when no NFHS match

    # CG: capability density from evidence mix
    if n > 0:
        sr = strong / n
        cg = 10.0 if sr >= 0.4 else (30.0 if sr >= 0.2 else (50.0 if sr > 0 else
             (65.0 if (partial / n) >= 0.2 else 85.0)))
    else:
        cg = 90.0

    score = round(0.40 * ga + 0.25 * hb + 0.25 * sv + 0.10 * cg, 1)
    tier  = ("CRITICAL" if score >= 57 else "HIGH" if score >= 47 else
             "ELEVATED" if score >= 35 else "MODERATE" if score >= 22 else "LOW")
    return score, tier, round(ga, 1), round(hb, 1), round(sv, 1), round(cg, 1)

# ── Facility directory ────────────────────────────────────────────────────────

def load_fac_dir():
    if _dir_cache["data"] is not None and time.time() - _dir_cache["ts"] < 3600:
        return
    # Ensure pin/district indexes are ready
    load_data()
    rows = _query(f"""
        SELECT name, facilityTypeId, operatorTypeId,
               address_line1, address_city, address_stateOrRegion, address_zipOrPostcode,
               numberDoctors, capacity, yearEstablished,
               officialPhone, officialWebsite,
               affiliated_staff_presence, custom_logo_presence,
               distinct_social_media_presence_count,
               engagement_metrics_n_followers,
               engagement_metrics_n_likes,
               engagement_metrics_n_engagements,
               number_of_facts_about_the_organization,
               recency_of_page_update,
               latitude, longitude,
               SUBSTRING(COALESCE(CAST(affiliationTypeIds AS STRING),''),1,300) AS affiliations,
               SUBSTRING(COALESCE(CAST(specialties        AS STRING),''),1,400) AS specialties
        FROM {FAC_TABLE} LIMIT 8000
    """)
    # Enrich with district (3-tier lookup) and compute rating
    enriched = []
    for r in rows:
        dist, _ = _district_of(r)
        enriched.append({**r, "district": dist or ""})
    _dir_cache["data"] = enriched
    _dir_cache["ts"]   = time.time()

def fac_rating(row):
    """Profile completeness score 0-5 derived from data quality signals."""
    pts = 0.0
    if (_f(row.get("numberDoctors") or 0) or 0) > 0:                           pts += 1.0
    if (_f(row.get("capacity") or 0) or 0) > 0:                                pts += 0.75
    if row.get("officialPhone"):                                                 pts += 0.5
    if row.get("officialWebsite"):                                               pts += 0.5
    if row.get("yearEstablished") and str(row["yearEstablished"]) not in ("","None","null"): pts += 0.25
    if str(row.get("affiliated_staff_presence","")).lower() in ("true","1"):    pts += 0.75
    if str(row.get("custom_logo_presence","")).lower()      in ("true","1"):    pts += 0.25
    try:
        sm = int(row.get("distinct_social_media_presence_count") or 0)
        pts += 0.5 if sm >= 2 else (0.2 if sm == 1 else 0)
    except: pass
    try:
        f = int(row.get("engagement_metrics_n_followers") or 0)
        pts += 0.5 if f > 1000 else (0.25 if f > 100 else 0)
    except: pass
    try:
        n = int(row.get("number_of_facts_about_the_organization") or 0)
        pts += 0.5 if n > 20 else (0.25 if n > 5 else 0)
    except: pass
    return round(min(pts / 5.0 * 5, 5.0), 1)

# ── Scenario persistence ────────────────────────────────────────────────────
# Durable storage: a Delta table (not local disk), so saved scenarios survive
# app restarts/redeploys - Databricks Apps containers are ephemeral and wipe
# /tmp on every redeploy or compute restart.

def load_scenarios():
    try:
        rows = _query(f"""
            SELECT id, name, notes, capability, geo_level, critical_regions,
                   total_regions, saved_at
            FROM {SCENARIO_TABLE} ORDER BY id ASC""")
        for r in rows:
            try: r["critical_regions"] = json.loads(r.get("critical_regions") or "[]")
            except: r["critical_regions"] = []
        return rows
    except Exception:
        return []

def save_scenario(sc):
    from databricks.sdk.service.sql import StatementParameterListItem as P
    sid      = datetime.now().strftime("%Y%m%d%H%M%S%f")
    saved_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    _query(f"""
        INSERT INTO {SCENARIO_TABLE}
            (id, name, notes, capability, geo_level, critical_regions, total_regions, saved_at)
        VALUES (:id, :name, :notes, :capability, :geo_level, :critical_regions,
                CAST(:total_regions AS INT), :saved_at)""",
        params=[
            P(name="id", value=sid),
            P(name="name", value=sc.get("name","")),
            P(name="notes", value=sc.get("notes","")),
            P(name="capability", value=sc.get("capability","")),
            P(name="geo_level", value=sc.get("geo_level","")),
            P(name="critical_regions", value=json.dumps(sc.get("critical_regions",[]))),
            P(name="total_regions", value=str(sc.get("total_regions",0))),
            P(name="saved_at", value=saved_at),
        ])
    return saved_at

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def home():
    fc = len(_cache["fac"]) if _cache["fac"] else "~10,000"
    return render_template_string(HOME_T, capabilities=CAPABILITIES,
        scenarios=load_scenarios()[-5:], facility_count=fc)

@app.route("/analyze", methods=["GET","POST"])
def analyze():
    p       = request.form if request.method=="POST" else request.args
    cap     = p.get("capability","Maternity")
    ftype   = p.get("facility_type","all")
    ref_lat = p.get("ref_lat","").strip()
    ref_lon = p.get("ref_lon","").strip()
    ref_city= p.get("ref_city","").strip()

    # Hierarchy drill-down filters
    filter_state    = p.get("filter_state","").strip()
    filter_district = p.get("filter_district","").strip()
    filter_city_val = p.get("filter_city","").strip()

    # Auto-determine geo level from filter depth
    if filter_city_val:
        geo = "pin";      drill_field = None
    elif filter_district:
        geo = "city";     drill_field = "filter_city"
    elif filter_state:
        geo = "district"; drill_field = "filter_district"
    else:
        geo = p.get("geo_level","state")
        drill_field = "filter_state" if geo == "state" else None

    # Build breadcrumb
    breadcrumb = [{"label":"All India","fs":"","fd":"","fc":""}]
    if filter_state:
        breadcrumb.append({"label":filter_state,"fs":filter_state,"fd":"","fc":""})
    if filter_district:
        breadcrumb.append({"label":filter_district,"fs":filter_state,"fd":filter_district,"fc":""})
    if filter_city_val:
        breadcrumb.append({"label":filter_city_val,"fs":filter_state,"fd":filter_district,"fc":filter_city_val})

    try:
        load_data()
        rows = list(_cache["fac"])
        if ftype=="hospital":
            rows=[r for r in rows if (r.get("facilityTypeId") or "").lower() in
                  ("hospital","medical center","health center")]
        elif ftype=="clinic":
            rows=[r for r in rows if (r.get("facilityTypeId") or "").lower() in
                  ("clinic","dispensary","health post")]

        # Apply hierarchy filters
        if filter_state:
            rows=[r for r in rows if
                  (r.get("address_stateOrRegion") or "").strip().lower()==filter_state.lower()]
        if filter_district:
            def _dist_of(row):
                d, _ = _district_of(row)
                return (d or "").lower()
            rows=[r for r in rows if _dist_of(r)==filter_district.lower()]
        if filter_city_val:
            rows=[r for r in rows if
                  (r.get("address_city") or "").strip().lower()==filter_city_val.lower()]

        scored = []
        for row in rows:
            s  = score_facility(row, cap)
            hc = valid_coords(row.get("latitude"), row.get("longitude"))
            dist=None
            if ref_lat and ref_lon and hc:
                try: dist=haversine(float(ref_lat),float(ref_lon),float(row["latitude"]),float(row["longitude"]))
                except: pass
            scored.append({
                "name":    row.get("name") or "Unknown",
                "city":    row.get("address_city") or "Unknown",
                "state":   row.get("address_stateOrRegion") or "Unknown",
                "pin":     str(row.get("address_zipOrPostcode") or "").strip(),
                "ftype":   row.get("facilityTypeId") or "",
                "lat":     float(row["latitude"])  if hc else None,
                "lon":     float(row["longitude"]) if hc else None,
                "hc":      hc,
                **{k:s[k] for k in ("score","level","spec_hits","evidence","quality",
                                     "spec_score","text_score","quality_score")},
                "dist_km": dist,
                "doctors": row.get("numberDoctors") or "",
                "capacity":row.get("capacity") or "",
                "_row":    row,
            })

        agg = {}
        for item in scored:
            k = region_key(item["_row"], geo)
            if k not in agg:
                agg[k]={"items":[],"strong":0,"partial":0,"weak":0,"none":0,
                        "lats":[],"lons":[],"nfhs":None,"district":None,"state":item["state"]}
            g=agg[k]; g["items"].append(item)
            lv=item["level"]
            if lv=="STRONG":   g["strong"]+=1
            elif lv=="PARTIAL":g["partial"]+=1
            elif lv=="WEAK":   g["weak"]+=1
            else:              g["none"]+=1
            if item["hc"]: g["lats"].append(item["lat"]); g["lons"].append(item["lon"])
            if g["nfhs"] is None:
                nrec, dname = get_nfhs(item["_row"])
                g["nfhs"]=nrec; g["district"]=dname

        results=[]
        for region,g in agg.items():
            n   = len(g["items"])
            cov = coverage_pct(g["strong"],g["partial"],g["weak"],n)
            has_ev=(g["strong"]+g["partial"]+g["weak"])>0
            need,dlabel,dunit,ddesc = demand_score(g["nfhs"],cap)
            pri,note,risk = priority_label(cov,n,has_ev,need)
            clat=round(sum(g["lats"])/len(g["lats"]),4) if g["lats"] else None
            clon=round(sum(g["lons"])/len(g["lons"]),4) if g["lons"] else None
            conf = confidence_pct(n, g["strong"], g["partial"], bool(g["district"]))
            moe  = margin_of_error(n)
            if conf >= 70 and n >= 10:
                dq_label, dq_color = "Verified",  "#2E7D32"
            elif conf >= 40:
                dq_label, dq_color = "Moderate",  "#F57C00"
            else:
                dq_label, dq_color = "Sparse",    "#9E9E9E"
            cgr, cgr_tier, cgr_ga, cgr_hb, cgr_sv, cgr_cg = cgr_score(
                g["nfhs"], cov, g["strong"], g["partial"], g["weak"], n)
            # drill_val: raw value to pass to the next filter level
            if drill_field == "filter_district":
                dv = region.split(",")[0].strip()
            elif drill_field == "filter_city":
                dv = region.split("(")[0].strip()
            else:
                dv = region
            # Top cited evidence backing this region's score/ranking - shown
            # inline in the row-detail panel so the claim is never just a
            # number with no traceable source text.
            top_evidence = []
            for it in sorted(g["items"], key=lambda x: -x["score"])[:2]:
                if it["evidence"]:
                    e = it["evidence"][0]
                    top_evidence.append({"name": it["name"], "field": e["field"], "text": e["text"]})
            results.append({
                "region":region,"n":n,"coverage":cov,"state":g["state"],
                "priority":pri,"color":COLORS[pri],"note":note,"risk":risk,
                "strong":g["strong"],"partial":g["partial"],"weak":g["weak"],"none":g["none"],
                "lat":clat,"lon":clon,
                "need":need,"dlabel":dlabel,"dunit":dunit,"ddesc":ddesc,
                "district":g["district"],
                "confidence":conf,"moe":moe,"dq_label":dq_label,"dq_color":dq_color,
                "cgr":cgr,"cgr_tier":cgr_tier,"cgr_color":_CGR_TIER_COLORS[cgr_tier],
                "cgr_ga":cgr_ga,"cgr_hb":cgr_hb,"cgr_sv":cgr_sv,"cgr_cg":cgr_cg,
                "drill_val":dv,
                "top_evidence":top_evidence,
                "items":sorted(g["items"],key=lambda x:-x["score"]),
            })

        def _sort(r):
            return (PRIORITY_ORDER.index(r["priority"]), -(r["risk"] or 0))
        results.sort(key=_sort)
        for i, r in enumerate(results, 1):
            r["rank"] = i

        # When viewing districts/cities across all of India (no state filter applied
        # yet), group rows by state so the table shows state -> district/city
        # hierarchy instead of a flat list. Within a single drilled-down state this
        # is unnecessary since every row already belongs to the same state.
        grouped = None
        if geo in ("district", "city") and not filter_state:
            buckets, order = {}, []
            for r in results:
                st = r.get("state") or "Unknown"
                if st not in buckets:
                    buckets[st] = []; order.append(st)
                buckets[st].append(r)
            grouped = []
            for st in order:
                items = buckets[st]
                n_critical = sum(1 for r in items if r["priority"] == "CRITICAL GAP")
                n_high     = sum(1 for r in items if r["priority"] == "HIGH PRIORITY")
                worst      = min(PRIORITY_ORDER.index(r["priority"]) for r in items)
                avg_cov    = round(sum(r["coverage"] for r in items) / len(items), 1)
                grouped.append({"state":st,"rows":items,"n":len(items),
                                 "n_critical":n_critical,"n_high":n_high,
                                 "worst_priority":worst,"avg_coverage":avg_cov})
            grouped.sort(key=lambda g: (g["worst_priority"], -g["n_critical"]))

        geo_json=json.dumps([{k:v for k,v in r.items() if k!="items"} for r in results])
        return render_template_string(RESULTS_T,
            cap=cap,geo=geo,ftype=ftype,results=results,grouped=grouped,geo_json=geo_json,
            total=len(scored),ref_city=ref_city,has_dist=bool(ref_lat and ref_lon),
            capabilities=CAPABILITIES,
            filter_state=filter_state,filter_district=filter_district,
            filter_city=filter_city_val,drill_field=drill_field,
            breadcrumb=breadcrumb)

    except Exception as exc:
        import traceback
        return render_template_string(ERR_T,err=str(exc),trace=traceback.format_exc(),cap=cap)

@app.route("/facility_detail")
def facility_detail():
    region=request.args.get("region","")
    cap   =request.args.get("capability","")
    geo   =request.args.get("geo_level","state")
    ftype =request.args.get("facility_type","all")
    try:
        load_data()
        rows=list(_cache["fac"])
        if ftype=="hospital":
            rows=[r for r in rows if (r.get("facilityTypeId") or "").lower() in ("hospital","medical center","health center")]
        elif ftype=="clinic":
            rows=[r for r in rows if (r.get("facilityTypeId") or "").lower() in ("clinic","dispensary","health post")]
        out=[]
        for row in rows:
            if region_key(row,geo)!=region: continue
            s =score_facility(row,cap)
            hc=valid_coords(row.get("latitude"),row.get("longitude"))
            out.append({
                "name":row.get("name") or "Unknown","city":row.get("address_city") or "",
                "ftype":row.get("facilityTypeId") or "",
                "level":s["level"],"score":s["score"],
                "spec_hits":s["spec_hits"],"evidence":s["evidence"],"quality":s["quality"],
                "spec_score":s["spec_score"],"text_score":s["text_score"],"quality_score":s["quality_score"],
                "doctors":row.get("numberDoctors") or "","capacity":row.get("capacity") or "",
                "lat":float(row["latitude"])  if hc else None,
                "lon":float(row["longitude"]) if hc else None,
            })
        out.sort(key=lambda x:-x["score"])
        return jsonify({"region":region,"items":out[:100]})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route("/save_scenario", methods=["POST"])
def save_scenario_route():
    data=request.get_json(); sid=save_scenario(data)
    return jsonify({"ok":True,"id":sid})

@app.route("/scenarios")
def scenarios():
    return render_template_string(SC_T,scenarios=load_scenarios())

@app.route("/export_csv")
def export_csv():
    cap  =request.args.get("capability","Maternity")
    geo  =request.args.get("geo_level","state")
    ftype=request.args.get("facility_type","all")
    load_data()
    rows=list(_cache["fac"])
    if ftype=="hospital":
        rows=[r for r in rows if (r.get("facilityTypeId") or "").lower() in ("hospital","medical center","health center")]
    elif ftype=="clinic":
        rows=[r for r in rows if (r.get("facilityTypeId") or "").lower() in ("clinic","dispensary","health post")]
    buf=io.StringIO(); wc=csv.writer(buf)
    wc.writerow(["region","facilities","coverage_pct","priority","risk_score",
                 "demand_need_pct","demand_indicator","strong","partial","weak","no_claim","lat","lon"])
    agg={}
    for row in rows:
        k=region_key(row,geo); s=score_facility(row,cap)
        if k not in agg: agg[k]={"strong":0,"partial":0,"weak":0,"none":0,"lats":[],"lons":[],"nfhs":None}
        g=agg[k]; lv=s["level"]
        if lv=="STRONG": g["strong"]+=1
        elif lv=="PARTIAL": g["partial"]+=1
        elif lv=="WEAK": g["weak"]+=1
        else: g["none"]+=1
        hc=valid_coords(row.get("latitude"),row.get("longitude"))
        if hc: g["lats"].append(float(row["latitude"])); g["lons"].append(float(row["longitude"]))
        if g["nfhs"] is None:
            nrec,_=get_nfhs(row); g["nfhs"]=nrec
    cfg=DEMAND.get(cap,("","","","")); dlabel=cfg[1] if cfg else ""
    for region,g in agg.items():
        n=g["strong"]+g["partial"]+g["weak"]+g["none"]
        cov=coverage_pct(g["strong"],g["partial"],g["weak"],n)
        has_ev=(g["strong"]+g["partial"]+g["weak"])>0
        need,_,_,_=demand_score(g["nfhs"],cap)
        pri,_,risk=priority_label(cov,n,has_ev,need)
        clat=round(sum(g["lats"])/len(g["lats"]),4) if g["lats"] else ""
        clon=round(sum(g["lons"])/len(g["lons"]),4) if g["lons"] else ""
        wc.writerow([region,n,cov,pri,risk or "",need or "",dlabel,
                     g["strong"],g["partial"],g["weak"],g["none"],clat,clon])
    buf.seek(0)
    return Response(buf.getvalue(),mimetype="text/csv",
        headers={"Content-Disposition":f"attachment;filename=gap-{cap}-{geo}.csv"})

_CHAT_SYSTEM = """You are a healthcare gap analysis expert for the Medical Desert Planner, a data analytics tool that maps healthcare facility gaps across India.

DATA SOURCES: 10,000+ real Indian healthcare facilities; NFHS-5 (National Family Health Survey) district health indicators; India POST PIN directory for geographic mapping.

KEY METRICS:
- Coverage % = (Strong x 1.0 + Partial x 0.5 + Weak x 0.25) / total facilities x 100. Trust-weighted: quality of evidence, not just facility count.
- Facility evidence scoring: 0-12 pts total. Strong (8-12 pts) = specialty taxonomy match + keyword text evidence + data quality signals. Partial (4-7 pts) = limited evidence. Weak (1-3 pts) = minor keyword only. None = 0 pts.
- Gap Risk = (1 - Coverage/100) x (Demand/100) x 100. Combines supply shortage with population health need. Range 0-100.
- Confidence = Sample size (n/20 x 40 pts, max 40) + Evidence quality ((Strong + Partial x 0.5)/n x 40 pts) + Data source (20 pts for district-level NFHS, 8 pts for state-level average). Total max 100.
- Margin of Error = max(5, round(50 / sqrt(n))). Higher n = tighter estimate.

PRIORITY TIERS:
- CRITICAL GAP: Coverage < 20% and Confidence >= 40%: immediate intervention needed
- HIGH PRIORITY: Coverage 20-40%: near-term investment required
- MODERATE: Coverage 40-65%: monitor and reassess regularly
- DATA-POOR: fewer than 5 facilities or Confidence < 25%: collect more data before acting
- ADEQUATE: Coverage >= 65%: acceptable coverage

INDIA HEALTHCARE CONTEXT (use when suggesting interventions):
- National Health Mission (NHM) for rural and urban primary care funding
- Pradhan Mantri Jan Arogya Yojana (PM-JAY / Ayushman Bharat) for secondary/tertiary coverage
- ABDM (Ayushman Bharat Digital Mission) for facility digitisation
- ASHAs and ANMs for last-mile primary care outreach
- Public-Private Partnership (PPP) models for specialist care in underserved areas
- State-specific schemes (e.g. Aarogyasri in Telangana, Mukhyamantri Amrutum in Gujarat)

Keep answers concise (3-5 sentences unless the user asks for more detail). Be specific and actionable. When analysis context is provided, refer to it directly."""

def _llm_reply(messages):
    models = [
        "databricks-llama-4-maverick",
        "databricks-meta-llama-3-3-70b-instruct",
        "databricks-gemini-3-5-flash",
    ]
    last_err = None
    for model in models:
        try:
            resp = w.api_client.do(
                "POST",
                f"/serving-endpoints/{model}/invocations",
                body={"messages": messages, "max_tokens": 800, "temperature": 0.7}
            )
            return resp["choices"][0]["message"]["content"]
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"All LLM models failed. Last error: {last_err}")

@app.route("/chat", methods=["POST"])
def chat_route():
    data     = request.get_json(silent=True) or {}
    user_msg = (data.get("message") or "").strip()
    history  = data.get("history") or []
    context  = data.get("context") or {}
    if not user_msg:
        return jsonify({"error": "No message"}), 400
    try:
        messages = [{"role": "system", "content": _CHAT_SYSTEM}]
        if context:
            ctx_lines = []
            if context.get("capability"):
                ctx_lines.append(f"Current analysis: {context['capability']} capability")
            if context.get("geo_level"):
                ctx_lines.append(f"Geographic level: {context['geo_level']}")
            if context.get("total"):
                ctx_lines.append(f"Total facilities analysed: {context['total']}")
            if context.get("critical_regions"):
                ctx_lines.append(f"Critical gap regions: {', '.join(context['critical_regions'])}")
            if context.get("region"):
                ctx_lines.append(f"User is viewing region: {context['region']}")
                if context.get("coverage") is not None:
                    ctx_lines.append(f"Coverage: {context['coverage']}%, Gap Risk: {context.get('risk','N/A')}, Confidence: {context.get('confidence','N/A')}%")
            if ctx_lines:
                messages.append({"role": "system",
                                  "content": "Current analysis context:\n" + "\n".join(ctx_lines)})
        for m in (history or [])[-8:]:
            if m.get("role") in ("user", "assistant") and m.get("content"):
                messages.append({"role": m["role"], "content": m["content"]})
        messages.append({"role": "user", "content": user_msg})
        reply = _llm_reply(messages)
        return jsonify({"reply": reply})
    except Exception as e:
        return jsonify({"error": str(e), "reply": "I encountered an error connecting to the AI service. Please try again in a moment."}), 500

@app.route("/facilities")
def facilities():
    load_fac_dir()
    rows = list(_dir_cache["data"] or [])

    search = (request.args.get("search") or "").strip().lower()
    fstate = (request.args.get("state")  or "").strip().lower()
    ftype  = (request.args.get("ftype")  or "").strip().lower()
    sort   =  request.args.get("sort",  "rating")
    page   = max(1, int(request.args.get("page", 1) or 1))
    PER    = 50

    if search:
        rows = [r for r in rows if
                search in (r.get("name") or "").lower() or
                search in (r.get("address_city") or "").lower() or
                search in (r.get("district") or "").lower()]
    if fstate:
        rows = [r for r in rows if (r.get("address_stateOrRegion") or "").lower() == fstate]
    if ftype:
        rows = [r for r in rows if (r.get("facilityTypeId") or "").lower() == ftype]

    for r in rows:
        r["_rating"] = fac_rating(r)

    if sort == "name":
        rows.sort(key=lambda r: (r.get("name") or "").lower())
    elif sort == "doctors":
        rows.sort(key=lambda r: -(_f(r.get("numberDoctors") or 0) or 0))
    elif sort == "city":
        rows.sort(key=lambda r: (r.get("address_city") or "").lower())
    else:
        rows.sort(key=lambda r: -r["_rating"])

    total   = len(rows)
    pages   = max(1, (total + PER - 1) // PER)
    page    = min(page, pages)
    chunk   = rows[(page-1)*PER : page*PER]

    all_states = sorted({(r.get("address_stateOrRegion") or "").strip()
                          for r in _dir_cache["data"] or [] if r.get("address_stateOrRegion")})
    all_types  = sorted({(r.get("facilityTypeId") or "").strip()
                          for r in _dir_cache["data"] or [] if r.get("facilityTypeId")})

    return render_template_string(FAC_DIR_T,
        rows=chunk, total=total, page=page, pages=pages,
        search=request.args.get("search",""), state=request.args.get("state",""),
        ftype=request.args.get("ftype",""), sort=sort,
        all_states=all_states, all_types=all_types)

# ── Templates ─────────────────────────────────────────────────────────────────

HOME_T = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Medical Desert Planner</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css"/>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',sans-serif;background:#f0f4f0;color:#222}
.hdr{background:linear-gradient(135deg,#1B5E20,#2E7D32);color:#fff;padding:36px 20px;text-align:center}
.hdr h1{font-size:2.4em;font-weight:800;letter-spacing:-.5px}
.hdr p{opacity:.85;margin-top:10px;font-size:1.05em;max-width:600px;margin-left:auto;margin-right:auto}
.pills{margin-top:16px;display:flex;justify-content:center;gap:10px;flex-wrap:wrap}
.pill{display:inline-flex;align-items:center;gap:6px;background:rgba(255,255,255,.18);
      border-radius:20px;padding:6px 16px;font-size:.85em}
.wrap{max-width:960px;margin:28px auto;padding:0 16px}
.card{background:#fff;border-radius:12px;box-shadow:0 2px 10px rgba(0,0,0,.07);padding:24px 28px;margin-bottom:20px}
.card h2{color:#1B5E20;margin-bottom:16px;font-size:1.05em;display:flex;align-items:center;gap:8px;
         border-bottom:1px solid #f0f0f0;padding-bottom:12px}
.g2{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.g3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px}
label{display:block;font-weight:600;margin-bottom:5px;font-size:.78em;color:#555;
      text-transform:uppercase;letter-spacing:.5px}
select,input{width:100%;padding:10px 12px;border:1.5px solid #ddd;border-radius:8px;
             font-size:.95em;background:#fff;color:#222}
select:focus,input:focus{outline:none;border-color:#2E7D32;box-shadow:0 0 0 3px rgba(46,125,50,.12)}
.hint{color:#aaa;font-size:.76em;margin-top:4px;line-height:1.4}
.btn{background:#2E7D32;color:#fff;border:none;border-radius:8px;padding:14px;font-size:1em;
     font-weight:700;cursor:pointer;width:100%;margin-top:16px;transition:.15s;letter-spacing:.2px}
.btn:hover{background:#1B5E20;box-shadow:0 4px 12px rgba(27,94,32,.3)}
.steps{display:flex;gap:8px;flex-wrap:wrap;counter-reset:s;margin-bottom:22px}
.step{flex:1;min-width:130px;padding:14px 12px 14px 44px;background:#E8F5E9;border-radius:10px;
      position:relative;counter-increment:s;border:1px solid #C8E6C9}
.step::before{content:counter(s);position:absolute;left:12px;top:14px;background:#2E7D32;color:#fff;
              border-radius:50%;width:24px;height:24px;display:flex;align-items:center;justify-content:center;
              font-weight:800;font-size:.8em}
.step b{font-size:.85em;color:#1B5E20;display:block}
.step p{font-size:.74em;color:#666;margin-top:3px;line-height:1.4}
.sc-list{list-style:none}
.sc-item{padding:11px 14px;border-left:3px solid #4CAF50;background:#f9fbe7;
         margin-bottom:7px;border-radius:0 8px 8px 0;font-size:.88em}
.badge{display:inline-block;padding:2px 8px;border-radius:10px;color:#fff;font-size:.72em;font-weight:600}
.cap-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:10px}
.cap-item{background:#f8f8f8;border-radius:8px;padding:12px 14px;border-left:3px solid #2E7D32}
.cap-item strong{display:block;color:#1B5E20;font-size:.88em;margin-bottom:4px}
.cap-item p{font-size:.76em;color:#666;line-height:1.4}
.pri-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:8px}
.pri-item{padding:10px 12px;border-radius:0 8px 8px 0;font-size:.82em}
.pri-item strong{display:block;font-size:.85em;margin-bottom:3px}
.pri-item p{font-size:.76em;color:#555;line-height:1.4;margin-top:2px}
.dq-box{background:#E3F2FD;border-radius:8px;padding:12px 16px;margin-top:14px;
        display:flex;gap:18px;flex-wrap:wrap;font-size:.82em;align-items:center}
.dq-box b{margin-right:4px}
@media(max-width:620px){.g2,.g3{grid-template-columns:1fr}}
</style>
</head>
<body>
<div class="hdr">
  <h1><i class="fa-solid fa-hospital-user"></i> Medical Desert Planner</h1>
  <p>Identify where healthcare gaps are highest-risk across India, scored against real facility data and NFHS-5 district health indicators</p>
  <div class="pills">
    <span class="pill"><i class="fa-solid fa-chart-bar"></i> {{ facility_count }} real facilities</span>
    <span class="pill"><i class="fa-solid fa-clipboard-list"></i> NFHS-5 district demand data</span>
    <span class="pill"><i class="fa-solid fa-map-pin"></i> India POST PIN directory</span>
  </div>
  <div style="margin-top:14px">
    <a href="/facilities" style="display:inline-flex;align-items:center;gap:8px;background:rgba(255,255,255,.22);
       border:1px solid rgba(255,255,255,.4);border-radius:8px;padding:8px 18px;color:#fff;text-decoration:none;font-weight:600;font-size:.9em">
      <i class="fa-solid fa-hospital"></i> Browse Facility Directory
    </a>
  </div>
</div>

<div class="wrap">

  <!-- Step-by-step analysis form -->
  <div class="card">
    <h2><i class="fa-solid fa-magnifying-glass"></i> Run a Gap Analysis</h2>
    <div class="steps">
      <div class="step"><b>Select capability</b><p>Pick the care type to assess (ICU, Maternity, etc.)</p></div>
      <div class="step"><b>Choose geography</b><p>State, city, district, or PIN code level</p></div>
      <div class="step"><b>Optional filters</b><p>Limit to hospitals only, clinics only, or all</p></div>
      <div class="step"><b>Run analysis</b><p>View ranked gaps with confidence scores</p></div>
      <div class="step"><b>Save scenario</b><p>Bookmark findings for your planning team</p></div>
    </div>

    <form method="POST" action="/analyze">
      <div class="g2">
        <div>
          <label>Healthcare capability</label>
          <select name="capability">
            {% for c in capabilities %}<option>{{ c }}</option>{% endfor %}
          </select>
          <p class="hint">Scored using specialty taxonomy, keyword evidence, and data quality signals</p>
        </div>
        <div>
          <label>Geographic aggregation</label>
          <select name="geo_level">
            <option value="state">By State</option>
            <option value="city">By City</option>
            <option value="district">By District (via PIN lookup)</option>
            <option value="pin">By PIN Code</option>
          </select>
          <p class="hint">District and PIN use India POST directory to resolve geography</p>
        </div>
      </div>
      <div style="margin-top:14px">
        <label>Facility type filter</label>
        <select name="facility_type">
          <option value="all">All types (hospitals + clinics + others)</option>
          <option value="hospital">Hospitals only</option>
          <option value="clinic">Clinics only</option>
        </select>
      </div>
      <div style="margin-top:14px">
        <label>Reference location <span style="font-weight:400;color:#bbb;text-transform:none">(optional, for distance context)</span></label>
        <div class="g3" style="margin-top:6px">
          <input type="text" name="ref_city" placeholder="Label (e.g. Jaipur)">
          <input type="text" name="ref_lat"  placeholder="Latitude (e.g. 26.9)">
          <input type="text" name="ref_lon"  placeholder="Longitude (e.g. 75.8)">
        </div>
      </div>
      <button class="btn" type="submit"><i class="fa-solid fa-magnifying-glass"></i> Run Gap Analysis</button>
    </form>
  </div>

  <!-- Capability guide -->
  <div class="card">
    <h2><i class="fa-solid fa-stethoscope"></i> What each capability covers</h2>
    <div class="cap-grid">
      <div class="cap-item"><strong>ICU</strong><p>Critical care, intensive care units, ventilator support, anesthesia</p></div>
      <div class="cap-item"><strong>Maternity</strong><p>Obstetrics, delivery, prenatal/postnatal care, neonatal (NICU)</p></div>
      <div class="cap-item"><strong>Emergency</strong><p>Emergency medicine, trauma, casualty, ambulance services</p></div>
      <div class="cap-item"><strong>Dialysis</strong><p>Hemodialysis, renal care, nephrology services</p></div>
      <div class="cap-item"><strong>Oncology</strong><p>Cancer treatment, chemotherapy, radiation therapy, tumor care</p></div>
      <div class="cap-item"><strong>Cardiology</strong><p>Heart care, coronary care, angioplasty, echocardiography</p></div>
      <div class="cap-item"><strong>Pediatrics</strong><p>Child health, NICU, pediatric surgery, under-5 care</p></div>
      <div class="cap-item"><strong>Surgery</strong><p>General surgery, laparoscopic, neurosurgery, orthopedics</p></div>
    </div>
  </div>

  <!-- Priority legend -->
  <div class="card">
    <h2><i class="fa-solid fa-ranking-star"></i> How gap priority is determined</h2>
    <div class="pri-grid">
      <div class="pri-item" style="border-left:4px solid #D32F2F;background:#FFF5F5">
        <strong style="color:#D32F2F">CRITICAL GAP</strong>
        <p>Coverage below 20% with enough facilities to be confident. Immediate intervention needed.</p>
      </div>
      <div class="pri-item" style="border-left:4px solid #F57C00;background:#FFF8F0">
        <strong style="color:#F57C00">HIGH PRIORITY</strong>
        <p>Coverage 20-40%. Evidence is weak across most facilities. Plan for near-term investment.</p>
      </div>
      <div class="pri-item" style="border-left:4px solid #FBC02D;background:#FFFDE7">
        <strong style="color:#856404">MODERATE</strong>
        <p>Coverage 40-65%. Some evidence present. Monitor and reassess regularly.</p>
      </div>
      <div class="pri-item" style="border-left:4px solid #9E9E9E;background:#F5F5F5">
        <strong style="color:#666">DATA-POOR</strong>
        <p>Too few records to make a confident call. Collect more data before acting.</p>
      </div>
      <div class="pri-item" style="border-left:4px solid #388E3C;background:#F1F8F1">
        <strong style="color:#388E3C">ADEQUATE</strong>
        <p>Coverage above 65%. Most facilities show evidence of this capability.</p>
      </div>
    </div>
    <div class="dq-box">
      <i class="fa-solid fa-database" style="color:#1565C0"></i>
      <span><b style="color:#2E7D32">Verified</b> = 10+ facilities, strong evidence, district-level NFHS data</span>
      <span><b style="color:#F57C00">Moderate</b> = some evidence, may use state-level averages</span>
      <span><b style="color:#9E9E9E">Sparse</b> = few records, treat results with caution</span>
    </div>
  </div>

  {% if scenarios %}
  <div class="card">
    <h2><i class="fa-solid fa-clipboard-list"></i> Recent Planning Scenarios</h2>
    <ul class="sc-list">
      {% for s in scenarios %}
      <li class="sc-item">
        <strong>{{ s.name }}</strong>
        <span class="badge" style="background:#2E7D32;margin-left:6px">{{ s.capability }}</span>
        <span class="badge" style="background:#555;margin-left:4px">{{ s.geo_level or 'state' }}</span>
        <span style="color:#bbb;font-size:.78em;margin-left:8px">{{ s.saved_at }}</span>
        {% if s.notes %}<p style="margin-top:4px;color:#555;font-size:.82em">{{ s.notes }}</p>{% endif %}
      </li>
      {% endfor %}
    </ul>
    <a href="/scenarios" style="color:#2E7D32;font-size:.87em">View all saved scenarios →</a>
  </div>
  {% endif %}

</div>

<!-- AI Chat Widget (home page) -->
<style>
#chat-bubble{position:fixed;bottom:24px;right:24px;background:#2E7D32;color:#fff;
  border-radius:50px;padding:11px 20px;cursor:pointer;z-index:1100;
  display:flex;align-items:center;gap:8px;font-weight:700;font-size:.88em;
  box-shadow:0 4px 18px rgba(0,0,0,.28);transition:.2s;border:none}
#chat-bubble:hover{background:#1B5E20;transform:translateY(-2px)}
#chat-bubble .chat-dot{width:8px;height:8px;background:#69F0AE;border-radius:50%;animation:pulse-dot 2s infinite}
@keyframes pulse-dot{0%,100%{opacity:1}50%{opacity:.4}}
#chat-panel{position:fixed;bottom:80px;right:24px;width:370px;height:530px;
  background:#fff;border-radius:16px;box-shadow:0 12px 48px rgba(0,0,0,.25);
  z-index:1101;display:none;flex-direction:column;overflow:hidden;border:1px solid #e0e0e0}
#chat-panel.open{display:flex}
.chat-hdr{background:linear-gradient(135deg,#1B5E20,#2E7D32);color:#fff;
  padding:13px 16px;display:flex;align-items:center;gap:10px}
.chat-hdr-ico{width:34px;height:34px;background:rgba(255,255,255,.2);border-radius:50%;
  display:flex;align-items:center;justify-content:center}
.chat-hdr-txt{flex:1}.chat-hdr-txt b{display:block;font-size:.95em}.chat-hdr-txt span{font-size:.74em;opacity:.8}
.chat-hdr button{background:none;border:none;color:#fff;cursor:pointer;font-size:1.1em;padding:4px 8px;border-radius:6px}
.chat-hdr button:hover{background:rgba(255,255,255,.2)}
#chat-msgs{flex:1;overflow-y:auto;padding:14px;display:flex;flex-direction:column;gap:10px;background:#fafffe}
.cm{display:flex;max-width:88%}.cm.user{align-self:flex-end}.cm.bot{align-self:flex-start}
.cb{padding:10px 14px;border-radius:14px;font-size:.84em;line-height:1.55;word-break:break-word}
.cm.user .cb{background:#2E7D32;color:#fff;border-bottom-right-radius:3px}
.cm.bot .cb{background:#f0f4f0;color:#222;border-bottom-left-radius:3px;border:1px solid #e8e8e8}
.typing{display:flex;gap:5px;align-items:center;padding:4px 0}
.typing span{width:7px;height:7px;background:#bbb;border-radius:50%;animation:tbounce 1.2s infinite}
.typing span:nth-child(2){animation-delay:.2s}.typing span:nth-child(3){animation-delay:.4s}
@keyframes tbounce{0%,60%,100%{transform:translateY(0)}30%{transform:translateY(-6px)}}
#chat-qs{display:flex;flex-wrap:wrap;gap:6px;margin-top:2px}
.cq-btn{background:#E8F5E9;border:1px solid #C8E6C9;color:#1B5E20;padding:5px 10px;
  border-radius:8px;font-size:.75em;cursor:pointer;text-align:left;line-height:1.35}
.cq-btn:hover{background:#C8E6C9}
#chat-inp-row{display:flex;gap:8px;padding:10px 12px;border-top:1px solid #eee;background:#fff}
#chat-inp{flex:1;border:1.5px solid #ddd;border-radius:8px;padding:8px 12px;font-size:.87em;outline:none;color:#222}
#chat-inp:focus{border-color:#2E7D32;box-shadow:0 0 0 2px rgba(46,125,50,.1)}
#chat-inp-row .cs-btn{background:#2E7D32;color:#fff;border:none;border-radius:8px;padding:8px 14px;cursor:pointer}
#chat-inp-row .cs-btn:hover{background:#1B5E20}
#chat-foot{padding:5px 14px 8px;font-size:.7em;color:#bbb;text-align:center;background:#fff}
</style>
<button id="chat-bubble" onclick="toggleChat()">
  <span class="chat-dot"></span><i class="fa-solid fa-comments"></i> Ask AI
</button>
<div id="chat-panel">
  <div class="chat-hdr">
    <div class="chat-hdr-ico"><i class="fa-solid fa-robot"></i></div>
    <div class="chat-hdr-txt"><b>Medical Desert AI</b><span>Healthcare gap planning assistant</span></div>
    <button onclick="toggleChat()"><i class="fa-solid fa-xmark"></i></button>
  </div>
  <div id="chat-msgs">
    <div class="cm bot"><div class="cb">Hi! I can help you understand healthcare gaps in India, explain the analysis metrics, or suggest interventions. What capability are you planning to analyse?</div></div>
    <div id="chat-qs">
      <button class="cq-btn" onclick="askQ('Which Indian states have the worst ICU coverage gaps?')">Worst ICU gap states?</button>
      <button class="cq-btn" onclick="askQ('What is Coverage % and how is it calculated?')">What is Coverage %?</button>
      <button class="cq-btn" onclick="askQ('How is Gap Risk calculated?')">How is Gap Risk scored?</button>
      <button class="cq-btn" onclick="askQ('What interventions work best for maternity care gaps in rural India?')">Maternity gap interventions?</button>
      <button class="cq-btn" onclick="askQ('What does NFHS-5 data tell us about healthcare demand?')">What is NFHS-5?</button>
    </div>
  </div>
  <div id="chat-inp-row">
    <input id="chat-inp" type="text" placeholder="Ask about healthcare gaps in India..."
           onkeydown="if(event.key==='Enter')sendChat()">
    <button class="cs-btn" onclick="sendChat()"><i class="fa-solid fa-paper-plane"></i></button>
  </div>
  <div id="chat-foot"><i class="fa-solid fa-robot"></i> Powered by Databricks AI &bull; Medical Desert Planner</div>
</div>
<script>
var _chatHistory=[], _chatOpen=false;
function toggleChat(){
  _chatOpen=!_chatOpen;
  document.getElementById('chat-panel').classList.toggle('open',_chatOpen);
  if(_chatOpen) document.getElementById('chat-inp').focus();
}
function _escH(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function _addMsg(role,text){
  var box=document.getElementById('chat-msgs');
  var d=document.createElement('div'); d.className='cm '+role;
  var html=role==='user'?'<div class="cb">'+_escH(text)+'</div>'
    :'<div class="cb">'+text.replace(/\*\*(.*?)\*\*/g,'<b>$1</b>').replace(/\n/g,'<br>')+'</div>';
  d.innerHTML=html; box.appendChild(d); box.scrollTop=box.scrollHeight;
  _chatHistory.push({role:role,content:text});
}
function sendChat(){
  var inp=document.getElementById('chat-inp');
  var msg=inp.value.trim(); if(!msg)return; inp.value='';
  document.getElementById('chat-qs').style.display='none';
  _addMsg('user',msg); _callChat(msg);
}
function askQ(q){
  document.getElementById('chat-qs').style.display='none';
  _addMsg('user',q); _callChat(q);
}
function _callChat(msg){
  var box=document.getElementById('chat-msgs');
  var td=document.createElement('div'); td.className='cm bot'; td.id='chat-typing';
  td.innerHTML='<div class="cb typing"><span></span><span></span><span></span></div>';
  box.appendChild(td); box.scrollTop=box.scrollHeight;
  fetch('/chat',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({message:msg,history:_chatHistory.slice(-8),context:{}})})
  .then(function(r){return r.json();})
  .then(function(d){
    var t=document.getElementById('chat-typing'); if(t)t.remove();
    _addMsg('bot',d.reply||'Sorry, I could not get a response. Please try again.');
  }).catch(function(){
    var t=document.getElementById('chat-typing'); if(t)t.remove();
    _addMsg('bot','Connection error. Please try again.');
  });
}
</script>
</body></html>"""

RESULTS_T = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ cap }} Gap Analysis</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css"/>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css"/>
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',sans-serif;background:#f0f4f0;color:#222}
.hdr{background:linear-gradient(135deg,#1B5E20,#2E7D32);color:#fff;padding:16px 20px}
.hdr h1{font-size:1.4em;font-weight:700}
.hdr .sub{opacity:.8;font-size:.87em;margin-top:4px}
.metrics{display:flex;gap:12px;padding:16px 20px;flex-wrap:wrap;max-width:1400px;margin:0 auto}
.metric{background:#fff;border-radius:10px;padding:14px 20px;flex:1;min-width:140px;
        box-shadow:0 1px 6px rgba(0,0,0,.07);text-align:center}
.mv{font-size:2em;font-weight:800;color:#2E7D32}
.ml{color:#888;font-size:.78em;margin-top:3px}
.map-hero{max-width:1400px;margin:0 auto;padding:0 16px}
.body-full{max-width:1400px;margin:0 auto;padding:0 16px 24px}
.card{background:#fff;border-radius:10px;box-shadow:0 1px 8px rgba(0,0,0,.07);padding:18px;margin-bottom:14px}
.card h2{color:#1B5E20;margin-bottom:12px;font-size:1em;display:flex;align-items:center;gap:6px}
#map{height:520px;border-radius:8px;z-index:0}
table{width:100%;border-collapse:collapse;font-size:.84em}
th{background:#E8F5E9;padding:9px 10px;text-align:left;font-size:.78em;color:#444;font-weight:700}
td{padding:9px 10px;border-bottom:1px solid #f0f0f0;vertical-align:top}
tr:hover td{background:#fafff9;cursor:pointer}
.badge{display:inline-block;padding:2px 9px;border-radius:10px;color:#fff;font-size:.74em;font-weight:700}
.drill-btn{background:#E8F5E9;border:1px solid #4CAF50;color:#2E7D32;padding:4px 10px;
           border-radius:6px;cursor:pointer;font-size:.78em;white-space:nowrap}
.drill-btn:hover{background:#C8E6C9}
.drill-row{display:none}
.drill-panel{background:#fafff9;border-top:2px solid #4CAF50;padding:14px}
.fac-card{border:1px solid #e8f5e9;border-radius:8px;padding:12px;margin-bottom:10px;background:#fff}
.fac-hdr{display:flex;align-items:flex-start;gap:8px;margin-bottom:6px;flex-wrap:wrap}
.fac-name{font-weight:700;font-size:.9em;flex:1}
.fac-meta{font-size:.78em;color:#888;margin-bottom:6px}
.score-bar{display:flex;align-items:center;gap:10px;font-size:.78em;color:#666;
           background:#f5f5f5;padding:5px 10px;border-radius:6px;margin-bottom:6px}
.score-bar strong{color:#1B5E20}
.spec-tags{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:6px}
.spec-tag{background:#E3F2FD;color:#1565C0;padding:2px 7px;border-radius:8px;font-size:.72em;font-weight:600}
.ev-cite{background:#f9f9f9;border-left:3px solid #4CAF50;padding:5px 10px;margin:3px 0;
         font-size:.78em;border-radius:0 4px 4px 0}
.ev-field{font-weight:700;color:#555}
.no-ev{color:#bbb;font-size:.78em;font-style:italic}
.q-sigs{font-size:.74em;color:#888;margin-top:4px}
.bar-wrap{display:flex;height:7px;border-radius:3px;overflow:hidden;margin-top:3px;gap:1px}
.bar-seg{height:100%}
.note{font-size:.74em;color:#888;font-style:italic;margin-top:2px}
.demand-cell{font-size:.8em}
.demand-val{font-weight:700}
.risk-badge{font-size:.75em;padding:2px 7px;border-radius:8px;font-weight:700}
.save-form{background:#E8F5E9;border-radius:8px;padding:14px}
.save-form input,.save-form textarea{width:100%;padding:8px;border:1.5px solid #ccc;
           border-radius:6px;margin:4px 0 10px;font-size:.88em}
.save-form textarea{height:56px;resize:vertical}
.save-btn{background:#2E7D32;color:#fff;border:none;border-radius:6px;padding:9px 16px;
          cursor:pointer;font-weight:700;font-size:.88em}
.links{display:flex;gap:12px;flex-wrap:wrap;margin-top:4px}
.links a{color:#2E7D32;font-size:.87em;text-decoration:none}
.links a:hover{text-decoration:underline}
.conf-STRONG{color:#2E7D32;font-weight:700}
.conf-PARTIAL{color:#F57C00;font-weight:700}
.conf-WEAK{color:#FBC02D;font-weight:700}
.conf-NO{color:#bbb}
.dist-tag{background:#E3F2FD;color:#1565C0;padding:2px 7px;border-radius:8px;font-size:.74em;font-weight:600;margin-left:4px}
.nfhs-note{font-size:.72em;color:#888;margin-top:4px;font-style:italic}
@media(max-width:900px){ #map{height:360px} }
/* --- new UX styles --- */
.alert-banner{background:#FFEBEE;border-left:5px solid #D32F2F;border-radius:8px;
              padding:16px 20px;margin:0 auto 12px;max-width:1400px}
.alert-banner h3{color:#C62828;margin-bottom:12px;font-size:.95em;font-weight:700}
.alert-regions{display:flex;gap:10px;flex-wrap:wrap}
.alert-card{background:#fff;border:1px solid #FFCDD2;border-radius:8px;padding:12px 16px;
            flex:1;min-width:180px;max-width:260px}
.alert-card strong{display:block;color:#C62828;font-size:.85em;margin-bottom:5px}
.alert-card .ac-row{font-size:.78em;color:#555;margin-top:3px}
.filter-bar{max-width:1400px;margin:0 auto 10px;padding:0 16px;display:flex;gap:8px;
            flex-wrap:wrap;align-items:center}
.filter-tab{padding:6px 14px;border-radius:20px;border:1.5px solid #ddd;background:#fff;
            cursor:pointer;font-size:.8em;font-weight:600;color:#555;transition:.15s;line-height:1}
.filter-tab:hover{border-color:#2E7D32;color:#2E7D32}
.filter-tab.active{background:#2E7D32;color:#fff;border-color:#2E7D32}
.filter-tab.fc{color:#D32F2F;border-color:#FFCDD2}
.filter-tab.fc:hover,.filter-tab.fc.active{background:#D32F2F;color:#fff;border-color:#D32F2F}
.filter-tab.fh{color:#F57C00;border-color:#FFE0B2}
.filter-tab.fh:hover,.filter-tab.fh.active{background:#F57C00;color:#fff;border-color:#F57C00}
tr.dr{border-left:4px solid transparent}
tr.dr[data-pri="CRITICAL GAP"]{border-left-color:#D32F2F}
tr.dr[data-pri="CRITICAL GAP"] td{background:#FFF5F5}
tr.dr[data-pri="HIGH PRIORITY"]{border-left-color:#F57C00}
tr.dr[data-pri="HIGH PRIORITY"] td{background:#FFF8F0}
tr.dr[data-pri="MODERATE"]{border-left-color:#FBC02D}
tr.dr[data-pri="MODERATE"] td{background:#FFFDE7}
tr.dr:hover td{filter:brightness(.97);cursor:pointer}
.rank{background:#eee;color:#666;font-weight:800;border-radius:50%;
      width:26px;height:26px;display:inline-flex;align-items:center;justify-content:center;
      font-size:.78em;flex-shrink:0}
tr.dr[data-pri="CRITICAL GAP"] .rank{background:#D32F2F;color:#fff}
tr.dr[data-pri="HIGH PRIORITY"] .rank{background:#F57C00;color:#fff}
.dq-badge{padding:3px 8px;border-radius:10px;font-size:.72em;font-weight:700;color:#fff;
          display:inline-block;white-space:nowrap}
.conf-wrap{display:flex;flex-direction:column;gap:3px}
.conf-row{display:flex;align-items:center;gap:6px}
.conf-track{flex:1;height:6px;background:#eee;border-radius:3px;overflow:hidden;min-width:50px}
.conf-fill{height:100%;border-radius:3px}
.conf-pct{font-weight:700;font-size:.82em;white-space:nowrap}
.conf-moe{font-size:.7em;color:#aaa}
.drill-summary{display:flex;gap:12px;flex-wrap:wrap;padding:10px 14px;
               background:#f5f5f5;border-bottom:1px solid #e0e0e0;font-size:.8em}
.ds-item{display:flex;align-items:center;gap:5px;font-weight:600}
.no-rows-msg{padding:16px;text-align:center;color:#aaa;font-size:.88em;display:none}
.bc-bar{background:#fff;border-bottom:1px solid #e8f5e9;padding:10px 20px;
        max-width:1400px;margin:0 auto;display:flex;align-items:center;gap:6px;
        font-size:.85em;flex-wrap:wrap}
.bc-link{color:#2E7D32;text-decoration:none;font-weight:600;padding:4px 8px;
         border-radius:6px;background:#E8F5E9}
.bc-link:hover{background:#C8E6C9;text-decoration:none}
.bc-cur{color:#555;font-weight:700;padding:4px 8px}
.bc-sep{color:#bbb;font-size:.9em}
.bc-level{font-size:.75em;color:#aaa;margin-left:4px}
.drill-nav-btn{background:#1565C0;border-color:#1565C0;color:#fff;font-weight:700}
.drill-nav-btn:hover{background:#0D47A1;border-color:#0D47A1}
.fac-btn{background:#E8F5E9;border:1px solid #4CAF50;color:#2E7D32}
.fac-btn:hover{background:#C8E6C9}
/* Chat widget */
#chat-bubble{position:fixed;bottom:24px;right:24px;background:#2E7D32;color:#fff;
  border-radius:50px;padding:11px 20px;cursor:pointer;z-index:1100;
  display:flex;align-items:center;gap:8px;font-weight:700;font-size:.88em;
  box-shadow:0 4px 18px rgba(0,0,0,.28);transition:.2s;border:none}
#chat-bubble:hover{background:#1B5E20;transform:translateY(-2px)}
#chat-bubble .chat-dot{width:8px;height:8px;background:#69F0AE;border-radius:50%;
  animation:pulse-dot 2s infinite}
@keyframes pulse-dot{0%,100%{opacity:1}50%{opacity:.4}}
#chat-panel{position:fixed;bottom:80px;right:24px;width:370px;height:530px;
  background:#fff;border-radius:16px;box-shadow:0 12px 48px rgba(0,0,0,.25);
  z-index:1101;display:none;flex-direction:column;overflow:hidden;
  border:1px solid #e0e0e0;transition:.2s}
#chat-panel.open{display:flex}
.chat-hdr{background:linear-gradient(135deg,#1B5E20,#2E7D32);color:#fff;
  padding:13px 16px;display:flex;align-items:center;gap:10px}
.chat-hdr-ico{width:34px;height:34px;background:rgba(255,255,255,.2);border-radius:50%;
  display:flex;align-items:center;justify-content:center;font-size:1em}
.chat-hdr-txt{flex:1}
.chat-hdr-txt b{display:block;font-size:.95em}
.chat-hdr-txt span{font-size:.74em;opacity:.8}
.chat-hdr button{background:none;border:none;color:#fff;cursor:pointer;
  font-size:1.1em;padding:4px 8px;border-radius:6px}
.chat-hdr button:hover{background:rgba(255,255,255,.2)}
#chat-msgs{flex:1;overflow-y:auto;padding:14px;display:flex;flex-direction:column;gap:10px;
  background:#fafffe}
.cm{display:flex;max-width:88%}
.cm.user{align-self:flex-end}
.cm.bot{align-self:flex-start}
.cb{padding:10px 14px;border-radius:14px;font-size:.84em;line-height:1.55;word-break:break-word}
.cm.user .cb{background:#2E7D32;color:#fff;border-bottom-right-radius:3px}
.cm.bot .cb{background:#f0f4f0;color:#222;border-bottom-left-radius:3px;border:1px solid #e8e8e8}
.typing{display:flex;gap:5px;align-items:center;padding:4px 0}
.typing span{width:7px;height:7px;background:#bbb;border-radius:50%;
  animation:tbounce 1.2s infinite}
.typing span:nth-child(2){animation-delay:.2s}
.typing span:nth-child(3){animation-delay:.4s}
@keyframes tbounce{0%,60%,100%{transform:translateY(0)}30%{transform:translateY(-6px)}}
#chat-qs{display:flex;flex-wrap:wrap;gap:6px;margin-top:2px}
.cq-btn{background:#E8F5E9;border:1px solid #C8E6C9;color:#1B5E20;padding:5px 10px;
  border-radius:8px;font-size:.75em;cursor:pointer;text-align:left;line-height:1.35}
.cq-btn:hover{background:#C8E6C9}
#chat-inp-row{display:flex;gap:8px;padding:10px 12px;border-top:1px solid #eee;background:#fff}
#chat-inp{flex:1;border:1.5px solid #ddd;border-radius:8px;padding:8px 12px;
  font-size:.87em;outline:none;color:#222}
#chat-inp:focus{border-color:#2E7D32;box-shadow:0 0 0 2px rgba(46,125,50,.1)}
#chat-inp-row .cs-btn{background:#2E7D32;color:#fff;border:none;border-radius:8px;
  padding:8px 14px;cursor:pointer;font-size:.9em}
#chat-inp-row .cs-btn:hover{background:#1B5E20}
#chat-foot{padding:5px 14px 8px;font-size:.7em;color:#bbb;text-align:center;background:#fff}
/* KPI tooltip */
.tip-icon{font-size:.72em;opacity:.55;margin-left:5px;cursor:help;vertical-align:middle;transition:.15s}
th:hover .tip-icon{opacity:1;color:#2E7D32}
#kpi-tip{position:fixed;z-index:9999;background:#1a2e1a;color:#e8f5e9;border-radius:12px;
         padding:15px 18px;max-width:340px;font-size:.82em;line-height:1.55;
         box-shadow:0 10px 32px rgba(0,0,0,.45);pointer-events:none;display:none;
         border:1px solid rgba(255,255,255,.08)}
#kpi-tip .tt{font-weight:800;font-size:1.08em;margin-bottom:9px;color:#81C784;
             display:flex;align-items:center;gap:7px}
#kpi-tip .tf{background:rgba(255,255,255,.08);border-radius:7px;padding:8px 11px;
             font-family:'Courier New',monospace;font-size:.88em;margin:7px 0 9px;
             color:#A5D6A7;border-left:3px solid #4CAF50}
#kpi-tip p{color:#cce5cc;margin-bottom:5px}
#kpi-tip ul{margin:6px 0 6px 14px;color:#cce5cc}
#kpi-tip li{margin-bottom:3px}
#kpi-tip .tb{color:#aaa;font-size:.88em;margin-top:9px;border-top:1px solid rgba(255,255,255,.1);padding-top:8px}
#kpi-tip b{color:#fff}
/* State -> district/city hierarchy + compact row expansion */
.exp-caret{color:#bbb;font-size:.8em;transition:.15s}
tr.dr:hover .exp-caret{color:#2E7D32}
.state-grp-row{background:#EDF5ED;cursor:pointer}
.state-grp-row:hover{background:#E0EFE0}
.state-grp-row td{padding:10px 14px;border-bottom:2px solid #cfe8cf}
.grp-caret{color:#2E7D32;margin-right:8px;font-size:.85em;transition:.15s;display:inline-block}
.grp-name{font-size:.95em;color:#1B5E20}
.grp-stats{margin-left:12px;display:inline-flex;gap:6px;flex-wrap:wrap;vertical-align:middle}
.grp-pill{font-size:.72em;font-weight:700;padding:2px 9px;border-radius:10px;background:#fff;color:#555;border:1px solid #ddd}
.grp-pill.crit{background:#FFEBEE;color:#C62828;border-color:#FFCDD2}
.grp-pill.high{background:#FFF3E0;color:#E65100;border-color:#FFE0B2}
.row-detail td{padding:0;border-bottom:1px solid #f0f0f0}
.row-detail-grid{display:flex;gap:24px;flex-wrap:wrap;padding:14px 18px;background:#fafffa;border-left:3px solid #C8E6C9}
.rd-block{min-width:110px}
.rd-lbl{font-size:.7em;color:#999;font-weight:700;text-transform:uppercase;letter-spacing:.03em;margin-bottom:5px;cursor:help}
.sub-loc{font-size:.74em;color:#888;margin-top:2px}
.sub-loc i{margin-right:2px}
</style>
</head>
<body>
<div id="kpi-tip"></div>
<!-- Hidden form for hierarchy navigation -->
<form id="drill-form" method="POST" action="/analyze" style="display:none">
  <input type="hidden" id="df-cap"      name="capability"    value="{{ cap }}">
  <input type="hidden" id="df-ftype"    name="facility_type" value="{{ ftype }}">
  <input type="hidden" id="df-state"    name="filter_state"    value="">
  <input type="hidden" id="df-district" name="filter_district" value="">
  <input type="hidden" id="df-city"     name="filter_city"     value="">
</form>

<div class="hdr">
  <h1><i class="fa-solid fa-chart-column"></i> {{ cap }} Gap Analysis</h1>
  <div class="sub">
    {{ total }} facilities scored · {{ geo|capitalize }}-level ·
    {% if ref_city %}Reference: {{ ref_city }} · {% endif %}
    <a href="/export_csv?capability={{ cap }}&geo_level={{ geo }}&facility_type={{ ftype }}"
       style="color:#fff;opacity:.8"><i class="fa-solid fa-download"></i> Download CSV</a>
    &nbsp;|&nbsp;
    <a href="/" style="color:#fff;opacity:.8"><i class="fa-solid fa-arrow-left"></i> New analysis</a>
  </div>
</div>

<!-- Breadcrumb hierarchy nav -->
<div style="background:#fff;border-bottom:1px solid #e8f5e9">
<nav class="bc-bar">
  <i class="fa-solid fa-layer-group" style="color:#2E7D32;margin-right:4px"></i>
  {% for bc in breadcrumb %}
    {% if not loop.first %}<span class="bc-sep"><i class="fa-solid fa-chevron-right"></i></span>{% endif %}
    {% if loop.last %}
      <span class="bc-cur">{{ bc.label }}</span>
    {% else %}
      <a class="bc-link" href="#" onclick="bcNav({{ bc|tojson }});return false">{{ bc.label }}</a>
    {% endif %}
  {% endfor %}
  <span class="bc-level">
    &nbsp;|&nbsp;
    {% if not filter_state %}All States{% elif not filter_district %}Districts in {{ filter_state }}{% elif not filter_city %}Cities in {{ filter_district }}{% else %}PIN codes in {{ filter_city }}{% endif %}
  </span>
  <span class="badge" style="background:#2E7D32;margin-left:6px">{{ cap }}</span>
</nav>
</div>

<div class="metrics">
  <div class="metric">
    <div class="mv">{{ results|length }}</div>
    <div class="ml">Regions analysed</div>
  </div>
  <div class="metric">
    <div class="mv" style="color:#D32F2F">{{ results|selectattr('priority','eq','CRITICAL GAP')|list|length }}</div>
    <div class="ml">Critical gaps</div>
  </div>
  <div class="metric">
    <div class="mv" style="color:#F57C00">{{ results|selectattr('priority','eq','HIGH PRIORITY')|list|length }}</div>
    <div class="ml">High priority</div>
  </div>
  <div class="metric">
    <div class="mv">{{ total }}</div>
    <div class="ml">Facilities scored</div>
  </div>
  <div class="metric">
    <div class="mv" style="color:#1565C0">{{ results|selectattr('confidence','ge',70)|list|length }}</div>
    <div class="ml">High-confidence gaps</div>
  </div>
</div>

{% set crit_list = results|selectattr('priority','eq','CRITICAL GAP')|list %}
{% if crit_list %}
<div style="max-width:1400px;margin:0 auto;padding:0 16px 4px">
<div class="alert-banner">
  <h3><i class="fa-solid fa-circle-exclamation"></i>
    {{ crit_list|length }} Critical Care Gap{{ 's' if crit_list|length != 1 }} Identified: immediate attention required
  </h3>
  <div class="alert-regions">
    {% for r in crit_list[:6] %}
    <div class="alert-card">
      <strong>{{ r.region }}</strong>
      <div class="ac-row"><i class="fa-solid fa-gauge-simple-high" style="color:#D32F2F"></i> Coverage: <b>{{ r.coverage }}%</b></div>
      <div class="ac-row"><i class="fa-solid fa-triangle-exclamation" style="color:#F57C00"></i> Risk score: <b>{{ r.risk or 'N/A' }}</b></div>
      <div class="ac-row"><i class="fa-solid fa-hospital" style="color:#888"></i> {{ r.n }} facilities · {{ r.confidence }}% confident</div>
    </div>
    {% endfor %}
  </div>
</div>
</div>
{% endif %}

<!-- Filter tabs -->
<div class="filter-bar">
  <span style="font-size:.78em;color:#aaa;font-weight:600;padding:6px 0">Filter:</span>
  <button class="filter-tab active" onclick="filterRows('ALL',this)">All ({{ results|length }})</button>
  {% set nc = results|selectattr('priority','eq','CRITICAL GAP')|list|length %}
  {% if nc %}<button class="filter-tab fc" onclick="filterRows('CRITICAL GAP',this)"><i class="fa-solid fa-circle-exclamation"></i> Critical ({{ nc }})</button>{% endif %}
  {% set nh = results|selectattr('priority','eq','HIGH PRIORITY')|list|length %}
  {% if nh %}<button class="filter-tab fh" onclick="filterRows('HIGH PRIORITY',this)">High Priority ({{ nh }})</button>{% endif %}
  {% set nm = results|selectattr('priority','eq','MODERATE')|list|length %}
  {% if nm %}<button class="filter-tab" onclick="filterRows('MODERATE',this)">Moderate ({{ nm }})</button>{% endif %}
  {% set ndp = results|selectattr('priority','eq','DATA-POOR')|list|length %}
  {% if ndp %}<button class="filter-tab" onclick="filterRows('DATA-POOR',this)">Data-Poor ({{ ndp }})</button>{% endif %}
  {% set na = results|selectattr('priority','eq','ADEQUATE')|list|length %}
  {% if na %}<button class="filter-tab" onclick="filterRows('ADEQUATE',this)">Adequate ({{ na }})</button>{% endif %}
</div>

<div class="map-hero">
  <div class="card" style="margin-bottom:14px">
    <h2><i class="fa-solid fa-map"></i> Gap Map</h2>
    <div id="map"></div>
    <div style="margin-top:10px;display:flex;flex-wrap:wrap;gap:14px;align-items:center;font-size:.78em;color:#555">
      <span><i class="fa-solid fa-circle" style="color:#D32F2F"></i> Critical</span>
      <span><i class="fa-solid fa-circle" style="color:#F57C00"></i> High</span>
      <span><i class="fa-solid fa-circle" style="color:#FBC02D"></i> Moderate</span>
      <span><i class="fa-solid fa-circle" style="color:#388E3C"></i> Adequate</span>
      <span><i class="fa-solid fa-circle" style="color:#9E9E9E"></i> Data-poor</span>
      <span style="margin-left:auto;color:#aaa">{{ results|length }} regions mapped &middot; click a marker for details</span>
    </div>
  </div>
</div>

<div class="body-full">
  <div class="card">
    <h2><i class="fa-solid fa-table-list"></i> Regional Coverage
      <span style="font-size:.78em;color:#999;font-weight:400">(click a row for full detail)</span>
    </h2>
    {% if grouped %}
    <div style="font-size:.78em;color:#999;margin:-6px 0 10px">
      <i class="fa-solid fa-circle-info"></i> Grouped by state &middot; click a state row to expand its {{ 'districts' if geo=='district' else 'cities' }}
    </div>
    {% endif %}
    <table>
      <thead>
        <tr>
          <th style="width:28px"></th>
          <th data-tipkey="rank">#</th>
          <th>Region</th>
          <th data-tipkey="coverage">Coverage <i class="fa-solid fa-circle-info tip-icon"></i></th>
          <th data-tipkey="cgr">CGR score <i class="fa-solid fa-circle-info tip-icon"></i></th>
          <th data-tipkey="priority">Priority <i class="fa-solid fa-circle-info tip-icon"></i></th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody>
        {% macro render_row(r, grpid=None, show=True) %}
        <tr class="dr" data-pri="{{ r.priority }}" data-idx="{{ r.rank }}"
            {% if grpid %}data-grpid="{{ grpid }}"{% endif %}
            {% if not show %}style="display:none"{% endif %}
            onclick="toggleRowDetail({{ r.rank }})">
          <td><i class="fa-solid fa-chevron-right exp-caret" id="car-{{ r.rank }}"></i></td>
          <td><span class="rank">{{ r.rank }}</span></td>
          <td>
            <strong>{{ r.region }}</strong>
            {% if r.district %}<div class="sub-loc"><i class="fa-solid fa-location-dot"></i> {{ r.district }} district</div>{% endif %}
            <div class="note">{{ r.note }}</div>
          </td>
          <td><strong style="font-size:1.05em">{{ r.coverage }}%</strong></td>
          <td>
            <span class="risk-badge" style="background:{{ r.cgr_color }};color:#fff;font-size:.82em">{{ r.cgr }}</span>
            <div style="font-size:.7em;font-weight:700;margin-top:2px;color:{{ r.cgr_color }}">{{ r.cgr_tier }}</div>
          </td>
          <td><span class="badge" style="background:{{ r.color }}">{{ r.priority }}</span></td>
          <td style="white-space:nowrap">
            {% if drill_field %}
            <button class="drill-btn drill-nav-btn" title="Drill into {{ r.drill_val }}"
                onclick="event.stopPropagation();drillDown({{ r.drill_val|tojson }})">
              <i class="fa-solid fa-chevron-right"></i> Drill in
            </button>
            {% endif %}
            <button class="drill-btn fac-btn" title="Show facilities in {{ r.region }}"
                onclick="event.stopPropagation();toggleDrill({{ r.rank }},{{ r.region|tojson }})">
              <i class="fa-solid fa-building-columns"></i> Facilities
            </button>
          </td>
        </tr>
        <tr class="row-detail" id="rd-{{ r.rank }}" style="display:none">
          <td colspan="7">
            <div class="row-detail-grid">
              <div class="rd-block">
                <div class="rd-lbl" data-tipkey="evidence">Evidence mix</div>
                <div class="bar-wrap" style="min-width:120px" title="Strong:{{ r.strong }} Partial:{{ r.partial }} Weak:{{ r.weak }} None:{{ r.none }}">
                  {% if r.n %}
                  <div class="bar-seg" style="background:#2E7D32;width:{{ (r.strong/r.n*100)|int }}%"></div>
                  <div class="bar-seg" style="background:#F57C00;width:{{ (r.partial/r.n*100)|int }}%"></div>
                  <div class="bar-seg" style="background:#FBC02D;width:{{ (r.weak/r.n*100)|int }}%"></div>
                  <div class="bar-seg" style="background:#ddd;width:{{ (r.none/r.n*100)|int }}%"></div>
                  {% endif %}
                </div>
                <div style="font-size:.7em;color:#999;margin-top:3px">
                  <span style="color:#2E7D32;font-weight:700">{{ r.strong }} strong</span> &middot;
                  <span style="color:#F57C00">{{ r.partial }} partial</span> &middot;
                  <span style="color:#FBC02D">{{ r.weak }} weak</span> &middot;
                  <span style="color:#aaa">{{ r.none }} none</span>
                </div>
              </div>
              <div class="rd-block">
                <div class="rd-lbl" data-tipkey="facilities">Facilities</div>
                <strong style="font-size:1.05em">{{ r.n }}</strong>
              </div>
              {% if r.dlabel %}
              <div class="rd-block">
                <div class="rd-lbl" data-tipkey="demand">{{ r.dlabel }}</div>
                {% if r.need is not none %}
                <span class="demand-val" style="color:{{ '#D32F2F' if r.need > 60 else ('#F57C00' if r.need > 40 else '#388E3C') }};font-size:1.05em">{{ r.need }}%</span>
                {% else %}<span style="color:#bbb">-</span>{% endif %}
              </div>
              {% endif %}
              <div class="rd-block">
                <div class="rd-lbl" data-tipkey="risk">Gap risk</div>
                {% if r.risk is not none %}
                <span class="risk-badge" style="background:{{ '#D32F2F' if r.risk>50 else ('#F57C00' if r.risk>25 else '#FBC02D') }};color:#fff;font-size:.85em">{{ r.risk }}</span>
                {% else %}<span style="color:#bbb">-</span>{% endif %}
              </div>
              <div class="rd-block">
                <div class="rd-lbl" data-tipkey="confidence">Confidence</div>
                <div class="conf-wrap">
                  <div class="conf-row">
                    <div class="conf-track">
                      <div class="conf-fill" style="width:{{ r.confidence }}%;background:{{ '#2E7D32' if r.confidence>=70 else ('#F57C00' if r.confidence>=40 else '#D32F2F') }}"></div>
                    </div>
                    <span class="conf-pct" style="color:{{ '#2E7D32' if r.confidence>=70 else ('#F57C00' if r.confidence>=40 else '#D32F2F') }}">{{ r.confidence }}%</span>
                  </div>
                  <div class="conf-moe">±{{ r.moe }}% margin of error</div>
                </div>
              </div>
              <div class="rd-block">
                <div class="rd-lbl" data-tipkey="dataquality">Data quality</div>
                <span class="dq-badge" style="background:{{ r.dq_color }}">{{ r.dq_label }}</span>
              </div>
              <div class="rd-block" style="margin-left:auto">
                <div class="rd-lbl">A/H/S/C breakdown</div>
                <div style="font-size:.78em;color:#888">A{{ r.cgr_ga }} &middot; H{{ r.cgr_hb }} &middot; S{{ r.cgr_sv }} &middot; C{{ r.cgr_cg }}</div>
              </div>
              <div class="rd-block" style="flex:1 1 100%;min-width:280px">
                <div class="rd-lbl"><i class="fa-solid fa-quote-left"></i> Cited evidence behind this score</div>
                {% if r.top_evidence %}
                  {% for ev in r.top_evidence %}
                  <div class="ev-cite"><span class="ev-field">[{{ ev.field }}]</span> <strong>{{ ev.name }}</strong>: "{{ ev.text }}"</div>
                  {% endfor %}
                {% else %}
                  <span style="color:#bbb;font-size:.85em">No facility text evidence found for this capability here - score and priority reflect that absence, not a confirmed gap.</span>
                {% endif %}
              </div>
            </div>
          </td>
        </tr>
        <tr id="dr-{{ r.rank }}" class="drill-row">
          <td colspan="7" style="padding:0">
            <div class="drill-panel" id="dp-{{ r.rank }}">Loading…</div>
          </td>
        </tr>
        {% endmacro %}
        {% if grouped %}
          {% for grp in grouped %}
          {% set gid = "grp" ~ loop.index %}
          {% set gexpanded = loop.index == 1 %}
          <tr class="state-grp-row" onclick="toggleGroup('{{ gid }}')">
            <td colspan="7">
              <i class="fa-solid {{ 'fa-chevron-down' if gexpanded else 'fa-chevron-right' }} grp-caret" id="gcar-{{ gid }}"></i>
              <strong class="grp-name">{{ grp.state }}</strong>
              <span class="grp-stats">
                <span class="grp-pill">{{ grp.n }} {{ 'districts' if geo=='district' else 'cities' }}</span>
                {% if grp.n_critical %}<span class="grp-pill crit">{{ grp.n_critical }} critical</span>{% endif %}
                {% if grp.n_high %}<span class="grp-pill high">{{ grp.n_high }} high priority</span>{% endif %}
                <span class="grp-pill">{{ grp.avg_coverage }}% avg coverage</span>
              </span>
            </td>
          </tr>
          {% for r in grp.rows %}{{ render_row(r, gid, gexpanded) }}{% endfor %}
          {% endfor %}
        {% else %}
          {% for r in results %}{{ render_row(r) }}{% endfor %}
        {% endif %}
        <tr class="no-rows-msg" id="no-rows"><td colspan="7">No regions match this filter.</td></tr>
      </tbody>
    </table>
  </div>

  <div class="card">
    <h2><i class="fa-solid fa-floppy-disk"></i> Save Planning Scenario</h2>
    <div class="save-form">
      <input type="text" id="sc-name" placeholder="Scenario name (e.g. Rajasthan ICU gap - June 2026)">
      <textarea id="sc-notes" placeholder="Key findings, next steps, data caveats…"></textarea>
      <button class="save-btn" onclick="saveScenario()"><i class="fa-solid fa-floppy-disk"></i> Save</button>
      <span id="save-msg" style="margin-left:10px;font-size:.84em;color:#2E7D32"></span>
    </div>
  </div>

  <div class="links">
    <a href="/"><i class="fa-solid fa-arrow-left"></i> New analysis</a>
    <a href="/scenarios">Saved scenarios <i class="fa-solid fa-arrow-right"></i></a>
    <a href="/facilities"><i class="fa-solid fa-hospital"></i> Facility Directory</a>
    <a href="/export_csv?capability={{ cap }}&geo_level={{ geo }}&facility_type={{ ftype }}"><i class="fa-solid fa-download"></i> Download CSV</a>
  </div>
</div>

<script>
var GEO        = {{ geo_json|safe }};
var CAP        = {{ cap|tojson }};
var GEO_L      = {{ geo|tojson }};
var FTYPE      = {{ ftype|tojson }};
var DRILL_FIELD= {{ (drill_field or '')|tojson }};
var FS         = {{ filter_state|tojson }};
var FD         = {{ filter_district|tojson }};
var FC         = {{ filter_city|tojson }};

// Leaflet map
var _lmap=L.map('map',{zoomControl:true}).setView([22.5,80.0],5);
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',{
  attribution:'© OpenStreetMap © CARTO',
  subdomains:'abcd',maxZoom:19
}).addTo(_lmap);

var BAND={CRITICAL:"#B71C1C",HIGH:"#E64A19",ELEVATED:"#F57C00",MODERATE:"#FBC02D",LOW:"#388E3C"};
GEO.forEach(function(r){
  if(!r.lat||!r.lon) return;
  var sz=Math.max(6,Math.min(22,6+Math.sqrt(r.n||1)*1.8));
  var col=r.cgr_color||r.color||"#888";
  var cgr=r.cgr!==undefined?r.cgr:"N/A";
  var tier=r.cgr_tier||"";
  var popup='<div style="font-size:12.5px;min-width:200px">'+
    '<b style="font-size:13px">'+r.region+'</b><br>'+
    '<span style="display:inline-block;padding:2px 9px;border-radius:10px;background:'+col+
    ';color:#fff;font-weight:700;font-size:.8em;margin:4px 0">'+tier+' · CGR '+cgr+'</span>'+
    '<table style="width:100%;border-collapse:collapse;font-size:11.5px;margin-top:4px">'+
    '<tr><td style="color:#555;padding:2px 4px">Coverage</td><td style="font-weight:700;text-align:right">'+r.coverage+'%</td></tr>'+
    '<tr><td style="color:#555;padding:2px 4px">Facilities</td><td style="font-weight:700;text-align:right">'+r.n+'</td></tr>'+
    (r.risk!=null?'<tr><td style="color:#555;padding:2px 4px">Gap risk</td><td style="font-weight:700;text-align:right">'+r.risk+'</td></tr>':'')+
    '<tr><td style="color:#555;padding:2px 4px">Confidence</td><td style="font-weight:700;text-align:right">'+r.confidence+'% ±'+r.moe+'%</td></tr>'+
    '</table>'+
    '<div style="margin-top:6px;font-size:10.5px;color:#666;background:#f5f5f5;border-radius:5px;padding:5px 8px">'+
    'Access deficit: '+r.cgr_ga+' · Health burden: '+r.cgr_hb+'<br>'+
    'Social vuln: '+r.cgr_sv+' · Capability gap: '+r.cgr_cg+'</div>'+
    '</div>';
  L.circleMarker([r.lat,r.lon],{
    radius:sz,fillColor:col,color:'rgba(255,255,255,0.7)',
    weight:1.5,fillOpacity:0.85
  }).bindPopup(popup).addTo(_lmap);
});

window.focusRegion=function(region){
  var r=GEO.find(function(x){return x.region===region;});
  if(r&&r.lat&&r.lon) _lmap.setView([r.lat,r.lon],8);
};

function drillDown(val) {
  document.getElementById('df-cap').value   = CAP;
  document.getElementById('df-ftype').value = FTYPE;
  document.getElementById('df-state').value    = FS;
  document.getElementById('df-district').value = FD;
  document.getElementById('df-city').value     = FC;
  if(DRILL_FIELD==='filter_state')    document.getElementById('df-state').value    = val;
  else if(DRILL_FIELD==='filter_district') document.getElementById('df-district').value = val;
  else if(DRILL_FIELD==='filter_city')     document.getElementById('df-city').value     = val;
  document.getElementById('drill-form').submit();
}

function bcNav(bc) {
  document.getElementById('df-cap').value      = CAP;
  document.getElementById('df-ftype').value    = FTYPE;
  document.getElementById('df-state').value    = bc.fs || '';
  document.getElementById('df-district').value = bc.fd || '';
  document.getElementById('df-city').value     = bc.fc || '';
  document.getElementById('drill-form').submit();
}

// focusRegion is set by the ArcGIS require block above

var drillLoaded={};

function toggleGroup(gid){
  var rows=document.querySelectorAll('tr.dr[data-grpid="'+gid+'"]');
  if(!rows.length) return;
  var collapsing = rows[0].style.display!=='none';
  rows.forEach(function(tr){
    tr.style.display = collapsing ? 'none' : '';
    var idx=tr.dataset.idx;
    if(collapsing){
      var rd=document.getElementById('rd-'+idx); if(rd) rd.style.display='none';
      var dr=document.getElementById('dr-'+idx); if(dr) dr.style.display='none';
      var car=document.getElementById('car-'+idx);
      if(car){car.classList.remove('fa-chevron-down');car.classList.add('fa-chevron-right');}
    }
  });
  var caret=document.getElementById('gcar-'+gid);
  if(caret){
    caret.classList.toggle('fa-chevron-down', !collapsing);
    caret.classList.toggle('fa-chevron-right', collapsing);
  }
}

function toggleRowDetail(idx){
  var row=document.getElementById('rd-'+idx);
  var caret=document.getElementById('car-'+idx);
  var open=row.style.display!=='none';
  row.style.display = open ? 'none' : 'table-row';
  if(caret){
    caret.classList.toggle('fa-chevron-down', !open);
    caret.classList.toggle('fa-chevron-right', open);
  }
}

function filterRows(prio, el){
  document.querySelectorAll('.filter-tab').forEach(function(t){t.classList.remove('active');});
  el.classList.add('active');
  // Expand any collapsed state groups first so matching rows aren't hidden behind a closed header
  document.querySelectorAll('.state-grp-row .grp-caret.fa-chevron-right').forEach(function(caret){
    toggleGroup(caret.id.replace('gcar-',''));
  });
  var groupHasVisible={};
  document.querySelectorAll('tr.dr').forEach(function(tr){
    var show = prio==='ALL' || tr.dataset.pri===prio;
    tr.style.display = show ? '' : 'none';
    var idx=tr.dataset.idx;
    if(!show){
      var rd=document.getElementById('rd-'+idx); if(rd) rd.style.display='none';
      var dr=document.getElementById('dr-'+idx); if(dr) dr.style.display='none';
    }
    if(show && tr.dataset.grpid) groupHasVisible[tr.dataset.grpid]=true;
  });
  document.querySelectorAll('.state-grp-row').forEach(function(g){
    var caret=g.querySelector('.grp-caret');
    var gid=caret? caret.id.replace('gcar-',''):null;
    if(gid) g.style.display = groupHasVisible[gid] ? '' : 'none';
  });
  var anyVisible=false;
  document.querySelectorAll('tr.dr').forEach(function(tr){ if(tr.style.display!=='none') anyVisible=true; });
  var noMsg=document.getElementById('no-rows');
  if(noMsg) noMsg.style.display = anyVisible ? 'none' : '';
}

function toggleDrill(idx, region){
  var row=document.getElementById('dr-'+idx);
  var panel=document.getElementById('dp-'+idx);
  if(window.getComputedStyle(row).display !== 'none'){
    row.style.display='none'; return;
  }
  row.style.display='table-row';
  if(drillLoaded[region]){ focusRegion(region); return; }
  drillLoaded[region]=true;
  panel.innerHTML='<div style="padding:16px;color:#888;font-size:.85em"><i class="fa-solid fa-spinner fa-spin"></i> Loading facilities…</div>';
  fetch('/facility_detail?region='+encodeURIComponent(region)
    +'&capability='+encodeURIComponent(CAP)
    +'&geo_level='+encodeURIComponent(GEO_L)
    +'&facility_type='+encodeURIComponent(FTYPE))
  .then(function(r){return r.json()})
  .then(function(data){renderDrill(idx,region,data,panel)})
  .catch(function(e){panel.innerHTML='<span style="color:red">Error: '+e+'</span>'});
  focusRegion(region);
}

function renderDrill(idx,region,data,panel){
  if(data.error){panel.innerHTML='<span style="color:red">'+data.error+'</span>';return;}
  var items=data.items, bounds=[];
  var nS=0,nP=0,nW=0,nN=0,nVerified=0;
  items.forEach(function(f){
    if(f.level==='STRONG') nS++;
    else if(f.level==='PARTIAL') nP++;
    else if(f.level==='WEAK') nW++;
    else nN++;
    if(f.quality && f.quality.length>=2) nVerified++;
  });

  // Summary header
  var html='<div class="drill-summary">'+
    '<span class="ds-item" style="color:#1B5E20"><i class="fa-solid fa-hospital"></i> '+items.length+' facilities</span>'+
    '<span class="ds-item" style="color:#2E7D32"><i class="fa-solid fa-circle-check"></i> '+nS+' strong evidence</span>'+
    '<span class="ds-item" style="color:#F57C00"><i class="fa-solid fa-circle-half-stroke"></i> '+nP+' partial</span>'+
    '<span class="ds-item" style="color:#FBC02D"><i class="fa-solid fa-circle-dot"></i> '+nW+' weak</span>'+
    '<span class="ds-item" style="color:#aaa"><i class="fa-solid fa-circle-xmark"></i> '+nN+' no evidence</span>'+
    '<span class="ds-item" style="color:#1565C0"><i class="fa-solid fa-database"></i> '+nVerified+' multi-signal verified</span>'+
    '</div>';

  items.forEach(function(f){
    var ECOL={STRONG:'#2E7D32',PARTIAL:'#F57C00',WEAK:'#FBC02D','NO CLAIM':'#9E9E9E'};
    var cc=ECOL[f.level]||'#9E9E9E';
    html+='<div class="fac-card">';
    html+='<div class="fac-hdr"><span class="fac-name">'+f.name+'</span>'+
          '<span class="badge" style="background:'+cc+'">'+f.level+'</span>';
    if(f.ftype) html+='<span class="badge" style="background:#eee;color:#555;margin-left:3px">'+f.ftype+'</span>';
    html+='</div>';
    html+='<div class="fac-meta">';
    if(f.city) html+='<i class="fa-solid fa-location-dot"></i> '+f.city+'&nbsp; ';
    if(f.doctors) html+='<i class="fa-solid fa-user-doctor"></i> '+f.doctors+' doctors&nbsp; ';
    if(f.capacity) html+='<i class="fa-solid fa-bed"></i> '+f.capacity+' beds';
    html+='</div>';
    // Score breakdown
    html+='<div class="score-bar">'+
          '<span title="Specialty taxonomy match (0-6 pts)"><i class="fa-solid fa-microscope"></i> Specialty: <b>'+f.spec_score+'</b></span>'+
          '<span title="Text keyword evidence (0-4 pts)"><i class="fa-solid fa-file-lines"></i> Text: <b>'+f.text_score+'</b></span>'+
          '<span title="Data quality signals (0-2 pts)"><i class="fa-solid fa-star"></i> Quality: <b>'+f.quality_score+'</b></span>'+
          '<strong title="Total trust score / 12"> = '+f.score+' / 12</strong></div>';
    // Specialty matches
    if(f.spec_hits&&f.spec_hits.length){
      html+='<div class="spec-tags">';
      f.spec_hits.forEach(function(s){html+='<span class="spec-tag"><i class="fa-solid fa-tag"></i> '+s+'</span>';});
      html+='</div>';
    }
    // Text evidence
    if(f.evidence&&f.evidence.length){
      f.evidence.slice(0,3).forEach(function(e){
        html+='<div class="ev-cite"><span class="ev-field">['+e.field+']</span> '+
              e.text.replace(/</g,'&lt;').replace(/>/g,'&gt;')+'</div>';
      });
    } else {
      html+='<div class="no-ev"><i class="fa-solid fa-circle-info"></i> No text evidence found for this capability</div>';
    }
    // Data quality signals
    if(f.quality&&f.quality.length){
      html+='<div class="q-sigs"><i class="fa-solid fa-database" style="color:#1565C0"></i> Data signals: <b>'+f.quality.join('</b> · <b>')+'</b></div>';
    } else {
      html+='<div class="q-sigs" style="color:#ccc"><i class="fa-solid fa-circle-xmark"></i> No data quality signals found</div>';
    }
    html+='</div>';
    if(f.lat&&f.lon) bounds.push([f.lat,f.lon]);
  });
  panel.innerHTML=html;
  if(bounds.length) _lmap.fitBounds(bounds,{padding:[40,40],maxZoom:10});
}

function saveScenario(){
  var name=document.getElementById('sc-name').value.trim();
  if(!name){alert('Please enter a scenario name');return;}
  var critical=GEO.filter(function(r){return r.priority==='CRITICAL GAP';}).map(function(r){return r.region;});
  fetch('/save_scenario',{
    method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({
      name:name,
      notes:document.getElementById('sc-notes').value,
      capability:CAP,geo_level:GEO_L,
      critical_regions:critical,total_regions:GEO.length
    })
  }).then(function(r){return r.json()}).then(function(d){
    document.getElementById('save-msg').innerHTML='<i class="fa-solid fa-circle-check"></i> Saved '+d.id;
  });
}

// ---- Chat widget ----
var _chatHistory=[], _chatOpen=false;
function toggleChat(){
  _chatOpen=!_chatOpen;
  document.getElementById('chat-panel').classList.toggle('open',_chatOpen);
  if(_chatOpen) document.getElementById('chat-inp').focus();
}
function _escH(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function _addMsg(role,text){
  var box=document.getElementById('chat-msgs');
  var d=document.createElement('div'); d.className='cm '+role;
  var html=role==='user'?'<div class="cb">'+_escH(text)+'</div>'
    :'<div class="cb">'+text.replace(/\*\*(.*?)\*\*/g,'<b>$1</b>').replace(/\n/g,'<br>')+'</div>';
  d.innerHTML=html; box.appendChild(d); box.scrollTop=box.scrollHeight;
  _chatHistory.push({role:role,content:text});
}
function _chatCtx(){
  var ctx={capability:CAP,geo_level:GEO_L,total:{{ total|default(0) }}};
  var crit=[];
  document.querySelectorAll('tr[data-pri="CRITICAL GAP"]').forEach(function(r,i){
    if(i<4){var s=r.querySelector('td:nth-child(3) strong');if(s)crit.push(s.textContent.trim());}
  });
  if(crit.length) ctx.critical_regions=crit;
  return ctx;
}
function sendChat(){
  var inp=document.getElementById('chat-inp');
  var msg=inp.value.trim(); if(!msg)return; inp.value='';
  document.getElementById('chat-qs').style.display='none';
  _addMsg('user',msg); _callChat(msg);
}
function askQ(q){
  document.getElementById('chat-qs').style.display='none';
  _addMsg('user',q); _callChat(q);
}
function _callChat(msg){
  var box=document.getElementById('chat-msgs');
  var td=document.createElement('div'); td.className='cm bot'; td.id='chat-typing';
  td.innerHTML='<div class="cb typing"><span></span><span></span><span></span></div>';
  box.appendChild(td); box.scrollTop=box.scrollHeight;
  fetch('/chat',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({message:msg,history:_chatHistory.slice(-8),context:_chatCtx()})})
  .then(function(r){return r.json();})
  .then(function(d){
    var t=document.getElementById('chat-typing'); if(t)t.remove();
    _addMsg('bot', d.reply||'Sorry, I could not get a response. Please try again.');
  }).catch(function(){
    var t=document.getElementById('chat-typing'); if(t)t.remove();
    _addMsg('bot','Connection error. Please try again.');
  });
}

// ---- KPI tooltips ----
var TIPS = {
  cgr: '<div class="tt"><i class="fa-solid fa-layer-group"></i> CGR Score (Care Gap Risk)</div>' +
    '<p>A <b>composite 0-100 index</b> adapted from the India Health Atlas methodology. Combines four dimensions:</p>' +
    '<div class="tf">CGR = 40% Access Deficit + 25% Health Burden + 25% Social Vulnerability + 10% Capability Gap</div>' +
    '<ul>' +
    '<li><b style="color:#81C784">A (Access deficit, 40%)</b> - Inverse of coverage %: how far from full supply</li>' +
    '<li><b style="color:#FFB74D">H (Health burden, 25%)</b> - NFHS-5: anaemia + hypertension + blood sugar + obesity + child illness</li>' +
    '<li><b style="color:#F48FB1">S (Social vulnerability, 25%)</b> - Inverse of literacy, sanitation, insurance, water, electricity, institutional births</li>' +
    '<li><b style="color:#90CAF9">C (Capability gap, 10%)</b> - Facility evidence mix: few strong-evidence facilities = high gap</li>' +
    '</ul>' +
    '<p>Risk tiers: <b style="color:#B71C1C">CRITICAL</b> &ge;57 &bull; <b style="color:#E64A19">HIGH</b> 47-57 &bull; <b style="color:#F57C00">ELEVATED</b> 35-47 &bull; <b style="color:#FBC02D">MODERATE</b> 22-35 &bull; <b style="color:#388E3C">LOW</b> &lt;22</p>' +
    '<div class="tb">The A/H/S/C breakdown in each cell shows the individual component scores.</div>',

  rank: '<div class="tt"><i class="fa-solid fa-ranking-star"></i> Rank</div>' +
    '<p>Rows are sorted by <b>gap risk</b> (highest urgency first). ' +
    'Critical gaps and high-demand areas with low coverage bubble to the top.</p>' +
    '<div class="tb">Ties are broken by coverage % (lower = higher rank).</div>',

  facilities: '<div class="tt"><i class="fa-solid fa-hospital"></i> Facilities</div>' +
    '<p>Total number of real healthcare facilities found in this region from the dataset.</p>' +
    '<div class="tb">A low count means the gap estimate is less reliable. Check the Confidence score.</div>',

  evidence: '<div class="tt"><i class="fa-solid fa-microscope"></i> Evidence Mix</div>' +
    '<p>Each facility is scored <b>0-12 points</b> across three signal types:</p>' +
    '<ul>' +
    '<li><b style="color:#81C784">Strong (8-12 pts)</b> - specialty taxonomy match + keyword text evidence + quality signals</li>' +
    '<li><b style="color:#FFB74D">Partial (4-7 pts)</b> - limited keyword evidence or indirect match</li>' +
    '<li><b style="color:#FFF176">Weak (1-3 pts)</b> - minor keyword match only</li>' +
    '<li><b style="color:#aaa">None (0 pts)</b> - no evidence found for this capability</li>' +
    '</ul>' +
    '<div class="tb">The bar shows the proportion of each tier in this region.</div>',

  coverage: '<div class="tt"><i class="fa-solid fa-gauge-simple-high"></i> Coverage %</div>' +
    '<div class="tf">Coverage = (Strong x 1.0 + Partial x 0.5 + Weak x 0.25) / Total x 100</div>' +
    '<p>A <b>trust-weighted</b> score, not a simple facility count. ' +
    'A region with 10 strong-evidence facilities scores higher than one with 50 weak-evidence facilities.</p>' +
    '<div class="tb">Range 0-100%. Lower = larger gap.</div>',

  demand: '<div class="tt"><i class="fa-solid fa-chart-line"></i> Demand / Need Index</div>' +
    '<p>Derived from <b>NFHS-5 district health survey</b> indicators. ' +
    'Reflects actual population health need in this region, e.g. institutional delivery rates, ' +
    'skilled birth attendance, malnutrition prevalence, or disease burden depending on the capability selected.</p>' +
    '<div class="tb">Higher % = more unmet need. Used alongside coverage to compute Gap Risk.</div>',

  risk: '<div class="tt"><i class="fa-solid fa-triangle-exclamation"></i> Gap Risk Score</div>' +
    '<div class="tf">Gap Risk = (1 - Coverage / 100) x (Demand / 100) x 100</div>' +
    '<p>Combines <b>supply shortage</b> with <b>demand pressure</b>. A region with low coverage AND high need ' +
    'scores the highest risk. A region with low coverage but also low demand scores lower.</p>' +
    '<ul>' +
    '<li><b style="color:#ef9a9a">&gt; 50</b> - Critical: immediate action needed</li>' +
    '<li><b style="color:#FFB74D">25-50</b> - High: near-term investment required</li>' +
    '<li><b style="color:#FFF176">&lt; 25</b> - Moderate or low risk</li>' +
    '</ul>' +
    '<div class="tb">Range 0-100. A score of 0 means no risk (adequate coverage or no demand data).</div>',

  confidence: '<div class="tt"><i class="fa-solid fa-chart-pie"></i> Confidence %</div>' +
    '<div class="tf">Confidence = Sample size (0-40) + Evidence quality (0-40) + Data source (0-20)</div>' +
    '<p>Three components:</p>' +
    '<ul>' +
    '<li><b>Sample size</b> - n/20 x 40 pts. 20+ facilities = full 40 pts</li>' +
    '<li><b>Evidence quality</b> - (Strong + Partial x 0.5) / n x 40 pts</li>' +
    '<li><b>Data source</b> - 20 pts if district-level NFHS data, 8 pts if state-level average</li>' +
    '</ul>' +
    '<p>The <b>margin of error</b> (±%) uses: <span style="font-family:monospace">max(5, round(50 / sqrt(n)))</span></p>' +
    '<div class="tb">Confidence below 40% means the gap estimate is uncertain. More data needed.</div>',

  dataquality: '<div class="tt"><i class="fa-solid fa-database"></i> Data Quality</div>' +
    '<p>Summarises how trustworthy this region\'s estimate is:</p>' +
    '<ul>' +
    '<li><b style="color:#81C784">Verified</b> - Confidence >= 70. 10+ facilities, strong evidence, district-level NFHS data matched</li>' +
    '<li><b style="color:#FFB74D">Moderate</b> - Confidence 40-70. Some evidence present, may use state-level averages</li>' +
    '<li><b style="color:#aaa">Sparse</b> - Confidence &lt; 40. Few records or weak signals - treat with caution</li>' +
    '</ul>' +
    '<div class="tb">Based on sample size, evidence strength, and geographic data resolution.</div>',

  priority: '<div class="tt"><i class="fa-solid fa-flag"></i> Priority Label</div>' +
    '<ul>' +
    '<li><b style="color:#ef9a9a">CRITICAL GAP</b> - Coverage &lt; 20% and Confidence >= 40%</li>' +
    '<li><b style="color:#FFB74D">HIGH PRIORITY</b> - Coverage 20-40%</li>' +
    '<li><b style="color:#FFF176">MODERATE</b> - Coverage 40-65%</li>' +
    '<li><b style="color:#aaa">DATA-POOR</b> - Fewer than 5 facilities or Confidence &lt; 25%</li>' +
    '<li><b style="color:#81C784">ADEQUATE</b> - Coverage >= 65%</li>' +
    '</ul>' +
    '<div class="tb">Priority is based on coverage alone; use Gap Risk to factor in demand.</div>'
};

(function(){
  var tip = document.getElementById('kpi-tip');
  function moveTip(e){
    var W=window.innerWidth, H=window.innerHeight;
    var x=e.clientX+16, y=e.clientY+16;
    tip.style.left='-9999px'; tip.style.top='-9999px'; tip.style.display='block';
    var tw=tip.offsetWidth, th_=tip.offsetHeight;
    if(x+tw>W-12) x=e.clientX-tw-16;
    if(y+th_>H-12) y=e.clientY-th_-16;
    tip.style.left=x+'px'; tip.style.top=y+'px';
  }
  document.querySelectorAll('[data-tipkey]').forEach(function(el){
    var key=el.dataset.tipkey;
    if(!TIPS[key]) return;
    el.style.cursor='help';
    el.addEventListener('mouseenter', function(e){
      tip.innerHTML=TIPS[key];
      moveTip(e);
    });
    el.addEventListener('mousemove', moveTip);
    el.addEventListener('mouseleave', function(){ tip.style.display='none'; });
  });
})();
</script>

<!-- AI Chat Widget -->
<button id="chat-bubble" onclick="toggleChat()">
  <span class="chat-dot"></span>
  <i class="fa-solid fa-comments"></i> Ask AI
</button>
<div id="chat-panel">
  <div class="chat-hdr">
    <div class="chat-hdr-ico"><i class="fa-solid fa-robot"></i></div>
    <div class="chat-hdr-txt">
      <b>Medical Desert AI</b>
      <span>Ask about gaps, metrics & interventions</span>
    </div>
    <button onclick="toggleChat()" title="Close"><i class="fa-solid fa-xmark"></i></button>
  </div>
  <div id="chat-msgs">
    <div class="cm bot">
      <div class="cb">Hi! I can help you interpret this <b>{{ cap }}</b> gap analysis. Ask me about coverage scores, gap risk, confidence metrics, or interventions for specific regions.</div>
    </div>
    <div id="chat-qs">
      <button class="cq-btn" onclick="askQ('What does Coverage % mean and how is it calculated?')">What is Coverage %?</button>
      <button class="cq-btn" onclick="askQ('How is Gap Risk calculated and what makes a region high risk?')">How is Gap Risk scored?</button>
      <button class="cq-btn" onclick="askQ('What interventions work best for CRITICAL GAP regions in India for {{ cap }}?')">Best interventions for CRITICAL GAP?</button>
      <button class="cq-btn" onclick="askQ('When should I trust the Confidence score and what does margin of error mean?')">When to trust Confidence %?</button>
      <button class="cq-btn" onclick="askQ('What is DATA-POOR and what should I do about those regions?')">What is DATA-POOR?</button>
    </div>
  </div>
  <div id="chat-inp-row">
    <input id="chat-inp" type="text" placeholder="Ask about this analysis..."
           onkeydown="if(event.key==='Enter')sendChat()">
    <button class="cs-btn" onclick="sendChat()"><i class="fa-solid fa-paper-plane"></i></button>
  </div>
  <div id="chat-foot"><i class="fa-solid fa-robot"></i> Powered by Databricks AI &bull; Context: {{ cap }} / {{ geo }}</div>
</div>
</body></html>"""

SC_T = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><title>Saved Scenarios</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css"/>
<style>
body{font-family:'Segoe UI',sans-serif;background:#f0f4f0;padding:28px;max-width:900px;margin:0 auto}
h1{color:#1B5E20;margin-bottom:20px}
.card{background:#fff;border-radius:12px;box-shadow:0 1px 8px rgba(0,0,0,.07);padding:20px;margin-bottom:14px}
.badge{display:inline-block;padding:2px 8px;border-radius:10px;color:#fff;font-size:.74em;font-weight:600;background:#2E7D32;margin-right:5px}
.gap-list{background:#FFF3E0;padding:10px 14px;border-radius:6px;margin-top:10px;font-size:.84em}
a{color:#2E7D32;text-decoration:none}a:hover{text-decoration:underline}
</style>
</head>
<body>
<h1><i class="fa-solid fa-clipboard-list"></i> All Planning Scenarios</h1>
{% if scenarios %}
  {% for s in scenarios|reverse %}
  <div class="card">
    <strong style="font-size:1.05em">{{ s.name }}</strong>
    <span style="color:#aaa;font-size:.82em;margin-left:8px">{{ s.saved_at }}</span><br><br>
    <span class="badge">{{ s.capability }}</span>
    <span class="badge" style="background:#555">{{ s.geo_level or 'state' }}</span>
    <span class="badge" style="background:#777">{{ s.total_regions or 0 }} regions</span>
    {% if s.notes %}<p style="margin-top:10px;color:#444;font-size:.88em"><i class="fa-solid fa-note-sticky"></i> {{ s.notes }}</p>{% endif %}
    {% if s.critical_regions %}
    <div class="gap-list">
      <strong style="color:#D32F2F">Critical gaps:</strong> {{ s.critical_regions|join(', ') }}
    </div>
    {% endif %}
  </div>
  {% endfor %}
{% else %}
<p style="color:#aaa">No scenarios saved yet. Run an analysis and save from the results page.</p>
{% endif %}
<br><a href="/"><i class="fa-solid fa-arrow-left"></i> Back to planner</a>
</body></html>"""

FAC_DIR_T = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Facility Directory</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css"/>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',sans-serif;background:#f0f4f0;color:#222;font-size:.92em}
.hdr{background:linear-gradient(135deg,#1B5E20,#2E7D32);color:#fff;padding:18px 20px;display:flex;align-items:center;gap:14px;flex-wrap:wrap}
.hdr h1{font-size:1.5em;font-weight:800;flex:1}
.hdr a{color:#fff;text-decoration:none;font-size:.88em;white-space:nowrap}
.hdr a:hover{text-decoration:underline}
.bar{background:#fff;border-bottom:1px solid #e0e0e0;padding:10px 20px;display:flex;gap:10px;flex-wrap:wrap;align-items:center}
.bar form{display:flex;gap:8px;flex-wrap:wrap;width:100%;align-items:center}
.bar input[type=text]{flex:1;min-width:160px;padding:7px 12px;border:1.5px solid #ddd;border-radius:7px;font-size:.9em}
.bar select{padding:7px 10px;border:1.5px solid #ddd;border-radius:7px;font-size:.88em;background:#fff}
.bar button{background:#2E7D32;color:#fff;border:none;border-radius:7px;padding:7px 16px;cursor:pointer;font-weight:600}
.bar button:hover{background:#1B5E20}
.wrap{max-width:1280px;margin:16px auto;padding:0 14px}
.meta{color:#666;font-size:.82em;margin-bottom:10px}
.tbl-wrap{overflow-x:auto;background:#fff;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,.07)}
table{width:100%;border-collapse:collapse;min-width:900px}
th{background:#1B5E20;color:#fff;padding:9px 10px;text-align:left;font-size:.8em;font-weight:700;white-space:nowrap;position:sticky;top:0;z-index:1}
th a{color:#fff;text-decoration:none}
th a:hover{text-decoration:underline}
td{padding:8px 10px;border-bottom:1px solid #f0f0f0;vertical-align:middle;font-size:.84em}
tr:hover{background:#f9fbe7}
tr:last-child td{border-bottom:none}
.stars{color:#FBC02D;letter-spacing:-1px;font-size:1em}
.stars-empty{color:#e0e0e0}
.badge{display:inline-block;padding:2px 7px;border-radius:8px;font-size:.75em;font-weight:600;white-space:nowrap}
.badge-h{background:#E3F2FD;color:#1565C0}
.badge-c{background:#F3E5F5;color:#6A1B9A}
.badge-o{background:#FFF9C4;color:#795548}
.no-data{color:#ccc;font-style:italic}
.icon-link{color:#2E7D32;text-decoration:none}
.icon-link:hover{color:#1B5E20;text-decoration:underline}
.rating-bar{display:flex;align-items:center;gap:6px}
.r-bg{flex:1;max-width:80px;background:#eee;border-radius:10px;height:6px;overflow:hidden}
.r-fill{height:6px;border-radius:10px;background:linear-gradient(90deg,#FBC02D,#F57C00)}
.pg{display:flex;gap:6px;justify-content:center;margin:18px 0;flex-wrap:wrap}
.pg a,.pg span{padding:6px 14px;border-radius:6px;text-decoration:none;font-size:.85em;border:1px solid #ddd;color:#333}
.pg a:hover{background:#E8F5E9}
.pg .cur{background:#2E7D32;color:#fff;border-color:#2E7D32}
.doctor-chip{background:#E8F5E9;color:#1B5E20;padding:2px 8px;border-radius:10px;font-size:.8em;font-weight:600}
@media(max-width:700px){.bar form{flex-direction:column}.bar select,.bar input[type=text]{width:100%}}
</style>
</head>
<body>
<div class="hdr">
  <a href="/"><i class="fa-solid fa-arrow-left"></i> Back to Planner</a>
  <h1><i class="fa-solid fa-hospital"></i> Facility Directory</h1>
  <span style="font-size:.82em;opacity:.85">{{ total }} facilities shown</span>
</div>

<div class="bar">
  <form method="GET" action="/facilities">
    <input type="text" name="search" placeholder="Search name, city, district..." value="{{ search }}">
    <select name="state">
      <option value="">All States</option>
      {% for s in all_states %}<option value="{{ s }}" {% if s==state %}selected{% endif %}>{{ s }}</option>{% endfor %}
    </select>
    <select name="ftype">
      <option value="">All Types</option>
      {% for t in all_types %}<option value="{{ t }}" {% if t==ftype %}selected{% endif %}>{{ t }}</option>{% endfor %}
    </select>
    <select name="sort">
      <option value="rating"  {% if sort=='rating'  %}selected{% endif %}>Sort: Profile Rating</option>
      <option value="doctors" {% if sort=='doctors' %}selected{% endif %}>Sort: Doctor Count</option>
      <option value="name"    {% if sort=='name'    %}selected{% endif %}>Sort: Name A-Z</option>
      <option value="city"    {% if sort=='city'    %}selected{% endif %}>Sort: City A-Z</option>
    </select>
    <button type="submit"><i class="fa-solid fa-filter"></i> Filter</button>
    {% if search or state or ftype %}<a href="/facilities" style="color:#E53935;font-size:.85em;white-space:nowrap">Clear filters</a>{% endif %}
  </form>
</div>

<div class="wrap">
  <div class="meta">
    Showing {{ ((page-1)*50)+1 }}--{{ [page*50, total]|min }} of {{ total }} facilities
    &nbsp;&bull;&nbsp; Profile Rating based on data completeness (doctors, contacts, social presence, engagement)
  </div>

  <div class="tbl-wrap">
  <table>
  <thead><tr>
    <th>#</th>
    <th><a href="?search={{ search }}&state={{ state }}&ftype={{ ftype }}&sort=name&page=1">Facility <i class="fa-solid fa-sort"></i></a></th>
    <th>Type</th>
    <th><a href="?search={{ search }}&state={{ state }}&ftype={{ ftype }}&sort=city&page=1">City <i class="fa-solid fa-sort"></i></a></th>
    <th>District</th>
    <th>State</th>
    <th><a href="?search={{ search }}&state={{ state }}&ftype={{ ftype }}&sort=doctors&page=1">Doctors <i class="fa-solid fa-sort"></i></a></th>
    <th>Capacity</th>
    <th>Est.</th>
    <th>Contact</th>
    <th><a href="?search={{ search }}&state={{ state }}&ftype={{ ftype }}&sort=rating&page=1">Profile Rating <i class="fa-solid fa-sort"></i></a></th>
  </tr></thead>
  <tbody>
  {% set offset = (page-1)*50 %}
  {% for r in rows %}
  {% set rating = r._rating %}
  {% set full   = (rating | int) %}
  {% set half   = 1 if (rating - full) >= 0.5 else 0 %}
  {% set empty  = 5 - full - half %}
  <tr>
    <td style="color:#bbb">{{ offset + loop.index }}</td>
    <td>
      <strong style="color:#1B5E20">{{ r.name or 'Unknown' }}</strong>
      {% if r.address_line1 %}<div style="color:#999;font-size:.77em;margin-top:2px">{{ r.address_line1 }}</div>{% endif %}
    </td>
    <td>
      {% if r.facilityTypeId %}
        {% set ft = (r.facilityTypeId or '')|lower %}
        {% if 'hospital' in ft or 'medical center' in ft %}
          <span class="badge badge-h"><i class="fa-solid fa-hospital"></i> {{ r.facilityTypeId }}</span>
        {% elif 'clinic' in ft or 'dispensary' in ft %}
          <span class="badge badge-c"><i class="fa-solid fa-stethoscope"></i> {{ r.facilityTypeId }}</span>
        {% else %}
          <span class="badge badge-o">{{ r.facilityTypeId }}</span>
        {% endif %}
      {% else %}<span class="no-data">--</span>{% endif %}
    </td>
    <td>{{ r.address_city or '' }}</td>
    <td>{% if r.district %}<span style="color:#555">{{ r.district }}</span>{% else %}<span class="no-data">--</span>{% endif %}</td>
    <td style="font-size:.82em">{{ r.address_stateOrRegion or '' }}</td>
    <td>
      {% set nd = r.numberDoctors|string %}
      {% if nd and nd not in ('None','') and nd != '0' %}
        <span class="doctor-chip"><i class="fa-solid fa-user-doctor"></i> {{ nd }}</span>
      {% else %}<span class="no-data">--</span>{% endif %}
    </td>
    <td>{% if r.capacity %}<span style="color:#555">{{ r.capacity }}</span>{% else %}<span class="no-data">--</span>{% endif %}</td>
    <td>{% if r.yearEstablished %}<span style="color:#555">{{ r.yearEstablished }}</span>{% else %}<span class="no-data">--</span>{% endif %}</td>
    <td>
      {% if r.officialPhone %}
        <a class="icon-link" href="tel:{{ r.officialPhone }}" title="{{ r.officialPhone }}"><i class="fa-solid fa-phone"></i></a>
      {% endif %}
      {% if r.officialWebsite %}
        <a class="icon-link" href="{{ r.officialWebsite }}" target="_blank" title="Website" style="margin-left:6px"><i class="fa-solid fa-globe"></i></a>
      {% endif %}
      {% if not r.officialPhone and not r.officialWebsite %}<span class="no-data">--</span>{% endif %}
    </td>
    <td>
      <div class="rating-bar">
        <span style="font-weight:700;color:#F57C00;min-width:26px">{{ "%.1f"|format(rating) }}</span>
        <div class="r-bg"><div class="r-fill" style="width:{{ (rating/5*100)|int }}%"></div></div>
        <span style="color:#aaa;font-size:.78em">/5</span>
      </div>
      <div style="margin-top:3px;font-size:.72em;color:#888">
        {% if r.numberDoctors and r.numberDoctors|string not in ('None','0','') %}<i class="fa-solid fa-circle-check" style="color:#4CAF50"></i> doctors&nbsp;{% endif %}
        {% if r.officialPhone %}<i class="fa-solid fa-circle-check" style="color:#4CAF50"></i> phone&nbsp;{% endif %}
        {% if r.officialWebsite %}<i class="fa-solid fa-circle-check" style="color:#4CAF50"></i> web&nbsp;{% endif %}
        {% if r.affiliated_staff_presence|string|lower in ('true','1') %}<i class="fa-solid fa-circle-check" style="color:#4CAF50"></i> staff&nbsp;{% endif %}
        {% set sm = r.distinct_social_media_presence_count|string %}
        {% if sm and sm not in ('None','0','') %}<i class="fa-brands fa-square-instagram" style="color:#C2185B"></i> {{ sm }}&nbsp;{% endif %}
      </div>
    </td>
  </tr>
  {% else %}
  <tr><td colspan="11" style="text-align:center;padding:32px;color:#bbb">
    <i class="fa-solid fa-circle-info" style="font-size:1.4em;display:block;margin-bottom:8px"></i>
    No facilities match your filters.
  </td></tr>
  {% endfor %}
  </tbody>
  </table>
  </div>

  {% if pages > 1 %}
  <div class="pg">
    {% if page > 1 %}<a href="?search={{ search }}&state={{ state }}&ftype={{ ftype }}&sort={{ sort }}&page={{ page-1 }}">&laquo; Prev</a>{% endif %}
    {% for p in range([1, page-3]|max, [pages+1, page+4]|min) %}
      {% if p == page %}<span class="cur">{{ p }}</span>
      {% else %}<a href="?search={{ search }}&state={{ state }}&ftype={{ ftype }}&sort={{ sort }}&page={{ p }}">{{ p }}</a>{% endif %}
    {% endfor %}
    {% if page < pages %}<a href="?search={{ search }}&state={{ state }}&ftype={{ ftype }}&sort={{ sort }}&page={{ page+1 }}">Next &raquo;</a>{% endif %}
  </div>
  {% endif %}

  <div style="text-align:center;color:#bbb;font-size:.78em;margin:10px 0 24px">
    Profile Rating is computed from data completeness signals -- not an independent quality assessment.
    Higher ratings indicate more complete facility records in the dataset.
  </div>
</div>
</body></html>"""

ERR_T = r"""<!DOCTYPE html>
<html><head><title>Error</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css"/>
<style>body{font-family:'Segoe UI',sans-serif;max-width:900px;margin:40px auto;padding:20px}
.err{background:#FFEBEE;border-left:4px solid #D32F2F;padding:20px;border-radius:4px}
pre{background:#f5f5f5;padding:14px;overflow:auto;font-size:.8em;margin-top:12px;border-radius:6px}
a{color:#2E7D32}</style>
</head><body>
<h2><i class="fa-solid fa-triangle-exclamation"></i> Error analysing {{ cap }}</h2>
<div class="err"><p>{{ err }}</p><pre>{{ trace }}</pre></div>
<br><a href="/">← Back</a>
</body></html>"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
