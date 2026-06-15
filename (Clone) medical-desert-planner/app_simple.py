#!/usr/bin/env python
"""Medical Desert Planner - Production · DAIS 2026 Hackathon Track 2

Gap risk = trust-weighted supply coverage × NFHS-5 district demand.
Data: 10K real facilities · NFHS-5 district health indicators · India POST PIN directory.
"""

import os, json, math, time, io, csv
from datetime import datetime
from flask import Flask, render_template_string, request, jsonify, Response
from databricks.sdk import WorkspaceClient

app = Flask(__name__)
w   = WorkspaceClient()

FAC_TABLE  = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities"
NFHS_TABLE = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.nfhs_5_district_health_indicators"
PIN_TABLE  = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.india_post_pincode_directory"

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

_cache: dict = {"fac": None, "nfhs": None, "pin": None, "ts": 0.0}

# ── Data layer ────────────────────────────────────────────────────────────────

def _wh():
    whs = list(w.warehouses.list())
    if not whs: raise RuntimeError("No SQL warehouse available.")
    return whs[0].id

def _query(sql):
    from databricks.sdk.service.sql import StatementState
    TERMINAL = {StatementState.SUCCEEDED, StatementState.FAILED,
                StatementState.CANCELED, StatementState.CLOSED}
    r = w.statement_execution.execute_statement(
        warehouse_id=_wh(), statement=sql, wait_timeout="50s")
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

def load_data():
    if _cache["fac"] is not None and time.time() - _cache["ts"] < 3600:
        return

    # Truncate large JSON text fields so the inline result stays under 25MB.
    # capability/procedure/equipment can be 5KB+ per row; 500/250/100 chars is
    # enough for keyword search. specialties stays as raw JSON for text matching.
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
               prev_diarrhoea_2wk_child_u5_pct,
               births_delivered_by_csection_5y_pct,
               births_attended_by_skilled_hp_5y_10_pct
        FROM {NFHS_TABLE}
    """)

    pin_rows = _query(f"""
        SELECT CAST(pincode AS STRING) AS pincode, district, statename
        FROM {PIN_TABLE}
        GROUP BY pincode, district, statename
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

    _cache["fac"]  = fac
    _cache["nfhs"] = {"dist": by_dist, "state": by_state}
    _cache["pin"]  = pin_idx
    _cache["ts"]   = time.time()

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

def get_nfhs(row):
    pin  = str(row.get("address_zipOrPostcode") or "").strip()
    info = _cache["pin"].get(pin)
    if info:
        d = info["district"].lower(); s = info["state"].lower()
        rec = _cache["nfhs"]["dist"].get((d, s))
        if rec: return rec, info["district"]
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
        info = _cache["pin"].get(pin) if _cache["pin"] else None
        return f"{info['district']}, {info['state']}" if info else f"Unknown District ({state})"
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

# ── Scenario persistence ──────────────────────────────────────────────────────

def _sc_path(): return "/tmp/scenarios.json"

def load_scenarios():
    try:
        if os.path.exists(_sc_path()): return json.load(open(_sc_path()))
    except: pass
    return []

def save_scenario(sc):
    scs = load_scenarios()
    sc["id"]=""; sc["saved_at"]=datetime.now().strftime("%Y-%m-%d %H:%M")
    scs.append(sc)
    json.dump(scs, open(_sc_path(),"w"), indent=2)
    return sc["saved_at"]

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
    geo     = p.get("geo_level","state")
    ftype   = p.get("facility_type","all")
    ref_lat = p.get("ref_lat","").strip()
    ref_lon = p.get("ref_lon","").strip()
    ref_city= p.get("ref_city","").strip()
    try:
        load_data()
        rows = list(_cache["fac"])
        if ftype=="hospital":
            rows=[r for r in rows if (r.get("facilityTypeId") or "").lower() in
                  ("hospital","medical center","health center")]
        elif ftype=="clinic":
            rows=[r for r in rows if (r.get("facilityTypeId") or "").lower() in
                  ("clinic","dispensary","health post")]

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
                        "lats":[],"lons":[],"nfhs":None,"district":None}
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
            results.append({
                "region":region,"n":n,"coverage":cov,
                "priority":pri,"color":COLORS[pri],"note":note,"risk":risk,
                "strong":g["strong"],"partial":g["partial"],"weak":g["weak"],"none":g["none"],
                "lat":clat,"lon":clon,
                "need":need,"dlabel":dlabel,"dunit":dunit,"ddesc":ddesc,
                "district":g["district"],
                "confidence":conf,"moe":moe,"dq_label":dq_label,"dq_color":dq_color,
                "items":sorted(g["items"],key=lambda x:-x["score"]),
            })

        def _sort(r):
            return (PRIORITY_ORDER.index(r["priority"]), -(r["risk"] or 0))
        results.sort(key=_sort)

        geo_json=json.dumps([{k:v for k,v in r.items() if k!="items"} for r in results])
        return render_template_string(RESULTS_T,
            cap=cap,geo=geo,ftype=ftype,results=results,geo_json=geo_json,
            total=len(scored),ref_city=ref_city,has_dist=bool(ref_lat and ref_lon),
            capabilities=CAPABILITIES)

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
  <p>Identify where healthcare gaps are highest-risk across India — scored against real facility data and NFHS-5 district health indicators</p>
  <div class="pills">
    <span class="pill"><i class="fa-solid fa-chart-bar"></i> {{ facility_count }} real facilities</span>
    <span class="pill"><i class="fa-solid fa-clipboard-list"></i> NFHS-5 district demand data</span>
    <span class="pill"><i class="fa-solid fa-map-pin"></i> India POST PIN directory</span>
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
        <label>Reference location <span style="font-weight:400;color:#bbb;text-transform:none">(optional — for distance context)</span></label>
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
      <span><b style="color:#9E9E9E">Sparse</b> = few records — treat results with caution</span>
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
</body></html>"""

RESULTS_T = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ cap }} Gap Analysis</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css"/>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
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
.body{max-width:1400px;margin:0 auto;padding:0 16px 24px;display:grid;grid-template-columns:1fr 420px;gap:16px}
.card{background:#fff;border-radius:10px;box-shadow:0 1px 8px rgba(0,0,0,.07);padding:18px;margin-bottom:14px}
.card h2{color:#1B5E20;margin-bottom:12px;font-size:1em;display:flex;align-items:center;gap:6px}
#map{height:440px;border-radius:8px}
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
@media(max-width:900px){.body{grid-template-columns:1fr}.sticky-col{position:static}}
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
</style>
</head>
<body>
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
    {{ crit_list|length }} Critical Care Gap{{ 's' if crit_list|length != 1 }} Identified — immediate attention required
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

<div class="body">
  <!-- LEFT: table + drill-down -->
  <div>
    <div class="card">
      <h2><i class="fa-solid fa-table-list"></i> Regional Coverage — click any row to inspect facilities</h2>
      <table>
        <thead>
          <tr>
            <th title="Rank by urgency">#</th>
            <th>Region</th>
            <th title="Number of facilities analysed">Facilities</th>
            <th title="Strong / Partial / Weak / No evidence">Evidence mix</th>
            <th title="Trust-weighted coverage score">Coverage</th>
            {% if results and results[0].dlabel %}<th title="{{ results[0].ddesc }}">{{ results[0].dlabel }}</th>{% endif %}
            <th title="Gap risk = (1 - coverage) x demand need">Gap risk</th>
            <th title="How confident we are the gap is real">Confidence</th>
            <th title="Based on sample size, evidence strength and data source">Data quality</th>
            <th>Priority</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
        {% for r in results %}
        <tr class="dr" data-pri="{{ r.priority }}" data-idx="{{ loop.index }}"
            onclick="toggleDrill({{ loop.index }},{{ r.region|tojson }})">
          <td><span class="rank">{{ loop.index }}</span></td>
          <td>
            <strong>{{ r.region }}</strong>
            {% if r.district %}<div style="font-size:.74em;color:#888;margin-top:2px"><i class="fa-solid fa-location-dot"></i> {{ r.district }} district</div>{% endif %}
            <div class="note">{{ r.note }}</div>
          </td>
          <td style="text-align:center"><strong>{{ r.n }}</strong></td>
          <td>
            <div class="bar-wrap" title="Strong:{{ r.strong }} Partial:{{ r.partial }} Weak:{{ r.weak }} None:{{ r.none }}">
              {% if r.n %}
              <div class="bar-seg" style="background:#2E7D32;width:{{ (r.strong/r.n*100)|int }}%"></div>
              <div class="bar-seg" style="background:#F57C00;width:{{ (r.partial/r.n*100)|int }}%"></div>
              <div class="bar-seg" style="background:#FBC02D;width:{{ (r.weak/r.n*100)|int }}%"></div>
              <div class="bar-seg" style="background:#ddd;width:{{ (r.none/r.n*100)|int }}%"></div>
              {% endif %}
            </div>
            <div style="font-size:.7em;color:#999;margin-top:2px">
              <span style="color:#2E7D32;font-weight:700">{{ r.strong }} strong</span> ·
              <span style="color:#F57C00">{{ r.partial }} partial</span> ·
              <span style="color:#FBC02D">{{ r.weak }} weak</span> ·
              <span style="color:#aaa">{{ r.none }} none</span>
            </div>
          </td>
          <td>
            <strong style="font-size:1.05em">{{ r.coverage }}%</strong>
            <div style="font-size:.7em;color:#aaa">covered</div>
          </td>
          {% if results[0].dlabel %}
          <td class="demand-cell">
            {% if r.need is not none %}
              <span class="demand-val" style="color:{{ '#D32F2F' if r.need > 60 else ('#F57C00' if r.need > 40 else '#388E3C') }};font-size:1em">
                {{ r.need }}%
              </span>
              <div style="font-size:.7em;color:#aaa">need index</div>
            {% else %}<span style="color:#bbb">-</span>{% endif %}
          </td>
          {% endif %}
          <td>
            {% if r.risk is not none %}
              <span class="risk-badge"
                style="background:{{ '#D32F2F' if r.risk>50 else ('#F57C00' if r.risk>25 else '#FBC02D') }};color:#fff;font-size:.85em">
                {{ r.risk }}
              </span>
              <div style="font-size:.7em;color:#aaa;margin-top:2px">risk score</div>
            {% else %}<span style="color:#bbb">-</span>{% endif %}
          </td>
          <td>
            <div class="conf-wrap">
              <div class="conf-row">
                <div class="conf-track">
                  <div class="conf-fill" style="width:{{ r.confidence }}%;background:{{ '#2E7D32' if r.confidence>=70 else ('#F57C00' if r.confidence>=40 else '#D32F2F') }}"></div>
                </div>
                <span class="conf-pct" style="color:{{ '#2E7D32' if r.confidence>=70 else ('#F57C00' if r.confidence>=40 else '#D32F2F') }}">{{ r.confidence }}%</span>
              </div>
              <div class="conf-moe">±{{ r.moe }}% margin of error</div>
            </div>
          </td>
          <td><span class="dq-badge" style="background:{{ r.dq_color }}">{{ r.dq_label }}</span></td>
          <td><span class="badge" style="background:{{ r.color }}">{{ r.priority }}</span></td>
          <td><button class="drill-btn" onclick="event.stopPropagation();toggleDrill({{ loop.index }},{{ r.region|tojson }})"><i class="fa-solid fa-chevron-down"></i></button></td>
        </tr>
        <tr id="dr-{{ loop.index }}" class="drill-row">
          <td colspan="11" style="padding:0">
            <div class="drill-panel" id="dp-{{ loop.index }}">Loading…</div>
          </td>
        </tr>
        {% endfor %}
        <tr class="no-rows-msg" id="no-rows"><td colspan="11">No regions match this filter.</td></tr>
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
      <a href="/export_csv?capability={{ cap }}&geo_level={{ geo }}&facility_type={{ ftype }}"><i class="fa-solid fa-download"></i> Download CSV</a>
    </div>
  </div>

  <!-- RIGHT: sticky map -->
  <div class="sticky-col" style="position:sticky;top:16px;height:fit-content">
    <div class="card">
      <h2><i class="fa-solid fa-map"></i> Gap Map</h2>
      <div id="map"></div>
      <div style="margin-top:10px;display:flex;flex-wrap:wrap;gap:8px;font-size:.78em;color:#555">
        <span><i class="fa-solid fa-circle" style="color:#D32F2F"></i> Critical</span>
        <span><i class="fa-solid fa-circle" style="color:#F57C00"></i> High</span>
        <span><i class="fa-solid fa-circle" style="color:#FBC02D"></i> Moderate</span>
        <span><i class="fa-solid fa-circle" style="color:#388E3C"></i> Adequate</span>
        <span><i class="fa-solid fa-circle" style="color:#9E9E9E"></i> Data-poor</span>
      </div>
    </div>
  </div>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
var map = L.map('map').setView([22.5,79.0],4);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{
  attribution:'© OpenStreetMap',maxZoom:18}).addTo(map);

var GEO   = {{ geo_json|safe }};
var CAP   = {{ cap|tojson }};
var GEO_L = {{ geo|tojson }};
var FTYPE = {{ ftype|tojson }};

var rLayers={}, facLayer=L.layerGroup().addTo(map);
var CONF_C={STRONG:'#2E7D32',PARTIAL:'#F57C00',WEAK:'#FBC02D','NO CLAIM':'#9E9E9E'};

GEO.forEach(function(r){
  if(!r.lat||!r.lon) return;
  var radius = Math.max(8, Math.min(30, 8+Math.sqrt(r.n)*1.8));
  var m = L.circleMarker([r.lat,r.lon],{
    radius:radius,fillColor:r.color,color:'#fff',weight:2,opacity:1,fillOpacity:.82
  }).bindPopup(
    '<b>'+r.region+'</b><br>'+
    '<span style="color:'+r.color+'"><b>'+r.priority+'</b></span><br>'+
    'Coverage: <b>'+r.coverage+'%</b><br>'+
    'Facilities: <b>'+r.n+'</b>'+
    (r.need!==null ? '<br>'+r.dlabel+': <b>'+r.need+'%</b>' : '')+
    (r.risk!==null ? '<br>Gap risk: <b>'+r.risk+'</b>' : '')+
    '<br>Confidence: <b>'+r.confidence+'%</b> <span style="color:#888">(±'+r.moe+'%)</span>'+
    '<br><small style="color:#888">'+r.note+'</small>'
  ).addTo(map);
  rLayers[r.region]=m;
});

var drillLoaded={};

function filterRows(prio, el){
  document.querySelectorAll('.filter-tab').forEach(function(t){t.classList.remove('active');});
  el.classList.add('active');
  var anyVisible=false;
  document.querySelectorAll('tr.dr').forEach(function(tr){
    var show = prio==='ALL' || tr.dataset.pri===prio;
    tr.style.display = show ? '' : 'none';
    if(show) anyVisible=true;
    var idx=tr.dataset.idx;
    var drRow=document.getElementById('dr-'+idx);
    if(drRow && !show) drRow.style.display='none';
  });
  var noMsg=document.getElementById('no-rows');
  if(noMsg) noMsg.style.display = anyVisible ? 'none' : '';
}

function toggleDrill(idx, region){
  var row=document.getElementById('dr-'+idx);
  var panel=document.getElementById('dp-'+idx);
  if(row.style.display===''){
    row.style.display='none'; return;
  }
  row.style.display='';
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

function focusRegion(region){
  var m=rLayers[region];
  if(m){map.flyTo(m.getLatLng(),7,{duration:0.8}); m.openPopup();}
}

function renderDrill(idx,region,data,panel){
  facLayer.clearLayers();
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
    var cc=CONF_C[f.level]||'#9E9E9E';
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
    if(f.lat&&f.lon){
      var mk=L.circleMarker([f.lat,f.lon],{
        radius:6,fillColor:cc,color:'#fff',weight:1.5,opacity:1,fillOpacity:.9
      }).bindPopup('<b>'+f.name+'</b><br>'+f.level+'<br>Trust score: '+f.score+'/12');
      facLayer.addLayer(mk); bounds.push([f.lat,f.lon]);
    }
  });
  panel.innerHTML=html;
  if(bounds.length>1) map.fitBounds(bounds,{padding:[20,20],maxZoom:10});
  else if(bounds.length===1) map.flyTo(bounds[0],10);
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
</script>
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
