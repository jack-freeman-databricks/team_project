#!/usr/bin/env python3
"""Generate reference/dimension seed data for the Ironbark iron ore plant demo.

Emits four CSVs that seed the Unity Catalog `ironbark.ref` schema:

    dim_tag.csv             one row per SCADA tag (the tag register)
    dim_flowsheet_node.csv  unit operations that take part in the mass balance
    dim_flowsheet_arc.csv   material streams between nodes + the tag measuring each
    dim_rule.csv            data-driven alarm rules for the streaming rule engine

The tag register is the single source of truth for three things at once:
  - the synthetic generator's operating envelope (nominal + limits per tag)
  - the streaming rule engine's thresholds (joined in as a broadcast static side)
  - the app's tag metadata (units, asset hierarchy, display grouping)

Run:  python3 contract/gen_reference_data.py
"""

import csv
import os

OUT_DIR = os.path.dirname(os.path.abspath(__file__))

# --------------------------------------------------------------------------
# Plant design basis: 2,500 t/h ROM feed, ~2,100 t/h product to stockpile,
# ~400 t/h fines rejected via the desands circuit. Tertiary crusher runs a
# 250 t/h recirculating load back to the secondary screen, which is what makes
# the mass balance non-trivial (and worth visualising).
# --------------------------------------------------------------------------
ROM_TPH = 2500.0

# Solids specific gravity for hematite, t/m3. Required to derive dry solids
# tonnage from a slurry flow + density pair in the desands circuit:
#   Cw     = SG * (rho - 1) / (rho * (SG - 1))        solids mass fraction
#   t/h    = Q_m3h * rho * Cw
# Both the analytics layer and the app must use this same value or the wet and
# dry sides of the mass balance will not reconcile.
SOLIDS_SG = 4.9

AREAS = {
    "ROM": "ROM & Reclaim",
    "CRU": "Crushing",
    "SCR": "Screening",
    "DSD": "Desands",
    "STK": "Stacking & Stockpiles",
    "UTL": "Utilities",
    "LAB": "Laboratory",
}

# instrument_type -> (measure, unit, value_class)
INSTRUMENTS = {
    "WI": ("mass_flow",       "t/h",  "analog"),
    "SI": ("belt_speed",      "m/s",  "analog"),
    "RI": ("rotational_speed", "rpm", "analog"),
    "LI": ("level",           "pct",  "analog"),
    "PI": ("pressure",        "kPa",  "analog"),
    "II": ("motor_current",   "A",    "analog"),
    "TI": ("temperature",     "degC", "analog"),
    "VI": ("vibration",       "mm/s", "analog"),
    "FI": ("volume_flow",     "m3/h", "analog"),
    "DI": ("density",         "t/m3", "analog"),
    "QI": ("assay",           "pct",  "analog"),
    "MI": ("moisture",        "pct",  "analog"),
    "XI": ("position",        "deg",  "analog"),
    "GI": ("gap",             "mm",   "analog"),
    "ZI": ("state",           "enum", "discrete"),
}

tags = []
assets = {}


def asset(asset_id, name, asset_type, area):
    assets[asset_id] = (name, asset_type, area)
    return asset_id


def tag(asset_id, inst, seq, nominal, lolo, lo, hi, hihi,
        measure=None, unit=None, sample_hz=1.0, mass_balance=False,
        pdm=False, enum_values="", desc=""):
    """Append one tag to the register.

    Limits are the *engineering* alarm limits: lolo/hihi are trip-level,
    lo/hi are warning-level. The synthetic generator centres on `nominal`
    and must stay inside lo..hi except when deliberately injecting a fault.
    """
    m, u, vclass = INSTRUMENTS[inst]
    tags.append({
        "tag_id": f"{asset_id}-{inst}{seq:02d}",
        "asset_id": asset_id,
        "asset_name": assets[asset_id][0],
        "asset_type": assets[asset_id][1],
        "area_code": assets[asset_id][2],
        "area_name": AREAS[assets[asset_id][2]],
        "measure": measure or m,
        "instrument_type": inst,
        "unit": unit or u,
        "value_class": vclass,
        "sample_hz": sample_hz,
        "nominal": nominal,
        "lo_lo": lolo,
        "lo": lo,
        "hi": hi,
        "hi_hi": hihi,
        "is_mass_balance": mass_balance,
        "is_pdm": pdm,
        "enum_values": enum_values,
        "description": desc,
    })


RUN_STATES = "RUNNING|STOPPED|FAULT"


def motor_tags(aid, amps, with_run=True):
    tag(aid, "II", 1, amps, amps * 0.15, amps * 0.55, amps * 1.15, amps * 1.30,
        pdm=True, desc="Motor current")
    if with_run:
        tag(aid, "ZI", 1, 1, 0, 0, 1, 1, enum_values=RUN_STATES,
            desc="Run status")


def crusher_tags(aid, amps, press_kpa, css_mm, rpm):
    motor_tags(aid, amps)
    tag(aid, "PI", 1, press_kpa, press_kpa * 0.5, press_kpa * 0.7,
        press_kpa * 1.20, press_kpa * 1.35, desc="Hydraulic / crushing pressure")
    tag(aid, "GI", 1, css_mm, css_mm - 15, css_mm - 8, css_mm + 8, css_mm + 15,
        desc="Closed side setting")
    tag(aid, "RI", 1, rpm, rpm * 0.8, rpm * 0.92, rpm * 1.08, rpm * 1.15,
        desc="Eccentric / mantle speed")
    tag(aid, "TI", 1, 62.0, 10.0, 30.0, 80.0, 92.0, pdm=True,
        desc="Main bearing temperature")
    tag(aid, "TI", 2, 55.0, 10.0, 25.0, 72.0, 85.0, desc="Lube oil temperature")
    tag(aid, "VI", 1, 4.5, 0.0, 0.5, 7.1, 11.0, sample_hz=10.0, pdm=True,
        desc="Bearing vibration RMS, drive end (ISO 10816)")
    tag(aid, "VI", 2, 3.8, 0.0, 0.5, 7.1, 11.0, sample_hz=10.0, pdm=True,
        desc="Bearing vibration RMS, non-drive end (ISO 10816)")


def screen_tags(aid, amps, rpm, stroke_nominal=6.0):
    motor_tags(aid, amps)
    tag(aid, "RI", 1, rpm, rpm * 0.8, rpm * 0.93, rpm * 1.07, rpm * 1.15,
        desc="Screen exciter speed")
    tag(aid, "TI", 1, 58.0, 10.0, 28.0, 78.0, 90.0, pdm=True,
        desc="Exciter bearing temperature")
    tag(aid, "VI", 1, stroke_nominal, 0.0, 0.5, 11.0, 18.0, sample_hz=10.0,
        pdm=True, desc="Side plate vibration RMS, feed end (ISO 10816)")
    tag(aid, "VI", 2, stroke_nominal - 0.6, 0.0, 0.5, 11.0, 18.0,
        sample_hz=10.0, pdm=True,
        desc="Side plate vibration RMS, discharge end (ISO 10816)")


def conveyor_tags(aid, tph, amps, speed_ms, mass_balance=True, vib=False):
    tag(aid, "WI", 1, tph, 0.0, tph * 0.55, tph * 1.12, tph * 1.25,
        mass_balance=mass_balance, desc="Belt weightometer")
    tag(aid, "SI", 1, speed_ms, speed_ms * 0.5, speed_ms * 0.9,
        speed_ms * 1.05, speed_ms * 1.12, desc="Belt speed")
    motor_tags(aid, amps)
    if vib:
        tag(aid, "VI", 1, 2.8, 0.0, 0.3, 4.5, 7.1, sample_hz=10.0, pdm=True,
            desc="Head pulley bearing vibration RMS")


# --------------------------------------------------------------------------
# ROM & reclaim
# --------------------------------------------------------------------------
a = asset("ROM-BIN01", "ROM bin", "bin", "ROM")
tag(a, "LI", 1, 68.0, 5.0, 20.0, 92.0, 98.0, desc="ROM bin level")

a = asset("ROM-APF01", "ROM apron feeder", "feeder", "ROM")
tag(a, "WI", 1, ROM_TPH, 0.0, ROM_TPH * 0.55, ROM_TPH * 1.12, ROM_TPH * 1.25,
    mass_balance=True, desc="Apron feeder mass flow (plant feed)")
tag(a, "SI", 1, 0.22, 0.05, 0.10, 0.30, 0.35, unit="m/s",
    desc="Apron feeder speed")
motor_tags(a, 310.0)

a = asset("LAB-ROM01", "ROM feed sampler", "sampler", "LAB")
tag(a, "QI", 1, 61.2, 55.0, 58.5, 64.0, 66.0, sample_hz=0.00056,
    desc="ROM feed Fe grade (hourly composite)")
tag(a, "QI", 2, 4.1, 1.0, 2.0, 6.5, 8.5, sample_hz=0.00056,
    desc="ROM feed SiO2")
tag(a, "QI", 3, 2.4, 0.5, 1.0, 4.0, 5.5, sample_hz=0.00056,
    desc="ROM feed Al2O3")
tag(a, "MI", 1, 3.8, 0.5, 1.5, 6.5, 9.0, desc="ROM feed moisture")

# --------------------------------------------------------------------------
# Crushing
# --------------------------------------------------------------------------
a = asset("CRU-PCR01", "Primary gyratory crusher", "crusher", "CRU")
crusher_tags(a, 1450.0, 9800.0, 150.0, 155.0)

a = asset("CRU-SGB01", "Crushed ore surge bin", "bin", "CRU")
tag(a, "LI", 1, 55.0, 5.0, 15.0, 90.0, 97.0, desc="Surge bin level")

a = asset("CRU-SCC01", "Secondary cone crusher", "crusher", "CRU")
crusher_tags(a, 780.0, 7200.0, 45.0, 240.0)

a = asset("CRU-TCC01", "Tertiary cone crusher", "crusher", "CRU")
crusher_tags(a, 520.0, 6400.0, 22.0, 290.0)

# --------------------------------------------------------------------------
# Screening
# --------------------------------------------------------------------------
a = asset("SCR-SCN01", "Scalping screen", "screen", "SCR")
screen_tags(a, 240.0, 850.0, stroke_nominal=7.2)

a = asset("SCR-SCN02", "Secondary sizing screen", "screen", "SCR")
screen_tags(a, 195.0, 900.0, stroke_nominal=6.8)

# --------------------------------------------------------------------------
# Conveyors. Nominal t/h values are the design mass balance (see arcs below).
# --------------------------------------------------------------------------
CONVEYORS = [
    # asset_id, name, area, design t/h, motor A, belt m/s, has vibration tags
    ("CV-CV001", "Primary crusher discharge conveyor",      "CRU", 2500.0, 420.0, 3.5, True),
    ("CV-CV002", "Surge bin reclaim conveyor",              "CRU", 2500.0, 400.0, 3.5, False),
    ("CV-CV003", "Scalping screen oversize conveyor",       "SCR",  900.0, 185.0, 2.8, False),
    ("CV-CV004", "Scalping screen undersize conveyor",      "SCR", 1600.0, 275.0, 3.2, False),
    ("CV-CV005", "Secondary crusher discharge conveyor",    "CRU",  900.0, 185.0, 3.0, False),
    ("CV-CV006", "Secondary screen oversize conveyor",      "SCR",  250.0,  95.0, 2.4, False),
    ("CV-CV007", "Secondary screen undersize conveyor",     "SCR",  900.0, 180.0, 2.8, False),
    ("CV-CV008", "Tertiary crusher recirculation conveyor", "CRU",  250.0,  98.0, 2.4, False),
    ("CV-CV009", "Dewatering screen product conveyor",      "DSD", 2100.0, 355.0, 3.4, True),
    ("CV-CV010", "Product stockpile feed conveyor",          "STK", 2100.0, 350.0, 3.6, True),
]
for cid, cname, carea, tph, amps, spd, vib in CONVEYORS:
    a = asset(cid, cname, "conveyor", carea)
    conveyor_tags(a, tph, amps, spd, vib=vib)

# --------------------------------------------------------------------------
# Desands circuit (wet). Solids tonnage is derived from slurry flow x density,
# which is deliberately different from a belt weightometer -- it forces the
# mass balance to reconcile two different measurement methods.
# --------------------------------------------------------------------------
a = asset("DSD-CYC01", "Desands cyclone cluster", "cyclone", "DSD")
tag(a, "PI", 1, 165.0, 60.0, 110.0, 210.0, 240.0, desc="Cyclone feed pressure")
# Flow/density pairs below are solved so that derived solids tonnage matches the
# dry-side design balance: 2500 t/h feed -> 400 t/h overflow + 2100 t/h underflow.
tag(a, "FI", 1, 3735.0, 900.0, 2600.0, 4400.0, 5000.0, mass_balance=True,
    desc="Cyclone feed slurry flow")
tag(a, "DI", 1, 1.533, 1.05, 1.28, 1.72, 1.85, mass_balance=True,
    desc="Cyclone feed slurry density")
tag(a, "FI", 2, 1769.0, 300.0, 1100.0, 2300.0, 2700.0, mass_balance=True,
    desc="Cyclone overflow (fines reject) slurry flow")
tag(a, "DI", 2, 1.18, 1.02, 1.08, 1.32, 1.42, mass_balance=True,
    desc="Cyclone overflow slurry density")
tag(a, "FI", 3, 1966.0, 500.0, 1350.0, 2500.0, 2900.0, mass_balance=True,
    desc="Cyclone underflow slurry flow")
tag(a, "DI", 3, 1.85, 1.30, 1.62, 2.02, 2.15, mass_balance=True,
    desc="Cyclone underflow slurry density")

a = asset("DSD-DWS01", "Dewatering screen", "screen", "DSD")
screen_tags(a, 165.0, 950.0, stroke_nominal=6.4)
tag(a, "MI", 1, 8.2, 2.0, 5.0, 11.0, 13.5, desc="Product moisture")

a = asset("DSD-THK01", "Fines thickener", "thickener", "DSD")
tag(a, "LI", 1, 62.0, 10.0, 25.0, 88.0, 95.0, desc="Thickener bed level")
tag(a, "DI", 1, 1.42, 1.05, 1.20, 1.65, 1.80, desc="Thickener underflow density")
motor_tags(a, 85.0)

a = asset("UTL-WTR01", "Process water supply", "utility", "UTL")
tag(a, "FI", 1, 3225.0, 600.0, 2300.0, 4000.0, 4600.0, mass_balance=True,
    desc="Process water addition to desands")
tag(a, "PI", 1, 480.0, 200.0, 350.0, 600.0, 680.0,
    desc="Process water header pressure")

# --------------------------------------------------------------------------
# Stacking & stockpiles
# --------------------------------------------------------------------------
a = asset("STK-STK01", "Product stockpile stacker", "stacker", "STK")
tag(a, "WI", 1, 2100.0, 0.0, 1150.0, 2350.0, 2600.0, mass_balance=True,
    desc="Stacker boom weightometer (tonnes to stockpile)")
tag(a, "XI", 1, 0.0, -95.0, -90.0, 90.0, 95.0, desc="Stacker slew angle")
tag(a, "XI", 2, 12.0, -2.0, 0.0, 24.0, 28.0, desc="Stacker luff angle")
motor_tags(a, 145.0)
tag(a, "ZI", 2, 1, 0, 0, 3, 3, enum_values="IDLE|SP01|SP02|SP03",
    desc="Stockpile stacking indicator (which stockpile is being stacked to)")

a = asset("LAB-PRD01", "Product sampler", "sampler", "LAB")
tag(a, "QI", 1, 62.4, 57.0, 60.0, 65.0, 67.0, sample_hz=0.00056,
    desc="Product Fe grade (hourly composite)")
tag(a, "QI", 2, 3.2, 0.5, 1.5, 5.0, 6.5, sample_hz=0.00056,
    desc="Product SiO2")
tag(a, "QI", 3, 1.9, 0.3, 0.8, 3.2, 4.5, sample_hz=0.00056,
    desc="Product Al2O3")

# --------------------------------------------------------------------------
# Flowsheet topology: nodes (unit operations) and arcs (material streams).
# The app reads these to draw the mass balance diagram -- nothing is
# hardcoded in the UI. `accumulation_tag_id` + `capacity_t` let bins
# contribute a genuine accumulation term to the balance.
# --------------------------------------------------------------------------
NODES = [
    # node_id, name, type, area, balance_role, accumulation_tag_id, capacity_t, x, y
    # balance_role: source and sink nodes are never expected to close;
    # only `unit` nodes get an imbalance calculated against them.
    ("N01", "ROM bin",                   "bin",       "ROM", "source", "ROM-BIN01-LI01", 1200.0,  0, 2),
    ("N02", "ROM apron feeder",          "feeder",    "ROM", "unit",   "",                  0.0,  1, 2),
    ("N03", "Primary gyratory crusher",  "crusher",   "CRU", "unit",   "",                  0.0,  2, 2),
    ("N04", "Crushed ore surge bin",     "bin",       "CRU", "unit",   "CRU-SGB01-LI01",  850.0,  3, 2),
    ("N05", "Scalping screen",           "screen",    "SCR", "unit",   "",                  0.0,  4, 2),
    ("N06", "Secondary cone crusher",    "crusher",   "CRU", "unit",   "",                  0.0,  5, 1),
    ("N07", "Secondary sizing screen",   "screen",    "SCR", "unit",   "",                  0.0,  6, 1),
    ("N08", "Tertiary cone crusher",     "crusher",   "CRU", "unit",   "",                  0.0,  7, 0),
    ("N09", "Desands cyclone cluster",   "cyclone",   "DSD", "unit",   "",                  0.0,  7, 3),
    ("N10", "Dewatering screen",         "screen",    "DSD", "unit",   "",                  0.0,  8, 3),
    ("N11", "Product stockpile stacker", "stacker",   "STK", "unit",   "",                  0.0,  9, 3),
    ("N12", "Fines thickener",           "thickener", "DSD", "sink",   "DSD-THK01-LI01", 2400.0,  8, 4),
    ("N13", "Product stockpiles",        "stockpile", "STK", "sink",   "",                  0.0, 10, 3),
]

ARCS = [
    # arc_id, from_node, to_node, name, design_tph, measure_tag_id,
    #   measure_method, stream_type
    ("A01", "N01", "N02", "ROM bin drawdown",           2500.0, "ROM-APF01-WI01", "weightometer", "dry_solids"),
    ("A02", "N02", "N03", "Primary crusher feed",       2500.0, "",               "inferred",     "dry_solids"),
    ("A03", "N03", "N04", "Primary crusher discharge",  2500.0, "CV-CV001-WI01",  "weightometer", "dry_solids"),
    ("A04", "N04", "N05", "Surge bin reclaim",          2500.0, "CV-CV002-WI01",  "weightometer", "dry_solids"),
    ("A05", "N05", "N06", "Scalping oversize",           900.0, "CV-CV003-WI01",  "weightometer", "dry_solids"),
    ("A06", "N05", "N09", "Scalping undersize to desands", 1600.0, "CV-CV004-WI01", "weightometer", "dry_solids"),
    ("A07", "N06", "N07", "Secondary crusher discharge",  900.0, "CV-CV005-WI01",  "weightometer", "dry_solids"),
    ("A08", "N07", "N08", "Secondary screen oversize",    250.0, "CV-CV006-WI01",  "weightometer", "dry_solids"),
    ("A09", "N07", "N09", "Secondary screen undersize to desands", 900.0, "CV-CV007-WI01", "weightometer", "dry_solids"),
    ("A10", "N08", "N07", "Tertiary recirculating load",  250.0, "CV-CV008-WI01",  "weightometer", "dry_solids"),
    ("A11", "N09", "N12", "Cyclone overflow (fines reject)", 400.0, "DSD-CYC01-FI02", "slurry_flow_density", "slurry"),
    ("A12", "N09", "N10", "Cyclone underflow",           2100.0, "DSD-CYC01-FI03", "slurry_flow_density", "slurry"),
    ("A13", "N10", "N11", "Dewatered product",           2100.0, "CV-CV009-WI01",  "weightometer", "dry_solids"),
    ("A14", "N11", "N13", "Stacked product",             2100.0, "STK-STK01-WI01", "weightometer", "dry_solids"),
    ("A15", "N09", "N09", "Process water addition",         0.0, "UTL-WTR01-FI01", "volume_flow",  "water"),
]

# --------------------------------------------------------------------------
# Alarm rules. Kept as data so the streaming rule engine is a broadcast
# stream-static join rather than hardcoded predicates -- and so the three of
# us can add rules without touching pipeline code.
# --------------------------------------------------------------------------
RULES = [
    # rule_id, rule_name, scope_type, scope_value, rule_type, severity,
    #   window_seconds, message_template
    ("R01", "Trip limit exceeded",        "all",         "",          "limit_trip",     "CRITICAL", 0,
     "{tag_id} on {asset_name} at {value} {unit} breached trip limit"),
    ("R02", "Warning limit exceeded",     "all",         "",          "limit_warn",     "WARNING",  0,
     "{tag_id} on {asset_name} at {value} {unit} outside normal range"),
    ("R03", "Vibration ISO zone C/D",     "measure",     "vibration", "limit_warn",     "HIGH",     0,
     "{asset_name} vibration {value} mm/s in ISO 10816 zone C or worse"),
    ("R04", "Bearing temperature rising", "measure",     "temperature", "rate_of_change", "HIGH",   600,
     "{asset_name} bearing temperature rising {value} degC over 10 min"),
    ("R05", "Weightometer stale",         "measure",     "mass_flow", "stale",          "WARNING",  120,
     "{tag_id} has not updated for {value} seconds"),
    ("R06", "Belt running empty",         "asset_type",  "conveyor",  "deviation",      "WARNING",  60,
     "{asset_name} running with mass flow at {value} t/h"),
    ("R07", "Crusher choke condition",    "asset_type",  "crusher",   "limit_trip",     "CRITICAL", 0,
     "{asset_name} crushing pressure {value} kPa indicates choke"),
    ("R08", "Bin high level",             "asset_type",  "bin",       "limit_warn",     "HIGH",     0,
     "{asset_name} level {value}% approaching overflow"),
    ("R09", "Mass balance not closing",   "node",        "",          "imbalance",      "HIGH",     300,
     "Node {node_id} imbalance {value}% over 5 min"),
    ("R10", "Bad instrument quality",     "all",         "",          "quality",        "WARNING",  0,
     "{tag_id} reporting quality {value}"),
]


def write_csv(name, rows, fieldnames):
    path = os.path.join(OUT_DIR, name)
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"{path}  ({len(rows)} rows)")


write_csv("dim_tag.csv", tags, list(tags[0].keys()))

write_csv(
    "dim_flowsheet_node.csv",
    [dict(zip(("node_id", "node_name", "node_type", "area_code", "balance_role",
               "accumulation_tag_id", "capacity_t", "layout_x", "layout_y"), n))
     for n in NODES],
    ["node_id", "node_name", "node_type", "area_code", "balance_role",
     "accumulation_tag_id", "capacity_t", "layout_x", "layout_y"],
)

write_csv(
    "dim_flowsheet_arc.csv",
    [dict(zip(("arc_id", "from_node_id", "to_node_id", "arc_name", "design_tph",
               "measure_tag_id", "measure_method", "stream_type"), r))
     for r in ARCS],
    ["arc_id", "from_node_id", "to_node_id", "arc_name", "design_tph",
     "measure_tag_id", "measure_method", "stream_type"],
)

write_csv(
    "dim_rule.csv",
    [dict(zip(("rule_id", "rule_name", "scope_type", "scope_value", "rule_type",
               "severity", "window_seconds", "message_template"), r))
     for r in RULES],
    ["rule_id", "rule_name", "scope_type", "scope_value", "rule_type",
     "severity", "window_seconds", "message_template"],
)

# Design mass balance check -- fails loudly if the arc tonnages don't close.
inflow = {}
outflow = {}
for arc_id, frm, to, _n, tph, _t, _m, stype in ARCS:
    if stype == "water" or frm == to:
        continue
    outflow[frm] = outflow.get(frm, 0.0) + tph
    inflow[to] = inflow.get(to, 0.0) + tph

print("\nDesign mass balance per node (t/h):")
failures = 0
for nid, nname, _t, _a, role, *_ in NODES:
    i, o = inflow.get(nid, 0.0), outflow.get(nid, 0.0)
    if role != "unit":                 # sources and sinks never close
        print(f"  {nid} {nname:32s} in={i:8.1f}  out={o:8.1f}   ({role})")
        continue
    ok = abs(i - o) < 0.01
    failures += 0 if ok else 1
    flag = "" if ok else "   <-- DOES NOT CLOSE"
    print(f"  {nid} {nname:32s} in={i:8.1f}  out={o:8.1f}{flag}")
if failures:
    raise SystemExit(f"\nERROR: {failures} node(s) do not close -- fix ARCS before committing.")
print("\nAll unit nodes close.")


def solids_from_slurry(q_m3h, rho):
    """Dry solids t/h from slurry volumetric flow and slurry density."""
    cw = SOLIDS_SG * (rho - 1.0) / (rho * (SOLIDS_SG - 1.0))
    return q_m3h * rho * cw


by_id = {t["tag_id"]: t for t in tags}
feed = solids_from_slurry(by_id["DSD-CYC01-FI01"]["nominal"], by_id["DSD-CYC01-DI01"]["nominal"])
ovf = solids_from_slurry(by_id["DSD-CYC01-FI02"]["nominal"], by_id["DSD-CYC01-DI02"]["nominal"])
unf = solids_from_slurry(by_id["DSD-CYC01-FI03"]["nominal"], by_id["DSD-CYC01-DI03"]["nominal"])

print("\nDesands circuit derived solids (t/h), from slurry flow x density:")
print(f"  cyclone feed      {feed:8.1f}   (dry side delivers 2500.0)")
print(f"  cyclone overflow  {ovf:8.1f}   (design  400.0)")
print(f"  cyclone underflow {unf:8.1f}   (design 2100.0)")
print(f"  split closure     {feed - ovf - unf:8.1f}   (should be ~0)")
if abs(feed - ovf - unf) > 5.0 or abs(feed - 2500.0) > 5.0:
    raise SystemExit("\nERROR: desands slurry tags do not reconcile with the dry side.")
print("\nWet and dry sides reconcile.")
