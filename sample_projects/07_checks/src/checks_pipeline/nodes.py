"""A producer that can be told to violate its own contract."""

MEASUREMENTS = [
    {"analyte": "PND", "value": 12.5, "unit": "umol/L"},
    {"analyte": "PND", "value": 9.75, "unit": "umol/L"},
    {"analyte": "CRP", "value": 3.0, "unit": "mg/L"},
    {"analyte": "CRP", "value": 5.5, "unit": "mg/L"},
]


def extract_measurements(ctx) -> {"measurements": list[dict]}:
    rows = [dict(row) for row in MEASUREMENTS[: ctx.vars["rows"]]]
    if ctx.vars["inject_bad_units"]:
        # The exact bug class this sample exists for: an upstream source silently
        # switches units, and every downstream number is wrong by 1000x.
        rows[0]["unit"] = "nmol/L"
        rows[0]["value"] *= 1000
        print("injected a unit-contract violation")
    print(f"extracted {len(rows)} measurements")
    return {"measurements": rows}


def aggregate(ctx, measurements: list[dict]) -> {"by_analyte": dict}:
    totals: dict[str, float] = {}
    for row in measurements:
        totals[row["analyte"]] = totals.get(row["analyte"], 0.0) + row["value"]
    print(f"aggregated into {len(totals)} analytes")
    return {"by_analyte": totals}
