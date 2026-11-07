"""Pure-Python oracle of the reference Rust `compute_trip_stats` (lambda/src/stats.rs).

A line-by-line port used only by the tests: sums are sequential float additions in row order
(Rust `Iterator::sum`), and `round2` is Rust's `(x * 100.0).round() / 100.0`, i.e. round half
away from zero on the exact binary value of the f64 product.
"""

from __future__ import annotations

import decimal
import math
from typing import Any, Dict, Iterable, List, Mapping


def round2(value: float) -> float:
    scaled = value * 100.0
    if not math.isfinite(scaled):
        return scaled / 100.0
    rounded = decimal.Decimal(scaled).to_integral_value(rounding=decimal.ROUND_HALF_UP)
    return float(rounded) / 100.0


def compute_trip_stats(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    rows = list(rows)
    if not rows:
        return {}
    total_trips = len(rows)
    total_distance = 0.0
    total_fare = 0.0
    total_tips = 0.0
    total_passengers = 0
    vendors: Dict[str, int] = {}
    payments: Dict[str, int] = {}
    distances: List[float] = []
    for r in rows:
        total_distance += float(r["trip_distance"])
        total_fare += float(r["fare_amount"])
        total_tips += float(r["tip_amount"])
        total_passengers += int(r["passenger_count"])
        vendors[r["vendor_id"]] = vendors.get(r["vendor_id"], 0) + 1
        payments[r["payment_type"]] = payments.get(r["payment_type"], 0) + 1
        distances.append(float(r["trip_distance"]))
    tip_rate = total_tips / total_fare * 100.0 if total_fare > 0.0 else 0.0
    return {
        "total_trips": total_trips,
        "total_passengers": total_passengers,
        "total_distance_miles": round2(total_distance),
        "total_fare_usd": round2(total_fare),
        "total_tips_usd": round2(total_tips),
        "avg_distance_miles": round2(total_distance / total_trips),
        "avg_fare_usd": round2(total_fare / total_trips),
        "avg_tip_usd": round2(total_tips / total_trips),
        "tip_rate_percent": round2(tip_rate),
        "max_distance_miles": max(distances),
        "min_distance_miles": min(distances),
        "vendor_breakdown": dict(sorted(vendors.items())),
        "payment_type_breakdown": dict(sorted(payments.items())),
    }
