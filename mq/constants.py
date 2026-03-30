"""Shared constants used across the application."""

NEIGHBORHOOD_COLORS: dict[str, str] = {
    "Mainframe":        "#FF6B6B",
    "Consumer Lending": "#4ECDC4",
    "Core Banking":     "#45B7D1",
    "Wholesale Banking":"#96CEB4",
    "Private PaaS":     "#FFEAA7",
}

TRTC_LABELS: dict[str, str] = {
    "00": "Critical (0-30 min RTO)",
    "02": "High (2-4 hr RTO)",
    "03": "Normal (4-12 hr RTO)",
}

COMPLEXITY_THRESHOLDS = {
    "low":      50,
    "medium":   100,
    "high":     200,
}

DR_SITE_PAIRS: dict[str, str] = {
    "Mainframe":        "Core Banking",
    "Core Banking":     "Mainframe",
    "Consumer Lending": "Private PaaS",
    "Private PaaS":     "Consumer Lending",
    "Wholesale Banking":"Private PaaS",
}
