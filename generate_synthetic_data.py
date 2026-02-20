"""
VitaShop – Synthetic Data Generator
====================================
Generates realistic fake data that mirrors the schema of
tb_dimensions_customer_orders, allowing the SQL pipeline
to be demonstrated and tested without any real customer data.

Usage:
  pip install pandas faker numpy
  python generate_synthetic_data.py
  → Outputs: data/sample_orders.csv  (~5 000 rows)
"""

import random
import csv
from datetime import date, timedelta

# ── Reproducibility ───────────────────────────────────────────
random.seed(42)

# ── Domain constants ──────────────────────────────────────────
BRANDS           = ["VitaShop Premium", "VitaShop Express", "VitaShop Pro"]
REGIONS          = ["Southeast", "South", "Northeast", "Midwest", "North"]
STORES           = [f"Store_{i:03d}" for i in range(1, 31)]
PRODUCT_CATS     = ["Supplement - Protein", "Supplement - Vitamins",
                    "Device - Monitor", "Device - Wearable",
                    "Service - Nutritionist", "Service - Personal Trainer"]
PAYMENT_METHODS  = ["credit_card", "vitashop_wallet", "health_plan"]
HEALTH_PLANS     = ["PlanA", "PlanB", "PlanC"]
FULFILLMENT      = ["IN_STORE_PICKUP", "HOME_DELIVERY"]
CHANNELS         = ["Organic Search", "Paid Search", "Direct",
                    "Email", "Social", "Referral", "Unidentified"]
ORIGINS          = ["DIGITAL", "CALL_CENTER", "IN_STORE"]
GENDERS          = ["M", "F", "N/A"]
CAMPAIGNS        = ["brand_awareness_q1", "retention_email_q2",
                    "black_friday", "new_year_promo", None, None, None]

START_DATE = date(2022, 1, 1)
END_DATE   = date(2025, 12, 31)

# ── Helper utilities ──────────────────────────────────────────

def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def random_birthdate() -> date:
    """Generate a birthdate for someone aged 18–80 at END_DATE."""
    age_days = random.randint(18 * 365, 80 * 365)
    return END_DATE - timedelta(days=age_days)

def fake_cpf(seed: int) -> str:
    """Deterministic fake CPF (not valid, just numeric)."""
    r = random.Random(seed)
    digits = [r.randint(0, 9) for _ in range(11)]
    return "".join(str(d) for d in digits)

# ── Generate customers ────────────────────────────────────────
N_CUSTOMERS = 800
customers = []
for i in range(N_CUSTOMERS):
    bd    = random_birthdate()
    store = random.choice(STORES)
    # Bias region by store index for plausibility
    region = REGIONS[int(store.split("_")[1]) % len(REGIONS)]
    customers.append({
        "customer_id": f"CUST_{i:05d}",
        "customer_cpf": fake_cpf(i),
        "gender": random.choice(GENDERS),
        "birthdate": bd,
        "brand": random.choice(BRANDS),
        "region": region,
        "store": store,
    })

# ── Generate orders ───────────────────────────────────────────
rows = []
order_counter = 1

for cust in customers:
    # Each customer has 1–12 orders over the period
    n_orders = random.choices(
        [1, 2, 3, 4, 5, 6, 8, 10, 12],
        weights=[30, 20, 15, 10, 8, 6, 5, 4, 2],
        k=1
    )[0]

    for _ in range(n_orders):
        order_date  = random_date(START_DATE, END_DATE)
        payment     = random.choice(PAYMENT_METHODS)
        product_cat = random.choice(PRODUCT_CATS)
        origin      = random.choices(ORIGINS, weights=[55, 25, 20])[0]
        channel     = random.choice(CHANNELS) if origin == "DIGITAL" else None
        campaign    = random.choice(CAMPAIGNS) if origin == "DIGITAL" else None
        modality    = random.choice(FULFILLMENT)
        quantity    = random.randint(1, 5)

        # Revenue: devices are more expensive than supplements/services
        if "Device" in product_cat:
            revenue = round(random.uniform(150, 800), 2)
        elif "Service" in product_cat:
            revenue = round(random.uniform(80, 350), 2)
        else:
            revenue = round(random.uniform(20, 150), 2)

        health_plan = random.choice(HEALTH_PLANS) if payment == "health_plan" else None

        rows.append({
            "order_line_id":      f"OL_{order_counter:07d}",
            "order_id":           f"ORD_{order_counter:07d}",
            "order_date":         order_date.isoformat(),
            "customer_id":        cust["customer_id"],
            "customer_cpf":       cust["customer_cpf"],
            "customer_gender":    cust["gender"],
            "customer_birthdate": cust["birthdate"].isoformat(),
            "brand_name":         cust["brand"],
            "region_name":        cust["region"],
            "store_name":         cust["store"],
            "product_category":   product_cat,
            "payment_method":     payment,
            "health_plan_name":   health_plan,
            "fulfillment_type":   modality,
            "booking_channel":    origin,
            "channel_group":      channel,
            "campaign_non_direct": campaign,
            "revenue_amount":     revenue,
            "quantity":           quantity,
            "market_segment":     random.choices(["B2C", "PREMIUM"], weights=[80, 20])[0],
            "is_excluded":        False,
            "order_status":       random.choices(
                                    ["COMPLETED", "COMPLETED", "COMPLETED", "CANCELLED"],
                                    weights=[90, 5, 4, 1])[0],
        })
        order_counter += 1

# ── Write CSV ──────────────────────────────────────────────────
output_path = "data/sample_orders.csv"
fieldnames  = list(rows[0].keys())

with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"✅  Generated {len(rows):,} order lines for {N_CUSTOMERS} customers")
print(f"    Saved to: {output_path}")
print(f"\n    Date range : {START_DATE} → {END_DATE}")
print(f"    Brands     : {BRANDS}")
print(f"    Regions    : {REGIONS}")
