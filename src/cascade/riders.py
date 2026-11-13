"""Week 9: generate the synthetic riders table (provided).

    uv run python -m cascade.riders                  # writes data/restricted/riders.csv

Cascade Mobility's rider accounts contain personal data (PII): names, emails,
birth dates, home ZIP codes. We can't use real people, so this script invents
5,000 riders with a fixed seed: same seed, same riders, on every computer.

The output goes to <data_root>/restricted/, which is gitignored, so PII never
lands in git. That habit matters more than the fact that these riders are fake.
dbt reads the file into the `restricted` schema (never exported) and builds the
masked `gold.dim_rider` that analysts and the dashboard are allowed to see.

Standard library only: no extra packages to install.
"""

import argparse
import csv
import random
import sys
from datetime import date, timedelta
from pathlib import Path

from cascade import config

SEED = 323
ROWS = 5_000

FIRST_NAMES = ["Aaliyah", "Adrian", "Aisha", "Alejandro", "Amara", "Andre", "Ava", "Benjamin", "Camila", "Carlos",
               "Chloe", "Daniel", "Destiny", "Diego", "Elena", "Elijah", "Emily", "Ethan", "Fatima", "Gabriel",
               "Grace", "Hannah", "Isaac", "Jamal", "Jasmine", "Javier", "Jayden", "Kenji", "Keisha", "Liam",
               "Lucia", "Malik", "Maria", "Mateo", "Maya", "Mei", "Mohammed", "Nadia", "Noah", "Olivia",
               "Omar", "Priya", "Rafael", "Riya", "Samuel", "Sofia", "Tariq", "Tyler", "Valentina", "Zoe"]
LAST_NAMES = ["Adams", "Ali", "Brown", "Chen", "Clark", "Davis", "Diaz", "Evans", "Garcia", "Gonzalez",
              "Green", "Hall", "Harris", "Hernandez", "Jackson", "Johnson", "Kim", "King", "Lee", "Lewis",
              "Lopez", "Martin", "Martinez", "Moore", "Nguyen", "Okafor", "Patel", "Perez", "Ramirez", "Robinson",
              "Rodriguez", "Sanchez", "Scott", "Shah", "Singh", "Smith", "Taylor", "Thomas", "Thompson", "Walker",
              "White", "Williams", "Wilson", "Wright", "Young", "Zhang"]
# Real Chicago ZIP codes, so the data looks plausible.
CHICAGO_ZIPS = ["60601", "60602", "60603", "60604", "60605", "60606", "60607", "60608", "60609", "60610",
                "60611", "60612", "60613", "60614", "60615", "60616", "60617", "60618", "60619", "60620",
                "60622", "60623", "60624", "60625", "60626", "60628", "60629", "60630", "60631", "60632",
                "60636", "60637", "60638", "60639", "60640", "60641", "60642", "60643", "60644", "60645",
                "60646", "60647", "60649", "60651", "60652", "60653", "60654", "60655", "60656", "60657",
                "60659", "60660", "60661", "60201", "60202"]
# Reserved for documentation, so a generated email can never belong to a real person.
EMAIL_DOMAINS = ["example.com", "example.org", "example.net"]


def generate_riders(rows: int = ROWS, seed: int = SEED) -> list[dict]:
    rng = random.Random(seed)
    riders, used_emails = [], set()
    for i in range(1, rows + 1):
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        member_casual = "member" if rng.random() < 0.6 else "casual"
        email = None
        while email is None or email in used_emails:      # one account per email address
            email = f"{first}.{last}{rng.randint(1, 999)}@{rng.choice(EMAIL_DOMAINS)}".lower()
        used_emails.add(email)
        riders.append({
            "rider_id": f"R{i:05d}",
            "first_name": first,
            "last_name": last,
            "email": email,
            "birth_date": (date(1950, 1, 1) + timedelta(days=rng.randint(0, 365 * 57))).isoformat(),
            "home_zip": rng.choice(CHICAGO_ZIPS),
            "member_casual": member_casual,
            "signup_date": (date(2019, 1, 1) + timedelta(days=rng.randint(0, 365 * 7 + 300))).isoformat(),
        })
    return riders


def write_riders(data_root: Path = config.DEFAULT_DATA_ROOT, rows: int = ROWS, seed: int = SEED) -> Path:
    path = Path(data_root) / "restricted" / "riders.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    riders = generate_riders(rows, seed)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(riders[0]))
        writer.writeheader()
        writer.writerows(riders)
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate the synthetic riders table (fake PII).")
    parser.add_argument("--data-root", type=Path, default=config.DEFAULT_DATA_ROOT)
    parser.add_argument("--rows", type=int, default=ROWS)
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args(argv)
    path = write_riders(args.data_root, args.rows, args.seed)
    print(f"Wrote {args.rows:,} synthetic riders to {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
