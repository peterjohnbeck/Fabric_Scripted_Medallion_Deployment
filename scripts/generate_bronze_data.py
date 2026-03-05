"""
generate_bronze_data.py

Generates synthetic OLTP data for a machine tools manufacturer (US + global sales).
Run once locally from the repo root:
    python scripts/generate_bronze_data.py

Output: 9 CSV files written to data/bronze/
Dependencies: pip install pandas numpy
"""

import os
import math
import random
from datetime import date, timedelta, datetime

import numpy as np
import pandas as pd

# ── Reproducibility ──────────────────────────────────────────────────────────
np.random.seed(42)
random.seed(42)

# ── Constants ─────────────────────────────────────────────────────────────────
START_DATE     = date(2022, 1, 1)
END_DATE       = date(2024, 12, 31)
NUM_CUSTOMERS  = 500
NUM_ORDERS     = 50_000
NUM_EMPLOYEES  = 28
NOW            = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
OUT_DIR        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "bronze")

os.makedirs(OUT_DIR, exist_ok=True)


def save_csv(df: pd.DataFrame, name: str) -> None:
    path = os.path.join(OUT_DIR, f"{name}.csv")
    df.to_csv(path, index=False)
    print(f"  {name}.csv  —  {len(df):,} rows")


# =============================================================================
# 1. raw_product_categories  (10 rows)
# =============================================================================
print("Generating raw_product_categories...")
CATEGORIES = [
    (1,  "CNC Machining Centers",    "3-, 4-, and 5-axis vertical and horizontal milling centers", True),
    (2,  "CNC Turning Centers",      "2-axis and multi-tasking CNC lathes and turning centers",    True),
    (3,  "EDM Equipment",            "Wire and sinker electrical discharge machining systems",      True),
    (4,  "Grinding Machines",        "Surface, cylindrical, and centerless grinding equipment",    True),
    (5,  "Metal Cutting Inserts",    "Carbide and ceramic indexable cutting inserts (box/10)",     False),
    (6,  "Toolholding Systems",      "BT/CAT/HSK holders, collets, chucks, and adapters",         False),
    (7,  "Metrology Equipment",      "CMMs, probing systems, and precision gauging instruments",   True),
    (8,  "Coolant Systems",          "Through-spindle, high-pressure, and filtration systems",     False),
    (9,  "Automation Components",    "Robotic arms, pallet changers, and material handling cells", True),
    (10, "Maintenance & Spare Parts","Spindles, ballscrews, way covers, lubricants, consumables",  False),
]
df_cat = pd.DataFrame(CATEGORIES,
    columns=["category_id","category_name","category_desc","is_capital_equip"])
df_cat["created_at"] = NOW
save_csv(df_cat, "raw_product_categories")


# =============================================================================
# 2. raw_products  (~500 rows)
# =============================================================================
print("Generating raw_products...")

# (cat_id, list_price_low, list_price_high, gross_margin, num_skus)
CAT_CFG = {
    1:  dict(low=80_000,  high=650_000, gm=0.38, count=60,
             prefixes=["VMC","HMC","UMC","5AX","VF","MX","DM","EC"]),
    2:  dict(low=40_000,  high=320_000, gm=0.36, count=50,
             prefixes=["LT","ST","MT","SW","QL","CL","GT","NL"]),
    3:  dict(low=55_000,  high=280_000, gm=0.40, count=30,
             prefixes=["WC","AP","HS","FW","SP","AC","ZH"]),
    4:  dict(low=30_000,  high=200_000, gm=0.35, count=40,
             prefixes=["SG","CG","CL","RG","IG","BG","SGA"]),
    5:  dict(low=8,       high=95,      gm=0.55, count=120,
             prefixes=["CNMG","APMT","VCMT","TCMT","DNMG","WNMG","SNMG","CCMT","DCMT","RCMT"]),
    6:  dict(low=150,     high=4_500,   gm=0.48, count=80,
             prefixes=["CAT40-ER","BT40-ER","BT50","HSK63A","HSK100A","SK40","NT40","CAT50"]),
    7:  dict(low=15_000,  high=180_000, gm=0.42, count=35,
             prefixes=["CMM","OMP","TS","RP","GX","SX","TCP","RMP"]),
    8:  dict(low=3_500,   high=45_000,  gm=0.44, count=30,
             prefixes=["TCS","HPP","FIL","CFC","MCS","CLP","BCS"]),
    9:  dict(low=25_000,  high=220_000, gm=0.32, count=25,
             prefixes=["RAC","PC","DC","CS","ATC","TL","RB"]),
    10: dict(low=50,      high=18_000,  gm=0.60, count=30,
             prefixes=["SP","BS","WC-KIT","LUB","BRG","SGP","RPK"]),
}

AXIS_CHOICES = ["3-axis","4-axis","5-axis simultaneous"]
GRADE_CHOICES = ["P25","M25","K20","S15","H10","P15","K15","M35"]
SWING_CHOICES = [300,400,500,600,800]

product_rows = []
pid = 1
for cat_id, cfg in CAT_CFG.items():
    for _ in range(cfg["count"]):
        pfx    = random.choice(cfg["prefixes"])
        num    = random.randint(100, 9999)
        var    = random.choice(["","","","-A","-B","-HD","-XL"])
        code   = f"{pfx}-{num}{var}"

        log_lo = math.log(cfg["low"])
        log_hi = math.log(cfg["high"])
        list_p = round(math.exp(random.uniform(log_lo, log_hi)), 2)
        cost   = round(list_p * (1 - cfg["gm"]) * random.uniform(0.93, 1.07), 2)

        if cat_id in (1, 2, 3, 4, 7, 9):
            weight_kg  = round(random.uniform(300, 30_000), 1)
            lead_days  = random.randint(8, 24)
        else:
            weight_kg  = round(random.uniform(0.05, 50), 2)
            lead_days  = random.randint(1, 7)

        if cat_id == 1:
            axes  = random.choice(AXIS_CHOICES)
            name  = f"{code} Machining Center ({axes})"
            desc  = f"High-precision {axes} machining center, table {random.randint(400,1200)}mm"
        elif cat_id == 2:
            swing = random.choice(SWING_CHOICES)
            name  = f"{code} CNC Turning Center"
            desc  = f"CNC turning center, max swing {swing}mm, bar capacity {random.randint(52,102)}mm"
        elif cat_id == 3:
            style = random.choice(["Wire","Sinker"])
            name  = f"{code} {style} EDM"
            desc  = f"{style} EDM, work tank {random.randint(400,900)}x{random.randint(300,700)}mm"
        elif cat_id == 4:
            style = random.choice(["Surface","Cylindrical","Centerless"])
            name  = f"{code} {style} Grinder"
            desc  = f"{style} grinding machine, table {random.randint(300,900)}mm"
        elif cat_id == 5:
            grade = random.choice(GRADE_CHOICES)
            name  = f"{code} Carbide Insert Grade {grade} (Pkg/10)"
            desc  = f"Indexable carbide turning/milling insert, grade {grade}"
        elif cat_id == 6:
            name  = f"{code} Toolholder"
            desc  = f"Precision toolholder, runout <3μm, balanced to G2.5"
        elif cat_id == 7:
            style = random.choice(["Fixed CMM","Portable CMM","Touch-Trigger Probe","Scanning Probe"])
            name  = f"{code} {style}"
            desc  = f"Precision measurement system, accuracy ±{random.choice([1.5,2.0,2.5,3.0])}μm"
        elif cat_id == 8:
            style = random.choice(["High-Pressure","Through-Spindle","Coolant Filtration"])
            name  = f"{code} {style} System"
            desc  = f"{style} coolant system, flow {random.randint(20,200)} L/min"
        elif cat_id == 9:
            style = random.choice(["Robotic Loading Cell","Pallet Changer","Conveyor System","Bar Feeder"])
            name  = f"{code} {style}"
            desc  = f"Automation solution for {random.choice(['machining','turning','grinding'])} cells"
        else:  # 10
            style = random.choice(["Spindle Assembly","Ballscrew Kit","Way Cover Set","Way Lube Kit",
                                   "Bearing Set","Maintenance Kit","Coolant Additive","Seal Kit"])
            name  = f"{code} {style}"
            desc  = "OEM-quality replacement part for machine tool maintenance"

        product_rows.append(dict(
            product_id       = pid,
            category_id      = cat_id,
            product_code     = code,
            product_name     = name,
            description      = desc,
            list_price_usd   = list_p,
            standard_cost_usd= cost,
            weight_kg        = weight_kg,
            lead_time_days   = lead_days,
            is_active        = True,
            created_at       = NOW,
        ))
        pid += 1

df_prod = pd.DataFrame(product_rows)
save_csv(df_prod, "raw_products")


# =============================================================================
# 3. raw_territories  (13 rows)
# =============================================================================
print("Generating raw_territories...")
TERRITORIES = [
    (1,  "Great Lakes",     "Domestic",      "GL",  "Robert Danforth"),
    (2,  "Southeast",       "Domestic",      "SE",  "Patricia Okafor"),
    (3,  "Mid-Atlantic",    "Domestic",      "MA",  "James Kowalski"),
    (4,  "Southwest",       "Domestic",      "SW",  "Maria Gutierrez"),
    (5,  "West Coast",      "Domestic",      "WC",  "Kevin Nguyen"),
    (6,  "Mountain West",   "Domestic",      "MW",  "Sandra Reeves"),
    (7,  "New England",     "Domestic",      "NE",  "William Frost"),
    (8,  "Canada",          "International", "CAN", "Claire Beaumont"),
    (9,  "Mexico",          "International", "MEX", "Alejandro Torres"),
    (10, "Western Europe",  "International", "WEU", "Hans Bergmann"),
    (11, "East Asia",       "International", "EAS", "Yuki Tanaka"),
    (12, "India",           "International", "IND", "Priya Sharma"),
    (13, "Brazil",          "International", "BRA", "Felipe Santos"),
]
df_terr = pd.DataFrame(TERRITORIES,
    columns=["territory_id","territory_name","territory_type","region_code","manager_name"])
df_terr["created_at"] = NOW
save_csv(df_terr, "raw_territories")


# =============================================================================
# 4. raw_employees  (28 rows)
# =============================================================================
print("Generating raw_employees...")
FIRST_NAMES = [
    "James","Patricia","Robert","Jennifer","Michael","Linda","William","Barbara",
    "David","Susan","Richard","Jessica","Joseph","Sarah","Thomas","Karen","Charles",
    "Lisa","Christopher","Nancy","Daniel","Betty","Matthew","Margaret","Anthony",
    "Sandra","Donald","Ashley"
]
LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
    "Wilson","Martinez","Anderson","Taylor","Thomas","Hernandez","Moore","Jackson",
    "Martin","Lee","Perez","Thompson","White","Harris","Sanchez","Clark",
    "Lewis","Robinson","Walker","Young"
]

# Domestic territories get 3-4 reps each, international 1-2
TERR_REP_COUNTS = {1:4, 2:4, 3:3, 4:3, 5:3, 6:2, 7:2, 8:2, 9:2, 10:1, 11:1, 12:1, 13:0}
terr_assignments = []
for tid, cnt in TERR_REP_COUNTS.items():
    terr_assignments.extend([tid] * cnt)
# Assign one extra to Brazil manager via territory 13 → use 10 instead
terr_assignments.append(10)  # total = 28
random.shuffle(terr_assignments)

emp_rows = []
used_names: set = set()
for i in range(NUM_EMPLOYEES):
    while True:
        fn = random.choice(FIRST_NAMES)
        ln = random.choice(LAST_NAMES)
        if (fn, ln) not in used_names:
            used_names.add((fn, ln))
            break
    hire_days_ago = random.randint(180, 4380)
    hire_date = (START_DATE - timedelta(days=hire_days_ago)).isoformat()
    emp_rows.append(dict(
        employee_id  = i + 1,
        first_name   = fn,
        last_name    = ln,
        email        = f"{fn.lower()}.{ln.lower()}@machinetoolco.com",
        territory_id = terr_assignments[i],
        hire_date    = hire_date,
        is_active    = True,
        created_at   = NOW,
    ))

df_emp = pd.DataFrame(emp_rows)
save_csv(df_emp, "raw_employees")


# =============================================================================
# 5. raw_currencies  (10 rows)
# =============================================================================
print("Generating raw_currencies...")
CURRENCIES = [
    ("USD","US Dollar",        "$",  True,  2),
    ("EUR","Euro",             "€",  False, 2),
    ("JPY","Japanese Yen",     "¥",  False, 0),
    ("KRW","South Korean Won", "₩",  False, 0),
    ("MXN","Mexican Peso",     "$",  False, 2),
    ("CAD","Canadian Dollar",  "$",  False, 2),
    ("GBP","British Pound",    "£",  False, 2),
    ("INR","Indian Rupee",     "₹",  False, 2),
    ("BRL","Brazilian Real",   "R$", False, 2),
    ("CNY","Chinese Yuan",     "¥",  False, 2),
]
df_curr = pd.DataFrame(CURRENCIES,
    columns=["currency_code","currency_name","currency_symbol","is_base_currency","decimal_places"])
df_curr["created_at"] = NOW
save_csv(df_curr, "raw_currencies")


# =============================================================================
# 6. raw_customers  (500 rows)
# =============================================================================
print("Generating raw_customers...")

# US states by territory: (state_abbr, state_name, sample_cities)
US_BY_TERR: dict[int, list] = {
    1: [  # Great Lakes — 110 customers
        ("MI", "Michigan",     ["Detroit","Grand Rapids","Flint","Lansing","Warren","Sterling Heights","Ann Arbor"]),
        ("OH", "Ohio",         ["Columbus","Cleveland","Cincinnati","Toledo","Akron","Dayton","Canton"]),
        ("IN", "Indiana",      ["Indianapolis","Fort Wayne","Evansville","South Bend","Gary"]),
        ("IL", "Illinois",     ["Chicago","Rockford","Peoria","Aurora","Naperville","Joliet","Waukegan"]),
        ("WI", "Wisconsin",    ["Milwaukee","Madison","Green Bay","Kenosha","Racine","Appleton"]),
    ],
    2: [  # Southeast — 60 customers
        ("TN", "Tennessee",    ["Nashville","Memphis","Knoxville","Chattanooga","Clarksville"]),
        ("NC", "North Carolina",["Charlotte","Raleigh","Greensboro","Durham","Winston-Salem"]),
        ("SC", "South Carolina",["Columbia","Charleston","Greenville","Rock Hill","Spartanburg"]),
        ("AL", "Alabama",      ["Birmingham","Huntsville","Montgomery","Mobile","Tuscaloosa"]),
        ("GA", "Georgia",      ["Atlanta","Augusta","Columbus","Macon","Savannah"]),
    ],
    3: [  # Mid-Atlantic — 50 customers
        ("PA", "Pennsylvania", ["Philadelphia","Pittsburgh","Allentown","Erie","Reading"]),
        ("NY", "New York",     ["Buffalo","Rochester","Syracuse","Albany","Utica"]),
        ("NJ", "New Jersey",   ["Newark","Jersey City","Paterson","Elizabeth","Edison"]),
        ("VA", "Virginia",     ["Virginia Beach","Norfolk","Richmond","Newport News","Hampton"]),
    ],
    4: [  # Southwest — 45 customers
        ("TX", "Texas",        ["Houston","San Antonio","Dallas","Fort Worth","El Paso","Arlington"]),
        ("AZ", "Arizona",      ["Phoenix","Tucson","Mesa","Chandler","Scottsdale"]),
        ("OK", "Oklahoma",     ["Oklahoma City","Tulsa","Norman","Broken Arrow","Lawton"]),
    ],
    5: [  # West Coast — 40 customers
        ("CA", "California",   ["Los Angeles","San Jose","San Diego","San Francisco","Fresno","Sacramento"]),
        ("WA", "Washington",   ["Seattle","Spokane","Tacoma","Bellevue","Everett","Renton"]),
        ("OR", "Oregon",       ["Portland","Salem","Eugene","Hillsboro","Beaverton"]),
    ],
    6: [  # Mountain West — 20 customers
        ("CO", "Colorado",     ["Denver","Colorado Springs","Aurora","Fort Collins","Lakewood"]),
        ("UT", "Utah",         ["Salt Lake City","Provo","West Jordan","Orem","Sandy"]),
        ("ID", "Idaho",        ["Boise","Nampa","Meridian","Idaho Falls","Pocatello"]),
    ],
    7: [  # New England — 15 customers
        ("CT", "Connecticut",  ["Bridgeport","New Haven","Stamford","Hartford","Waterbury"]),
        ("MA", "Massachusetts",["Boston","Worcester","Springfield","Lowell","Brockton"]),
        ("NH", "New Hampshire",["Manchester","Nashua","Concord","Dover","Rochester"]),
    ],
}
US_COUNTS = {1:110, 2:60, 3:50, 4:45, 5:40, 6:20, 7:15}

# International: (territory_id, country_code, country_name, currency_code, num_customers, sample_cities)
INTL_SPEC = [
    (10,"DE","Germany",        "EUR", 20, ["Munich","Stuttgart","Hamburg","Cologne","Nuremberg","Frankfurt","Berlin","Dusseldorf"]),
    (10,"FR","France",         "EUR",  8, ["Paris","Lyon","Marseille","Toulouse","Nantes","Strasbourg","Bordeaux"]),
    (10,"IT","Italy",          "EUR",  8, ["Milan","Turin","Bologna","Florence","Genoa","Brescia","Modena"]),
    (10,"GB","United Kingdom", "GBP",  8, ["Birmingham","Manchester","Leeds","Sheffield","Bristol","Glasgow","Coventry"]),
    (10,"ES","Spain",          "EUR",  5, ["Madrid","Barcelona","Valencia","Seville","Zaragoza","Bilbao"]),
    (10,"NL","Netherlands",    "EUR",  6, ["Amsterdam","Rotterdam","Eindhoven","Utrecht","Groningen","Breda"]),
    (11,"JP","Japan",          "JPY", 15, ["Tokyo","Osaka","Nagoya","Sapporo","Yokohama","Kobe","Hiroshima","Fukuoka"]),
    (11,"KR","South Korea",    "KRW", 15, ["Seoul","Busan","Incheon","Daegu","Daejeon","Gwangju","Suwon","Changwon"]),
    (11,"CN","China",          "CNY", 10, ["Shanghai","Beijing","Shenzhen","Guangzhou","Chengdu","Hangzhou","Wuhan","Suzhou"]),
    ( 8,"CA","Canada",         "CAD", 25, ["Toronto","Montreal","Vancouver","Calgary","Edmonton","Ottawa","Winnipeg","Hamilton"]),
    ( 9,"MX","Mexico",         "MXN", 20, ["Monterrey","Mexico City","Guadalajara","Tijuana","Puebla","Queretaro","Saltillo","Leon"]),
    (12,"IN","India",          "INR", 12, ["Mumbai","Pune","Chennai","Bengaluru","Hyderabad","Ahmedabad","Coimbatore","Nashik"]),
    (13,"BR","Brazil",         "BRL",  8, ["Sao Paulo","Porto Alegre","Curitiba","Belo Horizonte","Campinas","Joinville"]),
]

CO_FIRST = [
    "Apex","Acme","Allied","American","Atlas","Benchmark","Central","Continental",
    "Cornerstone","Crown","Delta","Dynamic","Eagle","Eastern","Elite","Empire",
    "Federal","General","Global","Heritage","Industrial","Integrity","International",
    "Keystone","Liberty","Lincoln","Midwest","National","Northern","Pacific",
    "Patriot","Pioneer","Premier","Precision","Prime","Progressive","Quality",
    "Reliable","Rocky","Southern","Star","Sterling","Summit","Superior","Tri-State",
    "United","Universal","Valley","Western","Wolverine","Great Lakes","Gulf",
]
CO_SECOND = [
    "Machining","Manufacturing","Fabrication","Metalworks","Industries","Precision",
    "Components","Engineering","Technology","Solutions","Products","Systems",
    "Parts","Tool","Equipment","Processing","Works","Forge","Casting","Dynamics",
]
CO_SUFFIX = ["LLC","Inc.","Corp.","Co.","Ltd.","Manufacturing","Industries","Group","Works"]

INTL_SURNAME: dict[str,list] = {
    "DE": ["Müller","Fischer","Schmidt","Weber","Wagner","Becker","Schulz","Hoffmann","Koch","Richter"],
    "FR": ["Durand","Martin","Bernard","Lefebvre","Thomas","Moreau","Simon","Laurent","Leroy","Petit"],
    "IT": ["Ferrari","Rossi","Bianchi","Romano","Ricci","Colombo","Barbieri","De Luca","Greco","Russo"],
    "GB": ["Smith","Jones","Williams","Taylor","Brown","Davies","Evans","Wilson","Thomas","Roberts"],
    "ES": ["García","Martínez","López","Sánchez","Fernández","González","Rodríguez","Pérez","Gómez"],
    "NL": ["De Vries","Janssen","Bakker","Visser","Smit","Meijer","De Boer","Mulder","Peters","Van Dam"],
    "JP": ["Yamamoto","Tanaka","Watanabe","Ito","Suzuki","Sato","Kobayashi","Kato","Nakamura","Abe"],
    "KR": ["Kim","Lee","Park","Choi","Jung","Kang","Cho","Yoon","Jang","Lim"],
    "CN": ["Wang","Li","Zhang","Liu","Chen","Yang","Huang","Zhao","Wu","Zhou"],
    "CA": ["Anderson","MacKenzie","Tremblay","Gagnon","Roy","Bouchard","Côté","Leblanc"],
    "MX": ["Hernández","García","Martínez","González","López","Pérez","Ramírez","Torres","Flores"],
    "IN": ["Sharma","Patel","Singh","Kumar","Gupta","Joshi","Mehta","Shah","Reddy","Nair"],
    "BR": ["Silva","Santos","Oliveira","Souza","Rodrigues","Ferreira","Alves","Pereira","Lima","Gomes"],
}
INTL_SUFFIX: dict[str,list] = {
    "DE": ["GmbH","AG","GmbH & Co. KG"],   "FR": ["SAS","SARL","SA"],
    "IT": ["S.r.l.","S.p.A."],             "GB": ["Ltd","Limited","PLC"],
    "ES": ["S.A.","S.L."],                 "NL": ["B.V.","N.V."],
    "JP": ["Co., Ltd.","K.K."],            "KR": ["Co., Ltd.","Corp."],
    "CN": ["Co., Ltd.","Technology Co., Ltd."],
    "CA": ["Inc.","Ltd.","Corp."],         "MX": ["S.A. de C.V.","S.A."],
    "IN": ["Pvt. Ltd.","Ltd."],            "BR": ["Ltda.","S.A."],
}
INTL_INDUSTRY = [
    "Machining","Manufacturing","Precision","Industries","Engineering",
    "Werkzeug","Technik","Systems","Components","Technology",
]
CO_CONTACT_FIRST = [
    "James","Patricia","Robert","Jennifer","Michael","Linda","William","Barbara",
    "David","Susan","Richard","Jessica","Joseph","Sarah","Thomas","Karen",
    "Charles","Lisa","Christopher","Nancy","Daniel","Betty","Matthew","Margaret",
]
CO_STREET_TYPE  = ["Industrial Dr","Commerce Blvd","Manufacturing Ave","Enterprise Way","Technology Pkwy","Business Park Rd"]
CO_STREET_PFXS  = ["North","South","East","West","Old","New","Central","Lower","Upper"]

customer_specs: list[dict] = []

# Build US customer records
for terr_id, state_groups in US_BY_TERR.items():
    count = US_COUNTS[terr_id]
    for _ in range(count):
        st_abbr, st_name, cities = random.choice(state_groups)
        city = random.choice(cities)
        company_name = f"{random.choice(CO_FIRST)} {random.choice(CO_SECOND)} {random.choice(CO_SUFFIX)}"
        customer_specs.append(dict(
            territory_id   = terr_id,
            country_code   = "US",
            country_name   = "United States",
            state_province = st_abbr,
            city           = city,
            postal_code    = f"{random.randint(10000,99999):05d}",
            company_name   = company_name,
            currency_code  = "USD",
        ))

# Build international customer records
for terr_id, cc, cn, ccy, count, cities in INTL_SPEC:
    for _ in range(count):
        city = random.choice(cities)
        sn   = random.choice(INTL_SURNAME.get(cc, ["Global"]))
        ind  = random.choice(INTL_INDUSTRY)
        sfx  = random.choice(INTL_SUFFIX.get(cc, ["Ltd."]))
        company_name = f"{sn} {ind} {sfx}"
        customer_specs.append(dict(
            territory_id   = terr_id,
            country_code   = cc,
            country_name   = cn,
            state_province = "",
            city           = city,
            postal_code    = "",
            company_name   = company_name,
            currency_code  = ccy,
        ))

random.shuffle(customer_specs)
customer_specs = customer_specs[:NUM_CUSTOMERS]

# Log-normal purchase weights → Pareto-like concentration
purchase_weights = np.random.lognormal(mean=0.0, sigma=1.5, size=NUM_CUSTOMERS)

cust_rows = []
for i, spec in enumerate(customer_specs):
    cid = i + 1
    fn  = random.choice(CO_CONTACT_FIRST)
    ln  = random.choice(LAST_NAMES)
    slug = spec["company_name"].split()[0].lower().replace(",","").replace(".","")
    email = f"{fn.lower()}.{ln.lower()}@{slug}.example.com"
    area  = random.randint(200, 999)
    phone = f"({area}) {random.randint(200,999)}-{random.randint(1000,9999)}"
    credit = random.choice([50_000, 100_000, 200_000, 500_000, 1_000_000, 2_000_000])
    since  = (START_DATE - timedelta(days=random.randint(365, 3_650))).isoformat()
    st_pfx = random.choice(CO_STREET_PFXS)
    st_suf = random.choice(CO_STREET_TYPE)
    addr   = f"{random.randint(100,9999)} {st_pfx} {st_suf}"
    cust_rows.append(dict(
        customer_id      = cid,
        company_name     = spec["company_name"],
        contact_first    = fn,
        contact_last     = ln,
        contact_email    = email,
        phone            = phone,
        address_line1    = addr,
        city             = spec["city"],
        state_province   = spec["state_province"],
        postal_code      = spec["postal_code"],
        country_code     = spec["country_code"],
        country_name     = spec["country_name"],
        territory_id     = spec["territory_id"],
        currency_code    = spec["currency_code"],
        credit_limit_usd = float(credit),
        is_active        = True,
        customer_since   = since,
        purchase_weight  = round(float(purchase_weights[i]), 6),
        created_at       = NOW,
    ))

df_cust = pd.DataFrame(cust_rows)
save_csv(df_cust, "raw_customers")


# =============================================================================
# 7. raw_exchange_rates  (9 currencies × 1096 days = 9864 rows)
# =============================================================================
print("Generating raw_exchange_rates...")

FX_START = {
    "EUR": 0.8800, "JPY": 115.00, "KRW": 1200.00,
    "MXN": 20.50,  "CAD": 1.2700, "GBP": 0.7400,
    "INR": 74.50,  "BRL": 5.5700, "CNY": 6.3700,
}
FX_VOL = {
    "EUR": 0.07, "JPY": 0.12, "KRW": 0.10, "MXN": 0.15,
    "CAD": 0.07, "GBP": 0.08, "INR": 0.08, "BRL": 0.18, "CNY": 0.04,
}

date_range = [START_DATE + timedelta(days=d)
              for d in range((END_DATE - START_DATE).days + 1)]
DT = 1.0 / 365.0

rate_rows = []
rate_id   = 1
for ccy, r0 in FX_START.items():
    sigma  = FX_VOL[ccy]
    mu_dt  = -0.5 * sigma ** 2 * DT
    rate   = r0
    floor  = r0 * 0.50
    ceil_  = r0 * 2.00
    for d in date_range:
        z    = np.random.standard_normal()
        rate = rate * math.exp(mu_dt + sigma * math.sqrt(DT) * z)
        rate = max(floor, min(ceil_, rate))
        rate_rows.append(dict(
            rate_id       = rate_id,
            rate_date     = d.isoformat(),
            from_currency = "USD",
            to_currency   = ccy,
            exchange_rate = round(rate, 6),
            created_at    = NOW,
        ))
        rate_id += 1

df_fx = pd.DataFrame(rate_rows)
save_csv(df_fx, "raw_exchange_rates")


# =============================================================================
# 8. raw_sales_orders  (50,000 rows)
# 9. raw_sales_order_lines  (~75,000 rows)
# =============================================================================
print("Generating raw_sales_orders and raw_sales_order_lines...")

# Build FX lookup: (date_str, to_currency) → rate
fx_lookup: dict[tuple, float] = {
    (row["rate_date"], row["to_currency"]): row["exchange_rate"]
    for _, row in df_fx.iterrows()
}

# Build employee pool by territory
terr_to_emps: dict[int, list] = {}
for _, row in df_emp.iterrows():
    tid = int(row["territory_id"])
    terr_to_emps.setdefault(tid, []).append(int(row["employee_id"]))

# Customer arrays for weighted sampling
cust_ids   = df_cust["customer_id"].to_numpy()
cust_w_raw = df_cust["purchase_weight"].to_numpy()
cust_w_norm = cust_w_raw / cust_w_raw.sum()

# Lookups from customer_id
cust_territory = dict(zip(df_cust["customer_id"], df_cust["territory_id"]))
cust_currency  = dict(zip(df_cust["customer_id"], df_cust["currency_code"]))
cust_city      = dict(zip(df_cust["customer_id"], df_cust["city"]))
cust_state     = dict(zip(df_cust["customer_id"], df_cust["state_province"]))
cust_country   = dict(zip(df_cust["customer_id"], df_cust["country_code"]))

# Products by category
prod_by_cat: dict[int, list] = {}
for _, row in df_prod.iterrows():
    prod_by_cat.setdefault(int(row["category_id"]), []).append(dict(
        product_id        = int(row["product_id"]),
        list_price_usd    = float(row["list_price_usd"]),
        standard_cost_usd = float(row["standard_cost_usd"]),
    ))

CAPITAL_CATS    = [1, 2, 3, 4, 7, 9]
CONSUMABLE_CATS = [5, 6, 8, 10]

# Seasonal weight by quarter (monthly grain)
def quarter_weight(d: date) -> float:
    return {1: 0.22, 2: 0.26, 3: 0.24, 4: 0.28}[(d.month - 1) // 3 + 1]

date_weights = np.array([quarter_weight(d) for d in date_range])
date_weights /= date_weights.sum()

# Pre-sample date indices and customer IDs for all orders
sampled_date_idx = np.random.choice(len(date_range), size=NUM_ORDERS, p=date_weights)
sampled_cust_ids = np.random.choice(cust_ids, size=NUM_ORDERS, p=cust_w_norm)

STREET_NOUNS = ["Industrial","Commerce","Manufacturing","Enterprise"]
STREET_TYPES = ["Dr","Blvd","Ave","Pkwy"]

order_rows: list[dict] = []
line_rows:  list[dict] = []
line_id = 1

for oid in range(1, NUM_ORDERS + 1):
    order_date  = date_range[sampled_date_idx[oid - 1]]
    customer_id = int(sampled_cust_ids[oid - 1])
    territory_id= int(cust_territory[customer_id])
    ccy         = str(cust_currency[customer_id])
    date_str    = order_date.isoformat()

    fx_rate = fx_lookup.get((date_str, ccy), 1.0) if ccy != "USD" else 1.0

    emp_pool  = terr_to_emps.get(territory_id) or [1]
    employee_id = random.choice(emp_pool)

    is_capital = random.random() < 0.30

    if is_capital:
        n_lines       = random.choices([1, 2],       weights=[70, 30])[0]
        cats_pool     = CAPITAL_CATS
        qty_range     = (1, 1)
        disc_range    = (0.05, 0.20)
        lead_delta    = random.randint(30, 180)
    else:
        n_lines       = random.choices([1, 2, 3, 4, 5], weights=[30, 35, 20, 10, 5])[0]
        cats_pool     = CONSUMABLE_CATS
        qty_range     = (10, 300)
        disc_range    = (0.00, 0.15)
        lead_delta    = random.randint(1, 21)

    required_date = (order_date + timedelta(days=lead_delta + 7)).isoformat()
    shipped_offset = lead_delta + random.randint(-3, 14)
    shipped_date_raw = order_date + timedelta(days=max(1, shipped_offset))

    if shipped_date_raw <= END_DATE:
        status = "Cancelled" if random.random() < 0.02 else "Shipped"
        shipped_str = shipped_date_raw.isoformat() if status == "Shipped" else None
    else:
        status      = "Processing"
        shipped_str = None

    order_rows.append(dict(
        order_id         = oid,
        order_number     = f"SO-{order_date.year}-{oid:05d}",
        customer_id      = customer_id,
        employee_id      = employee_id,
        territory_id     = territory_id,
        order_date       = date_str,
        required_date    = required_date,
        shipped_date     = shipped_str,
        ship_address_line1 = f"{random.randint(100,9999)} {random.choice(STREET_NOUNS)} {random.choice(STREET_TYPES)}",
        ship_city        = cust_city[customer_id],
        ship_state_prov  = cust_state[customer_id],
        ship_country_code= cust_country[customer_id],
        currency_code    = ccy,
        status           = status,
        notes            = None,
        created_at       = NOW,
    ))

    for ln_num in range(1, n_lines + 1):
        cat  = random.choice(cats_pool)
        pool = prod_by_cat.get(cat, prod_by_cat[5])
        prod = random.choice(pool)

        qty  = 1 if is_capital else random.randint(*qty_range)
        disc = round(random.uniform(*disc_range), 4)

        price_usd   = prod["list_price_usd"]
        if ccy == "USD":
            unit_price = price_usd
        else:
            unit_price = round(price_usd * fx_rate, 2)
        unit_price = round(unit_price * random.uniform(0.95, 1.05), 2)  # ±5% negotiation
        ext_price  = round(qty * unit_price * (1 - disc), 2)

        line_rows.append(dict(
            line_id        = line_id,
            order_id       = oid,
            line_number    = ln_num,
            product_id     = prod["product_id"],
            quantity       = qty,
            unit_price     = unit_price,
            discount_pct   = disc,
            extended_price = ext_price,
            unit_cost_usd  = prod["standard_cost_usd"],
            created_at     = NOW,
        ))
        line_id += 1

df_orders = pd.DataFrame(order_rows)
df_lines  = pd.DataFrame(line_rows)

save_csv(df_orders, "raw_sales_orders")
save_csv(df_lines,  "raw_sales_order_lines")

# =============================================================================
# Summary
# =============================================================================
print()
print("=" * 55)
print("Data generation complete.")
print("=" * 55)
totals = {
    "raw_product_categories":  len(df_cat),
    "raw_products":            len(df_prod),
    "raw_territories":         len(df_terr),
    "raw_employees":           len(df_emp),
    "raw_currencies":          len(df_curr),
    "raw_customers":           len(df_cust),
    "raw_exchange_rates":      len(df_fx),
    "raw_sales_orders":        len(df_orders),
    "raw_sales_order_lines":   len(df_lines),
}
for tbl, cnt in totals.items():
    print(f"  {tbl:<30}  {cnt:>8,} rows")
print(f"\n  Output directory: {os.path.abspath(OUT_DIR)}")
