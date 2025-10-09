import asyncio, os, re, json, pathlib
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import pandas as pd

# LV-only page
URL = "https://bis.gov.lv/bisp/lv/planned_constructions"

# ------- FILTERS (exact strings) -------
ALL_AUTHORITIES = [
    "RĪGAS VALSTSPILSĒTAS PAŠVALDĪBAS PILSĒTAS ATTĪSTĪBAS DEPARTAMENTS",
    "Ādažu novada būvvalde",
    "Saulkrastu novada būvvalde",
    "Ropažu novada pašvaldības būvvalde",
    "Siguldas novada būvvalde",
    "Salaspils novada pašvaldības iestāde \"Salaspils novada Būvvalde\"",
    "Ogres novada pašvaldības centrālās administrācijas Ogres novada būvvalde",
    "Ķekavas novada pašvaldības būvvalde",
    "OLAINES NOVADA PAŠVALDĪBAS BŪVVALDE",
    "Mārupes novada Būvvalde",
    "Jūrmalas Būvvalde",
]

PHASES = [
    "Iecere",
    "Būvniecības ieceres publiskā apspriešana",
    "Projektēšanas nosacījumu izpilde",
    "Būvdarbu uzsākšanas nosacījumu izpilde",
]

TYPES = [
    "Atjaunošana",
    "Vienkāršota atjaunošana",
    "Jauna būvniecība",
    "Pārbūve",
    "Vienkāršota pārbūve",
]

INTENT_TYPES = ["Būvatļauja"]  # Ieceres veids

# Keep only usage codes starting with "1" (ĒKAS). If the column is absent, we keep the row.
USAGE_CODE_KEEP_PREFIX = "1"

# Cap pages per (authority × phase × type × intention); can be overridden via env
MAX_PAGES_PER_COMBO = int(os.getenv("MAX_PAGES_PER_COMBO", "50"))

# Shard control: list of authorities to run in THIS job (JSON array), else all
AUTHORITIES = json.loads(os.getenv("AU
