from __future__ import annotations
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Iterable, Iterator
from psycopg.types.json import Jsonb
from base import Connector, Fetched, Reject
from kk import fetch_stations

stations = fetch_stations()
