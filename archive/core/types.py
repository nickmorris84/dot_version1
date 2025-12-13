# types.py
from __future__ import annotations
from typing import Protocol, Iterable, Dict, Any, Callable, Sequence, List, Optional, Union
import pandas as pd
from dataclasses import dataclass, field, asdict
from datetime import datetime
from ..services.initialise_events_service import Event

Standardiser = Callable[[pd.DataFrame], pd.DataFrame]  # df -> df
DQStep = Callable[[pd.DataFrame], pd.DataFrame]  # df -> df
RowStep     = Callable[[Event], Event]                # record -> record
Classifier  = Callable[[Event], Any]                  # record -> label/value
