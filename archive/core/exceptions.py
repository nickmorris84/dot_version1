from typing import Any, Callable, Dict, Iterable, List, Optional, Union
from datetime import datetime
import pandas as pd

class DQError(Exception):
    def __init__(self, code: str, message: str, details: Dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}