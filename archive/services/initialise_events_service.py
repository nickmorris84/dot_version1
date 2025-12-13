from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Dict, Iterable, List, Optional, Union
from datetime import datetime
import pandas as pd
from config.config_loader import load_yaml, deep_merge
from pathlib import Path
from ..core.io.event import Event
from ..core.transforms.standardiser import StandardiserPipeline
from ..core.transforms.normaliser import NormalizerPipeline
from ..services.data_quality_service import DataQualityService


class InitialiseEventsService:

    def __init__(self,
                 customer_id = "customer_a",
                 source = Union[str, Path, pd.DataFrame, Dict[str, Any], List[Dict[str, Any]]],
                 *,
                 read_csv_kwargs: Optional[Dict[str, Any]] = None
                 ) -> None:
        
        self.customer_id = customer_id
        self.source = source.copy()

    def run(self):
        self.get_config()
        df = self.convert_data_to_pd(source=self.source)
        df = NormalizerPipeline(self.final_cfg["data_config"]["normaliser"]).run(df=df)
        df = StandardiserPipeline(self.final_cfg["data_config"]["standardiser"]).run(df=df)
        df = DataQualityService(self.final_cfg["data_config"]["data_quality"]).run(df=df)
        self.df = df
        self.to_records()

    def convert_data_to_pd(self, 
                           source: Union[str, Path, pd.DataFrame, Dict[str, Any], list[Dict[str, Any]]],
                           read_csv_kwargs: Optional[Dict[str, Any]] = None
                           ) -> pd.DataFrame:
        """
        Convert various data source types to a pandas DataFrame.
        
        Args:
            source: A CSV file path, DataFrame, dict, or list of dicts.
            read_csv_kwargs: Optional dictionary of arguments to pass to pd.read_csv.

        Returns:
            pd.DataFrame: The resulting DataFrame.
        """
        read_csv_kwargs = read_csv_kwargs or {}

        if isinstance(source, (str, Path)):
            df = pd.read_csv(str(source), **read_csv_kwargs)
        elif isinstance(source, pd.DataFrame):
            df = source.copy()
        elif isinstance(source, dict):
            df = pd.DataFrame([source])
        elif isinstance(source, list) and (not source or isinstance(source[0], dict)):
            df = pd.DataFrame(source)
        else:
            raise TypeError("source must be CSV path/Path, DataFrame, dict, or list[dict]")

        return df

    def get_config(self):
        # load config
        base_cfg = load_yaml("config/base.yaml")
        cust_cfg = load_yaml(f"config/{self.customer_id}.yaml")
        self.final_cfg = deep_merge(base_cfg, cust_cfg)
        print(f' yaml fil: {self.final_cfg}')
        
    # ---------- dataset utilities ----------
    def metrics(self) -> Dict[str, Any]:
        numeric = self.df.select_dtypes(include="number")
        return {
            "rows": int(len(self.df)),
            "cols": list(self.df.columns),
            "nulls": self.df.isna().sum().to_dict(),
            "dtypes": {c: str(t) for c, t in self.df.dtypes.items()},
            "numeric_summary": (numeric.describe().to_dict() if not numeric.empty else {}),
        }
        
    # ---------- record materialization ----------
    def to_records(self) -> "InitialiseEventsService":
        self.records = [Event.from_dict(row._asdict()) for row in self.df.itertuples(index=False)]
        return self
