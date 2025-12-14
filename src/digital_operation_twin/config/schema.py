from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, ConfigDict


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    debug: bool = False
    host: str = "0.0.0.0"
    port: int = Field(default=8000, ge=1, le=65535)


class UploadConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    dir: str = "./uploads"
    max_bytes: int = Field(default=25 * 1024 * 1024, ge=0)


class CsvConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    allowed_mime_types: List[str] = Field(default_factory=lambda: ["text/csv"])


class SecurityConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    # Keep secrets out of YAML in real deployments; inject via env / secrets manager.
    csv_api_token: Optional[str] = None


class DatabaseConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    url: str = "sqlite:///data/app.db"
    echo: bool = False


class StandardiserConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    to_snake_case: Optional[bool] = None
    # Column rename mapping
    rename: Dict[str, str] = Field(default_factory=dict)


class DQConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    dataset_steps: List[str] = Field(default_factory=list)
    row_steps: List[str] = Field(default_factory=list)


class ClassifierConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    params: Dict[str, Any] = Field(default_factory=dict)


class DataConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source: Optional[str] = None
    standardiser: StandardiserConfig = StandardiserConfig()
    dq: DQConfig = DQConfig()
    classifiers: List[ClassifierConfig] = Field(default_factory=list)
    # Allow additional config blocks that your pipeline expects today without breaking.
    # You can gradually type these later.
    extra: Dict[str, Any] = Field(default_factory=dict)


class Settings(BaseModel):
    """Final merged settings (base + env + customer)."""
    model_config = ConfigDict(extra="forbid")

    # Identify tenant/customer
    customer_id: str

    # Runtime / service settings
    app: AppConfig = AppConfig()
    upload: UploadConfig = UploadConfig()
    csv: CsvConfig = CsvConfig()
    security: SecurityConfig = SecurityConfig()
    database: DatabaseConfig = DatabaseConfig()

    # Pipeline configuration
    data_config: DataConfig = DataConfig()
