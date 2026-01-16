from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union
from pydantic import BaseModel, Field, ConfigDict, field_validator


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


# ---------- Shared / helpers ----------

class SchemaValidationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mode: Literal["report", "enforce"] = "report"
    input_source: Literal["records", "df"] = "records"
    attach_metric: bool = False
    reject_on_report: bool = False
    supported_versions: List[str] = Field(default_factory=list)


class RequiredColumnsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    columns: List[str] = Field(default_factory=list)


# ---------- Pipeline blocks ----------

class GateConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    extract_envelope: bool = False
    normalize_payload: bool = False
    idempotency_check: bool = False
    schema_validation: Optional[SchemaValidationConfig] = None


class NormalizeCaseConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    columns: str = "*"                      # "*" or comma-list etc (your convention)
    case: Literal["lower", "upper"] = "lower"


class NormalizerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    trim_strings: bool = False
    normalize_case: Optional[NormalizeCaseConfig] = None

    @field_validator("normalize_case", mode="before")
    @classmethod
    def _coerce_normalize_case(cls, v: Any) -> Any:
        # Allow normalize_case to be supplied as a dict (YAML map) — it already is,
        # but this also protects against odd types.
        if v is None:
            return None
        if isinstance(v, dict):
            return v
        raise TypeError("normalize_case must be a mapping like {columns: '*', case: 'lower'}")


class StandardizerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    to_snake_case: Optional[bool] = None
    rename: Dict[str, str] = Field(default_factory=dict)

    @field_validator("rename", mode="before")
    @classmethod
    def _coerce_rename(cls, v: Any) -> Any:
        # Support BOTH:
        # rename: {old: new}
        # rename: {mapping: {old: new}}
        if v is None:
            return {}
        if isinstance(v, dict) and "mapping" in v and isinstance(v["mapping"], dict):
            return v["mapping"]
        if isinstance(v, dict):
            return v
        raise TypeError("rename must be a mapping like {old: new} or {mapping: {old: new}}")


class ValidatorConfig(BaseModel):
    """
    Your YAML uses `validater:` (note spelling). We'll support it via aliasing in DataConfig.
    """
    model_config = ConfigDict(extra="forbid")
    check_required_columns: Optional[RequiredColumnsConfig] = None


class SchemaConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_validation_gate: Optional[SchemaValidationConfig] = None
    schema_validation_post_transformation: Optional[SchemaValidationConfig] = None


class ClassifierConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    params: Dict[str, Any] = Field(default_factory=dict)


class DataConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: Optional[str] = None

    gate: Optional[GateConfig] = None
    normalizer: Optional[NormalizerConfig] = None
    standardizer: StandardizerConfig = StandardizerConfig()

    validator: Optional[ValidatorConfig] = None

    schema: Optional[SchemaConfig] = None

    classifiers: List[ClassifierConfig] = Field(default_factory=list)

    @field_validator("validator", mode="before")
    @classmethod
    def _coerce_validator(cls, v: Any) -> Any:
        # allow providing `validator:` normally
        return v

    def effective_validator(self) -> Optional[ValidatorConfig]:
        # helper in runtime: prefer `validator`, fallback to `validater`
        return self.validator



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
