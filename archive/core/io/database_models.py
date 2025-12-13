"""
ORM models for Journey/SubProcess/Step + raw EventFact.
Unique constraints let us upsert without drama.
"""
from datetime import datetime
from sqlalchemy import String, Integer, DateTime, Float, ForeignKey, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .database_init import Base

class Journey(Base):
    __tablename__ = "journey"
    journey_id: Mapped[str] = mapped_column(String, primary_key=True)
    status: Mapped[str | None] = mapped_column(String, nullable=True)
    start_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    end_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    tat_minutes: Mapped[float | None] = mapped_column(Float, nullable=True)
    age_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_updated: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    steps = relationship("Step", back_populates="journey", cascade="all, delete-orphan")

class SubProcess(Base):
    __tablename__ = "sub_process"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    journey_id: Mapped[str] = mapped_column(String, ForeignKey("journey.journey_id"), index=True)
    name: Mapped[str] = mapped_column(String, index=True)
    status: Mapped[str | None] = mapped_column(String, nullable=True)
    start_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    end_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    __table_args__ = (UniqueConstraint("journey_id", "name", name="uq_subproc"),)

class Step(Base):
    __tablename__ = "step"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    journey_id: Mapped[str] = mapped_column(String, ForeignKey("journey.journey_id"), index=True)
    step_name: Mapped[str] = mapped_column(String, index=True)
    status: Mapped[str | None] = mapped_column(String, nullable=True)
    start_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    end_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    tat_minutes: Mapped[float | None] = mapped_column(Float, nullable=True)
    performed_by: Mapped[str | None] = mapped_column(String, nullable=True)
    issues_count: Mapped[int] = mapped_column(Integer, default=0)
    journey = relationship("Journey", back_populates="steps")
    __table_args__ = (UniqueConstraint("journey_id", "step_name", name="uq_step"),)

class EventFact(Base):
    __tablename__ = "event_fact"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    journey_id: Mapped[str] = mapped_column(String, index=True)
    sub_process: Mapped[str | None] = mapped_column(String, nullable=True)
    step_name: Mapped[str | None] = mapped_column(String, nullable=True)
    event_ts: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    stage_start_ts: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    stage_end_ts: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status_after: Mapped[str | None] = mapped_column(String, nullable=True)
    performed_by: Mapped[str | None] = mapped_column(String, nullable=True)
    risk_grade: Mapped[str | None] = mapped_column(String, nullable=True)
    uw_decision: Mapped[str | None] = mapped_column(String, nullable=True)
    issue_flag: Mapped[str | None] = mapped_column(String, nullable=True)
    issue_code: Mapped[str | None] = mapped_column(String, nullable=True)
