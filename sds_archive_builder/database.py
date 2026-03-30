"""SQLAlchemy models and session management for the archive tracking database."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Generator, Optional

from sqlalchemy import (
    Boolean, Date, DateTime, Float, Integer, String, Text,
    UniqueConstraint, create_engine, func, select
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

logger = logging.getLogger(__name__)


# ── ORM Models ────────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


class Station(Base):
    """One row per unique SEED channel ID (net.sta.loc.cha)."""
    __tablename__ = "stations"
    __table_args__ = (
        UniqueConstraint("network", "station", "location", "channel",
                         name="uq_seed_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    network: Mapped[str] = mapped_column(String(8), nullable=False)
    station: Mapped[str] = mapped_column(String(8), nullable=False)
    location: Mapped[str] = mapped_column(String(4), nullable=False, default="")
    channel: Mapped[str] = mapped_column(String(4), nullable=False)
    latitude: Mapped[Optional[float]] = mapped_column(Float)
    longitude: Mapped[Optional[float]] = mapped_column(Float)
    elevation: Mapped[Optional[float]] = mapped_column(Float)
    # Metadata epoch — treated as a hint only, not ground truth for data availability
    start_date: Mapped[Optional[date]] = mapped_column(Date)
    end_date: Mapped[Optional[date]] = mapped_column(Date)
    # True if within the instance's operational geo_bounds (excluding buffer)
    in_geo_bounds: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_inventory_sync: Mapped[Optional[datetime]] = mapped_column(DateTime)

    def seed_id(self) -> str:
        return f"{self.network}.{self.station}.{self.location}.{self.channel}"

    def __repr__(self) -> str:
        return f"<Station {self.seed_id()}>"


class FetchRequest(Base):
    """One row per (SEED channel, day, server) fetch attempt."""
    __tablename__ = "fetch_requests"
    __table_args__ = (
        UniqueConstraint("network", "station", "location", "channel", "day", "server",
                         name="uq_request"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    network: Mapped[str] = mapped_column(String(8), nullable=False)
    station: Mapped[str] = mapped_column(String(8), nullable=False)
    location: Mapped[str] = mapped_column(String(4), nullable=False, default="")
    channel: Mapped[str] = mapped_column(String(4), nullable=False)
    day: Mapped[date] = mapped_column(Date, nullable=False)
    server: Mapped[str] = mapped_column(String(256), nullable=False)

    # pending | success | no_data | error | rate_limited
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_attempt: Mapped[Optional[datetime]] = mapped_column(DateTime)
    bytes_written: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    # When to next attempt this request (set for no_data and error statuses)
    retry_after: Mapped[Optional[date]] = mapped_column(Date)

    def seed_id(self) -> str:
        return f"{self.network}.{self.station}.{self.location}.{self.channel}"

    def __repr__(self) -> str:
        return f"<FetchRequest {self.seed_id()} {self.day} via {self.server} [{self.status}]>"


class ServerHealth(Base):
    """Adaptive health state per FDSN server."""
    __tablename__ = "server_health"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    server: Mapped[str] = mapped_column(String(256), unique=True, nullable=False)
    last_success: Mapped[Optional[datetime]] = mapped_column(DateTime)
    last_failure: Mapped[Optional[datetime]] = mapped_column(DateTime)
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    requests_today: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    requests_today_date: Mapped[Optional[date]] = mapped_column(Date)
    backoff_until: Mapped[Optional[datetime]] = mapped_column(DateTime)

    def is_backed_off(self) -> bool:
        if self.backoff_until is None:
            return False
        return datetime.utcnow() < self.backoff_until

    def today_request_count(self) -> int:
        """Return requests made today (resets if date has changed)."""
        today = date.today()
        if self.requests_today_date != today:
            return 0
        return self.requests_today

    def __repr__(self) -> str:
        return f"<ServerHealth {self.server} failures={self.consecutive_failures}>"


# ── Engine / Session ──────────────────────────────────────────────────────────

def get_engine(db_path: Path):
    """Create a SQLAlchemy engine for the given SQLite path."""
    url = f"sqlite:///{db_path}"
    engine = create_engine(url, connect_args={"check_same_thread": False})
    return engine


def init_db(db_path: Path):
    """Create all tables if they don't exist. Safe to call on existing DB."""
    engine = get_engine(db_path)
    Base.metadata.create_all(engine)
    logger.info("Database ready: %s", db_path)
    return engine


@contextmanager
def session_scope(engine) -> Generator[Session, None, None]:
    """Provide a transactional session scope."""
    SessionFactory = sessionmaker(bind=engine)
    session = SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ── Query Helpers ─────────────────────────────────────────────────────────────

def get_or_create_server_health(session: Session, server: str) -> ServerHealth:
    """Return existing ServerHealth row or create a new one."""
    row = session.execute(
        select(ServerHealth).where(ServerHealth.server == server)
    ).scalar_one_or_none()
    if row is None:
        row = ServerHealth(server=server)
        session.add(row)
        session.flush()
    return row


def upsert_station(session: Session, **kwargs) -> Station:
    """Insert or update a station row by SEED ID."""
    row = session.execute(
        select(Station).where(
            Station.network == kwargs["network"],
            Station.station == kwargs["station"],
            Station.location == kwargs["location"],
            Station.channel == kwargs["channel"],
        )
    ).scalar_one_or_none()

    if row is None:
        row = Station(**kwargs)
        session.add(row)
    else:
        for k, v in kwargs.items():
            setattr(row, k, v)
    session.flush()
    return row


def get_fetch_request(
    session: Session,
    network: str,
    station: str,
    location: str,
    channel: str,
    day: date,
    server: str,
) -> Optional[FetchRequest]:
    return session.execute(
        select(FetchRequest).where(
            FetchRequest.network == network,
            FetchRequest.station == station,
            FetchRequest.location == location,
            FetchRequest.channel == channel,
            FetchRequest.day == day,
            FetchRequest.server == server,
        )
    ).scalar_one_or_none()


def upsert_fetch_request(session: Session, **kwargs) -> FetchRequest:
    """Insert or update a FetchRequest row."""
    row = get_fetch_request(
        session,
        kwargs["network"], kwargs["station"], kwargs["location"],
        kwargs["channel"], kwargs["day"], kwargs["server"],
    )
    if row is None:
        row = FetchRequest(**kwargs)
        session.add(row)
    else:
        for k, v in kwargs.items():
            setattr(row, k, v)
    session.flush()
    return row


def get_stations_in_bounds(session: Session, network: Optional[str] = None) -> list[Station]:
    """Return all stations flagged as within geo bounds, optionally filtered by network."""
    q = select(Station).where(Station.in_geo_bounds == True)
    if network:
        q = q.where(Station.network == network)
    return list(session.execute(q).scalars())


def get_due_retries(session: Session, today: date) -> list[FetchRequest]:
    """Return all requests where retry_after <= today and status is not success."""
    return list(session.execute(
        select(FetchRequest).where(
            FetchRequest.status.in_(["no_data", "error", "rate_limited"]),
            FetchRequest.retry_after <= today,
        )
    ).scalars())
