from datetime import datetime, date
from sqlalchemy import (
    Column, Integer, String, Text, Date, DateTime, Boolean,
    ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class ListIQSnapshot(Base):
    __tablename__ = "listiq_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    list_name = Column(String, nullable=False)
    snapshot_date = Column(Date, nullable=False)
    raw_file_hash = Column(String, nullable=True)
    record_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (UniqueConstraint("list_name", "snapshot_date"),)

    records = relationship("ListIQRecord", back_populates="snapshot", cascade="all, delete-orphan")


class ListIQRecord(Base):
    __tablename__ = "listiq_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_id = Column(Integer, ForeignKey("listiq_snapshots.id"), nullable=False)
    list_name = Column(String, nullable=False)
    record_uid = Column(String, nullable=False)
    record_type = Column(String)
    primary_name = Column(String)
    akas = Column(Text, default="[]")        # JSON array
    ids = Column(Text, default="[]")         # JSON array
    addresses = Column(Text, default="[]")   # JSON array
    programs = Column(Text, default="[]")    # JSON array
    raw_data = Column(Text)                  # Full JSON
    snapshot_date = Column(Date)             # denormalized

    snapshot = relationship("ListIQSnapshot", back_populates="records")


class ListIQChange(Base):
    __tablename__ = "listiq_changes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    list_name = Column(String, nullable=False)
    change_date = Column(Date, nullable=False)
    record_uid = Column(String, nullable=False)
    change_type = Column(String, nullable=False)   # ADDITION, DELETION, MODIFICATION
    modification_fields = Column(Text, default="[]")  # JSON array
    before_data = Column(Text)   # JSON (null for additions)
    after_data = Column(Text)    # JSON (null for deletions)
    created_at = Column(DateTime, default=datetime.utcnow)


class ListIQConfig(Base):
    __tablename__ = "listiq_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String, unique=True, nullable=False)
    value = Column(Text)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
