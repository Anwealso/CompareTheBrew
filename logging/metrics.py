"""
Metrics Logging System

Provides a simple interface for tracking application metrics in the metrics database table.

Database Schema:
    metric_name: TEXT NOT NULL
    key: TEXT (NULL for single metrics, string for keyed metrics)
    value: INTEGER DEFAULT 0

Usage:
    from logging import Metric, ListMetric

    # Single-value metric (e.g., num_clicks)
    clicks = Metric("num_clicks")
    clicks.increment()  # +1 to default row
    clicks.decrement()  # -1 from default row
    clicks.get_value()  # returns current value

    # Multi-key metric (e.g., search_keyword_frequency)
    search_terms_metric = ListMetric("search_keyword_frequency")
    search_terms_metric.increment("beer")  # +1 for key "beer"
    search_terms_metric.decrement("beer") # -1 for key "beer"
    search_terms_metric.get_value("beer") # get value for specific key
    search_terms_metric.get_all_keys()    # returns dict of all keys/values

Defining New Metrics:
    Add to METRICS list in AbstractMetric:
        {"metric_name": "metric_name", "has_multiple_keys": False}
"""

import sqlite3
from pathlib import Path


class AbstractMetric:
    METRICS = [
        {"metric_name": "search_keyword_frequency", "has_multiple_keys": True},
    ]

    @classmethod
    def is_valid_metric(cls, metric_name: str) -> bool:
        return any(m["metric_name"] == metric_name for m in cls.METRICS)

    @classmethod
    def get_metric_config(cls, metric_name: str) -> dict | None:
        for m in cls.METRICS:
            if m["metric_name"] == metric_name:
                return m
        return None

    @classmethod
    def get_metrics(cls):
        return cls.METRICS.copy()

    @classmethod
    def _get_connection(cls):
        conn = sqlite3.connect(str(Path(__file__).parent.parent / "db" / "database.db"))
        return conn

    @classmethod
    def _ensure_metric_row(cls, metric_name: str, key=None):
        conn = cls._get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO metrics (metric_name, key, value) VALUES (?, ?, 0) "
            "ON CONFLICT(metric_name, key) DO NOTHING",
            (metric_name, key)
        )
        conn.commit()
        conn.close()


class Metric(AbstractMetric):
    def __init__(self, metric_name: str):
        if not self.is_valid_metric(metric_name):
            raise ValueError(f"Invalid metric: {metric_name}")
        config = self.get_metric_config(metric_name)
        if config and config.get("has_multiple_keys"):
            raise ValueError(f"Metric '{metric_name}' is defined as multi-key, use ListMetric instead")
        self.metric_name = metric_name
        self._ensure_metric_row(metric_name, None)

    def increment(self):
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO metrics (metric_name, key, value) VALUES (?, NULL, 1) "
            "ON CONFLICT(metric_name, key) DO UPDATE SET value = value + 1",
            (self.metric_name,)
        )
        conn.commit()
        conn.close()

    def decrement(self):
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE metrics SET value = value - 1 WHERE metric_name = ? AND key IS NULL",
            (self.metric_name,)
        )
        conn.commit()
        conn.close()

    def get_value(self):
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT value FROM metrics WHERE metric_name = ? AND key IS NULL",
            (self.metric_name,)
        )
        row = cur.fetchone()
        conn.close()
        return row[0] if row else 0


class ListMetric(AbstractMetric):
    def __init__(self, metric_name: str):
        if not self.is_valid_metric(metric_name):
            raise ValueError(f"Invalid metric: {metric_name}")
        config = self.get_metric_config(metric_name)
        if config and not config.get("has_multiple_keys"):
            raise ValueError(f"Metric '{metric_name}' is defined as single-key, use Metric instead")
        self.metric_name = metric_name
        self._ensure_metric_row(metric_name, None)

    def increment(self, key: str):
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO metrics (metric_name, key, value) VALUES (?, ?, 1) "
            "ON CONFLICT(metric_name, key) DO UPDATE SET value = value + 1",
            (self.metric_name, key)
        )
        conn.commit()
        conn.close()

    def decrement(self, key: str):
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE metrics SET value = value - 1 WHERE metric_name = ? AND key = ?",
            (self.metric_name, key)
        )
        conn.commit()
        conn.close()

    def get_value(self, key: str):
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT value FROM metrics WHERE metric_name = ? AND key = ?",
            (self.metric_name, key)
        )
        row = cur.fetchone()
        conn.close()
        return row[0] if row else 0

    def get_all_keys(self):
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT key, value FROM metrics WHERE metric_name = ? AND key IS NOT NULL",
            (self.metric_name,)
        )
        rows = cur.fetchall()
        conn.close()
        return {row[0]: row[1] for row in rows}