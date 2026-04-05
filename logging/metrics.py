import sqlite3
from pathlib import Path


class AbstractMetric:
    _metrics = []

    @classmethod
    def register_metric(cls, metric_name: str, has_multiple_keys: bool):
        cls._metrics.append({
            "metric_name": metric_name,
            "has_multiple_keys": has_multiple_keys
        })

    @classmethod
    def get_metrics(cls):
        return cls._metrics.copy()

    @classmethod
    def _get_connection(cls):
        conn = sqlite3.connect(str(Path(__file__).parent.parent / "db" / "database.db"))
        return conn

    @classmethod
    def _ensure_metric_row(cls, metric_name: str, key=None):
        conn = cls._get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO counters (metric_name, key, value) VALUES (?, ?, 0) "
            "ON CONFLICT(metric_name, key) DO NOTHING",
            (metric_name, key)
        )
        conn.commit()
        conn.close()


class Metric(AbstractMetric):
    def __init__(self, metric_name: str):
        self.metric_name = metric_name
        self._ensure_metric_row(metric_name, None)

    def increment(self):
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO counters (metric_name, key, value) VALUES (?, NULL, 1) "
            "ON CONFLICT(metric_name, key) DO UPDATE SET value = value + 1",
            (self.metric_name,)
        )
        conn.commit()
        conn.close()

    def decrement(self):
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE counters SET value = value - 1 WHERE metric_name = ? AND key IS NULL",
            (self.metric_name,)
        )
        conn.commit()
        conn.close()

    def get_value(self):
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT value FROM counters WHERE metric_name = ? AND key IS NULL",
            (self.metric_name,)
        )
        row = cur.fetchone()
        conn.close()
        return row[0] if row else 0


class ListMetric(AbstractMetric):
    def __init__(self, metric_name: str):
        self.metric_name = metric_name
        self._ensure_metric_row(metric_name, None)

    def increment(self, key: str):
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO counters (metric_name, key, value) VALUES (?, ?, 1) "
            "ON CONFLICT(metric_name, key) DO UPDATE SET value = value + 1",
            (self.metric_name, key)
        )
        conn.commit()
        conn.close()

    def decrement(self, key: str):
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE counters SET value = value - 1 WHERE metric_name = ? AND key = ?",
            (self.metric_name, key)
        )
        conn.commit()
        conn.close()

    def get_value(self, key: str):
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT value FROM counters WHERE metric_name = ? AND key = ?",
            (self.metric_name, key)
        )
        row = cur.fetchone()
        conn.close()
        return row[0] if row else 0

    def get_all_keys(self):
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT key, value FROM counters WHERE metric_name = ? AND key IS NOT NULL",
            (self.metric_name,)
        )
        rows = cur.fetchall()
        conn.close()
        return {row[0]: row[1] for row in rows}


Metric.register_metric("num_clicks", has_multiple_keys=False)
ListMetric.register_metric("search_keyword_frequency", has_multiple_keys=True)