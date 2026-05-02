import time
import logging
from collections import defaultdict
from typing import Dict, List

logger = logging.getLogger(__name__)


class MetricsCollector:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.counters: Dict[str, int] = defaultdict(int)
        self.gauges: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = defaultdict(list)
        self.start_time = time.time()

    def increment(self, metric: str, value: int = 1):
        self.counters[metric] += value

    def set_gauge(self, metric: str, value: float):
        self.gauges[metric] = value

    def record_timing(self, metric: str, duration: float):
        self.histograms[metric].append(duration)
        if len(self.histograms[metric]) > 1000:
            self.histograms[metric] = self.histograms[metric][-1000:]

    def get_stats(self) -> dict:
        stats = {
            'node_id': self.node_id,
            'uptime_seconds': round(time.time() - self.start_time, 2),
            'counters': dict(self.counters),
            'gauges': dict(self.gauges),
            'histograms': {},
        }

        for metric, values in self.histograms.items():
            if not values:
                continue
            sorted_vals = sorted(values)
            n = len(sorted_vals)
            stats['histograms'][metric] = {
                'count': n,
                'min': round(sorted_vals[0], 6),
                'max': round(sorted_vals[-1], 6),
                'avg': round(sum(sorted_vals) / n, 6),
                'p50': round(sorted_vals[n // 2], 6),
                'p95': round(sorted_vals[int(n * 0.95)], 6),
                'p99': round(sorted_vals[int(n * 0.99)], 6),
            }

        return stats

    def reset(self):
        self.counters.clear()
        self.gauges.clear()
        self.histograms.clear()

    def __repr__(self) -> str:
        return f"<MetricsCollector node={self.node_id} counters={len(self.counters)}>"
