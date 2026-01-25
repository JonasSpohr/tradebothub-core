import unittest

from bot.health.window import HealthWindow


class HealthWindowTests(unittest.TestCase):
    def test_counts_increment(self):
        window = HealthWindow(duration_seconds=60)
        window.inc("decision", timestamp=100.0)
        window.inc("decision", timestamp=101.0)
        self.assertEqual(window.count15m("decision", now=120.0), 2)
        window.inc("order_reject", timestamp=120.0)
        self.assertEqual(window.count15m("order_reject", now=130.0), 1)

    def test_prunes_old_entries(self):
        window = HealthWindow(duration_seconds=60)
        window.inc("decision", timestamp=0.0)
        window.inc("decision", timestamp=61.0)
        self.assertEqual(window.count15m("decision", now=121.0), 1)


if __name__ == "__main__":
    unittest.main()
