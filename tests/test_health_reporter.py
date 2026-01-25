import time
import unittest

from bot.health.reporter import HealthReporter


class FakeRpc:
    def __init__(self):
        self.calls = []
        self.fail_next = False

    def upsert_bot_health_evidence(self, bot_id, patch):
        self.calls.append((bot_id, patch))
        if self.fail_next:
            self.fail_next = False
            return False, 0.0
        return True, 1.0


class FakeReporterTests(unittest.TestCase):
    def setUp(self):
        self.rpc = FakeRpc()
        self.reporter = HealthReporter("bot", self.rpc, tier="standard", in_position=False)

    def test_throttle_respects_interval(self):
        self.reporter._last_flush_ts = time.monotonic() - 1000
        self.reporter.maybe_flush()
        self.assertEqual(len(self.rpc.calls), 1)
        prev = len(self.rpc.calls)
        self.reporter._last_flush_ts = time.monotonic()
        self.reporter.maybe_flush()
        self.assertEqual(len(self.rpc.calls), prev)

    def test_flush_now_respects_debounce(self):
        self.reporter._last_flush_ts = time.monotonic() - 1000
        self.reporter.flush_now("critical")
        self.assertEqual(len(self.rpc.calls), 1)

    def test_flush_now_schedules_when_too_soon(self):
        self.reporter._last_flush_ts = time.monotonic()
        self.reporter.flush_now("too_soon")
        self.assertEqual(len(self.rpc.calls), 0)
        self.reporter._scheduled_flush_ts = time.monotonic()
        self.reporter.maybe_flush()
        self.assertEqual(len(self.rpc.calls), 1)

    def test_pending_patch_preserved_on_failure(self):
        self.reporter._last_flush_ts = time.monotonic() - 1000
        self.reporter._pending_patch["foo"] = "bar"
        self.rpc.fail_next = True
        self.reporter.flush_now("fail")
        self.assertGreaterEqual(len(self.rpc.calls), 1)
        self.assertEqual(self.reporter._pending_patch.get("foo"), "bar")


if __name__ == "__main__":
    unittest.main()
