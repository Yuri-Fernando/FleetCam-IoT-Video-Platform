import unittest
from fleet import event

class FleetTest(unittest.TestCase):
    def test_identity_is_repeatable_and_distinct(self):
        self.assertEqual(event(1, "cam", 2, "healthy"), event(1, "cam", 2, "healthy"))
        self.assertNotEqual(event(1, "cam", 2, "healthy")["event_id"], event(1, "cam", 3, "healthy")["event_id"])

    def test_faults_are_explicitly_synthetic(self):
        self.assertTrue(event(1, "cam", 3, "sd_failure")["simulated"])
        self.assertGreater(event(1, "cam", 3, "sd_failure")["payload"]["sd"]["write_error_count"], 0)
        self.assertEqual(event(1, "cam", 3, "full_sd")["payload"]["sd"]["free_bytes"], 50)

if __name__ == "__main__": unittest.main()
