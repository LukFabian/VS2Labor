import unittest
import clientserver

# Assume the server is already running

class TestClientServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up a single client instance for all tests."""
        cls.client = clientserver.Client()

    @classmethod
    def tearDownClass(cls):
        """Close the client after all tests have run."""
        cls.client.close()

    def test_handle_get_existing_entry(self):
        """Test retrieving an existing entry from the server."""
        response = self.client.get("Alpha")
        self.assertEqual(response, "Alpha:1234567890")

    def test_handle_get_nonexistent_entry(self):
        """Test retrieving a non-existent entry from the server."""
        response = self.client.get("NonExistent")
        self.assertEqual(response, "NonExistent:NOT FOUND")

    def test_handle_getall_with_entries(self):
        """Test retrieving all entries from the server when there are entries."""
        response = self.client.get_all()
        expected = "Alpha:1234567890;Bravo:2345678901;Charlie:3456789012"
        self.assertEqual(response, expected)


if __name__ == "__main__":
    unittest.main()
