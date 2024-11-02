import random
import string
import unittest
import clientserver


# Assume the server is already running

class TestServer(clientserver.Server):
    def __init__(self):
        # In-memory telephone directory
        self.directory = {
            "Alpha": "1234567890",
            "Bravo": "2345678901",
            "Charlie": "3456789012"
        }


def generate_random_name():
    """Generate a random name for testing."""
    return ''.join(random.choices(string.ascii_letters, k=8))


def generate_random_phone_number():
    """Generate a random phone number for testing."""
    return ''.join(random.choices(string.digits, k=10))


class TestClientServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up a single client instance for all tests."""
        cls.client = clientserver.Client()

    def setUp(self):
        self.server = TestServer()

    @classmethod
    def tearDownClass(cls):
        """Close the client after all tests have run."""
        cls.client.close()

    def test_get_existing_entry(self):
        """Test retrieving an existing entry from the server."""
        response = self.client.get("Alpha")
        self.assertEqual(response, "Alpha:1234567890")

    def test_get_nonexistent_entry(self):
        """Test retrieving a non-existent entry from the server."""
        response = self.client.get("NonExistent")
        self.assertEqual(response, "NonExistent:NOT FOUND")

    def test_getall_with_entries(self):
        """Test retrieving all entries from the server when there are entries."""
        response = self.client.get_all()
        expected = "Alpha:1234567890;Bravo:2345678901;Charlie:3456789012"
        self.assertEqual(response, expected)

    def test_add_500_entries(self):
        """Stress test with 500 dynamically generated entries."""
        for _ in range(500):
            name = generate_random_name()
            phone = generate_random_phone_number()
            self.server.directory[name] = phone

        # Verify that we indeed have 500 entries, plus the initial three entries
        self.assertEqual(len(self.server.directory), 503, "The directory should contain 503 entries after adding 500.")

        # Confirm entries are correct by sampling a few random entries
        for name, phone in list(self.server.directory.items())[:5]:
            self.assertEqual(self.server.directory[name], phone, f"Entry {name} has an incorrect phone number.")


    def test_handle_get_existing_entry(self):
        """Test retrieving an existing entry using handle_get."""
        response = self.server.handle_get("Alpha")
        self.assertEqual(response, "Alpha:1234567890", "Expected to retrieve entry for Alpha.")

    def test_handle_get_nonexistent_entry(self):
        """Test retrieving a non-existent entry using handle_get."""
        response = self.server.handle_get("NonExistent")
        self.assertEqual(response, "NonExistent:NOT FOUND", "Expected NOT FOUND for a non-existent entry.")

    def test_handle_getall_with_entries(self):
        """Test retrieving all entries using handle_getall when entries are present."""
        response = self.server.handle_getall()
        expected = "Alpha:1234567890;Bravo:2345678901;Charlie:3456789012"
        self.assertEqual(response, expected, "Expected all directory entries in the specified format.")

    def test_handle_getall_empty_directory(self):
        """Test retrieving all entries using handle_getall when directory is empty."""
        self.server.directory.clear()  # Clear the directory to simulate empty state
        response = self.server.handle_getall()

        self.assertEqual(response, "EMPTY", "Expected EMPTY for an empty directory.")


if __name__ == "__main__":
    unittest.main()
