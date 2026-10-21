import os
import uuid
import unittest
from concurrent.futures import ThreadPoolExecutor

from pymongo import MongoClient

os.environ["MONGO_DATABASE"] = "seatsync_test_" + uuid.uuid4().hex

from venue import venue_server


class VenueBookingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = MongoClient(venue_server.MONGO_URI, serverSelectionTimeoutMS=3000)
        cls.database = cls.client[venue_server.MONGO_DATABASE]
        venue_server.initialize_seats()

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(venue_server.MONGO_DATABASE)
        cls.client.close()
        venue_server.mongo_client.close()

    def setUp(self):
        venue_server.seat_collection.update_one(
            {"_id": "A:A1"}, {"$set": {"owner": None}}
        )

    def test_initializes_all_seats(self):
        with venue_server.app.test_client() as client:
            response = client.get("/seats")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.get_json()["seats"]), 30)

    def test_only_one_concurrent_booking_succeeds(self):
        def book(index):
            with venue_server.app.test_client() as client:
                return client.post(
                    "/book", json={"seat": "A1", "user": f"racer-{index}"}
                ).status_code

        with ThreadPoolExecutor(max_workers=10) as workers:
            statuses = list(workers.map(book, range(10)))

        self.assertEqual(statuses.count(200), 1)
        self.assertEqual(statuses.count(409), 9)

    def test_booking_is_persisted_in_mongodb(self):
        with venue_server.app.test_client() as client:
            response = client.post(
                "/book", json={"seat": "A1", "user": "persistent-user"}
            )

        self.assertEqual(response.status_code, 200)
        saved = self.database.seats.find_one({"_id": "A:A1"})
        self.assertEqual(saved["owner"], "persistent-user")

    def test_cancel_releases_seat(self):
        venue_server.seat_collection.update_one(
            {"_id": "A:A1"}, {"$set": {"owner": "cancel-test"}}
        )

        with venue_server.app.test_client() as client:
            response = client.post("/cancel", json={"seat": "A1"})
            seats = client.get("/seats").get_json()["seats"]

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(seats["A1"])


if __name__ == "__main__":
    unittest.main()
