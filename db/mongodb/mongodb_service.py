from db.base_service import BaseService
from datetime import datetime
import pymongo
from datetime import timedelta


class MongoDBService(BaseService):

    def connect(self):
        self.client = pymongo.MongoClient("mongodb://mongodb:27017")
        self.db = self.client["cassandra_demo"]

        self.report = self.db["report"]
        self.reset()

        print("Connected to MongoDB database")

    def disconnect(self):
        print("Disconnected from MongoDB database")
        self.db.close()

    def create_report(self, report):
        report["date"] = report["time"].strftime("%Y-%m-%d")
        self.report.insert_one(report)

    def get_reports_by_sensor(self, sensor, day):
        reports = self.report.find(
            {
                "sensor": sensor,
                "date": day.strftime("%Y-%m-%d"),
            }
        )

        return list(reports)

    def get_reports_by_location(self, location, day):
        reports = self.report.find(
            {
                "location": location,
                "date": day.strftime("%Y-%m-%d"),
            }
        )

        return list(reports)

    def get_reports_by_environment(self, environment, day):
        reports = self.report.find(
            {
                "environment": environment,
                "date": day.strftime("%Y-%m-%d"),
            }
        )
        return list(reports)

    def get_time_range(self):
        first = self.report.find_one(sort=[("time", pymongo.ASCENDING)])
        last = self.report.find_one(sort=[("time", pymongo.DESCENDING)])

        return first["time"], last["time"]

    def reset(self):
        self.report.drop()

        self.report = self.db["report"]
        self.report.create_index("location")
        self.report.create_index("sensor")
        self.report.create_index("environment")
        self.report.create_index("date")
