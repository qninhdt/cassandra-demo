import random
from datetime import datetime, timedelta

SENSORS = [
    "earth-363",
    "earth-9001",
    "earth-42",
    "earth-1337",
    "earth-2021",
    "earth-2022",
    "moon-4",
    "moon-1",
    "moon-90",
    "moon-72",
    "titan-1.2",
    "titan-2.2",
    "titan-3.2",
    "mars-99",
    "venus-1",
    "venus-2",
]

LOCATIONS = [
    "earth",
    "moon",
    "titan",
    "mars",
    "venus",
]

ENVIROMENTS = [
    "valley",
    "mountain",
    "plains",
    "abyss",
    "cave",
    "underground",
    "ozonosphere",
    "stratosphere",
    "troposphere",
    "mesosphere",
    "thermosphere",
    "exosphere",
    "ionosphere",
]


class BaseService:

    def connect(self):
        pass

    def disconnect(self):
        pass

    def create_reports(self, n):
        timestamps = self._generate_random_increasing_timestamps(n)

        # generate random data
        reports = [
            {
                "time": timestamps[i],
                "sensor": random.choice(SENSORS),
                "location": random.choice(LOCATIONS),
                "environment": random.choice(ENVIROMENTS),
                "temperature": random.uniform(-50, 50),
                "humidity": random.uniform(0, 100),
                "air_quality": random.uniform(0, 100),
                "pressure": random.uniform(900, 1100),
                "wind_speed": random.uniform(0, 100),
                "wind_direction": random.choice(["N", "E", "S", "W"]),
                "gust_speed": random.uniform(0, 100),
                "dew_point": random.uniform(-50, 50),
                "cloud_cover": random.uniform(0, 100),
                "visibility": random.uniform(0, 100),
                "precipitation_intensity": random.uniform(0, 100),
                "precipitation_type": random.choice(["rain", "snow", "hail"]),
                "uv_index": random.uniform(0, 100),
                "solar_radiation": random.uniform(0, 1000),
                "soil_temperature": random.uniform(-50, 50),
                "soil_moisture": random.uniform(0, 100),
            }
            for i in range(n)
        ]

        def generate():
            # insert data
            for report in reports:
                yield self.create_report(report)

        return generate()

    def _get_random_day(self, start, end):
        day = start + timedelta(days=random.randint(0, (end - start).days))
        return datetime(day.year, day.month, day.day)

    def _generate_random_increasing_timestamps(self, count):
        timestamps = []
        current = datetime.now()

        for _ in range(count):
            current += timedelta(seconds=random.uniform(1, 60))  # 1 minute to 5 minutes
            timestamps.append(current)

        return timestamps

    def get_reports_by_sensors(self, n):
        start, end = self.get_time_range()

        def generate():
            for i in range(n):
                for sensor in SENSORS:
                    random.seed(i)
                    day = self._get_random_day(start, end)
                    yield self.get_reports_by_sensor(sensor, day)

        return generate()

    def get_reports_by_locations(self, n):
        start, end = self.get_time_range()

        def generate():
            for i in range(n):
                for location in LOCATIONS:
                    random.seed(i)
                    day = self._get_random_day(start, end)
                    yield self.get_reports_by_location(location, day)

        return generate()

    def get_reports_by_environments(self, n):
        start, end = self.get_time_range()

        def generate():
            for i in range(n):
                for environment in ENVIROMENTS:
                    random.seed(i)
                    day = self._get_random_day(start, end)
                    yield self.get_reports_by_environment(environment, day)

        return generate()

    def create_report(self, report):
        pass

    def get_reports_by_sensor(self, sensor, day):
        pass

    def get_reports_by_location(self, location, day):
        pass

    def get_reports_by_environment(self, environment, day):
        pass

    def get_time_range(self):
        pass
