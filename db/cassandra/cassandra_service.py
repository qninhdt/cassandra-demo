from cassandra.cluster import Cluster
from cassandra.query import PreparedStatement
from db.base_service import BaseService
from uuid import uuid1
import datetime
import cassandra


class CassandraService(BaseService):

    def connect(self):
        self.cluster = Cluster(["cassandra"])
        self.session = self.cluster.connect()
        self.session.execute("USE cassandra_demo;")

        self.reset()

        self.CREATE_REPORT = self.session.prepare(
            """
            BEGIN BATCH
                INSERT INTO report_by_sensor (sensor, date, time, temperature, humidity, air_quality, pressure, wind_speed, wind_direction, gust_speed, dew_point, cloud_cover, visibility, precipitation_intensity, precipitation_type, uv_index, solar_radiation, soil_temperature, soil_moisture)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

              
            APPLY BATCH;
            """
        )

        self.GET_REPORTS_BY_SENSOR = self.session.prepare(
            """
            SELECT *
            FROM report_by_sensor
            WHERE sensor = ? AND date = ?;
            """
        )

        self.GET_REPORTS_BY_LOCATION = self.session.prepare(
            """
            SELECT *
            FROM report_by_location
            WHERE location = ? AND date = ?;
            """
        )

        self.GET_REPORTS_BY_ENVIRONMENT = self.session.prepare(
            """
            SELECT *
            FROM report_by_environment
            WHERE environment = ? AND date = ?;
            """
        )

        print("Connected to Cassandra database")

    def disconnect(self):
        print("Disconnected from Cassandra database")
        self.cluster.shutdown()

    def get_time_range(self):
        start, end = self.session.execute(
            "SELECT MIN(time), MAX(time) FROM report_by_sensor;"
        ).one()

        return start, end

    def get_reports_by_sensor(self, sensor, day):
        day = day.strftime("%Y-%m-%d")

        self.session.execute(
            self.GET_REPORTS_BY_SENSOR,
            (sensor, day),
        )

    def get_reports_by_location(self, location, day):
        day = day.strftime("%Y-%m-%d")

        return self.session.execute(
            self.GET_REPORTS_BY_LOCATION,
            (location, day),
        ).all()

    def get_reports_by_environment(self, environment, day):
        day = day.strftime("%Y-%m-%d")

        return self.session.execute(
            self.GET_REPORTS_BY_ENVIRONMENT,
            (environment, day),
        ).all()

    def create_report(self, report):

        report["date"] = report["time"].strftime("%Y-%m-%d")

        # convert datetime to cassandra.cqltypes.TimeUUIDType
        self.session.execute(
            self.CREATE_REPORT,
            (
                report["sensor"],
                report["date"],
                report["time"],
                report["temperature"],
                report["humidity"],
                report["air_quality"],
                report["pressure"],
                report["wind_speed"],
                report["wind_direction"],
                report["gust_speed"],
                report["dew_point"],
                report["cloud_cover"],
                report["visibility"],
                report["precipitation_intensity"],
                report["precipitation_type"],
                report["uv_index"],
                report["solar_radiation"],
                report["soil_temperature"],
                report["soil_moisture"],
                # report["location"],
                # report["date"],
                # report["time"],
                # report["temperature"],
                # report["humidity"],
                # report["air_quality"],
                # report["pressure"],
                # report["wind_speed"],
                # report["wind_direction"],
                # report["gust_speed"],
                # report["dew_point"],
                # report["cloud_cover"],
                # report["visibility"],
                # report["precipitation_intensity"],
                # report["precipitation_type"],
                # report["uv_index"],
                # report["solar_radiation"],
                # report["soil_temperature"],
                # report["soil_moisture"],
                # report["environment"],
                # report["date"],
                # report["time"],
                # report["temperature"],
                # report["humidity"],
                # report["air_quality"],
                # report["pressure"],
                # report["wind_speed"],
                # report["wind_direction"],
                # report["gust_speed"],
                # report["dew_point"],
                # report["cloud_cover"],
                # report["visibility"],
                # report["precipitation_intensity"],
                # report["precipitation_type"],
                # report["uv_index"],
                # report["solar_radiation"],
                # report["soil_temperature"],
                # report["soil_moisture"],
            ),
        )

    def reset(self):
        self.session.execute("DROP TABLE IF EXISTS report_by_sensor;")
        self.session.execute("DROP TABLE IF EXISTS report_by_location;")
        self.session.execute("DROP TABLE IF EXISTS report_by_environment;")

        data_fields = """
            temperature DOUBLE,
            humidity DOUBLE,
            air_quality DOUBLE,
            pressure DOUBLE,
            wind_speed DOUBLE,
            wind_direction TEXT,
            gust_speed DOUBLE,
            dew_point DOUBLE,
            cloud_cover DOUBLE,
            visibility DOUBLE,
            precipitation_intensity DOUBLE,
            precipitation_type TEXT,
            uv_index DOUBLE,
            solar_radiation DOUBLE,
            soil_temperature DOUBLE,
            soil_moisture DOUBLE
        """

        # create table
        report_by_sensor_query = f"""
            CREATE TABLE IF NOT EXISTS report_by_sensor(
                sensor TEXT,
                time TIMESTAMP,
                date TEXT,
                {data_fields},
                PRIMARY KEY ((sensor, date))
            );
        """

        report_by_location_query = f"""
            CREATE TABLE IF NOT EXISTS report_by_location (
                location TEXT,
                time TIMESTAMP,
                date TEXT,
                {data_fields},
                PRIMARY KEY ((location, date))
            );
        """

        report_by_environment_query = f"""
            CREATE TABLE IF NOT EXISTS report_by_environment (
                environment TEXT,
                time TIMESTAMP,
                date TEXT,
                {data_fields},
                PRIMARY KEY ((environment, date))
            );
        """

        self.session.execute(report_by_sensor_query)
        self.session.execute(report_by_location_query)
        self.session.execute(report_by_environment_query)
