from db import MongoDBService, CassandraService
import streamlit as st
from timeit import default_timer as timer


@st.cache_resource
def get_service():
    services = [
        {
            "name": "MongoDB",
            "service": MongoDBService(),
            "color": "#6ab04c",
        },
        {
            "name": "Cassandra",
            "service": CassandraService(),
            "color": "#7ed6df",
        },
    ]

    for service in services:
        service["service"].connect()
        st.write(f"Connected to {service["name"]} database")

    return services

services = get_service()

# for service in services:
#     try:
#         for _ in service["service"].create_reports(100):
#             pass

#         for x in service["service"].get_reports_by_sensors(2):
#             st.write(service["name"])
#             st.json(x, expanded=False)
#     except Exception as e:
#         st.write(e)

# create random reports
st.header("Create random reports")

# number of reports to create
n = st.number_input("Number of reports", 1, 100000, 10000)

if st.button("Create reports"):
    for service in services:
        text = f"{service["name"]}"
        bar = st.progress(0, text=text)
        it = service["service"].create_reports(n)

        last = timer()
        i = 0
        for _ in it:
            i += 1
            bar.progress(i / n)

        delta = timer() - last

        service["creating_reports_time"] = delta
    
    st.write(f"Time to create {n} reports (lower is better)")
    st.bar_chart(
        x="DBMS", y="Time",
        color="color",
        data=[{"DBMS": service["name"], "Time": service["creating_reports_time"], "color": service["color"]} for service in services]
    )

st.header("Get reports by sensors")

n = st.number_input("Number of times to get reports", 1, 1000, 100, key="get_reports_by_sensors")

if st.button("Get reports by sensors"):
    for service in services:
        it = service["service"].get_reports_by_sensors(n)

        last = timer()
        i = 0
        for _ in it:
            pass
        delta = timer() - last

        service["getting_reports_time"] = delta

    st.write(f"Time to get reports by sensor in a particular day (lower is better)")
    st.bar_chart(
        x="DBMS", y="Time",
        color="color",
        data=[{"DBMS": service["name"], "Time": service["getting_reports_time"], "color": service["color"]} for service in services]
    )


st.header("Get reports by locations")

n = st.number_input("Number of times to get reports", 1, 1000, 100, key="get_reports_by_locations")

if st.button("Get reports by locations"):

    for service in services:
        it = service["service"].get_reports_by_locations(n)

        last = timer()
        i = 0
        for _ in it:
            pass
        delta = timer() - last

        service["getting_reports_time"] = delta

    st.write(f"Time to get reports by location in a particular day (lower is better)")
    st.bar_chart(
        x="DBMS", y="Time",
        color="color",
        data=[{"DBMS": service["name"], "Time": service["getting_reports_time"], "color": service["color"]} for service in services]
    )

st.header("Get reports by environments")

n = st.number_input("Number of times to get reports", 1, 1000, 100, key="get_reports_by_environments")

if st.button("Get reports by environments"):

    for service in services:
        it = service["service"].get_reports_by_environments(n)

        last = timer()
        i = 0
        for _ in it:
            pass
        delta = timer() - last

        service["getting_reports_time"] = delta

    st.write(f"Time to get reports by environment in a particular day (lower is better)")
    st.bar_chart(
        x="DBMS", y="Time",
        color="color",
        data=[{"DBMS": service["name"], "Time": service["getting_reports_time"], "color": service["color"]} for service in services]
    )

# reset databases
st.header("Reset databases")

if st.button("Reset databases"):
    for service in services:
        service["service"].reset()
        st.write(f"Reset {service["name"]} database")
