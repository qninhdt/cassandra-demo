from db import MySQLService, MongoDBService
import streamlit as st
from threading import Thread
from streamlit.runtime.scriptrunner import add_script_run_ctx
from timeit import default_timer as timer

services = [
    {
        "name": "MySQL",
        "service": MySQLService(),
        "color": "blue",
    },
    {
        "name": "MongoDB",
        "service": MongoDBService(),
        "color": "green",
    },
    # {
    #     "name": "Cassandra",
    #     "service": CassandraService(),
    #     "color": "red",
    # },
]


for service in services:
    service["service"].connect()
    st.write(f"Connected to {service["name"]} database")

# create random users
st.write("Create random users")

# number of users to create
n = st.number_input("Number of users", 1, 1000, 1000)

if st.button("Create users"):
    for service in services:
        text = f"{service["name"]}"
        bar = st.progress(0, text=text)
        
        it = service["service"].create_random_user(n)

        last = timer()
        i = 0
        for _ in it:
            i += 1
            bar.progress(int(i / n * 100), text)
        delta = timer() - last

        service["creating_users_time"] = delta
    
    st.write(f"Time to create {n} users (lower is better)")
    st.bar_chart(
        x="DBMS", y="Time",
        color="color",
        data=[{"DBMS": service["name"], "Time": service["creating_users_time"], "color": service["color"]} for service in services]
    )

# create random posts
st.write("Create random posts")

# number of posts to create
n = st.number_input("Number of posts", 1, 1000, 1000)

if st.button("Create posts"):
    for service in services:
        text = f"{service["name"]}"
        bar = st.progress(0, text=text)
        
        it = service["service"].create_random_post(n)

        last = timer()
        i = 0
        for _ in it:
            i += 1
            bar.progress(int(i / n * 100), text)
        delta = timer() - last

        service["creating_posts_time"] = delta
    
    st.write(f"Time to create {n} posts (lower is better)")
    st.bar_chart(
        x="DBMS", y="Time",
        color="color",
        data=[{"DBMS": service["name"], "Time": service["creating_posts_time"], "color": service["color"]} for service in services]
    )
        
# create random comments
st.write("Create random comments")

# number of comments to create
n = st.number_input("Number of comments", 1, 1000, 1000)

if st.button("Create comments"):
    for service in services:
        text = f"{service["name"]}"
        bar = st.progress(0, text=text)
        
        it = service["service"].create_random_comment(n)

        last = timer()
        i = 0
        for _ in it:
            i += 1
            bar.progress(int(i / n * 100), text)
        delta = timer() - last

        service["creating_comments_time"] = delta
    
    st.write(f"Time to create {n} comments (lower is better)")
    st.bar_chart(
        x="DBMS", y="Time",
        color="color",
        data=[{"DBMS": service["name"], "Time": service["creating_comments_time"], "color": service["color"]} for service in services]
    )

# get all users
st.write("Get users")

if st.button("Get users"):
    for service in services:
        it = service["service"].get_all_users()

        last = timer()
        for _ in it:
            pass
        delta = timer() - last

        service["getting_users_time"] = delta
    
    st.write(f"Time to get all users (lower is better)")
    st.bar_chart(
        x="DBMS", y="Time",
        color="color",
        data=[{"DBMS": service["name"], "Time": service["getting_users_time"], "color": service["color"]} for service in services]
    )

# get all posts
st.write("Get posts")

if st.button("Get posts"):
    for service in services:
        it = service["service"].get_all_posts()

        last = timer()
        i = 0
        for _ in it:
            pass
        delta = timer() - last

        service["getting_posts_time"] = delta
    
    st.write(f"Time to get all posts (lower is better)")
    st.bar_chart(
        x="DBMS", y="Time",
        color="color",
        data=[{"DBMS": service["name"], "Time": service["getting_posts_time"], "color": service["color"]} for service in services]
    )

# reset databases
if st.button("Reset databases"):
    for service in services:
        service["service"].reset()
        st.write(f"Reset {service["name"]} database")
