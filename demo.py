from db import MySQLService, MongoDBService, CassandraService
import streamlit as st
from timeit import default_timer as timer


@st.cache_resource
def get_service():
    services = [
        # {
        #     "name": "MySQL",
        #     "service": MySQLService(),
        #     "color": "blue",
        # },
        {
            "name": "MongoDB",
            "service": MongoDBService(),
            "color": "green",
        },
        {
            "name": "Cassandra",
            "service": CassandraService(),
            "color": "red",
        },
    ]

    for service in services:
        service["service"].connect()
        st.write(f"Connected to {service["name"]} database")

    return services

services = get_service()

# for service in services:
#     try:
#         for _ in service["service"].create_random_user(1):
#             pass 
#         for _ in service["service"].create_random_post(1):
#             pass
#         for _ in service["service"].create_random_comment(1):
#             pass

#         for x in service["service"].get_posts_by_users():
#             st.write(service["name"])
#             st.json(x, expanded=False)
#     except Exception as e:
#         st.write(e)

#     service["service"].reset()

# create random users
st.header("Create random users")

# number of users to create
n = st.number_input("Number of users", 1, 10000, 1000)

if st.button("Create users"):
    for service in services:
        text = f"{service["name"]}"

        last = timer()
        service["service"].create_random_user(n)
        delta = timer() - last

        service["creating_users_time"] = delta
    
    st.write(f"Time to create {n} users (lower is better)")
    st.bar_chart(
        x="DBMS", y="Time",
        color="color",
        data=[{"DBMS": service["name"], "Time": service["creating_users_time"], "color": service["color"]} for service in services]
    )

# create random posts
st.header("Create random posts")

# number of posts to create
n = st.number_input("Number of posts", 1, 10000, 1000)

if st.button("Create posts"):
    for service in services:
        text = f"{service["name"]}"
        
        last = timer()
        service["service"].create_random_post(n)
        delta = timer() - last

        service["creating_posts_time"] = delta
    
    st.write(f"Time to create {n} posts (lower is better)")
    st.bar_chart(
        x="DBMS", y="Time",
        color="color",
        data=[{"DBMS": service["name"], "Time": service["creating_posts_time"], "color": service["color"]} for service in services]
    )
        
# create random comments
st.header("Create random comments")

# number of comments to create
n = st.number_input("Number of comments", 1, 10000, 1000)

if st.button("Create comments"):
    for service in services:
        text = f"{service["name"]}"
        
        last = timer()
        service["service"].create_random_comment(n)
        delta = timer() - last

        service["creating_comments_time"] = delta
    
    st.write(f"Time to create {n} comments (lower is better)")
    st.bar_chart(
        x="DBMS", y="Time",
        color="color",
        data=[{"DBMS": service["name"], "Time": service["creating_comments_time"], "color": service["color"]} for service in services]
    )

# get posts by users
st.header("Get posts by users")

if st.button("Get posts by users"):
    for service in services:
        it = service["service"].get_posts_by_users()

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

# get comments by users
st.header("Get comments by users")

if st.button("Get comments by users"):
    for service in services:
        it = service["service"].get_comments_by_users()

        last = timer()
        i = 0
        for _ in it:
            pass
        delta = timer() - last

        service["getting_comments_time"] = delta
    
    st.write(f"Time to get all comments (lower is better)")
    st.bar_chart(
        x="DBMS", y="Time",
        color="color",
        data=[{"DBMS": service["name"], "Time": service["getting_comments_time"], "color": service["color"]} for service in services]
    )

# get comments by posts
st.header("Get comments by posts")

if st.button("Get comments by posts"):
    for service in services:
        it = service["service"].get_comments_by_posts()

        last = timer()
        i = 0
        for _ in it:
            pass
        delta = timer() - last

        service["getting_comments_time"] = delta
    
    st.write(f"Time to get all comments (lower is better)")
    st.bar_chart(
        x="DBMS", y="Time",
        color="color",
        data=[{"DBMS": service["name"], "Time": service["getting_comments_time"], "color": service["color"]} for service in services]
    )

# reset databases
st.header("Reset databases")

if st.button("Reset databases"):
    for service in services:
        service["service"].reset()
        st.write(f"Reset {service["name"]} database")
