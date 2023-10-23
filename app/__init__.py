from flask import Flask
from cassandra.cluster import Cluster

app = Flask(__name__)

# Cassandra connection setup
# cluster = Cluster(['localhost'])  # Replace 'localhost' with your Cassandra cluster address
# session = cluster.connect('your_keyspace')  # Replace 'your_keyspace' with your Cassandra keyspace

# Import routes
from app import routes
