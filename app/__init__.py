from flask import Flask
from cassandra.cluster import Cluster

app = Flask(__name__)

# Cassandra connection setup
cassandra_keyspace_name = 'mykeyspace'
cluster = Cluster(['localhost'])
session = cluster.connect(cassandra_keyspace_name)

# Import routes
from app import routes