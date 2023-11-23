from app import app, session
from flask import jsonify, request
from app import function as func
import datetime
from tqdm import tqdm
import matplotlib.pyplot as plt
import os
from cassandra.cluster import ResultSet
import json
import uuid


# Route to add new vehicle
@app.route('/ride/add', methods=['POST'])
def insert_new_vehicle():
    try:
        data = request.json

        print(data)

        ride_id = str(uuid.uuid4())
        print(ride_id)
        rideable_type = data.get('bike')['rideable_type']
        started_at = data.get('start_date')
        start_station_name = data.get('start_station')
        start_station_id = data.get('start_station_id')
        ended_at = data.get('end_date')
        end_station_name = data.get('end_station')
        end_station_id = data.get('end_station_id')
        member_casual = data.get('member_casual')
        bike_number = data.get('bike')['bike_number']
        user_id = data.get('user_id')

        add_query = f"""INSERT INTO capitalbikeshare 
        (ride_id, rideable_type, started_at, start_station_name, start_station_id, ended_at, end_station_name, end_station_id, member_casual, bike_number, user_id) VALUES 
        ('{ride_id}', '{rideable_type}', '{started_at}', '{start_station_name}', '{start_station_id}', '{ended_at}', '{end_station_name}', '{end_station_id}','{member_casual}', '{bike_number}', '{user_id}')"""
        session.execute(add_query)

        return jsonify({
            "result": True,
            "message": "Completed recording the information of the bike rental!",
        })
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)

@app.route('/station/add', methods=['POST'])
def add_station():
    try:
        data = request.args

        station_id = data.get('station_id')
        station_name = data.get('station_name')
        city = data.get('city')
        country = data.get('country')

        query = f"INSERT INTO stationbikeshare (station_id, station_name, city, country) VALUES ('{station_id}', '{station_name}', '{city}', '{country}')"
        session.execute(query)
        return jsonify('OK')
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)

@app.route('/station/update', methods=['POST'])
def update_station():
    try:
        data = request.args

        station_id = data.get('station_id')
        station_name = data.get('station_name')
        city = data.get('city')
        country = data.get('country')

        query = f"UPDATE stationbikeshare SET station_name = '{station_name}', city = '{city}', country = '{country}' WHERE station_id = '{station_id}'"
        session.execute(query)
        return jsonify('OK')
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)

# Route to sign up
@app.route('/signup', methods=['POST'])
def signup():
    try:
        data = request.args

        user_id = data.get('user_id')
        user_name = data.get('user_name')
        country = data.get('country')
        sign_up_date = data.get('sign_up_date')
        user_password = data.get('password')

        query_username = f"SELECT user_name FROM userbikeshare"
        rows = session.execute(query_username)
        for row in rows:
            if row.user_name == user_name:
                print(row.user_name)
                return jsonify('error: user_name is already exist!')

        add_query = f"INSERT INTO userbikeshare (user_id, user_name, country, sign_up_date, user_password) VALUES ('{user_id}', '{user_name}', '{country}', '{sign_up_date}', '{user_password}')"
        session.execute(add_query)

        return jsonify('OK')
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)
    
# Route to login
@app.route('/login', methods=['POST'])
def login():
    try:
        data = request.json
        user_id = str(data.get('user_name'))
        user_password = str(data.get('user_password'))
        query = f"SELECT * FROM userbikeshare WHERE user_id = '{user_id}' ALLOW FILTERING"
        rows = session.execute(query)
        match_found = False
        for row in rows:
            if row.user_password == user_password:
                match_found = True
                break
        if match_found == True:
            return jsonify({
                "isAuthenticated": True,
                "data": func.json_format(rows),
                "message": "Login successfully!"
            })
        else:
            return jsonify({
                "isAuthenticated": False,
                "message": "User name or password is not correct!"
            })
    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({
            "isAuthenticated": False,
            "message": "User name or password is not correct!",
            "error": str(e)
        })

@app.route('/history/bike', methods=['GET'])
def get_bike_history():
    try:
        data = request.args
        bike_number = str(data.get('bike_number'))
        start_date = str(data.get('start_date'))
        end_date = str(data.get('end_date'))
        view_query = f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS mykeyspace.bike_history AS
            SELECT *
            FROM capitalbikeshare
            WHERE ride_id IS NOT NULL
            AND bike_number IS NOT NULL
            PRIMARY KEY (bike_number, ride_id);
        """
        query = f"""SELECT * FROM bike_history 
        WHERE bike_number = '{bike_number}' 
        AND ended_at >= '{start_date}' 
        AND ended_at <= '{end_date}' ALLOW FILTERING;
        """
        session.execute(view_query)
        rows: ResultSet = session.execute(query)
        return func.json_format(rows)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)
    

@app.route('/history/user', methods=['GET'])
def get_user_history():
    try:
        data = request.args
        user_id = str(data.get('user_id'))
        start_date = str(data.get('start_date'))
        end_date = str(data.get('end_date'))
        view_query = f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS mykeyspace.user_history AS
            SELECT *
            FROM capitalbikeshare
            WHERE ride_id IS NOT NULL
            AND user_id IS NOT NULL
            PRIMARY KEY (user_id, ride_id);
        """
        query = f"""SELECT * FROM user_history 
        WHERE user_id = '{user_id}' 
        AND ended_at >= '{start_date}' 
        AND ended_at <= '{end_date}' ALLOW FILTERING;
        """
        session.execute(view_query)
        rows: ResultSet = session.execute(query)
        return func.json_format(rows)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)
    

@app.route('/city', methods=['GET'])
def get_city_lists():
    try:
        data = request.args
        country = str(data.get('country'))
        print(country)
        query = f"SELECT city FROM stationbikeshare WHERE country='{country}' ALLOW FILTERING"
        rows: ResultSet = session.execute(query)
        city_name = set()
        for row in rows:
            city_name.add(row.city)
        return json.dumps(list(city_name))
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)


@app.route('/station', methods=['GET'])
def get_station_lists():
    try:
        data = request.args
        city = data.get('city')
        if city == "" or city is None:
            query = f"SELECT * FROM stationbikeshare ALLOW FILTERING"
            rows: ResultSet = session.execute(query)
            return func.json_format(rows)
        else:
            query = f"SELECT station_id, station_name FROM stationbikeshare WHERE city='{city}' ALLOW FILTERING"
            rows: ResultSet = session.execute(query)
            station_name = set()
            for row in rows:
                station_name.add((row.station_id, row.station_name))
            return json.dumps(list(station_name))
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)
    

@app.route('/bike', methods=['GET'])
def get_bike_lists():
    try:
        data = request.args
        station_id = data.get('station_id')
        query = f"SELECT * FROM bikeshare LIMIT 50"
        rows: ResultSet = session.execute(query)
        return func.json_format(rows)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)
    

@app.route('/rideable_type_count', methods=['GET'])
def get_rideable_type_count():
    try:
        create_view_query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS mykeyspace.rideable_type_counts AS
            SELECT ride_id, rideable_type
            FROM capitalbikeshare
            WHERE ride_id IS NOT NULL
            AND rideable_type IS NOT NULL
            PRIMARY KEY (rideable_type, ride_id)
        """
        query = """
            SELECT rideable_type, COUNT(*) FROM rideable_type_counts GROUP BY rideable_type
        """
        session.execute(create_view_query)
        rows: ResultSet = session.execute(query)
        return func.json_format(rows)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)
    

@app.route('/station_count_in_rides', methods=['GET'])
def get_station_count_in_rides():
    try:
        create_view_query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS mykeyspace.station_counts_in_rides AS
            SELECT ride_id, start_station_id, start_station_name
            FROM capitalbikeshare
            WHERE ride_id IS NOT NULL
            AND start_station_id IS NOT NULL
            PRIMARY KEY (start_station_id, ride_id);
        """
        query = """
            SELECT start_station_id, start_station_name, COUNT(*) FROM station_counts_in_rides GROUP BY start_station_id;
        """
        session.execute(create_view_query)
        rows: ResultSet = session.execute(query)
        return func.json_format(rows)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)
    

@app.route('/station_count_in_country', methods=['GET'])
def get_station_count_in_country():
    try:
        create_view_query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS mykeyspace.station_counts_in_country AS
            SELECT station_id, station_name, country
            FROM stationbikeshare
            WHERE station_id IS NOT NULL
            AND country IS NOT NULL
            PRIMARY KEY (country, station_id);
        """
        query = """
            SELECT country, COUNT(*) FROM station_counts_in_country GROUP BY country;
        """
        session.execute(create_view_query)
        rows: ResultSet = session.execute(query)
        return func.json_format(rows)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)
    

@app.route('/station_count_in_city', methods=['GET'])
def get_station_count_in_city():
    try:
        create_view_query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS mykeyspace.station_counts_in_city AS
            SELECT station_id, station_name, city, country
            FROM stationbikeshare
            WHERE station_id IS NOT NULL
            AND city IS NOT NULL
            PRIMARY KEY (city, station_id);
        """
        query = """
            SELECT city, country, COUNT(*) FROM station_counts_in_city GROUP BY city;
        """
        session.execute(create_view_query)
        rows: ResultSet = session.execute(query)
        return func.json_format(rows)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)
    

@app.route('/bike_count_in_country', methods=['GET'])
def get_bike_count_in_country():
    try:
        create_view_query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS mykeyspace.bike_count_in_country AS
            SELECT bike_number, rideable_type, country
            FROM bikeshare
            WHERE bike_number IS NOT NULL
            AND country IS NOT NULL
            PRIMARY KEY (country, bike_number);
        """
        query = """
            SELECT country, COUNT(*) FROM bike_count_in_country GROUP BY country;
        """
        session.execute(create_view_query)
        rows: ResultSet = session.execute(query)
        return func.json_format(rows)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)
    
@app.route('/user_count_in_country', methods=['GET'])
def get_user_count_in_country():
    try:
        create_view_query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS mykeyspace.user_count_in_country AS
            SELECT user_id, user_name, country, sign_up_date
            FROM userbikeshare
            WHERE user_id IS NOT NULL
            AND country IS NOT NULL
            PRIMARY KEY (country, user_id);
        """
        query = """
            SELECT country, COUNT(*) FROM user_count_in_country GROUP BY country;
        """
        session.execute(create_view_query)
        rows: ResultSet = session.execute(query)
        return func.json_format(rows)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)
    