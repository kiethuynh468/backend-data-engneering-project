from app import session
import datetime
from cassandra.cluster import ResultSet
import json

def serialize_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

def json_format(rows: ResultSet) -> json:
    return json.dumps([dict(row._asdict()) for row in rows], default=serialize_datetime)

def get_station_name():
    try:
        query = f"SELECT end_station_name, start_station_name FROM capitalbikeshare ALLOW FILTERING"
        rows = session.execute(query)
        station_name = set()
        for row in rows:
            station_name.add(row.start_station_name)
            station_name.add(row.end_station_name)
        station_name = list(station_name)
        return station_name
    except Exception as e:
        print(f"Error: {str(e)}")
        return str(e)

def get_station_id(station_name):
    try:
        query = f"SELECT start_station_id FROM capitalbikeshare WHERE start_station_name = '{station_name}' ALLOW FILTERING"
        rows = session.execute(query)
        for row in rows:
            start_station_id = row.start_station_id
            return start_station_id
        return None
    except Exception as e:
        print(f"Error: {str(e)}")
        return str(e)

def get_bike_day(day):
    try:
        tomorrow = day + datetime.timedelta(days=1)
        day = day.strftime('%Y-%m-%d 00:00:00')
        tomorrow = tomorrow.strftime('%Y-%m-%d 00:00:00')
        query = f"SELECT ride_id, started_at FROM capitalbikeshare WHERE started_at >= '{day}' AND started_at < '{tomorrow}' ALLOW FILTERING"
        rows = session.execute(query)
        return rows
    except Exception as e:
        print(f"Error: {str(e)}")
        return str(e)

def ride_id_is_exist(ride_id):
    query = f"SELECT * FROM capitalbikeshare WHERE ride_id = '{ride_id}'"
    rows = session.execute(query)
    if rows:
        return True
    return False

def find_week_start_end(week_number, year):
    first_day = datetime.date(year, 1, 1)
    day_of_week = first_day.weekday()
    days_to_subtract = day_of_week - 0
    start_of_week = first_day - datetime.timedelta(days=days_to_subtract)
    start_of_desired_week = start_of_week + datetime.timedelta(weeks=week_number)
    end_of_desired_week = start_of_desired_week + datetime.timedelta(days=6)
    return start_of_desired_week, end_of_desired_week

def authenticate_user(user_name, password):
    # Prepare the query to retrieve the password based on user_name
    query = f"SELECT password FROM your_keyspace_name.user WHERE user_name = '{user_name}' ALLOW FILTERING"
    try:
        # Execute the query
        result = session.execute(query)
        # Check if the result is not empty and compare the password
        if result:
            stored_password = result.one().password
            return stored_password == password
        else:
            return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False