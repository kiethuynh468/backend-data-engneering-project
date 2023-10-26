from app import session
import datetime

def get_station_name():
    try:
        query = f"SELECT start_station_name FROM capitalbikeshare ALLOW FILTERING"
        rows = session.execute(query)
        station_name = set()
        for row in rows:
            station_name.add(row.start_station_name)
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

def find_week_start_end(week_number, year):
    first_day = datetime.date(year, 1, 1)
    day_of_week = first_day.weekday()
    days_to_subtract = day_of_week - 0
    start_of_week = first_day - datetime.timedelta(days=days_to_subtract)
    start_of_desired_week = start_of_week + datetime.timedelta(weeks=week_number)
    end_of_desired_week = start_of_desired_week + datetime.timedelta(days=6)
    return start_of_desired_week, end_of_desired_week