from app import session

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