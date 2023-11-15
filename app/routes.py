from app import app, session
from flask import jsonify, request
from app import function as func
import datetime
from tqdm import tqdm

# Route to get ID
@app.route('/getID', methods=['GET'])
def get_tasks():
    return jsonify(123)

# Route to get information about stations
@app.route('/vehicle', methods=['GET'])
def get_vehicle_lists():
    try:
        data = request.args
        station_name = data.get('station_name')

        query = f"SELECT ride_id, rideable_type FROM capitalbikeshare WHERE end_station_name = '{station_name}' ALLOW FILTERING"
        rows = session.execute(query)
        vehicle_info_list = []
        for row in rows:
            vehicle_info = {
                "ride_id": row.ride_id,
                "rideable_type": row.rideable_type
            }
            vehicle_info_list.append(vehicle_info)
        return jsonify(vehicle_info_list)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)

# Route to add new vehicle
@app.route('/vehicle/add', methods=['POST'])
def insert_new_vehicle():
    try:
        data = request.args

        ride_id = data.get('ride_id')
        rideable_type = data.get('rideable_type')
        started_at = data.get('started_at')
        start_station_name = data.get('start_station_name')
        start_station_id = func.get_station_id(start_station_name)
        start_lat = float(data.get('start_lat')) if data.get('start_lat') else None
        start_lng = float(data.get('start_lng')) if data.get('start_lng') else None
        member_casual = data.get('member_casual')
        bike_number = data.get('bike_number')

        if func.ride_id_is_exist(ride_id) == True:
            return jsonify('error: ID is already exist!')

        add_query = f"INSERT INTO capitalbikeshare (ride_id, rideable_type, started_at, start_station_name, start_station_id, start_lat, start_lng, member_casual, bike_number) VALUES ('{ride_id}', '{rideable_type}', '{started_at}', '{start_station_name}', '{start_station_id}', '{start_lat}', '{start_lng}', '{member_casual}', '{bike_number}')"
        session.execute(add_query)

        if func.ride_id_is_exist(ride_id) == True:
            return jsonify('OK')
        return jsonify('error: ADD FAIL!')
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)

# Route to update vehicle already exists
@app.route('/vehicle/update', methods=['POST'])
def update_vehicle():
    try:
        data = request.args

        ride_id = data.get('ride_id')
        rideable_type = data.get('rideable_type')
        started_at = data.get('started_at')
        start_station_name = data.get('start_station_name')
        start_station_id = func.get_station_id(start_station_name)
        end_station_name = data.get('end_station_name')
        end_station_id = func.get_station_id(end_station_name)
        start_lat = float(data.get('start_lat')) if data.get('start_lat') else None
        start_lng = float(data.get('start_lng')) if data.get('start_lng') else None
        end_lat = float(data.get('end_lat')) if data.get('end_lat') else None
        end_lng = float(data.get('end_lng')) if data.get('end_lng') else None
        member_casual = data.get('member_casual')
        bike_number = data.get('bike_number')

        update_query = f"UPDATE capitalbikeshare SET rideable_type = '{rideable_type}', " \
                       f"started_at = '{started_at}', " \
                       f"start_station_name = '{start_station_name}', " \
                       f"start_station_id = {start_station_id}, " \
                       f"end_station_name = '{end_station_name}', " \
                       f"end_station_id = {end_station_id}, " \
                       f"start_lat = {start_lat}, " \
                       f"start_lng = {start_lng}, " \
                       f"end_lat = {end_lat}, " \
                       f"end_lng = {end_lng}, " \
                       f"member_casual = '{member_casual}', " \
                       f"bike_number = '{bike_number}' " \
                       f"WHERE ride_id = '{ride_id}'"
        session.execute(update_query)
        return jsonify('OK')
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)

# Route to get number of bike in week of year
@app.route('/statistic/week', methods=['GET'])
def get_number_bike_of_3_week():
    try:
        data = request.args
        week_number = data.get("week_number")
        year = data.get("year")

        week_bike_info_list = []
        week_number = int(week_number)
        year = int(year)
        start_date, end_date = func.find_week_start_end(week_number, year)
        current_date = start_date
        while current_date <= end_date:
            number_of_bike = 0
            diff_date = current_date
            for i in range(3):
                print(diff_date)
                rows = func.get_bike_day(diff_date)
                diff_date = diff_date + datetime.timedelta(days=7)
                number_of_bike += len(list(rows))
                print(number_of_bike)
            week_bike = {
                "date": current_date.strftime("%A"),
                "number_of_bike": number_of_bike
            }
            week_bike_info_list.append(week_bike)
            current_date += datetime.timedelta(days=1)
        return jsonify(week_bike_info_list)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)

# Route to get number of bike in stations in time range
@app.route('/statistic/station', methods=['GET'])
def count_rides_by_end_station():
    try:
        data = request.args

        start_time_day = int(data.get('start_time_day'))
        start_time_month = int(data.get('start_time_month'))
        start_time_year = int(data.get('start_time_year'))

        end_time_day = int(data.get('end_time_day'))
        end_time_month = int(data.get('end_time_month'))
        end_time_year = int(data.get('end_time_year'))

        # test data
        start_time = datetime.datetime(start_time_year, start_time_month, start_time_day, 0, 0, 0)
        end_time = datetime.datetime(end_time_year, end_time_month, end_time_day, 23, 59, 59)
        # ----------
        start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')
        query = f"SELECT end_station_name FROM capitalbikeshare WHERE started_at >= '{start_time}' AND started_at < '{end_time}' ALLOW FILTERING"
        rows = session.execute(query)
        station_counts = {}
        for row in rows:
            end_station_name = row.end_station_name
            if end_station_name in station_counts:
                station_counts[end_station_name] += 1
            else:
                station_counts[end_station_name] = 1
        station_info_list = []
        for station_name, count in station_counts.items():
            station_info = {
                "station_name": station_name,
                "number_of_bike": count
            }
            station_info_list.append(station_info)
        return jsonify(station_info_list)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)

@app.route('/vehicle/delete', methods=['POST'])
def delete_vehicle():
    try:
        data = request.args

        ride_id = data.get('ride_id')

        if func.ride_id_is_exist(ride_id) == False:
            return jsonify('error: ride_id not exist!')

        query = f"DELETE FROM capitalbikeshare WHERE ride_id = '{ride_id}'"
        session.execute(query)

        if func.ride_id_is_exist(ride_id) == False:
            return jsonify('OK')
        return jsonify('error: DELETE FAIL!')
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

@app.route('/station',methods=['GET'])
def get_unique_station_list():
    try:
        query_all = f"""
            SELECT DISTINCT station_id
            FROM stationbikeshare
        """

        rows_all = session.execute(query_all)
        print('Query done')
        # Extracting start and end stations
        rows_all_ids = []
        for row in tqdm(rows_all):
            rows_all_ids.append(row.station_id)
        

        # Extracting unique stations
        station_list = list(set(rows_all_ids))

        return jsonify(station_list)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)
    
@app.route('/station_report/',methods=['GET'])
def get_number_vehicle_in_time_range():
    try:
        data = request.args
        day1 = int(data.get('day1'))
        month1 = int(data.get('month1'))
        year1 = int(data.get('year1'))

        day2 = int(data.get('day2'))
        month2 = int(data.get('month2'))
        year2 = int(data.get('year2'))

        start_time = datetime.datetime(int(year1), int(month1), int(day1), 0, 0, 0)
        end_time = datetime.datetime(int(year2), int(month2), int(day2), 0, 0, 0)

        start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')
        query1 = f"SELECT end_station_id,ride_id FROM capitalbikeshare WHERE ended_at >= '{start_time}' AND ended_at <= '{end_time}' ALLOW FILTERING"
        rows1 = session.execute(query1)
        
        query2 = f"SELECT start_station_id,ride_id FROM capitalbikeshare WHERE started_at >= '{start_time}' AND started_at <= '{end_time}' ALLOW FILTERING"
        rows2 = session.execute(query2)
        print('Query done')
        dict_end = {}
        for row in tqdm(rows1):
            if row.end_station_id in dict_end:
                dict_end[row.end_station_id].append(row.ride_id)
            else:
                dict_end[row.end_station_id] = []

        dict_start = {}
        for row in tqdm(rows2):
            if row.start_station_id in dict_start:
                dict_start[row.start_station_id].append(row.ride_id)
            else:
                dict_start[row.start_station_id] = []
        dict_stay = {}
        for key in dict_end:
            if key in dict_start:
                dict_stay[key] = len(list(set(dict_end[key]) - set(dict_start[key])))
            else:
                dict_stay[key] = len(dict_end[key])
        for key in dict_start:
            dict_start[key] = len(dict_start[key])
        for key in dict_end:
            dict_end[key] = len(dict_end[key])
        dict_all = {}
        dict_all['start'] = dict_start
        dict_all['end'] = dict_end
        dict_all['stay'] = dict_stay
        return jsonify(dict_all)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)

@app.route('/history/',methods=['GET'])
def get_vehicle_history():
    try:
        data = request.args
        id_vehicle = data.get('id_vehicle')
        query = f"SELECT started_at,ended_at,start_station_name,end_station_name FROM capitalbikeshare WHERE bike_number = '{id_vehicle}' ALLOW FILTERING"
        rows = session.execute(query)
        vehicle_info_list = []
        print('Query done')
        for row in tqdm(rows):
            vehicle_info = {
                "id_vehicle": id_vehicle,
                "started_at": row.started_at,
                "ended_at": row.ended_at,
                "start_station_name": row.start_station_name,
                "end_station_name": row.end_station_name
            }
            vehicle_info_list.append(vehicle_info)
        return jsonify(vehicle_info_list)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)