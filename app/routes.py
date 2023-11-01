from app import app, session
from flask import jsonify, request
from app import function as func
import datetime
import numpy as np

# Route to get ID
@app.route('/getID', methods=['GET'])
def get_tasks():
    return jsonify(123)

# Route to get information about stations
@app.route('/vehicle/<station_name>', methods=['GET'])
def get_vehicle_lists(station_name):
    try:
        query = f"SELECT ride_id, rideable_type FROM capitalbikeshare WHERE start_station_name = '{station_name}' ALLOW FILTERING"
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
        data = request.json

        ride_id = data.get('ride_id')
        rideable_type = data.get('rideable_type')
        started_at = data.get('started_at')
        start_station_name = data.get('start_station_name')
        start_station_id = func.get_station_id(start_station_name)
        start_lat = float(data.get('start_lat')) if data.get('start_lat') else None
        start_lng = float(data.get('start_lng')) if data.get('start_lng') else None
        member_casual = data.get('member_casual')

        add_query = f"INSERT INTO capitalbikeshare (ride_id, rideable_type, started_at, start_station_name, start_station_id, start_lat, start_lng, member_casual) VALUES ('{ride_id}', '{rideable_type}', '{started_at}', '{start_station_name}', '{start_station_id}', '{start_lat}', '{start_lng}', '{member_casual}')"
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
        data = request.json

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
                       f"member_casual = '{member_casual}' " \
                       f"WHERE ride_id = '{ride_id}'"
        session.execute(update_query)
        return jsonify('OK')
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)

# Route to get number of bike in week of year
@app.route('/statistic/week/<week_number>/<year>', methods=['GET'])
def get_number_bike_of_week(week_number, year):
    try:
        week_bike_info_list = []
        week_number = int(week_number)
        year = int(year)
        start_date, end_date = func.find_week_start_end(week_number, year)
        current_date = start_date
        while current_date <= end_date:
            number_of_bike = 0
            rows = func.get_bike_day(current_date)
            for row in rows:
                number_of_bike = number_of_bike + 1
            week_bike = {
                "date": current_date,
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
def count_rides_by_start_station():
    try:
        # test data
        start_time = datetime.datetime(2023, 7, 1, 0, 0, 0)
        end_time = datetime.datetime(2023, 7, 7, 23, 59, 59)
        # ----------
        start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')
        query = f"SELECT start_station_name FROM capitalbikeshare WHERE started_at >= '{start_time}' AND started_at <= '{end_time}' ALLOW FILTERING"
        rows = session.execute(query)
        station_counts = {}
        for row in rows:
            start_station_name = row.start_station_name
            if start_station_name in station_counts:
                station_counts[start_station_name] += 1
            else:
                station_counts[start_station_name] = 1
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
        data = request.json

        ride_id = data.get('ride_id')

        query = f"DELETE FROM your_table WHERE ride_id = '{ride_id}'"
        session.execute(query)

        if func.ride_id_is_exist(ride_id) == False:
            return jsonify('OK')
        return jsonify('error: DELETE FAIL!')
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)

@app.route('/station',methods=['GET'])
def get_unique_station_list():
    try:
        query = f"SELECT start_station_name FROM capitalbikeshare"
        rows = session.execute(query)
        station_list = []
        k = 0
        for row in rows:
            k += 1
            print(k)
            station_name = row.start_station_name
            if (station_name.replace(" ","_") not in station_list) and (len(station_name) > 0):
                station_list.append(station_name.replace(" ","_"))
        query = f"SELECT end_station_name FROM capitalbikeshare"
        rows = session.execute(query)
        for row in rows:
            k -= 1
            print(k)
            station_name = row.end_station_name
            if (station_name.replace(" ","_") not in station_list) and (len(station_name) > 0):
                station_list.append(station_name.replace(" ","_"))
        return jsonify(station_list)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)
    
@app.route('/station/<station_id>/<day1>_<month1>_<year1>/<day2>_<month2>_<year2>/',methods=['GET'])
# @app.route('/station/<station_name>/',methods=['GET'])
# def get_number_vehicle_in_time_range(station_name):
def get_number_vehicle_in_time_range(station_id,day1,month1,year1,day2,month2,year2):
    try:
        station_id = str(station_id)
        start_time = datetime.datetime(int(year1), int(month1), int(day1), 0, 0, 0)
        end_time = datetime.datetime(int(year2), int(month2), int(day2), 0, 0, 0)
        # start_time = datetime.datetime(2023, 3, 1, 0, 0, 0)
        # end_time = datetime.datetime(2023, 3, 7, 23, 59, 59)
        start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')
        query1 = f"SELECT end_station_id,ride_id FROM capitalbikeshare WHERE ended_at >= '{start_time}' AND ended_at <= '{end_time}' ALLOW FILTERING"
        rows1 = session.execute(query1)
        dict_end = {}
        for row in rows1:
            if row.end_station_id in dict_end:
                dict_end[row.end_station_id].append(row.ride_id)
            else:
                dict_end[row.end_station_id] = []
        query2 = f"SELECT start_station_id,ride_id FROM capitalbikeshare WHERE started_at >= '{start_time}' AND started_at <= '{end_time}' ALLOW FILTERING"
        rows2 = session.execute(query2)
        dict_start = {}
        for row in rows2:
            if row.start_station_id in dict_start:
                dict_start[row.start_station_id].append(row.ride_id)
            else:
                dict_start[row.start_station_id] = []
        num_dict = {}
        for key in dict_end:
            if key in dict_start:
                num_dict[key] = len(list(set(dict_end[key]) - set(dict_start[key])))
            else:
                num_dict[key] = len(dict_end[key])
        return jsonify(num_dict)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)

@app.route('/history/<id_vehicle>/',methods=['GET'])
def get_vehicle_history(id_vehicle):
    try:
        query = f"SELECT started_at,ended_at,start_station_name,end_station_name FROM capitalbikeshare WHERE ride_id = '{id_vehicle}' ALLOW FILTERING"
        rows = session.execute(query)
        vehicle_info_list = []
        for row in rows:
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
    
@app.route('/report/<month1>_<year1>/<month2>_<year2>/',methods=['GET'])
def get_report(month1,year1,month2,year2):
    try:
        start_time = datetime.datetime(int(year1), int(month1), 1, 0, 0, 0)
        end_time = datetime.datetime(int(year2), int(month2), 1, 0, 0, 0)
        start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')
        query = f"SELECT end_station_name FROM capitalbikeshare WHERE started_at >= '{start_time}' AND started_at <= '{end_time}' ALLOW FILTERING"
        rows = session.execute(query)
        res = {}
        for row in rows:
            if row.end_station_name in res:
                res[row.end_station_name] += 1
            else:
                res[row.end_station_name] = 1
        return jsonify(res)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)