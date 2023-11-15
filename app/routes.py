from app import app, session
from flask import jsonify, request
from app import function as func
import datetime
from tqdm import tqdm
import matplotlib.pyplot as plt
import os

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
            SELECT station_name,city,country
            FROM stationbikeshare
        """

        rows_all = session.execute(query_all)
        print('Query done')
        # Extracting start and end stations
        rows_all_ids = []
        for row in tqdm(rows_all):
            station_info = {}
            station_info['station_name'] = row.station_name
            station_info['city'] = row.city
            station_info['country'] = row.country

            rows_all_ids.append(station_info)
        


        return jsonify(rows_all_ids)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)
    
@app.route('/station_report/',methods=['GET'])
def get_number_vehicle_in_time_range():
    try:
        data = request.args
        month = int(data.get('month'))
        year = int(data.get('year'))
        start_time = datetime.datetime(int(year), int(month), 1, 0, 0, 0)
        end_time = datetime.datetime(int(year), int(month)+1, 1, 0, 0, 0)
        query = f"SELECT start_station_name FROM capitalbikeshare WHERE started_at >= '{start_time}' AND ended_at < '{end_time}' ALLOW FILTERING"
        rows = session.execute(query)
        print('Query done')
        dict_all = {}
        for row in tqdm(rows):
            if row.start_station_name not in dict_all:
                dict_all[row.start_station_name] = 1
            else:
                dict_all[row.start_station_name] += 1

        del dict_all['']
        # Sort dict by value and draw chart with top 20 most and least popular stations, then save to pdf
        dict_top = dict(sorted(dict_all.items(), key=lambda item: item[1], reverse=True))
        dict_bot = dict(sorted(dict_all.items(), key=lambda item: item[1], reverse=False))

        # Draw chart
        plt.rcParams.update({'figure.autolayout': True})
        fig, ax = plt.subplots()
        ax.barh(list(dict_top.keys())[:20][::-1], list(dict_top.values())[:20][::-1])
        labels = ax.get_xticklabels()
        plt.setp(labels, rotation=45, horizontalalignment='right')
        plt.title('Top 20 most popular stations')
        plt.savefig('top_20_most_popular_stations.pdf',format='pdf')

        plt.rcParams.update({'figure.autolayout': True})
        fig, ax = plt.subplots()
        ax.barh(list(dict_bot.keys())[:20], list(dict_bot.values())[:20])
        labels = ax.get_xticklabels()
        plt.title('Top 20 least popular stations')
        plt.savefig('top_20_least_popular_stations.pdf',format='pdf')

        print(os.getcwd())
        
        return jsonify(dict_all)
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)

@app.route('/history/',methods=['GET'])
def get_vehicle_history():
    try:
        data = request.args
        id_vehicle = str(data.get('id_vehicle'))
        month = int(data.get('month'))
        year = int(data.get('year'))
        start_time = datetime.datetime(int(year), int(month), 1, 0, 0, 0)
        end_time = datetime.datetime(int(year), int(month)+1, 1, 0, 0, 0)
        query = f"SELECT bike_number,start_station_name,end_station_name,started_at,ended_at FROM capitalbikeshare WHERE started_at >= '{start_time}' AND ended_at < '{end_time}' ALLOW FILTERING"
        
        rows = session.execute(query)
        vehicle_info_list = []
        print('Query done')
        for row in tqdm(rows):
            if row.bike_number != id_vehicle:
                continue
            # if True:
            vehicle_info = {
                "id_vehicle": id_vehicle,
                "start_station_name": row.start_station_name,
                "start_time": row.started_at,
                "end_station_name": row.end_station_name,
                "end_time": row.ended_at
            }
            vehicle_info_list.append(vehicle_info)
        return jsonify(vehicle_info_list)
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

# # Route to update user already exists
# @app.route('/user/update', methods=['POST'])
# def update_user():
#     try:
#         data = request.args

#         user_id = data.get('user_id')
#         user_name = data.get('user_name')
#         country = data.get('country')
#         sign_up_date = data.get('sign_up_date')

#         update_query = f"UPDATE userbikeshare SET user_name = '{user_name}', country = '{country}', sign_up_date = '{sign_up_date}' WHERE user_id = '{user_id}'"
#         session.execute(update_query)
#         return jsonify('OK')
#     except Exception as e:
#         print(f"Error: {str(e)}")
#         error_message = {"error": str(e)}
#         return jsonify(error_message)
    
# Route to login
@app.route('/login', methods=['POST'])
def login():
    try:
        data = request.args

        user_name = str(data.get('user_name'))
        user_password = str(data.get('password'))

        query = f"SELECT user_password FROM userbikeshare WHERE user_name = '{user_name}' ALLOW FILTERING"
        rows = session.execute(query)
        match_found = False
        for row in rows:
            if row.user_password == user_password:
                match_found = True
                break
        if match_found == True:
            return jsonify('OK')
        else:
            return jsonify('error: user_password is not correct!')
    except Exception as e:
        print(f"Error: {str(e)}")
        error_message = {"error": str(e)}
        return jsonify(error_message)