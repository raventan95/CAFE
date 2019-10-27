import requests
import json
import urllib
import urllib.parse
import time
import datetime
import numpy as np
from scipy.ndimage import gaussian_filter
from scipy.interpolate import interp2d
import matplotlib.pyplot as plt
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from firebase import firebase
from aiostream import stream, pipe

cred = credentials.Certificate("cafe-nasaspaceappkl-firebase-adminsdk-dh3hr-3dc183b46b.json")
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://cafe-nasaspaceappkl.firebaseio.com/',
    'databaseAuthVariableOverride': None
})
firebase = firebase.FirebaseApplication('https://cafe-nasaspaceappkl.firebaseio.com/', None)

def get_url(url):
	response = requests.get(url)
	content = response.content.decode("utf8")
	return content

def get_json_from_url(url):
	content = get_url(url)
	js = json.loads(content)
	return js

def ignore_first_call(fn):
    called = False

    def wrapper(*args, **kwargs):
        nonlocal called
        if called:
            return fn(*args, **kwargs)
        else:
            called = True
            return None

    return wrapper

def queryStations(lat1, lng1, lat2, lng2):
    url = "https://api.waqi.info/map/bounds/?token=fadc5043e75a3c4bc8838da1b08705c17e2f76bc"
    url += "&latlng={}".format(str(lat1)+","+str(lng1)+","+str(lat2)+","+str(lng2))
    results = get_json_from_url(url)
    return results["data"]

def queryAPIforStation(location_id):
    url = "https://api.waqi.info//feed/@{}/?token=fadc5043e75a3c4bc8838da1b08705c17e2f76bc".format(str(location_id))
    results = get_json_from_url(url)
    return results["data"]

def queryNearestStation(lat, lng):
    url = "https://api.waqi.info//feed/geo:{};{}/?token=fadc5043e75a3c4bc8838da1b08705c17e2f76bc".format(str(lat), str(lng))
    results = get_json_from_url(url)
    return results["data"]

def format_longlat_list(in_list, spatial_resolution, gaussian_sigma):
    nrows = int(180/spatial_resolution)
    ncols = int(360/spatial_resolution)
    gaussian_sigma = gaussian_sigma / spatial_resolution
    fine_arr = np.zeros((nrows,ncols), dtype=np.float32)
    
    for line in in_list:
        lat, long, val = line
        lat = int( np.floor( lat/spatial_resolution ) + 90/spatial_resolution )
        long = int( np.floor( long/spatial_resolution ) + 180/spatial_resolution )
        fine_arr[nrows-1-lat, long] = 0.5 * (fine_arr[nrows-1-lat, long] + val) if fine_arr[nrows-1-lat, long]!=0 else val
    max_before = np.amax(fine_arr)
    fine_arr = gaussian_filter(fine_arr, sigma=gaussian_sigma)
    print(np.amax(fine_arr), max(in_list)[0])
    fine_arr = fine_arr / np.amax(fine_arr) * max_before
    out_list = fine_arr.tolist()
    plt.figure()
    plt.imshow(fine_arr, cmap='gray')
    
    return out_list

def query_longlat_from_list(lat, long, in_list, method='linear'):
    assert len(lat)==len(long)
    lat = np.array(lat)
    long = np.array(long)
    spatial_resolution = 180/len(in_list)
    lat = len(in_list) - (lat/spatial_resolution + 90/spatial_resolution)
    long = long/spatial_resolution + 180/spatial_resolution
    
    result = []
    for i in range(len(lat)):
        x_ = int(np.floor(long[i]))
        y_ = int(np.floor(lat[i]))
        if method == 'linear':
            x = [x_, x_, (x_+1)%len(in_list[0]), (x_+1)%len(in_list[0])]
            y = [y_, (y_+1)%len(in_list), y_, (y_+1)%len(in_list)]
            print(x,y)
            z = [in_list[y[0]][x[0]], in_list[y[1]][x[1]], in_list[y[2]][x[2]], in_list[y[3]][x[3]]]
        else:
            print('TODO: cubic interpolation not implemented yet!')
            quit()

        f = interp2d(x, y, z, kind=method)
        result.append(f(long[i], lat[i])[0])
    
    return result

@ignore_first_call
def listener(event):
    global output_norm, output_so2, output_no2, output_o3, output_pm25, output_pm10, output_co
    print(event.event_type)  # can be 'put' or 'patch'
    print(event.path)  # relative to the reference, it seems
    print(event.data)  # new data at /reference/event.path. None if deleted

    node = str(event.path).split('/')[-1] #you can slice the path according to your requirement
    property = str(event.path).split('/')[-1] 
    value = event.data

    if (node=='XY'):
        lng, lat = firebase.get('/XY', None)[1:-1].split(", ")
        print(lat, lng)
        [localdata_norm] = query_longlat_from_list([float(lat)], [float(lng)], in_list=output_norm)
        otherAPIresults = queryNearestStation(lat, lng)
        firebase.put('', 'API', data=round(localdata_norm, 2))
        try:
            firebase.put('', 'NO2', data=otherAPIresults["iaqi"]["no2"]["v"])
            firebase.put('', 'SO2', data=otherAPIresults["iaqi"]["so2"]["v"])
            firebase.put('', 'CO', data=otherAPIresults["iaqi"]["co"]["v"])
            firebase.put('', 'O3', data=otherAPIresults["iaqi"]["o3"]["v"])
            firebase.put('', 'PM25', data=otherAPIresults["iaqi"]["pm25"]["v"])
            firebase.put('', 'PM10', data=otherAPIresults["iaqi"]["pm10"]["v"])
        except Exception:
            pass


def main():
    initialStage()
    db.reference('/').listen(listener)


def initialStage():
    global output_norm #, output_so2, output_no2, output_o3, output_pm25, output_pm10, output_co
    stations = queryStations(90, 0, -90, 180) + queryStations(-90, -180, 90, 0)
    dataset = []
    #xs = stream.iterate(stations) | pipe.map(fetch, ordered=True, task_limit=1000)

    for station in stations:
        #time.sleep(1/1000)
        latitude = station["lat"]
        longitude = station["lon"]
        location_id = station["uid"]
        try:
            final_aqi_value = int(station["aqi"])
            dataset.append([latitude, longitude, final_aqi_value])
        except Exception:
            pass


    output_norm = format_longlat_list(dataset, spatial_resolution=0.1, gaussian_sigma=10)

    #return [output_norm, output_so2, output_no2, output_o3, output_pm25, output_pm10, output_co]

output_norm = []

main()
