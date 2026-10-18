import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

import json
import requests

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from dags.factory import DagFactory
from operators.postgres import PostgresOp
from lib.requests import debug_requests

DAG_ID = "get_wot_gov_tw_aqi_info"

def get_aqi_info_op(dag: DAG):
    return PythonOperator(task_id="get_aqi_info",
         python_callable=_get_aqi_info_op,
         op_kwargs={"shard_id": "{{ params.shard_id }}", "total_shard": "{{ params.total_shard }}"},
         do_xcom_push=True,
         dag=dag)

def _get_aqi_info_op(shard_id: int=0, total_shard: int=10):
    print("aqi_op: shardid:{}, totalshard:{}".format(shard_id, total_shard))
    device_data_dict = {}

    device_id_in_dict = _list_geo_devices()
    print("aql_op: read {} devices".format(len(device_id_in_dict)))
    for device_id in device_id_in_dict:
        #print("deviceid:{}, devicedata:{}".format(device_id, device_id_in_dict[device_id]))
        if int(int(device_id) % int(total_shard)) == int(shard_id):
	    # my shard
            now = datetime.now()
            last_hour = datetime.now() - timedelta(hours = 1)

            device_data = _get_device_datapoint(device_id, last_hour, now)
            if 'latest' not in device_data:
                continue
            device_data_dict[device_id] = device_data['latest']
            if len(device_data_dict) % 100 == 0:
                print("progress {}/{}".format(len(device_data_dict), len(device_id_in_dict) / int(total_shard)))

    # convert the data into data model
    db_records = []
    for device_id in device_data_dict:
        device_data = device_data_dict[device_id]
        if 'pm2_5' not in device_data:
            continue
        if device_id not in device_id_in_dict:
            continue
        db_records.append({
            "device_id": device_id,
            "time": device_data['time'],
            "longitude": device_id_in_dict[device_id]['lon'],
            "latitude": device_id_in_dict[device_id]['lat'],
            "pm2_5": device_data['pm2_5'],
        })
    return {"status": "ok", "value": db_records}

def _list_geo_devices():
    # device_id2loc_dict is keyed by $device id with device's geo location data
    device_id2loc_dict = {}

    base_url = 'https://wot.moenv.gov.tw/Layer/get_json'

    query_iot = { 'url': 'http://10.0.100.184/api/v1/device/iot?fields=lon,lat,desc,name&display=true' }
    query_nat = { 'url': 'http://10.0.100.184/api/v1/device/national_hr?fields=lon,lat,desc,name' }

    query_params = [ query_iot, query_nat ]
    for query_param in query_params:
        #with debug_requests():
        resp = requests.get(base_url, params=query_param)
        device_json_list = resp.json()
        for device_json in device_json_list:
            if '_id' not in device_json or len(device_json['_id']) == 0:
                # check with valid device._id
                continue
            if 'lat' not in device_json or 'lon' not in device_json:
                # check valid lat/lon
                continue
            device_id2loc_dict[device_json['_id']] = {
                'lat':device_json['lat'],
                'lon':device_json['lon'],
            }
    return device_id2loc_dict

# _get_device_datapoint get $device_id 's sensor data from $start_time to $end_time
# and return the last data from the time range
def _get_device_datapoint(device_id: str, start_time:datetime, end_time:datetime):
    start_time_in_str = start_time.strftime("%Y/%m/%d %H:00:00")
    end_time_in_str = end_time.strftime("%Y/%m/%d %H:00:00")

    # device_datapoint is keyed by time, contain the last data from the device
    device_datapoint = {}

    base_url = 'https://wot.moenv.gov.tw/Layer/get_json'
    #query_type = 'pm2_5,pm10,co,voc'
    # we only query pm2.5
    query_type = 'pm2_5'
    query_params = {
        'url':
        'http://10.0.100.184/api/v1/data/device/{0}/{3}/{1}/{2}/0?'.format(device_id, start_time, end_time, query_type),
    }
    try:
        resp = requests.get(base_url, params=query_params, timeout=10)
        if resp.status_code != 200:
            return device_datapoint
        device_datapoints_json = resp.json()
        if not device_datapoints_json:
            return device_datapoint
    except requests.exceptions.Timeout:
        print("request timeout with device id:{}".format(device_id))
        return device_datapoint

    current_latest: datetime= None
    latest_val = {}
    for datapoint in device_datapoints_json:
        datapoint_time = datetime.strptime(datapoint['time'], '%Y-%m-%dT%H:%M:%S.%fZ')

        if not current_latest or current_latest < datapoint_time:
            val = { }
            if 'pm2_5' in datapoint:
                val['pm2_5'] = datapoint['pm2_5']
            if 'pm10' in datapoint:
                val['pm10'] = datapoint['pm10']
            if 'voc' in datapoint:
                val['voc'] = datapoint['voc']
            if 'time' in datapoint:
                val['time'] = datapoint_time
            current_latest = datapoint_time
            latest_val = val
    device_datapoint['latest'] = latest_val 
    return device_datapoint

def get_aqi_info_dag():
    with DagFactory(DAG_ID, 
                    "airflow", 
                    datetime(2023, 4, 27),
                    "@daily",
                    params={
                        "shard_id": Param(1, type="integer"),
                        "total_shard": Param(10, type="integer"),
                        "connection_id": Param("postgres_default", type="string") },
                    template_searchpath=["/tmp", os.path.dirname(os.path.realpath(__file__))],
                    ).dag() as dag:

        shard_id = dag.params["shard_id"]
        total_shard = dag.params["total_shard"]

        pop = PostgresOp(dag, dag.params["connection_id"])

        getraininfotask = get_aqi_info_op(dag)

        createtabletask = pop.create_table("sql/aqi_info_schema.sql")

        tmpfile = "/tmp/aqi_info_bulk_stmt_{}_{}.sql".format(shard_id, total_shard)
        generatestms = pop.generate_bulk_insert_stmt("aqi_info",
                                                   "get_aqi_info",
                                                     tmpfile, True, "device_id")
        bulkinserttask = pop.bulk_insert_from_stmt(tmpfile)

        createtabletask >> getraininfotask >> generatestms >>  bulkinserttask 

get_aqi_info_dag()
