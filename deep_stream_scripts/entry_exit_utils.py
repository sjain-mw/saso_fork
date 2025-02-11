from datetime import datetime
import pyds
import sys
sys.path.append('../')
sys.path.append('/opt/nvidia/deepstream/deepstream-6.4/sources/deepstream_python_apps/apps')
import gi
import configparser
gi.require_version('Gst', '1.0')
from gi.repository import  Gst
from ctypes import *
import time
import sys
import math
import platform
import re
import uuid
import pyds
import requests
import random
import json
from datetime import datetime
import pprint
from collections import  Counter
pp = pprint.PrettyPrinter(indent=4)
import cv2
import numpy as np
from json import dumps
from kafka import KafkaProducer
from logger_config import common_logger

import utils
import common_vars
import check_utils
import lane_stage_utils
import Levenshtein

# Intialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: dumps(x).encode('utf-8')
    # retries=5,
    # retry_backoff_ms=1000,  # Wait 1 second before retrying
    # request_timeout_ms=30000,  # Request timeout (30 seconds)
    # metadata_max_age_ms=600000 
)

def get_current_stage_prev_track_id(lane_id,stage_sequence_number):
    url = f"http://localhost:4004/inspection-stage-logs/get-vehicle-uuid-and-trackid/{lane_id}"

    # Query parameters
    params = {
        "sequence": stage_sequence_number
    }

    try:
        # Make the GET request
        response = requests.get(url, params=params)
        
        # Check for HTTP request success
        if response.status_code == 200:
            # Parse and print the JSON response
            data = response.json()
            # type_n  =type(data["number_plate"])
            # data_n,conf_n ,inspection_id= data['number_plate'],data['insp_log_confidence'],data['inspection_id']
            track_id , uuid , number_plate ,confidence= data['track_id'] , data['vehicle_uuid'],data['number_plate'],data['confidence']
            # common_logger.error(f"Response Data1:   {data_n} {data[0]}")
            # conf_n =  
            # inspection_id =  
            # common_logger.error(f"Response Data1:   {conf_n} {inspection_id}")
            
            # if not conf_n:
            #     conf_n=0
            # conf_n = float(conf_n)
            # common_logger.error(f"Response Data2:   {conf_n} {total_checks_n}")
            
            # common_logger.error(f"Response Data:  {conf}")
            return track_id,uuid,number_plate,confidence
        else:
            common_logger.error(f"Failed to retrieve data: {response.status_code} - {response.reason}")
    
    except requests.RequestException as e:
        common_logger.error(f"An error occurred: {e}")

    return None,None,None

def get_lp_360(license_plate):

   
    url  = f"http://localhost:4004/inspection/is-vehicle-visited-today?numberPlate={license_plate}"
    common_logger.error(f"{url} {type(license_plate)}")
    try:
        # Make the GET request
        response = requests.get(url)
        # response.raise_for_status()
        # Check for HTTP request success
        if response.status_code == 200:
            
            # Parse and print the JSON response
            txt_data = response.text
            common_logger.error(f"Response Data at 360 api : {txt_data}")
            data = response.json()

            type_n  =type(data)
            common_logger.error(f"Response Data at 360 api : {data}")
            data_n,conf_n ,inspection_id= data['number_plate'],data['confidence'],data['inspectionId']
            
            common_logger.error(f"Response Data at 360 api :   {data_n} {conf_n} {inspection_id} {data} ")
            # conf_n =  
            # inspection_id =  
            # common_logger.error(f"Response Data1:   {conf_n} {inspection_id}")
            
            if not conf_n:
                conf_n=0
            conf_n = float(conf_n)
            # common_logger.error(f"Response Data2:   {conf_n} {total_checks_n}")
            
            # common_logger.error(f"Response Data:  {conf}")
            return str(data['number_plate']),conf_n,inspection_id
        else:
            common_logger.error(f"Failed to retrieve data: {response.status_code} - {response.reason}")
    
    except requests.RequestException as e:
        common_logger.error(f"An error occurred: {e}")

    return None,None,None


def get_license_plate(lane_id, stage_sequence_number, uuid):

    url = f"http://localhost:4004/inspection-stage-logs/get-number-plate-and-logs-count/{lane_id}"

    # Query parameters
    params = {
        "sequence": stage_sequence_number
    }

    try:
        # Make the GET request
        response = requests.get(url, params=params)
        
        # Check for HTTP request success
        if response.status_code == 200:
            # Parse and print the JSON response
            data = response.json()
            # type_n  =type(data["number_plate"])
            data_n,conf_n ,inspection_id= data[0]['number_plate'],data[0]['insp_log_confidence'],data[0]['inspection_id']
            
            # common_logger.error(f"Response Data1:   {data_n} {data[0]}")
            # conf_n =  
            # inspection_id =  
            # common_logger.error(f"Response Data1:   {conf_n} {inspection_id}")
            
            if not conf_n:
                conf_n=0
            conf_n = float(conf_n)
            # common_logger.error(f"Response Data2:   {conf_n} {total_checks_n}")
            
            # common_logger.error(f"Response Data:  {conf}")
            return str(data[0]['number_plate']),conf_n,inspection_id
        else:
            common_logger.error(f"Failed to retrieve data: {response.status_code} - {response.reason}")
    
    except requests.RequestException as e:
        common_logger.error(f"An error occurred: {e}")

    return None,None,None


def update_exit_status(lane_no,stage,stream_id):
    """update exit status to lane stage details

    Args:
        lane_no ([string]): current lane string being processed
        stage ([string]): current stage string being processed
        stream_id ([int]): Id of the current stream being processed
    """
    print("In exit update function")
    curr_vehicle_id=0
    conf_value = 0
    max_conf_value = 0
    # Update lane stage dictionary with exit details
    common_vars.lane_stage_details[lane_no][stage]["STATUS"] = "Exit"
    now = datetime.now()
    common_vars.lane_stage_details[lane_no][stage]["exit_time"] = now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    if stage=="break":
        print("Vehicle ID deque ",common_vars.lane_stage_details[lane_no][stage]["vehicle_ID_deque"])
    if len(common_vars.lane_stage_details[lane_no][stage]["vehicle_ID_deque"]) > 0 or common_vars.lane_stage_details[lane_no][stage]["vehicle_id"]!="" :
        if len(common_vars.lane_stage_details[lane_no][stage]["vehicle_ID_deque"]) > 0:
            curr_vehicle_id = Counter(common_vars.lane_stage_details[lane_no][stage]["vehicle_ID_deque"])
            curr_vehicle_id = curr_vehicle_id.most_common(1)[0][0] 
            veh_id_conf = common_vars.lane_stage_details[lane_no][stage]["vehicle_ID_deque_conf"]
            # common_logger.debug(f"Confidence queue {veh_id_conf}")
            conf_value = [float(common_vars.lane_stage_details[lane_no][stage]["vehicle_ID_deque_conf"][i]) for i, x in enumerate(common_vars.lane_stage_details[lane_no][stage]["vehicle_ID_deque"]) if x == curr_vehicle_id]
            # print("vehicle deque ",common_vars.lane_stage_details[lane_no][stage]["vehicle_ID_deque"])
            # print("CONF VALUE ",conf_value)
            # common_logger.debug(f"CMax value conf {conf_value}")
            max_conf_value = max(conf_value)
            # if common_vars.lane_stage_details[lane_no][stage]["vehicle_id"] != "" and common_vars.lane_stage_details[lane_no][stage]["vehicle_id_conf"] > max_conf_value:
            #     curr_vehicle_id = common_vars.lane_stage_details[lane_no][stage]["vehicle_id"]
            #     max_conf_value = common_vars.lane_stage_details[lane_no][stage]["vehicle_id_conf"] 
            #     common_logger.debug(f"Updating lp from previous because conf is higher event in {stage} {curr_vehicle_id} {max_conf_value} ")

            common_vars.lane_stage_details[lane_no][stage]["vehicle_id"] = curr_vehicle_id
            common_vars.lane_stage_details[lane_no][stage]["vehicle_id_conf"] = max_conf_value
        # elif common_vars.lane_stage_details[lane_no][stage]["vehicle_id"] != "":
            
        #     curr_vehicle_id = common_vars.lane_stage_details[lane_no][stage]["vehicle_id"]
        #     max_conf_value = common_vars.lane_stage_details[lane_no][stage]["vehicle_id_conf"] 
        #     common_logger.debug(f"Updating lp from previous because current is empty event in {stage} {curr_vehicle_id} {max_conf_value} ")
        # print(common_vars.lane_stage_details[lane_no][stage]["vehicle_id_conf"])
        common_logger.debug(f"Current lp value in exit {curr_vehicle_id} {max_conf_value}")
        if stage == "360":
            prev_lp,prev_lp_conf_value, prev_inspection_id=get_lp_360(curr_vehicle_id)
            print(f"Prev lp in main_entrance {prev_lp} {prev_lp_conf_value} { prev_inspection_id} curr id {curr_vehicle_id}")
            common_logger.debug(f"Prev lp in main_entrance {prev_lp} {prev_lp_conf_value} { prev_inspection_id} curr id {curr_vehicle_id}") ## Need lane_id, stage_sequence_number, uuid
            if prev_lp!=None and prev_lp!="" and len(prev_lp)<9:
                common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP'] = prev_lp
                common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_conf'] = prev_lp_conf_value
                common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_inspection_id'] = prev_inspection_id

        if common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP'] !=0 and common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP'] !="" and len(common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP'])<9:
            similarity_distance = Levenshtein.distance(common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP'] , curr_vehicle_id)
            prev_lp  = common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP']
            prev_lp_conf = common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_conf']
            common_logger.debug(f"Similarity distance between {curr_vehicle_id} and {prev_lp} {prev_lp_conf} {similarity_distance}")
            
            if similarity_distance < 4 :
                if common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_conf'] > max_conf_value :
                    common_logger.debug(f"Updating curr lp {curr_vehicle_id} with {prev_lp} ")
                    common_vars.lane_stage_details[lane_no][stage]["vehicle_id"] = common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP']
                    common_vars.lane_stage_details[lane_no][stage]["vehicle_id_conf"] = common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_conf']
                    common_vars.lane_stage_details[lane_no][stage]["inspection_id"] = common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_inspection_id']
                else:
                   common_vars.lane_stage_details[lane_no][stage]["inspection_id"] = common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_inspection_id']
             
        else:
            common_vars.lane_stage_details[lane_no][stage]["inspection_id"] = common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_inspection_id']
        
    if common_vars.DEBUG:
        common_logger.debug(f"Lane stage details Exit: {common_vars.exit_count}")
        common_logger.debug(f"Lane stage details: {common_vars.lane_stage_details}")
    
    # Push data to platform 
    message_exit = {}
    message_exit["cameraId"]= common_vars.input_config[str(stream_id)]["cameraId"]
    message_exit['stageId'] = common_vars.input_config[str(stream_id)]["stageId"]
    if common_vars.lane_stage_details[lane_no][stage]["vehicle_id"] == "":
        vehicle_uid = common_vars.lane_stage_details[lane_no][stage]["vehicle_uuid"] 
        if common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP']!="" and common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP']!=0:
            prev_lp =  common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP']    
            prev_lp_conf = str(common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_conf'] )
            prev_lp_insp = str(common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_inspection_id'])
            common_logger.debug(f"{vehicle_uid} of lane {lane_no} { stage} updated to {prev_lp} {prev_lp_conf} {prev_lp_insp}")
            common_vars.lane_stage_details[lane_no][stage]["vehicle_id"] = common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP'] 
            common_vars.lane_stage_details[lane_no][stage]["vehicle_id_conf"] = common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_conf'] 
            common_vars.lane_stage_details[lane_no][stage]["inspection_id"] = common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_inspection_id']        
        elif len(common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP'])>8:
            common_vars.lane_stage_details[lane_no][stage]["vehicle_id"] = common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP']
            common_vars.lane_stage_details[lane_no][stage]["inspection_id"] = common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_inspection_id']
        else:
            common_vars.lane_stage_details[lane_no][stage]["vehicle_id"] = vehicle_uid
            common_vars.lane_stage_details[lane_no][stage]["inspection_id"] = common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_inspection_id']

    message_exit['vehicleNumberPlate'] = common_vars.lane_stage_details[lane_no][stage]["vehicle_id"]
    message_exit['checkEndTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    message_exit['checkDone'] = common_vars.lane_stage_details[lane_no][stage]["checking_status"]
    message_exit['useCaseType'] = "stageCameraExits"
    message_exit['vehicleUuid']= str(common_vars.lane_stage_details[lane_no][stage]["vehicle_uuid"])
    message_exit['inspectionId'] = str(common_vars.lane_stage_details[lane_no][stage]["inspection_id"])
    message_exit['trackId'] = str(common_vars.lane_stage_details[lane_no][stage]["object_id"])
    message_exit['confidence']  = str(common_vars.lane_stage_details[lane_no][stage]["vehicle_id_conf"])

    # Construct the message entrance object
    message_entrance = {}
    message_entrance['cameraId'] = common_vars.input_config[str(stream_id)]["cameraId"]
    message_entrance['vehicleNumberPlate'] = common_vars.lane_stage_details[lane_no][stage]["vehicle_id"]
    message_entrance['journeyStartTime'] = common_vars.lane_stage_details[lane_no][stage]["entry_time"]
    message_entrance['stationId'] = common_vars.input_config[str(stream_id)]["stationID"]
    # Added color and model
    # message_entrance['vehicleColor'] = common_vars.lane_stage_details[lane_no][stage]["vehicleColor"]
    # message_entrance['vehicleType'] = common_vars.lane_stage_details[lane_no][stage]["vehicleType"]
    message_entrance['useCaseType'] = "stationEntryCamera"
    
    # Check end time and start time > 7 sec
    t1 = common_vars.lane_stage_details[lane_no][stage]["entry_time"]
    t2 = message_exit['checkEndTime']
     
     # Convert the strings back to datetime objects
    t1 = datetime.strptime(t1, '%Y-%m-%d %H:%M:%S.%f')
    t2 = datetime.strptime(t2, '%Y-%m-%d %H:%M:%S.%f')

    # Calculate the difference
    t3 = t2 - t1
    print("Update exit status ",message_exit)
    # Get the difference in seconds
    difference_in_seconds = t3.total_seconds()
    if stage == "carbon" or stage == "ub" :
        if difference_in_seconds < 10:
            message_exit['checkDone'] = False
    if stage =="main":
        producer.send('testingsaso-v2', value=message_entrance)
        common_logger.debug(f"Sent vehicle entrance log for lane {lane_no} and stage {stage}: {message_entrance}")
    else:
        # print(message)
        # if len(message_exit['vehicleNumberPlate']) < 8 and difference_in_seconds >= 6:
        if difference_in_seconds >= 0:
            # producer.send('testingsaso-v2', value=message_entrance)
            producer.send('testingsaso-v2', value=message_exit)

            # common_logger.debug(f"Sent vehicle entrance log for lane {lane_no} and stage {stage}: {message_entrance}")
            common_logger.debug(f"Sent vehicle stage exit log for lane {lane_no} and stage {stage} : {message_exit}")
            common_logger.debug(f"Stage name ----- {common_vars.input_config[str(stream_id)]['name']}, {stage} ,{type(stage)},{type(lane_no)}")

            if lane_no == "10" and stage == "360" :

                common_logger.debug(f"Stage name ----- {common_vars.input_config[str(stream_id)]['name']}, {satge} ,{type(stage)}")
                message_exit['vehicleUuid']= str(common_vars.lane_stage_details[lane_no][stage]["carbon_checking_vehicle_uuid"])
                message_exit['stageId'] = common_vars.input_config[str(stream_id)]["carbonstageId"]
                message_exit['checkDone'] = common_vars.lane_stage_details[lane_no][stage]["carbon_checking_status"]
                producer.send('testingsaso-v2', value=message_exit)
                common_logger.debug(f"Sent vehicle stage exit log carbon stage for lane {lane_no} and stage {stage} : {message_exit}")
                
    print("Exit sent ",message_exit)
    # Reinitialize lane stage dictionary
    lane_stage_utils.default_lane_details(lane_no,stage)
    common_vars.exit_count+=1

def update_entry_status(lane_no,obj_id,stage,stream_id):
    print("In update entry")
    # Update lane stage dictionary with entry details
    common_vars.lane_stage_details[lane_no][stage]["STATUS"] = "Entry"
    now = datetime.now()
    common_vars.lane_stage_details[lane_no][stage]["entry_time"]=now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"] = 0
    common_vars.lane_stage_details[lane_no][stage]["object_id"] = obj_id
    

    # common_logger.debug(f"{get_license_plate('478c7a31-4900-45ab-838e-c502e6e30f67', 3, '478c7a31-4900-45ab-838e-c502e6e30f67')}")
    common_logger.debug(f"{common_vars.lane_id} {common_vars.stage_sequence}")
    # uuid = common_vars.lane_stage_details[lane_no][stage]["vehicle_uuid"]
    laneId = common_vars.lane_id[str(stream_id)]
    stage_sequence_number = common_vars.stage_sequence[stage]
    common_logger.debug(f"{laneId} {stage_sequence_number}")
    # common_logger.debug(f"Before calling api get license plate")
    # print("Before calling api get license plate")
    # st = time.time()
    
    # track_id, v_uuid,v_lp , v_confidence = get_current_stage_prev_track_id(laneId, stage_sequence_number)
    # common_logger.debug(f"Prev exit in {stage} {track_id} {v_uuid} {v_lp} {v_confidence} curr track id {obj_id} ") ## Need lane_id, stage_sequence_number, uuid
    # if track_id == str(obj_id):
    #     common_logger.debug(f"Updating old vehicle id ")
    #     common_vars.lane_stage_details[lane_no][stage]["vehicle_uuid"] = v_uuid
    #     if len(v_lp) < 9:
    #         common_vars.lane_stage_details[lane_no][stage]["vehicle_id"] = v_lp
    #         common_vars.lane_stage_details[lane_no][stage]["vehicle_id_conf"] = v_confidence
    # else:

        
    common_vars.lane_stage_details[lane_no][stage]["vehicle_uuid"] = str(uuid.uuid1())
    #     new_uuid = common_vars.lane_stage_details[lane_no][stage]["vehicle_uuid"]
        # common_logger.debug(f"Creating new uuid {new_uuid}")

    if stage_sequence_number!=1:
        # common_logger.debug(f"Calling get license plate ")
        prev_lp,prev_lp_conf_value, prev_inspection_id=get_license_plate(laneId, stage_sequence_number, uuid)
        print(f"Prev lp in entry {prev_lp} {prev_lp_conf_value} { prev_inspection_id} ")
        common_logger.debug(f"Prev lp in entry {prev_lp} {prev_lp_conf_value} { prev_inspection_id} ") ## Need lane_id, stage_sequence_number, uuid
        if prev_lp!=None and prev_lp!="" and len(prev_lp)<9:
            common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP'] = prev_lp
            common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_conf'] = prev_lp_conf_value
            common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_inspection_id'] = prev_inspection_id
        elif prev_lp!=None and prev_lp!="" and len(prev_lp)>8:
            common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP']  = prev_lp
            common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_inspection_id'] = prev_inspection_id
            common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP_conf'] = 0
        
    # prev_lp  = common_vars.lane_stage_details[lane_no][stage]['previous_stage_LP'] 
    # common_logger.debug(f"After calling api get license plate {time.time()-st} {prev_lp} ")
    # print("After calling api get license plate")
    

    if common_vars.DEBUG:
        common_logger.debug(f"Lane stage details Entry: {common_vars.entry_count}")
        common_logger.debug(f"Lane stage details: {common_vars.lane_stage_details}")

    if stage != 'main':
        message_entry = {}
        message_entry["cameraId"]= common_vars.input_config[str(stream_id)]["cameraId"]
        message_entry['vehicleNumberPlate'] = ""
        message_entry['stageId'] = common_vars.input_config[str(stream_id)]["stageId"]
        message_entry['checkStartTime'] = common_vars.lane_stage_details[lane_no][stage]["entry_time"]
        message_entry['useCaseType'] = "stageCameraEntries"
        message_entry['vehicleUuid']= str(common_vars.lane_stage_details[lane_no][stage]["vehicle_uuid"])
        
        common_vars.entry_count+=1
        producer.send('testingsaso-v2', value=message_entry)
        common_logger.debug(f"Sent vehicle entrance log for lane {lane_no} and stage {stage} : {message_entry}")

        if lane_no == 10 and "top_360" in common_vars.input_config[str(stream_id)]["name"]:
            #carbonstageId
            common_vars.lane_stage_details[lane_no][stage]["carbon_checking_vehicle_uuid"] = str(uuid.uuid1())
            message_entry['vehicleUuid']= str(common_vars.lane_stage_details[lane_no][stage]["carbon_checking_vehicle_uuid"])
            message_entry['stageId'] = common_vars.input_config[str(stream_id)]["carbonstageId"]

            common_vars.entry_count+=1
            producer.send('testingsaso-v2', value=message_entry)
            common_logger.debug(f"Sent vehicle entrance log for carbon check - lane {lane_no} and stage {stage} : {message_entry}")
    print("Update entry sent ",message_entry)

            

def entry_exit_360_update(user_meta_data,obj_meta,lane_no,stage,stream_id,coordinate_pos):
    
    if obj_meta.class_id != common_vars.PGIE_CLASS_ID_VEHICLE and obj_meta.class_id != common_vars.PGIE_CLASS_ID_TRUCK and obj_meta.class_id != common_vars.PGIE_CLASS_ID_TOP_TRUCK and obj_meta.class_id != common_vars.PGIE_CLASS_ID_TOP_CAR:
        return 
    
    if common_vars.DEBUG:
        print("Lane no ",lane_no," ",stage)

    bbox_info = obj_meta.detector_bbox_info
    cast_bbox_info = pyds.NvDsComp_BboxInfo.cast(bbox_info)
    bbox_coords = pyds.NvBbox_Coords.cast(cast_bbox_info)
    if coordinate_pos== "centre":
        print("Calculating centre")
        point = (bbox_coords.left+bbox_coords.width/2 , bbox_coords.top+bbox_coords.height/2)
    elif coordinate_pos == "bottom":
        print("Calculating bottom")
        point = (bbox_coords.left+bbox_coords.width/2 , bbox_coords.top+bbox_coords.height)
    # point = (bbox_coords.left+bbox_coords.width/2 , bbox_coords.top)
    # print("----------- point---------------------", point)
    # print("Checking point in polygon",point," ",obj_meta.object_id)
    # print("ROI coord",common_vars.roi_coordinates)
    # Check vehicle is in roi polygon for threshold amount of frames and update entry
    # print("ROI coord",common_vars.roi_coordinates[lane_no])
    # print("lane_no",lane_no)
    # print("Checking vehicle ",obj_meta.object_id," ",point," ",common_vars.roi_coordinates[lane_no])
    if int(lane_no) in [1,2,3,4]:
        point_in_polygon = utils.is_point_inside_polygon(point,common_vars.roi_coordinates[lane_no])
        print("Checking vehicle ",obj_meta.object_id," ",point," ",common_vars.roi_coordinates[lane_no])
    else:
        point_in_polygon = utils.is_point_inside_polygon(point,common_vars.top_360_roi_coordinates[lane_no])
        print("Checking vehicle ",obj_meta.object_id," ",point," ",common_vars.top_360_roi_coordinates[lane_no])
        
    
    if point_in_polygon:
        
        # if common_vars.DEBUG:
        print("Point in Polygon ",obj_meta.object_id)
        if common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]==0 and\
              common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Waiting" and obj_meta.object_id\
              not in common_vars.lane_stage_details[lane_no][stage]["car_ids"] :
            common_vars.lane_stage_details[lane_no][stage]["car_ids"].append(obj_meta.object_id)
        print("Car ids ",common_vars.lane_stage_details[lane_no][stage]["car_ids"])
        common_vars.lane_stage_details[lane_no][stage]["vehicle_in_roi"] .append(obj_meta.object_id)
        
        # if common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry" and \
        #     common_vars.lane_stage_details[lane_no][stage]["object_id"] != obj_meta.object_id:
        #     #print("trigger exit in fn")
        #     lane_stage_utils.default_check_details_360(lane_no,stage)
        #     update_exit_status(lane_no,stage ,stream_id)
        if common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry" and obj_meta.object_id ==common_vars.lane_stage_details[lane_no][stage]["object_id"]:
            print("reinitalising")
            common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"] =0
            
        print("No obj frames ",stage," ",common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]," ",common_vars.lane_stage_details[lane_no][stage]["STATUS"])
        if common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Waiting" and obj_meta.object_id in common_vars.lane_stage_details[lane_no][stage]["car_ids"]:
            common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]+=1
        

            if common_vars.DEBUG:
                print("NO obj frames ",common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"])
                
            if common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]>=common_vars.EVENT_THRESHOLD :
                print("trigger entry in fn ",obj_meta.object_id)
                if common_vars.lane_stage_details[lane_no][stage]["object_id"] =="":
                    update_entry_status(lane_no,obj_meta.object_id,stage,stream_id)
                
    # if vehicle is not in polygon check for threshold amount of frames and update exit
    elif common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry" and \
        obj_meta.object_id in common_vars.lane_stage_details[lane_no][stage]["car_ids"]:
        if common_vars.DEBUG:
            print("Point not in Polygon ",obj_meta.object_id)
            print("NO obj frames ",common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"])
            
        common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]+=1
        if common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"] >= common_vars.EXIT_EVENT_THRESHOLD:
            #print("trigger exit in fn")
            common_vars.check_details_360[lane_no][stage]['car_position'] = None
            common_vars.check_details_360[lane_no][stage]['stationary_frame_count'] = 0
            common_vars.check_details_360[lane_no][stage]['car_stationary'] = False
            common_vars.check_details_360[lane_no][stage]['new_car_count'] = 0
            #lane_stage_utils.default_check_details_360(lane_no,stage)
            print("trigger exit in fn ",obj_meta.object_id)
            update_exit_status(lane_no,stage ,stream_id)
            lane_stage_utils.default_check_details_360_truck(lane_no,stage)

               
def entry_exit_brake_update(user_meta_data,obj_meta,lane_no,stage,stream_id):
    
    if obj_meta.class_id != common_vars.PGIE_CLASS_ID_VEHICLE and obj_meta.class_id != common_vars.PGIE_CLASS_ID_TRUCK and obj_meta.class_id != common_vars.PGIE_CLASS_ID_TOP_TRUCK and obj_meta.class_id != common_vars.PGIE_CLASS_ID_TOP_CAR:
        return 
    
    if common_vars.DEBUG:
        print("Lane no ",lane_no," ",stage)

    bbox_info = obj_meta.detector_bbox_info
    cast_bbox_info = pyds.NvDsComp_BboxInfo.cast(bbox_info)
    bbox_coords = pyds.NvBbox_Coords.cast(cast_bbox_info)
    point = (bbox_coords.left+bbox_coords.width/2 , (bbox_coords.top+bbox_coords.height)-100)
    # point = (bbox_coords.left+bbox_coords.width/2 , bbox_coords.top)
    # print("----------- point---------------------", point)
    # print("Checking point in polygon",point," ",obj_meta.object_id)
    # print("ROI coord",common_vars.roi_coordinates)
    # Check vehicle is in roi polygon for threshold amount of frames and update entry
    # print("ROI coord",common_vars.roi_coordinates[lane_no])
    if utils.is_point_inside_polygon(point,common_vars.roi_coordinates[lane_no]):
        
        # if common_vars.DEBUG:
        print("Point in Polygon ",obj_meta.object_id)
        if common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]==0 and\
              common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Waiting" and obj_meta.object_id\
              not in common_vars.lane_stage_details[lane_no][stage]["car_ids"] :
            common_vars.lane_stage_details[lane_no][stage]["car_ids"].append(obj_meta.object_id)
        print("Car ids ",common_vars.lane_stage_details[lane_no][stage]["car_ids"])
        common_vars.lane_stage_details[lane_no][stage]["vehicle_in_roi"] .append(obj_meta.object_id)
        

        if common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry" and obj_meta.object_id==common_vars.lane_stage_details[lane_no][stage]["object_id"]:
            common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"] =0
            
        print("No obj frames ",stage," ",common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]," ",common_vars.lane_stage_details[lane_no][stage]["STATUS"])
        if common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Waiting" and obj_meta.object_id in common_vars.lane_stage_details[lane_no][stage]["car_ids"]:
            common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]+=1
        

            if common_vars.DEBUG:
                print("NO obj frames ",common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"])
                
            if common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]>=common_vars.EVENT_THRESHOLD :
                print("trigger entry in fn ",obj_meta.object_id)
                if common_vars.lane_stage_details[lane_no][stage]["object_id"] =="":
                    update_entry_status(lane_no,obj_meta.object_id,stage,stream_id)
                
    # if vehicle is not in polygon check for threshold amount of frames and update exit
    elif common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry" and \
        obj_meta.object_id in common_vars.lane_stage_details[lane_no][stage]["car_ids"]:
        if common_vars.DEBUG:
            print("Point not in Polygon ",obj_meta.object_id)
            print("NO obj frames ",common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"])
            
        common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]+=1
        if common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"] >= common_vars.EXIT_EVENT_THRESHOLD:
            #print("trigger exit in fn")
            print("trigger exit in fn ",obj_meta.object_id)
            update_exit_status(lane_no,stage ,stream_id)
            

def entry_exit_360_update_exit17(user_meta_data,obj_meta,lane_no,stage,stream_id):

    # Carbon check
    check_utils.carbon_check_exit17(user_meta_data,obj_meta,lane_no,stage,stream_id)
    
    
    if obj_meta.class_id != common_vars.PGIE_CLASS_ID_VEHICLE and obj_meta.class_id != common_vars.PGIE_CLASS_ID_TRUCK and obj_meta.class_id != common_vars.PGIE_CLASS_ID_TOP_TRUCK and obj_meta.class_id != common_vars.PGIE_CLASS_ID_TOP_CAR:
        return 
    
    if common_vars.DEBUG:
        print("Lane no ",lane_no," ",stage)

    bbox_info = obj_meta.detector_bbox_info
    cast_bbox_info = pyds.NvDsComp_BboxInfo.cast(bbox_info)
    bbox_coords = pyds.NvBbox_Coords.cast(cast_bbox_info)
    point = (bbox_coords.left+bbox_coords.width/2 , bbox_coords.top+bbox_coords.height/2)
    # point = (bbox_coords.left+bbox_coords.width/2 , bbox_coords.top)
    # print("----------- point---------------------", point)
    # print("Checking point in polygon",point," ",obj_meta.object_id)
    # print("ROI coord",common_vars.roi_coordinates)
    # Check vehicle is in roi polygon for threshold amount of frames and update entry
    # print("ROI coord",common_vars.roi_coordinates[lane_no])
    if utils.is_point_inside_polygon(point,common_vars.roi_coordinates[lane_no]):
        if obj_meta.object_id not in common_vars.lane_stage_details[lane_no][stage]["car_ids"]:
            common_vars.lane_stage_details[lane_no][stage]["car_ids"].append(obj_meta.object_id)
        # if common_vars.DEBUG:
        #print("Point in Polygon ",obj_meta.object_id)
            #
        common_vars.lane_stage_details[lane_no][stage]["vehicle_in_roi"].append(obj_meta.object_id)
        
        
        # if common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry" and \
        #     common_vars.lane_stage_details[lane_no][stage]["object_id"] != obj_meta.object_id:
        #     #print("trigger exit in fn")
        #     lane_stage_utils.default_check_details_360(lane_no,stage)
        #     update_exit_status(lane_no,stage ,stream_id)
            
            
        if common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry":
            common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]=0
        if common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Waiting":
            common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]+=1
            
            if common_vars.DEBUG:
                common_logger.debug(f"NO obj frames exit {common_vars.lane_stage_details[lane_no][stage]['no_obj_frames']}")
                
            if common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]>=common_vars.EVENT_THRESHOLD :
                #print("trigger entry in fn")
                if common_vars.lane_stage_details[lane_no][stage]["object_id"] =="":
                    update_entry_status(lane_no,obj_meta.object_id,stage,stream_id)
                
    # if vehicle is not in polygon check for threshold amount of frames and update exit
    elif common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry" and \
        obj_meta.object_id in common_vars.lane_stage_details[lane_no][stage]["car_ids"]:
        if common_vars.DEBUG:
            common_logger.debug(f"Point not in Polygon {obj_meta.object_id}")
            common_logger.debug(f"NO obj frames {common_vars.lane_stage_details[lane_no][stage]['no_obj_frames']}")
            
        common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]+=1
        if common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]>=common_vars.EXIT_EVENT_THRESHOLD:
            #print("trigger exit in fn")
            common_vars.check_details_360[lane_no][stage]['car_position'] = None
            common_vars.check_details_360[lane_no][stage]['stationary_frame_count'] = 0
            common_vars.check_details_360[lane_no][stage]['car_stationary'] = False
            common_vars.check_details_360[lane_no][stage]['new_car_count'] = 0
            #lane_stage_utils.default_check_details_360(lane_no,stage)
            update_exit_status(lane_no,stage ,stream_id)
                     
def entry_exit_update(user_meta_data,obj_meta,lane_no,stage,stream_id):

    if obj_meta.class_id != common_vars.PGIE_CLASS_ID_VEHICLE and obj_meta.class_id != common_vars.PGIE_CLASS_ID_TRUCK:
        return 
    if common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry":
        common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]=0
        
    if any("lane" in key for key in user_meta_data.roiStatus):
        common_vars.lane_stage_details[lane_no][stage]["vehicle_in_roi"].append(obj_meta.object_id)
        
    if common_vars.DEBUG:
        common_logger.debug(f"Incrementing vehicle in ROI {common_vars.lane_stage_details[lane_no][stage]['vehicle_in_roi']} {common_vars.lane_stage_details[lane_no][stage]['STATUS']} {user_meta_data.lcStatus}")
    
    frame_crossing_status=""
    if user_meta_data.lcStatus:
        if common_vars.DEBUG:
            common_logger.debug(f"lc status {user_meta_data.lcStatus}")
        frame_crossing_status = "Entry" if "Entry" in user_meta_data.lcStatus[0] else "Exit"
    # if common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry" and obj_meta.object_id not in common_vars.lane_stage_details[lane_no][stage]["car_ids"]:
    #     update_exit_status(lane_no,stage,stream_id)
    # To handle exit trigger miss
    # if common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry" and \
    #         common_vars.lane_stage_details[lane_no][stage]["object_id"] != obj_meta.object_id:
    #     print(" Triggering exit ")
    #     update_exit_status(lane_no,stage,stream_id)
    print("In entry exit update ",stage," ",common_vars.lane_stage_details[lane_no][stage]["STATUS"]," " ,common_vars.lane_stage_details[lane_no][stage]["car_ids"])
    
    if frame_crossing_status == "Entry":
        # Trigger exit for previous entry if another entry is logged
        if common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry":
            update_exit_status(lane_no,stage,stream_id)
            
        update_entry_status(lane_no,obj_meta.object_id,stage,stream_id)
        
    elif frame_crossing_status == "Exit" and common_vars.lane_stage_details[lane_no][stage]["STATUS"]=="Entry":
        update_exit_status(lane_no,stage,stream_id)

    # Reinitialising if any one detection miss while waiting to trigger entry

    elif common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry" and any("lane" in key for key in user_meta_data.roiStatus) \
        and obj_meta.object_id== common_vars.lane_stage_details[lane_no][stage]["object_id"]:
        common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"] = 0
        
    # update entry using vehicle's ROI presence ,after considering set threshold
    elif common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Waiting"  and any("lane" in key for key in user_meta_data.roiStatus):
        
        
        if common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]==0 \
        and obj_meta.object_id not in common_vars.lane_stage_details[lane_no][stage]["car_ids"] :
            common_vars.lane_stage_details[lane_no][stage]["car_ids"].append(obj_meta.object_id)

        if obj_meta.object_id in common_vars.lane_stage_details[lane_no][stage]["car_ids"]:
            common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"] += 1

            if common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"] >= common_vars.EVENT_THRESHOLD :
                update_entry_status(lane_no,obj_meta.object_id,stage,stream_id)
        
        
        if common_vars.DEBUG:
            common_logger.debug(f"NO obj frames exit {common_vars.lane_stage_details[lane_no][stage]['no_obj_frames']}")
            
        
        
def update_exit_in_empty_roi(stream_id,lane_no,stage):
    # if stage=="360":
    print("Vehicle in roi "," ",stage," ",common_vars.lane_stage_details[lane_no][stage]["vehicle_in_roi"]," ",common_vars.lane_stage_details[lane_no][stage]["object_id"], " ",common_vars.lane_stage_details[lane_no][stage]["STATUS"])
    print("No obj frames ",stage," ",common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]," ",common_vars.input_config[str(stream_id)]["name"])
            
    if "entry_exit" in common_vars.input_config[str(stream_id)]["functionName"] or "carbon" in common_vars.input_config[str(stream_id)]["name"]:
        if common_vars.lane_stage_details[lane_no][stage]["object_id"] not in common_vars.lane_stage_details[lane_no][stage]["vehicle_in_roi"] and \
            common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry":
            
            if common_vars.DEBUG:
                common_logger.debug(f"No obj frames entry {common_vars.lane_stage_details[lane_no][stage]['no_obj_frames']}")
            print("Incrementing no obj frames ",stage," ",common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"])
            common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"] +=1
            # if stage=="360":
            if common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"] >= common_vars.EXIT_EVENT_THRESHOLD:
                if stage=="360":
                    #print("exit_360")
                    common_vars.check_details_360[lane_no][stage]['car_position'] = None
                    common_vars.check_details_360[lane_no][stage]['stationary_frame_count'] = 0
                    common_vars.check_details_360[lane_no][stage]['car_stationary'] = False
                    common_vars.check_details_360[lane_no][stage]['new_car_count'] = 0
                    lane_stage_utils.default_check_details_360_truck(lane_no,stage)
                    
                print("trigger exit in ROIempty ",stage, " ",common_vars.lane_stage_details[lane_no][stage]["object_id"])
                update_exit_status(lane_no,stage,stream_id)
        # Reinitialising if any one detection miss while waiting to trigger entry
        if len(common_vars.lane_stage_details[lane_no][stage]["vehicle_in_roi"]) == 0 and \
            common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Waiting":
            common_vars.lane_stage_details[lane_no][stage]["no_obj_frames"]=0

def delete_tracker():
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_time = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")
    
    for lane_no in common_vars.lane_stage_details:
        for stage in common_vars.lane_stage_details[lane_no]:  
            for tracking_id in common_vars.lane_stage_details[lane_no][stage]:
                car = common_vars.lane_stage_details[lane_no][stage][tracking_id]
                if int((current_time - car['current_time']).total_seconds()) > 2:
                    car['exit_time'] = current_time
                    common_logger.debug(f"Deleting Tracker: {tracking_id} with details - {car}")
                    back_track_id = common_vars.lane_stage_details[lane_no][stage][tracking_id]["associated_tracking_id"]
                    del common_vars.lane_stage_details[lane_no][stage][tracking_id]
                    del common_vars.ub_top_back_details[lane_no][back_track_id]
                    break


def update_tracker(tracking_id, bbox, lane_no, stage, lp_objects):
    subregion = utils.find_subregion(bbox, common_vars.subregions[0])
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_time = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")
    
    if tracking_id in common_vars.lane_stage_details[lane_no][stage]:
        car = common_vars.lane_stage_details[lane_no][stage][tracking_id]
        entry_time = car['entry_time']
        elapsed_time = int((current_time - entry_time).total_seconds())
        car['elapsed_time'] = elapsed_time
        car['current_time'] = current_time
        car['bbox'] = bbox
        car['subregion'] = subregion

    else:
        common_logger.debug(f"New Tracker Added at {current_time}")
        car = lane_stage_utils.create_vehicle_details()
        car['entry_time'] = current_time
        car["current_time"] = current_time
        car['bbox'] = bbox
        car['subregion'] = subregion
        
    
    if car["vehicle_ID_deque"]:
        counter = Counter(car["vehicle_ID_deque"])
        most_common = counter.most_common(1)
        element, _ = most_common[0]
        car["vehicle_id"] = element
    
    common_vars.lane_stage_details[lane_no][stage][tracking_id] = car
    

def ub_entry_exit_back(lane_no, ub_points):
    for track_id, data in ub_points.items():
        x, y, w, h = data[0]
        x1, y1, x2, y2 = utils.convert_coordinates(data[0])
        bbox = [x1, y1, x2, y2]
        label = data[1]
        label_name = common_vars.classes[int(label)]
        if label_name == "car":
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            current_time = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")
            if track_id in common_vars.ub_top_back_details[lane_no]:
                car = common_vars.ub_top_back_details[lane_no][track_id]
                car["bbox"] = bbox
            else:
                common_logger.debug(f"New Tracker Added at {current_time}")
                car = lane_stage_utils.create_vehicle_details_back()
                car["entry_time"] = current_time
                car["bbox"] = bbox
            common_vars.ub_top_back_details[lane_no][track_id] = car


def ub_entry_exit(ub_points, lane_no, stage, lp_objects):
    # print(ub_points)
    for track_id, data in ub_points.items():
        x, y, w, h = data[0]
        x1, y1, x2, y2 = utils.convert_coordinates(data[0])
        bbox = [x1, y1, x2, y2]
        label = data[1]
        label_name = common_vars.classes[int(label)]
        if label_name == "car":
            update_tracker(track_id, bbox, lane_no, stage, lp_objects)
            # common_vars.lane_stage_details = utils.convert_to_dict(common_vars.lane_stage_details)
    
    

    # ub_points[obj_meta.object_id] = [[msg_meta["bbox_top"],msg_meta["bbox_left"], msg_meta["bbox_width"],msg_meta["bbox_height"]],obj_meta.class_id,user_meta_data.roiStatus]