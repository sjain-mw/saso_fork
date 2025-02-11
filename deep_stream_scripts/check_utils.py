import sys
sys.path.append('../')
sys.path.append('/opt/nvidia/deepstream/deepstream-6.4/sources/deepstream_python_apps/apps')
import gi
import configparser
gi.require_version('Gst', '1.0')
from gi.repository import  Gst
from ctypes import *
import time
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
pp = pprint.PrettyPrinter(indent=4)
import cv2
import numpy as np
# from kafka import KafkaProducer
from logger_config import common_logger

import common_vars
import check_utils
import utils
import entry_exit_utils

def under_body_check(user_meta_data,obj_meta,lane_no,stage,stream_id):
    
    # if ub_operator is present in platform ROI for threshold frames check true for underbody
    if len(user_meta_data.roiStatus) != 0  and common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry":
        
        common_vars.lane_stage_details[lane_no][stage]["checking_no_frames"] += 1
        if common_vars.DEBUG:
            common_logger.debug(f"ROI Frame count {lane_no} {stage} {common_vars.lane_stage_details[lane_no][stage]['checking_no_frames']}")
            
        if common_vars.lane_stage_details[lane_no][stage]["checking_no_frames"] > common_vars.UB_CHECK_FRAME_THRESHOLD:
            
            common_vars.lane_stage_details[lane_no][stage]["checking_status"] = True
            if common_vars.DEBUG:
                common_logger.debug(f"Checking status {lane_no} {stage} {common_vars.lane_stage_details[lane_no][stage]['checking_status']}")


def break_check(vehicle_points,tyre_objects,frontal_tyre,lane_no,stage):
    for obj_meta,user_meta_data in tyre_objects:
            if common_vars.DEBUG:
                common_logger.debug(f"Object {obj_meta.class_id} Len vehicle {len(vehicle_points)} STATUS {common_vars.lane_stage_details[lane_no][stage]['STATUS']} roi {user_meta_data.roiStatus}")
            if obj_meta.class_id == common_vars.PGIE_CLASS_ID_TYRE and len(vehicle_points) and common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry":
                if any("tyre" in key for key in user_meta_data.roiStatus):
                    if common_vars.DEBUG:
                        common_logger.debug("In tyre")
                    
                    if common_vars.lane_stage_details[lane_no][stage]["checking_no_frames"]==0:
                        frontal_tyre = vehicle_points[0][4]
                        common_vars.brake_check_details[lane_no][stage]['brake_check_status'] = False
                        common_vars.brake_check_details[lane_no][stage]['brake_check_count']=0
                        
                    elif common_vars.lane_stage_details[lane_no][stage]["checked"]=="" or (common_vars.lane_stage_details[lane_no][stage]["checked"]=="Front" and not frontal_tyre) :
                        common_vars.brake_check_details[lane_no][stage]['brake_check_count']+=1
                        #brake_check_status = brake_check(frame_copy,tyre_xy,False,60,False)
                        if common_vars.brake_check_details[lane_no][stage]['brake_check_count']==common_vars.BRAKE_CHECK_FRAME :
                            common_vars.brake_check_details[lane_no][stage]['brake_check_status'] = True

                        
                        if common_vars.DEBUG:
                            common_logger.debug(f"Break check status {common_vars.brake_check_details[lane_no][stage]['brake_check_status']}")
                        if common_vars.brake_check_details[lane_no][stage]['brake_check_status']:
                            if frontal_tyre:
                                common_vars.lane_stage_details[lane_no][stage]["checked"]+="Front"
                                common_vars.brake_check_details[lane_no][stage]['brake_check_count']=0
                                common_vars.brake_check_details[lane_no][stage]['brake_check_status']= False
                                
                            else:
                                common_vars.lane_stage_details[lane_no][stage]["checked"]+="Back"
                                common_vars.brake_check_details[lane_no][stage]['brake_check_count']=0
                                common_vars.brake_check_details[lane_no][stage]['brake_check_status'] = False

                            if common_vars.lane_stage_details[lane_no][stage]["checked"]=="FrontBack" or common_vars.lane_stage_details[lane_no][stage]["checked"]=="BackFront":
                                common_vars.lane_stage_details[lane_no][stage]["checking_status"]=True
                                
                        
                    common_vars.lane_stage_details[lane_no][stage]["checking_no_frames"] += 1
                elif any("lane" in key for key in user_meta_data.roiStatus):
                    common_vars.brake_check_details[lane_no][stage]['brake_check_status']=False
                    common_vars.lane_stage_details[lane_no][stage]["checking_no_frames"]=0
                    
def main_entrance(user_meta_data,obj_meta,lane_no,stage,stream_id):
    entry_exit_utils.entry_exit_update(user_meta_data,obj_meta,lane_no,stage,stream_id)

def check_360_truck(vehicle_points,operator_xyxy,lane_no,stage):

    status=common_vars.lane_stage_details[lane_no][stage]["STATUS"]
    print(f"in chck 360 {status} {vehicle_points} {len(operator_xyxy)}")
    if common_vars.lane_stage_details[lane_no][stage]["STATUS"]=="Entry" and len(vehicle_points)!=0:
        truck_points = utils.convert_coordinates(vehicle_points[0])
        x1_2, y1_2, x2_2, y2_2 = map(int, truck_points)
        for rect in operator_xyxy:
            x1_1, y1_1, x2_1, y2_1= map(int, rect)

            if x2_1 < x1_2:
                common_vars.check_details_360[lane_no][stage]['truck_left']= True
            if x1_1 > x2_2:
                common_vars.check_details_360[lane_no][stage]['truck_right']= True
            if y2_1 < y1_2:
                common_vars.check_details_360[lane_no][stage]['truck_front']= True
            if y1_1 > y2_2:
                common_vars.check_details_360[lane_no][stage]['truck_back']= True
            print(f"comparison points :{ x2_1} {x1_2} {x1_1} {x2_2}")
        print(f"left {truck_points} ")
        print("truck_lecft:",common_vars.check_details_360[lane_no][stage]['truck_left'], "truck_right:",common_vars.check_details_360[lane_no][stage]['truck_right'])

        common_vars.lane_stage_details[lane_no][stage]["checking_status"] = common_vars.check_details_360[lane_no][stage]['truck_left'] and common_vars.check_details_360[lane_no][stage]['truck_right']




def check_360(vehicle_points,operator_xyxy,operator_ids,lane_no,stage):
    
    if len(vehicle_points)!=0:
        car_points = utils.convert_coordinates(vehicle_points[0])
        x1, y1, x2, y2 = map(int, car_points)
        car_center_x,car_center_y = (x1+x2)//2,(y1+y2)//2
        new_car_position = (car_center_x, car_center_y)

        if common_vars.check_details_360[lane_no][stage]['car_position'] is not None:
    # Check if the car has moved significantly
            if abs(new_car_position[1] - common_vars.check_details_360[lane_no][stage]['car_position'][1]) < common_vars.PIXEL_DISTANCE:
                common_vars.check_details_360[lane_no][stage]['stationary_frame_count'] += 1
            else:
                common_vars.check_details_360[lane_no][stage]['stationary_frame_count'] = 0

            if common_vars.check_details_360[lane_no][stage]['stationary_frame_count'] >= common_vars.STATIOANRY_CAR_THRESHOLD:
                common_vars.check_details_360[lane_no][stage]['car_stationary'] = True
                #print("car is stationary",common_vars.check_details_360[lane_no][stage]['stationary_frame_count'],common_vars.lane_stage_details[lane_no][stage]["checking_status"])
            else:
                common_vars.check_details_360[lane_no][stage]['car_stationary'] = False         

        if common_vars.check_details_360[lane_no][stage]['car_stationary']==True and common_vars.lane_stage_details[lane_no][stage]["checking_status"] == False:
            #new_car = True if lane_stage_details[lane_no][stage]["checking_no_frames"]==0 else False
            if common_vars.check_details_360[lane_no][stage]['new_car_count'] ==0:
                new_car = True
                common_vars.check_details_360[lane_no][stage]['new_car_count']+=1
            else :
                #print("tested")
                new_car = False 
            common_vars.lane_stage_details[lane_no][stage]["checking_status"] = utils.check_360_status(car_points,operator_xyxy,operator_ids,new_car,1,False, lane_no, stage)
            
        common_vars.check_details_360[lane_no][stage]['car_position'] = new_car_position
        common_vars.lane_stage_details[lane_no][stage]["checking_no_frames"]+=1
        if common_vars.DEBUG:
            common_logger.debug(f"Check status {common_vars.lane_stage_details[lane_no][stage]['checking_status']}")
       
def decode_lpr_data(lp_objects,vehicle_points,lane_no,stage):
    
    for obj_meta in lp_objects:
        
        if common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry" and \
            obj_meta.class_id == common_vars.PGIE_CLASS_ID_LP_ENGLISH and len(vehicle_points):
            for i in range(0,len(vehicle_points),2):
                if any("lpr" in key for key in vehicle_points[i+1]):
                    
                    lp_centre = (obj_meta.rect_params.left + (obj_meta.rect_params.width/2) , \
                        obj_meta.rect_params.top + (obj_meta.rect_params.height/2) )
                    vehicle_polygon = utils.convert_detection_points(vehicle_points[i])
                    valid_lp = utils.is_point_inside_polygon(lp_centre ,vehicle_polygon)

                    if valid_lp:
                        l_class = obj_meta.classifier_meta_list
                        while l_class is not None:
                            try:
                                class_meta = pyds.NvDsClassifierMeta.cast(l_class.data)
                            except StopIteration:
                                break
                            l_label = class_meta.label_info_list
                            while l_label is not None:
                                try:
                                    label_info = pyds.NvDsLabelInfo.cast(l_label.data)
                                    if len(label_info.result_label) >= 5 and label_info.result_label[-3:].isalpha() :
                            
                                        common_vars.lane_stage_details[lane_no][stage]["vehicle_ID_deque"].append(str(label_info.result_label))
                                        common_vars.lane_stage_details[lane_no][stage]["vehicle_ID_deque_conf"].append((round(label_info.result_prob,2)))
                                        # common_logger.debug(f"Appending queue {str(label_info.result_label)} {round(label_info.result_prob,2)}")
                                except StopIteration:
                                    break
                                try:
                                    l_label=l_label.next
                                except StopIteration:
                                    break
                            try:
                                l_class=l_class.next
                            except StopIteration:
                                        break
                            
def carbon_check(user_meta_data,obj_meta,lane_no,stage,stream_id):

    entry_exit_utils.entry_exit_update(user_meta_data,obj_meta,lane_no,stage,stream_id)
    # if carbon_wire is present in platform ROI for threshold frames check true for carbon
    if len(user_meta_data.roiStatus) != 0  and common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry":
        
        if common_vars.DEBUG:
            common_logger.debug(f"ROI Frame count {lane_no} {stage} {common_vars.lane_stage_details[lane_no][stage]['checking_no_frames']}")
             
        if (obj_meta.class_id == common_vars.PGIE_CLASS_ID_PERSON or obj_meta.class_id == common_vars.PGIE_CLASS_ID_CARBON_WIRE )and \
            common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry":
            common_vars.lane_stage_details[lane_no][stage]["checking_no_frames"] += 1
        
        if common_vars.lane_stage_details[lane_no][stage]["checking_no_frames"] > common_vars.CARBON_CHECK_FRAME_THRESHOLD:
            common_vars.lane_stage_details[lane_no][stage]["checking_status"] = True
            if common_vars.DEBUG:
                common_logger.debug(f"Checking status {lane_no} {stage} {common_vars.lane_stage_details[lane_no][stage]['checking_status']}")

def carbon_check_exit17(user_meta_data,obj_meta,lane_no,stage,stream_id):

    # entry_exit_utils.entry_exit_update(user_meta_data,obj_meta,lane_no,stage,stream_id)
    # if carbon_wire is present in platform ROI for threshold frames check true for carbon
    if len(user_meta_data.roiStatus) != 0  and common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry":
        
        if common_vars.DEBUG:
            common_logger.debug(f"ROI Frame count {lane_no} {stage} {common_vars.lane_stage_details[lane_no][stage]['checking_no_frames']}")
             
        if obj_meta.class_id == common_vars.PGIE_CLASS_ID_CARBON_WIRE and \
            common_vars.lane_stage_details[lane_no][stage]["STATUS"] == "Entry":
            common_vars.lane_stage_details[lane_no][stage]["checking_no_frames"] += 1
        
        if common_vars.lane_stage_details[lane_no][stage]["checking_no_frames"] > common_vars.CARBON_CHECK_FRAME_THRESHOLD:
            common_vars.lane_stage_details[lane_no][stage]["carbon_checking_status"] = True
            if common_vars.DEBUG:
                common_logger.debug(f"Checking status {lane_no} {stage} {common_vars.lane_stage_details[lane_no][stage]['checking_status']}")
                
# Exit 17 UB lpr decode function
def decode_ub_lpr_data(lp_objects, lane_no, tracking_id, vehicle_data):
    
    x1, y1, x2, y2 = vehicle_data['bbox']
    vehicle_polygon = utils.rectangle_to_coordinates(x1, y1, x2, y2)
    
    for obj_meta in lp_objects:

            if obj_meta.class_id == common_vars.PGIE_CLASS_ID_LP_ENGLISH:
                
                lp_centre = (obj_meta.rect_params.left + (obj_meta.rect_params.width/2) , \
                    obj_meta.rect_params.top + (obj_meta.rect_params.height/2) )
                x_mid, y_mid = lp_centre
                valid_lp = utils.is_point_inside_polygon((x_mid, y_mid), vehicle_polygon)

                if valid_lp:
                    l_class = obj_meta.classifier_meta_list
                    while l_class is not None:
                        try:
                            class_meta = pyds.NvDsClassifierMeta.cast(l_class.data)
                        except StopIteration:
                            break
                        l_label = class_meta.label_info_list
                        while l_label is not None:
                            try:
                                label_info = pyds.NvDsLabelInfo.cast(l_label.data)
                                common_vars.lane_stage_details[lane_no]["ubtopfront"][tracking_id]["vehicle_ID_deque"].append(str(label_info.result_label))
                            except StopIteration:
                                break
                            try:
                                l_label=l_label.next
                            except StopIteration:
                                break
                        try:
                            l_class=l_class.next
                        except StopIteration:
                            break
                                                 
                            
def ub_check_status(ub_points, subregions, car_data):
            
        # find the lpr data to the corresponding vehicle in the roi and update common_vars.lane_stage_details
        # if stage == "360" or stage == "break":
    # print("\n\n UB Check Function called\n\n")
    # print(f"\n\n{ub_points}\n\n")
    for track_id, data in ub_points.items():
        # print("\n\nInside the for loop\n\n")
        x, y, w, h = data[0]
        x1, y1, x2, y2 = utils.convert_coordinates(data[0])
        bbox = [x1, y1, x2, y2]
        label = data[1]
        print(f"\nROI Status: {data[2]}")
        label_name = common_vars.classes[int(label)]
        # print(f"\n\nLabel Name : {label_name}")
        if label_name == "ub_operator":
            # print("\n\n ------------------- UB Operator detected -------------------------------")
            operator_subregion = utils.find_operator_subregion(((x1+x2)//2, y2), subregions)
            # print(f"\nOperator Subregion : {operator_subregion}")
            if car_data['subregion'] == 0:
                if operator_subregion in [0,1]:
                    return 1
            else:
                if operator_subregion in [1,2]:
                    return 1              
    
    return 0


# Exit 17 UB check 
def ub_check(lp_objects, ub_points, lane_no, stage):

    # ub_points[obj_meta.object_id] = [[msg_meta["bbox_top"],msg_meta["bbox_left"], \
    #                             msg_meta["bbox_width"],msg_meta["bbox_height"]],obj_meta.class_id,user_meta_data.roiStatus]

    if stage == "ubtopfront":
        entry_exit_utils.ub_entry_exit(ub_points, lane_no, stage, lp_objects)
        # print("----------- ub top front camera funtion  called ------------------")
    
    elif stage == "ubtopback":
        entry_exit_utils.ub_entry_exit_back(lane_no, ub_points)
    
    if common_vars.lane_stage_details[lane_no]["ubtopfront"]:
        for front_track_id, front_data in common_vars.lane_stage_details[lane_no]["ubtopfront"].items():
            if front_data["associated_tracking_id"] is None:
                if common_vars.ub_top_back_details[lane_no]:
                    for back_track_id, back_data in common_vars.ub_top_back_details[lane_no].items():
                        # Calculate time difference
                        time_diff = abs((front_data["entry_time"] - back_data["entry_time"]).total_seconds())
                        
                        # Debug print statements
                        #print(f"Comparing front car {front_track_id} with back car {back_track_id}")
                        # print(f"Front entry time: {front_data['entry_time']}, Back entry time: {back_data['entry_time']}")
                        #print(f"Time difference: {time_diff} seconds")

                        common_logger.debug(f"Comparing front car {front_track_id} with back car {back_track_id}")
                        common_logger.debug(f"Time difference: {time_diff} seconds")
                        
                        if time_diff < 2:
                            common_logger.debug(f"Car {front_track_id} is associated with {back_track_id}")
                            front_data["associated_tracking_id"] = back_track_id
                            common_vars.lane_stage_details[lane_no]["ubtopfront"][front_track_id] = front_data
                            break  # Exit the inner loop after finding a match
                    else:
                        # Continue with the next front track_id if no match was found
                        continue
                    # Exit the outer loop once a match is found
                    break
    
    
    if common_vars.lane_stage_details[lane_no]["ubtopfront"]:
        for tracking_id, data in common_vars.lane_stage_details[lane_no]["ubtopfront"].items():
            check_status = 0
            if stage == "ubbottomfront" and data['subregion'] == 0:
                check_status = ub_check_status(ub_points, common_vars.subregions[1], data)
            
            elif stage == "ubbottomback" and data['subregion'] in [1,2]:
                check_status = ub_check_status(ub_points, common_vars.subregions[2], data)
                
            if check_status == 1:
                common_logger.debug(f"Check performed for car {tracking_id}")
                common_vars.lane_stage_details[lane_no]["ubtopfront"][tracking_id]["checking_status"] = "Check performed"
                
    if common_vars.lane_stage_details[lane_no]["ubtopfront"]:
        for tracking_id, data in common_vars.lane_stage_details[lane_no]["ubtopfront"].items():
            if (data['subregion'] in [1,2] and stage == "ubtopfront"):
                # print("\n------------------------ Calling decode lpr function ---------------------------")
                decode_ub_lpr_data(lp_objects, lane_no, tracking_id, data)
            elif (data['subregion'] == 0 and stage == "ubtopback"):
                back_track_id = data["associated_tracking_id"]
                if back_track_id in common_vars.ub_top_back_details[lane_no]:
                    back_data = common_vars.ub_top_back_details[lane_no][back_track_id]
                    decode_ub_lpr_data(lp_objects, lane_no, tracking_id, data)
                    
    