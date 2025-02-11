from collections import Counter
from confluent_kafka import Producer, Consumer, KafkaError
import traceback
import json
import uuid
from config import KAFKA_CONSUMER_CONF, KAFKA_PRODUCER_CONF, CONSUMER_TOPICS, PLATFORM_PRODUCER_TOPIC, CAMERA_DETAILS,\
      LANE_TO_USE, EXIT_THRESHOLD, ROIS, TEST_KAFKA_PRODUCER_CONF, TEST_PLATFORM_PRODUCER_TOPIC,LPR_MATCHING_SECONDS
from utils import is_inside,freq_lpr_conf,lpr_current_car
import utils
import config
from rapidfuzz.distance import Levenshtein
from datetime import datetime

import os
from snapshot import fetch_snapshot

from logger import common_logger


def get_time_diff_in_seconds(time1,time2):
    time_format = '%Y-%m-%d %H:%M:%S.%f'
    entry_time = time1
    exit_time = time2
    entry_time = datetime.strptime(entry_time, time_format)
    exit_time = datetime.strptime(exit_time, time_format)

    time_difference = exit_time - entry_time
    return time_difference.total_seconds()

class LaneBase:
    def __init__(self, lane_name) -> None:
        self.lane_name = lane_name
        self.track_id_info = {}
        self.lpr_info = {}
        self.track_id_in_stage = {}
        self.history_track_ids = {}
        self.track_id_not_visible = {}
        # self.carbon_stage_global_vars = {"entry_status":False,"carbon_checking_no_frames":0,"carbon_check_threshold":15,"carbon_checkDone_status":False}

    def send_platform_kakfa_msg(self,message):
        
        producer = Producer(KAFKA_PRODUCER_CONF)
        test_producer = Producer(TEST_KAFKA_PRODUCER_CONF)

        producer.produce(PLATFORM_PRODUCER_TOPIC, value=json.dumps(message))
        producer.flush()


        test_producer.produce(TEST_PLATFORM_PRODUCER_TOPIC, value=json.dumps(message))
        test_producer.flush()

        common_logger.debug(f"Lane stage details: {message}")


    def lpr_360(self, bbox_info, timestamp, camera_id) -> None:
        
        camera_info = CAMERA_DETAILS[camera_id]

        if  camera_info['check_camera_id'] in self.track_id_in_stage and self.track_id_in_stage[camera_info['check_camera_id']]:
            # print('here')
            for car_bb in bbox_info['car']:
                
                track_id, confidence, x, y, w, h = car_bb
                
                if track_id not in self.track_id_info[camera_id]:
                    x_bottom_center = int(x + (w/2))
                    y_bottom_center = int(y + h)

                    entry_roi = ROIS[camera_id]['entry_roi']
                    
                    if is_inside(entry_roi, (x_bottom_center, y_bottom_center)):

                        # saving snapshots
                        filepath = f'./snapshots/{camera_info["lane_name"]}_stage1/{track_id}'
                        os.makedirs(filepath, exist_ok=True)
                        filename = f'{uuid.uuid4()}.jpg'
                        fetch_snapshot(camera_id, os.path.join(filepath, filename))


                        top_360_track_id = self.track_id_in_stage[camera_info['check_camera_id']]
                        
                        if top_360_track_id is not None:
                            self.track_id_info[camera_id][track_id] = {"entry_time": timestamp, 'top_360_track_id': top_360_track_id, 'bounding_box': [x,y,w,h]}
                        
                        for lpr_bb in bbox_info['lp_english']:
                            # print(len(lpr_bb))
                            if len(lpr_bb) == 7:
                                lp_tid, lp_confidence, lp_x, lp_y, lp_w, lp_h, lp_plate_number = lpr_bb

                                x_center = int(lp_x + lp_w/2)
                                y_center = int(lp_y +  lp_h/2)
                                corners = [
                                    (x, y),  # Top-left
                                    (x + w, y),  # Top-right
                                    (x + w, y + h),  # Bottom-right
                                    (x, y + h)  # Bottom-left
                                ]

                                if is_inside(corners,(x_center, y_center)):
                                    # print('inside')
                                    if 'vehicle_lpr' in self.track_id_info[camera_info['check_camera_id']][top_360_track_id]:
                                        self.track_id_info[camera_info['check_camera_id']][top_360_track_id]['vehicle_lpr'].append(lp_plate_number)
                                    else:
                                        self.track_id_info[camera_info['check_camera_id']][top_360_track_id]['vehicle_lpr'] = [lp_plate_number]
                else:
                    self.track_id_info[camera_id][track_id]['bounding_box'] = [x,y,w,h]
                    top_360_track_id = self.track_id_in_stage[camera_info['check_camera_id']]
                    for lpr_bb in bbox_info['lp_english']:
                            # print(len(lpr_bb))
                            if len(lpr_bb) == 7:
                                lp_tid, lp_confidence, lp_x, lp_y, lp_w, lp_h, lp_plate_number = lpr_bb

                                x_center = int(lp_x + lp_w/2)
                                y_center = int(lp_y +  lp_h/2)
                                corners = [
                                    (x, y),  # Top-left
                                    (x + w, y),  # Top-right
                                    (x + w, y + h),  # Bottom-right
                                    (x, y + h)  # Bottom-left
                                ]

                                if is_inside(corners,(x_center, y_center)):
                                    # print('inside')
                                    if 'vehicle_lpr' in self.track_id_info[camera_info['check_camera_id']][top_360_track_id]:
                                        self.track_id_info[camera_info['check_camera_id']][top_360_track_id]['vehicle_lpr'].append(lp_plate_number)
                                    else:
                                        self.track_id_info[camera_info['check_camera_id']][top_360_track_id]['vehicle_lpr'] = [lp_plate_number]
    
    
    def check_operator_positions_with_proximity(self,car_bbox, operators, camera_id, track_id):
        proximity_threshold = 200
        # print (car_bbox,"space",operators)
        check_status = self.track_id_info[camera_id][track_id]
        # Unpack the car bounding box details
        _, _, car_x, car_y, car_width, car_height = car_bbox
        car_left = car_x
        car_right = car_x + car_width
        car_top = car_y
        car_bottom = car_y + car_height
    
        # Iterate through all operator bounding boxes
        for operator in operators:
            _, _, op_x, op_y, op_width, op_height = operator
            op_left = op_x
            op_right = op_x + op_width
            op_top = op_y
            op_bottom = op_y + op_height
    
            # Check operator's position and proximity relative to the car
            if op_top < car_top and (car_top - op_bottom) <= proximity_threshold:  # Above the car (Front)
                self.track_id_info[camera_id][track_id]["front_check"] = True
            elif op_bottom > car_bottom and (op_top - car_bottom) <= proximity_threshold:  # Below the car (Back)
                self.track_id_info[camera_id][track_id]["back_check"] = True
            elif op_left < car_left and (car_left - op_right) <= proximity_threshold:  # Left of the car
                self.track_id_info[camera_id][track_id]["left_check"] = True
            elif op_right > car_right and (op_left - car_right) <= proximity_threshold:  # Right of the car
                self.track_id_info[camera_id][track_id]["right_check"] = True
    

    def check_360(self, bbox_info, timestamp, camera_id) -> None:
        '''
        Calculate service time and 360 check.
        '''
        is_track_id_present = False
        for car_bb in bbox_info['top_car']:
            
            track_id, confidence, x, y, w, h = car_bb
            
            if track_id not in self.track_id_info[camera_id]:
                x_center = int(x + (w/2))
                y_center = int(y + (h/2))

                entry_roi = ROIS[camera_id]['entry_roi']
                
                if is_inside(entry_roi, (x_center, y_center)):

                    vehicle_uuid = str(uuid.uuid1())
                    # saving snapshots
                    filepath = f'./snapshots/{camera_id.split("_")[0]}_stage1/{track_id}'
                    os.makedirs(filepath, exist_ok=True)
                    filename = f'{vehicle_uuid}.jpg'
                    fetch_snapshot(camera_id, os.path.join(filepath, filename))
                    
                    self.track_id_info[camera_id][track_id] = {"entry_time": timestamp, "vehicleUuid":vehicle_uuid, "vehicle_lpr":[],\
                                                               "front_check":False,"back_check":False,"left_check":False,"right_check":False,\
                                                               "carbon_checking_no_frames":0,"carbon_checkDone_status":False,"carbon_check_threshold":15}

                    if self.track_id_in_stage[camera_id] is None:
                        self.track_id_in_stage[camera_id] = track_id
                        self.track_id_not_visible[camera_id][track_id] = 0
                    else:
                        prev_track_id = self.track_id_in_stage[camera_id]
                        # self.history_track_ids[camera_id].append(prev_track_id)
                        
                        self.track_id_info[camera_id][prev_track_id]["exit_time"] = timestamp

                        time_format = '%Y-%m-%d %H:%M:%S.%f'
                        entry_time = self.track_id_info[camera_id][prev_track_id]['entry_time']
                        exit_time = self.track_id_info[camera_id][prev_track_id]['exit_time']
                        entry_time = datetime.strptime(entry_time, time_format)
                        exit_time = datetime.strptime(exit_time, time_format)

                        time_difference = exit_time - entry_time
                        print("360 check----:", time_difference.total_seconds())

                        sides = [self.track_id_info[camera_id][prev_track_id]["left_check"],
                                    self.track_id_info[camera_id][prev_track_id]["right_check"],
                                    self.track_id_info[camera_id][prev_track_id]["front_check"],
                                    self.track_id_info[camera_id][prev_track_id]["right_check"]]
                        if time_difference.total_seconds() > 60: 
                            if sum(sides)>=2:
                                self.track_id_info[camera_id][prev_track_id]['check_status'] = True
                            else:
                                self.track_id_info[camera_id][prev_track_id]['check_status'] = False
                        else:
                            self.track_id_info[camera_id][prev_track_id]['check_status'] = False
                        if sum(sides) == 2:
                            comment = "partial check done"
                        elif sum(sides) > 2:
                            comment = "Full check done"
                        else:
                            comment = "check not performed"

                        if self.track_id_info[camera_id][prev_track_id]["carbon_checkDone_status"]:
                            carbon_comment = "check done"
                        else:
                            carbon_comment = "Carbon check not done"   
                        previous_vehicle_lpr=''
                        curr_lpr=''
                        if 'vehicle_lpr' in self.track_id_info[camera_id][prev_track_id] and len(self.track_id_info[camera_id][prev_track_id]) > 0:
                            vehicle_lpr = utils.most_freq(self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'])
                            curr_lpr = vehicle_lpr
                            if len(self.history_track_ids[camera_id]):
                                previous_vehicle_lpr = self.track_id_info[camera_id][self.history_track_ids[camera_id][-1]]['vehicle_lpr'] 

                                common_logger.debug(f" Curr lpr {curr_lpr} Prev lpr {previous_vehicle_lpr}")  
                                time_diff_in_seconds = get_time_diff_in_seconds(self.track_id_info[camera_id][self.history_track_ids[camera_id][-1]]["exit_time"],timestamp)
                                
                                if previous_vehicle_lpr and vehicle_lpr and time_diff_in_seconds<LPR_MATCHING_SECONDS:
                                    distance = Levenshtein.distance(previous_vehicle_lpr, vehicle_lpr)
                                    if distance <= 3:
                                        vehicle_lpr = previous_vehicle_lpr
                                        self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = previous_vehicle_lpr
                                        print("Matched with prev event in same stage ",previous_vehicle_lpr," curr ",vehicle_lpr)
                        
                            
                            self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = vehicle_lpr
                            message = {"trackId":prev_track_id,\
                               "stageId":CAMERA_DETAILS[camera_id]["stage_id"],\
                               "cameraId":CAMERA_DETAILS[camera_id]["camera_uid"],\
                               "vehicleNumberPlate":self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'],\
                               "vehicleUuid": self.track_id_info[camera_id][prev_track_id]['vehicleUuid'],\
                               "checkStartTime": self.track_id_info[camera_id][prev_track_id]['entry_time'],\
                               "useCaseType":"stageCameraLogs",\
                               "checkEndTime": self.track_id_info[camera_id][prev_track_id]['exit_time'],\
                               "checkDone": self.track_id_info[camera_id][prev_track_id]['check_status'],\
                               "comment": comment}
                            carbon_message = {"trackId":prev_track_id,\
                               "stageId":CAMERA_DETAILS[camera_id]["carbonstageId"],\
                               "cameraId":CAMERA_DETAILS[camera_id]["camera_uid"],\
                               "vehicleNumberPlate":self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'],\
                               "vehicleUuid": self.track_id_info[camera_id][prev_track_id]['vehicleUuid'],\
                               "checkStartTime": self.track_id_info[camera_id][prev_track_id]['entry_time'],\
                               "useCaseType":"stageCameraLogs",\
                               "checkEndTime": self.track_id_info[camera_id][prev_track_id]['exit_time'],\
                               "checkDone": self.track_id_info[camera_id][prev_track_id]['carbon_checkDone_status'],\
                               "comment": carbon_comment}
                            # # send platform kafka message
                            # message = {"track_id":prev_track_id, "entry_time": self.track_id_info[camera_id][prev_track_id]['entry_time'],\
                            #        "exit_time": self.track_id_info[camera_id][prev_track_id]['exit_time'], 'lpr': lpr}
                        else:
                            message = {"trackId":prev_track_id,\
                               "stageId":CAMERA_DETAILS[camera_id]["stage_id"],\
                               "cameraId":CAMERA_DETAILS[camera_id]["camera_uid"],\
                               "vehicleNumberPlate":'',\
                               "vehicleUuid": self.track_id_info[camera_id][prev_track_id]['vehicleUuid'],\
                               "checkStartTime": self.track_id_info[camera_id][prev_track_id]['entry_time'],\
                               "useCaseType":"stageCameraLogs",\
                               "checkEndTime": self.track_id_info[camera_id][prev_track_id]['exit_time'],\
                               "checkDone": self.track_id_info[camera_id][prev_track_id]['check_status'],\
                               "comment": comment}
                            carbon_message = {"trackId":prev_track_id,\
                               "stageId":CAMERA_DETAILS[camera_id]["carbonstageId"],\
                               "cameraId":CAMERA_DETAILS[camera_id]["camera_uid"],\
                               "vehicleNumberPlate":'',\
                               "vehicleUuid": self.track_id_info[camera_id][prev_track_id]['vehicleUuid'],\
                               "checkStartTime": self.track_id_info[camera_id][prev_track_id]['entry_time'],\
                               "useCaseType":"stageCameraLogs",\
                               "checkEndTime": self.track_id_info[camera_id][prev_track_id]['exit_time'],\
                               "checkDone": self.track_id_info[camera_id][prev_track_id]['carbon_checkDone_status'],\
                               "comment": carbon_comment}
                            # send platform kafka message
                            # message = {"track_id":prev_track_id, "entry_time": self.track_id_info[camera_id][prev_track_id]['entry_time'],\
                            #         "exit_time": self.track_id_info[camera_id][prev_track_id]['exit_time']}
                        updated_lpr = self.track_id_info[camera_id][prev_track_id]['vehicle_lpr']

                        common_logger.debug(f"360 lp details: curr_lpr {curr_lpr} prev_lpr {previous_vehicle_lpr} updated_lpr {updated_lpr}")
                        self.send_platform_kakfa_msg(message)
                        self.send_platform_kakfa_msg(carbon_message)
                        self.track_id_info[camera_id][prev_track_id].update(message)
                        self.history_track_ids[camera_id].append(prev_track_id)
                        self.track_id_in_stage[camera_id] = track_id
                        self.track_id_not_visible[camera_id][track_id] = 0

            if track_id == self.track_id_in_stage[camera_id]:
                self.carbon_check_in_360(camera_id,bbox_info,track_id)

                is_track_id_present = True
                operators = []
                for oper_bb in bbox_info['top-person']:
                    operators.append(oper_bb)
                # for non_oper_bb in bbox_info['non operator']:
                #     operators.append(non_oper_bb)
                # print("--------------------",car_bb)
                self.check_operator_positions_with_proximity(car_bb, operators, camera_id, track_id)

                
            # print(track_id, confidence, x, y, h, w)
        if not is_track_id_present:
            if self.track_id_in_stage[camera_id] in self.track_id_not_visible[camera_id]:
                self.track_id_not_visible[camera_id][self.track_id_in_stage[camera_id]] += 1
            
        else:
            self.track_id_not_visible[camera_id][self.track_id_in_stage[camera_id]] = 0
        
        if self.track_id_in_stage[camera_id] and self.track_id_not_visible[camera_id][self.track_id_in_stage[camera_id]] > EXIT_THRESHOLD:

            self.track_id_info[camera_id][self.track_id_in_stage[camera_id]]["exit_time"] = timestamp
            # send platform kafka message
            prev_track_id = self.track_id_in_stage[camera_id]
            
            time_format = '%Y-%m-%d %H:%M:%S.%f'
            entry_time = self.track_id_info[camera_id][prev_track_id]['entry_time']
            exit_time = self.track_id_info[camera_id][prev_track_id]['exit_time']
            entry_time = datetime.strptime(entry_time, time_format)
            exit_time = datetime.strptime(exit_time, time_format)

            time_difference = exit_time - entry_time
            print("360 check----:", time_difference.total_seconds())
            sides = [self.track_id_info[camera_id][prev_track_id]["left_check"],
                        self.track_id_info[camera_id][prev_track_id]["right_check"],
                        self.track_id_info[camera_id][prev_track_id]["front_check"],
                        self.track_id_info[camera_id][prev_track_id]["right_check"]]
            if time_difference.total_seconds() > 60: 
                if sum(sides)>=2:
                    self.track_id_info[camera_id][prev_track_id]['check_status'] = True
                else:
                    self.track_id_info[camera_id][prev_track_id]['check_status'] = False
            else:
                self.track_id_info[camera_id][prev_track_id]['check_status'] = False
            if sum(sides) == 2:
                comment = "partial check done"
            elif sum(sides) > 2:
                comment = "Full check done"
            else:
                comment = "check not performed"

            if self.track_id_info[camera_id][prev_track_id]["carbon_checkDone_status"]:
                carbon_comment = "check done"
            else:
                carbon_comment = "Carbon check not done"

            curr_lpr = ''
            previous_vehicle_lpr=''
            if 'vehicle_lpr' in self.track_id_info[camera_id][prev_track_id]:               
                vehicle_lpr = utils.most_freq(self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'])
                curr_lpr = vehicle_lpr
                if len(self.history_track_ids[camera_id]):
                    previous_vehicle_lpr = self.track_id_info[camera_id][self.history_track_ids[camera_id][-1]]['vehicle_lpr']   
                    time_diff_in_seconds = get_time_diff_in_seconds(self.track_id_info[camera_id][self.history_track_ids[camera_id][-1]]["exit_time"],timestamp)
                    common_logger.debug(f" Curr lpr {curr_lpr} Prev lpr {previous_vehicle_lpr}")  
                    if previous_vehicle_lpr and vehicle_lpr and time_diff_in_seconds<LPR_MATCHING_SECONDS:
                        distance = Levenshtein.distance(previous_vehicle_lpr, vehicle_lpr)
                        if distance <= 3:
                            vehicle_lpr = previous_vehicle_lpr
                            self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = previous_vehicle_lpr
                            print("Matched with prev event in same stage ",previous_vehicle_lpr," curr ",vehicle_lpr)
            
                self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = vehicle_lpr
                message = {"trackId":prev_track_id,\
                    "stageId":CAMERA_DETAILS[camera_id]["stage_id"],\
                    "cameraId":CAMERA_DETAILS[camera_id]["camera_uid"],\
                    "vehicleNumberPlate":self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'],\
                    "vehicleUuid": self.track_id_info[camera_id][prev_track_id]['vehicleUuid'],\
                    "checkStartTime": self.track_id_info[camera_id][prev_track_id]['entry_time'],\
                    "useCaseType":"stageCameraLogs",\
                    "checkEndTime": self.track_id_info[camera_id][prev_track_id]['exit_time'],\
                    "checkDone": self.track_id_info[camera_id][prev_track_id]['check_status'],\
                    "comment": comment}
                carbon_message = {"trackId":prev_track_id,\
                    "stageId":CAMERA_DETAILS[camera_id]["carbonstageId"],\
                    "cameraId":CAMERA_DETAILS[camera_id]["camera_uid"],\
                    "vehicleNumberPlate":self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'],\
                    "vehicleUuid": self.track_id_info[camera_id][prev_track_id]['vehicleUuid'],\
                    "checkStartTime": self.track_id_info[camera_id][prev_track_id]['entry_time'],\
                    "useCaseType":"stageCameraLogs",\
                    "checkEndTime": self.track_id_info[camera_id][prev_track_id]['exit_time'],\
                    "checkDone": self.track_id_info[camera_id][prev_track_id]['carbon_checkDone_status'],\
                    "comment": carbon_comment}
            else:
                message = {"trackId":prev_track_id,\
                    "stageId":CAMERA_DETAILS[camera_id]["stage_id"],\
                    "cameraId":CAMERA_DETAILS[camera_id]["camera_uid"],\
                    "vehicleNumberPlate":'',\
                    "vehicleUuid": self.track_id_info[camera_id][prev_track_id]['vehicleUuid'],\
                    "checkStartTime": self.track_id_info[camera_id][prev_track_id]['entry_time'],\
                    "useCaseType":"stageCameraLogs",\
                    "checkEndTime": self.track_id_info[camera_id][prev_track_id]['exit_time'],\
                    "checkDone": self.track_id_info[camera_id][prev_track_id]['check_status'],\
                    "comment": comment}
                carbon_message = {"trackId":prev_track_id,\
                    "stageId":CAMERA_DETAILS[camera_id]["carbonstageId"],\
                    "cameraId":CAMERA_DETAILS[camera_id]["camera_uid"],\
                    "vehicleNumberPlate":'',\
                    "vehicleUuid": self.track_id_info[camera_id][prev_track_id]['vehicleUuid'],\
                    "checkStartTime": self.track_id_info[camera_id][prev_track_id]['entry_time'],\
                    "useCaseType":"stageCameraLogs",\
                    "checkEndTime": self.track_id_info[camera_id][prev_track_id]['exit_time'],\
                    "checkDone": self.track_id_info[camera_id][prev_track_id]['carbon_checkDone_status'],\
                    "comment": carbon_comment}
            # message = {"track_id":prev_track_id, "entry_time": self.track_id_info[camera_id][prev_track_id]['entry_time'],\
            #                        "exit_time": self.track_id_info[camera_id][prev_track_id]['exit_time']}

            updated_lpr = self.track_id_info[camera_id][prev_track_id]['vehicle_lpr']

            common_logger.debug(f"360 lp details: curr_lpr {curr_lpr} prev_lpr {previous_vehicle_lpr} updated_lpr {updated_lpr}")
                        
            self.send_platform_kakfa_msg(message)
            self.send_platform_kakfa_msg(carbon_message)
            self.track_id_info[camera_id][prev_track_id].update(message)
            self.history_track_ids[camera_id].append(prev_track_id)
            self.track_id_in_stage[camera_id] = None

    def check_brake(self, bbox_info, timestamp, camera_id) -> None:
        
        '''
        Calculate service time and  check.
        '''
        is_track_id_present = False
        is_back_tyre = False
        camera_info = CAMERA_DETAILS[camera_id]
        lp_details = {}
        for car_bb in bbox_info['car']:
            
            track_id, confidence, x, y, w, h = car_bb
            lp_details["track_id"] = track_id
            if track_id not in self.track_id_info[camera_id]:
                x_bottom_center = int(x + (w/2))
                y_bottom_center = int(y + h-100)

                entry_roi = ROIS[camera_id]['entry_roi']
                
                if is_inside(entry_roi, (x_bottom_center, y_bottom_center)):
                    # if camera_info['previous_stage'] in self.history_track_ids and len(self.history_track_ids[camera_info['previous_stage']]) > 0:
                    #     previous_vehicle_lpr = self.track_id_info[camera_info['previous_stage']][self.history_track_ids[camera_info['previous_stage']][-1]]['vehicle_lpr']   
                    # else:
                    #     previous_vehicle_lpr = []
                    # # print("brake.....", previous_vehicle_lpr)

                    vehicle_uuid = str(uuid.uuid1())

                    # saving snapshots
                    filepath = f'./snapshots/{camera_info["lane_name"]}_stage4/{track_id}'
                    os.makedirs(filepath, exist_ok=True)
                    filename = f'{vehicle_uuid}.jpg'
                    fetch_snapshot(camera_id, os.path.join(filepath, filename))

                    
                    self.track_id_info[camera_id][track_id] = {"entry_time": timestamp,"entry_status":True,"brake_checking_front_no_frames":0,"brake_check_threshold":15,\
                                                                "brake_checking_back_no_frames":0,\
                                                                # "previous_vehicle_lpr":previous_vehicle_lpr,\
                                                                "front_tyre_check":"not done","back_tyre_check":"not done",\
                                                                "vehicleUuid":vehicle_uuid,\
                                                                "previous_stage_message": [],\
                                                                "vehicle_lpr":[],"vehicle_conf":[] ,"brake_checkDone_status":False}

                    if self.track_id_in_stage[camera_id] is None:
                        self.track_id_in_stage[camera_id] = track_id
                        self.track_id_not_visible[camera_id][track_id] = 0
                    else:
                        prev_track_id = self.track_id_in_stage[camera_id]
                        #self.history_track_ids[camera_id].append(prev_track_id)
                        
                        previous_stage_message = []
                        if camera_info['previous_stage'] in self.history_track_ids and len(self.history_track_ids[camera_info['previous_stage']]):
                            for i in range(-1,max(-(len(self.history_track_ids[camera_info['previous_stage']])+1), -20),-1):
                                # print("Appending prev vehicle lprr ",self.track_id_info[camera_info['previous_stage']][self.history_track_ids[camera_info['previous_stage']][i]] )
                                previous_stage_message.append(self.lpr_info[camera_info['previous_stage']][self.history_track_ids[camera_info['previous_stage']][i]]   )
                            # previous_stage_message = self.track_id_info[camera_info['previous_stage']][self.history_track_ids[camera_info['previous_stage']][-1]]
                        
                        print("Previous stage message after filling ",previous_stage_message)
                        print("Brake Trackid ",track_id)
                        
                        self.track_id_info[camera_id][prev_track_id]["previous_stage_message"] = previous_stage_message

                        self.track_id_info[camera_id][prev_track_id]["exit_time"] = timestamp
                        vehicle_lpr = utils.most_freq(self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'])
                        self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = vehicle_lpr
                        lp_details["curr_lpr"] = vehicle_lpr


                        if len(self.history_track_ids[camera_id]):
                            previous_vehicle_lpr = self.track_id_info[camera_id][self.history_track_ids[camera_id][-1]]['vehicle_lpr']   
                            time_diff_in_seconds = get_time_diff_in_seconds(self.track_id_info[camera_id][self.history_track_ids[camera_id][-1]]["exit_time"],timestamp)
                            lp_details["prev_veh_lpr"] = previous_vehicle_lpr
                            if previous_vehicle_lpr and vehicle_lpr and time_diff_in_seconds < 600:
                                distance = Levenshtein.distance(previous_vehicle_lpr, vehicle_lpr)
                            
                                if distance <= 3:
                                    vehicle_lpr = previous_vehicle_lpr
                                    self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = previous_vehicle_lpr
                                    print("Matched with prev event in same stage ",previous_vehicle_lpr," curr ",vehicle_lpr)
                
                        print("Len of prev stage message ",len(self.track_id_info[camera_id][prev_track_id]['previous_stage_message']))
                        if len(self.track_id_info[camera_id][prev_track_id]['previous_stage_message']) > 0:
                           
                            match_found = False
                            
                            lp_details["prev_stage_lpr"] = []
                            for i in range(len(self.track_id_info[camera_id][prev_track_id]['previous_stage_message'])):
                                time_diff_in_seconds = get_time_diff_in_seconds(self.track_id_info[camera_id][prev_track_id]['previous_stage_message'][i]["exit_time"],timestamp)
                                previous_vehicle_lpr = self.track_id_info[camera_id][prev_track_id]['previous_stage_message'][i]['vehicle_lpr']   
                                lp_details["prev_stage_lpr"].append(previous_vehicle_lpr)
                                print("Checking prev lpr ",previous_vehicle_lpr ," with current lpr ",vehicle_lpr)

                                if previous_vehicle_lpr and time_diff_in_seconds < LPR_MATCHING_SECONDS:
                                    if vehicle_lpr:
                                        distance = Levenshtein.distance(previous_vehicle_lpr, vehicle_lpr)
                                        if distance <= 3:
                                            vehicle_lpr = previous_vehicle_lpr
                                            self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = previous_vehicle_lpr
                                            print("Found matching lpr and breaking")
                                            match_found=True
                                            break
                                    # else:
                                    #     vehicle_lpr = previous_vehicle_lpr
                                    #     self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = previous_vehicle_lpr
                                    #     print("No current lpr found so taking prev lpr  ",previous_vehicle_lpr)
                                    #     match_found=True
                                    #     break    
                        
                        # if len(self.track_id_info[camera_id][prev_track_id]['previous_vehicle_lpr']) > 0:
                        #     prev_vehicle_lpr = utils.most_freq(self.track_id_info[camera_id][prev_track_id]['previous_vehicle_lpr'])
                        #     # if vehicle_lpr:
                        #     #     distance = Levenshtein.distance(prev_vehicle_lpr, vehicle_lpr)
                        #     #     if distance <= 3:
                        #     #         vehicle_lpr = prev_vehicle_lpr
                        #     #         self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = [prev_vehicle_lpr]
                        #     # else:
                        #     vehicle_lpr = prev_vehicle_lpr
                            # self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = [prev_vehicle_lpr]

                            # if vehicle_lpr not in self.track_id_info[camera_id][prev_track_id]['previous_vehicle_lpr']:
                            #     vehicle_lpr = self.track_id_info[camera_id][prev_track_id]['previous_vehicle_lpr'][0]

                        lp_details["updated_lpr"] = vehicle_lpr
                        # send platform kafka message
                        message = {"trackId":prev_track_id,\
                               "stageId":CAMERA_DETAILS[camera_id]["stage_id"],\
                               "cameraId":CAMERA_DETAILS[camera_id]["camera_uid"],\
                               "vehicleNumberPlate":vehicle_lpr,\
                               "vehicleUuid": self.track_id_info[camera_id][prev_track_id]['vehicleUuid'],\
                               "checkStartTime": self.track_id_info[camera_id][prev_track_id]['entry_time'],\
                               "useCaseType":"stageCameraLogs",\
                               "checkEndTime": self.track_id_info[camera_id][prev_track_id]['exit_time'],\
                               "checkDone":self.track_id_info[camera_id][prev_track_id]["brake_checkDone_status"],\
                               "comment": f"Front tyre check {self.track_id_info[camera_id][prev_track_id]['front_tyre_check']} and back tyre check {self.track_id_info[camera_id][prev_track_id]['back_tyre_check']}"}
                        self.send_platform_kakfa_msg(message)
                        self.history_track_ids[camera_id].append(prev_track_id)
                        

                        self.track_id_in_stage[camera_id] = track_id
                        self.track_id_not_visible[camera_id][track_id] = 0

            if track_id == self.track_id_in_stage[camera_id]:
                # Get the vehicle bottom bbox center 
                x_bottom_center = int(x + (w/2))
                y_bottom_center = int(y + h)
                if is_inside(ROIS[camera_id]['entry_roi'], (x_bottom_center, y_bottom_center)):
                    is_track_id_present = True
                    
                    car_coordinates = (x,y,w,h)
                    # print(car_coordinates)
                    # print(bbox_info["lp_english"])
                    lpr,lpr_conf = utils.lpr_current_car(car_coordinates,bbox_info["lp_english"])
                    if lpr !=None and lpr_conf !=None:
                        self.track_id_info[camera_id][track_id]["vehicle_lpr"].append(lpr)
                        self.track_id_info[camera_id][track_id]["vehicle_conf"].append(lpr_conf)
                 
                    if is_inside(ROIS[camera_id]['front_roi'], (x_bottom_center, y_bottom_center)):
                        is_back_tyre = True
                    
                
                if self.track_id_info[camera_id][track_id]["entry_status"] == True:
                    if len(bbox_info['tyre']) >0:
                        for tyre_bb in bbox_info['tyre']:
                            _, _, c_x, c_y, c_w, c_h = tyre_bb
                            x_center = int(c_x + (c_w/2))
                            y_center = int(c_y + (c_h))
        
                            # If detection is inside the stage_roi
                            if is_inside(ROIS[camera_id]['entry_roi'], (x_center, y_center)):
                             
                                if is_back_tyre:
                                    self.track_id_info[camera_id][track_id]["brake_checking_back_no_frames"] += 1
                                    if self.track_id_info[camera_id][track_id]["brake_checking_back_no_frames"] >= self.track_id_info[camera_id][track_id]["brake_check_threshold"]:
                                        self.track_id_info[camera_id][track_id]["brake_checkDone_status"] = True
                                        self.track_id_info[camera_id][track_id]["back_tyre_check"]= "done"
                                else:
                                    self.track_id_info[camera_id][track_id]["brake_checking_front_no_frames"] += 1
                                    if self.track_id_info[camera_id][track_id]["brake_checking_front_no_frames"] >= self.track_id_info[camera_id][track_id]["brake_check_threshold"]:
                                        self.track_id_info[camera_id][track_id]["brake_checkDone_status"] = True
                                        self.track_id_info[camera_id][track_id]["front_tyre_check"]= "done"
        
        # If vehicle not present in the ROI
        if not is_track_id_present:
            if self.track_id_in_stage[camera_id] in self.track_id_not_visible[camera_id]:
                self.track_id_not_visible[camera_id][self.track_id_in_stage[camera_id]] += 1 
                if self.track_id_not_visible[camera_id][self.track_id_in_stage[camera_id]] > EXIT_THRESHOLD:
                    self.track_id_info[camera_id][self.track_id_in_stage[camera_id]]["exit_time"] = timestamp
                    # send platform kafka message
                    prev_track_id = self.track_id_in_stage[camera_id]

                    previous_stage_message = []
                    if camera_info['previous_stage'] in self.history_track_ids and len(self.history_track_ids[camera_info['previous_stage']]):
                        for i in range(-1,max(-(len(self.history_track_ids[camera_info['previous_stage']])+1), -20),-1):
                            # print("Appending prev vehicle lprr ",self.track_id_info[camera_info['previous_stage']][self.history_track_ids[camera_info['previous_stage']][i]] )
                            previous_stage_message.append(self.lpr_info[camera_info['previous_stage']][self.history_track_ids[camera_info['previous_stage']][i]]   )
                        # previous_stage_message = self.track_id_info[camera_info['previous_stage']][self.history_track_ids[camera_info['previous_stage']][-1]]
                    
                    print("Previous stage message after filling ",previous_stage_message)
                    print("Brake Trackid ",track_id)
                    
                    self.track_id_info[camera_id][prev_track_id]["previous_stage_message"] = previous_stage_message

                    lp_details["track_id"] = prev_track_id
                    vehicle_lpr = utils.most_freq(self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'])
                    self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = vehicle_lpr
                    lp_details["curr_lpr"] = vehicle_lpr
                    if len(self.history_track_ids[camera_id]):
                        previous_vehicle_lpr = self.track_id_info[camera_id][self.history_track_ids[camera_id][-1]]['vehicle_lpr']  
                        time_diff_in_seconds = get_time_diff_in_seconds(self.track_id_info[camera_id][self.history_track_ids[camera_id][-1]]["exit_time"],timestamp)
                        lp_details["prev_veh_lpr"] = previous_vehicle_lpr
                        if previous_vehicle_lpr and vehicle_lpr and time_diff_in_seconds < LPR_MATCHING_SECONDS: 
                            distance = Levenshtein.distance(previous_vehicle_lpr, vehicle_lpr)
                            if distance <= 3:
                                vehicle_lpr = previous_vehicle_lpr
                                self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = previous_vehicle_lpr
                                print("Matched with prev event in same stage ",previous_vehicle_lpr," curr ",vehicle_lpr)
                        
                    print("Len of prev stage message ",len(self.track_id_info[camera_id][prev_track_id]['previous_stage_message']))
                    lp_details["prev_stage_lpr"] =[]
                    if len(self.track_id_info[camera_id][prev_track_id]['previous_stage_message']) > 0:
                        
                        match_found = False
                        for i in range(len(self.track_id_info[camera_id][prev_track_id]['previous_stage_message'])):
                            time_diff_in_seconds = get_time_diff_in_seconds(self.track_id_info[camera_id][prev_track_id]['previous_stage_message'][i]["exit_time"],timestamp)
                         
                            
                            previous_vehicle_lpr = self.track_id_info[camera_id][prev_track_id]['previous_stage_message'][i]['vehicle_lpr']   
                            lp_details["prev_stage_lpr"] .append(previous_vehicle_lpr)
                            print("Checking prev lpr ",previous_vehicle_lpr ," with current lpr ",vehicle_lpr)
                            if previous_vehicle_lpr and time_diff_in_seconds < LPR_MATCHING_SECONDS:
                                if vehicle_lpr:
                                    distance = Levenshtein.distance(previous_vehicle_lpr, vehicle_lpr)
                                    if distance <= 3:
                                        vehicle_lpr = previous_vehicle_lpr
                                        self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = previous_vehicle_lpr
                                        print("Found matching lpr and breaking")
                                        match_found=True
                                        break
                                # else:
                                #     vehicle_lpr = previous_vehicle_lpr
                                #     self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = previous_vehicle_lpr
                                #     print("No current lpr found so taking prev lpr  ",previous_vehicle_lpr)
                                #     match_found=True
                                #     break    
                    
                       
                    # if len(self.track_id_info[camera_id][prev_track_id]['previous_vehicle_lpr']) > 0:
                            # prev_vehicle_lpr = utils.most_freq(self.track_id_info[camera_id][prev_track_id]['previous_vehicle_lpr'])
                            # # if vehicle_lpr:
                            # #     distance = Levenshtein.distance(prev_vehicle_lpr, vehicle_lpr)
                            # #     if distance <= 3:
                            # #         vehicle_lpr = prev_vehicle_lpr
                            # #         self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = [prev_vehicle_lpr]
                            # # else:
                            # vehicle_lpr = prev_vehicle_lpr
                            # self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = [prev_vehicle_lpr]
                    lp_details["updated_lpr"] = vehicle_lpr
                    message = {"trackId":prev_track_id,\
                               "stageId":CAMERA_DETAILS[camera_id]["stage_id"],\
                               "cameraId":CAMERA_DETAILS[camera_id]["camera_uid"],\
                               "vehicleNumberPlate":vehicle_lpr,\
                               "vehicleUuid": self.track_id_info[camera_id][prev_track_id]['vehicleUuid'],\
                               "checkStartTime": self.track_id_info[camera_id][prev_track_id]['entry_time'],\
                               "useCaseType":"stageCameraLogs",\
                               "checkEndTime": self.track_id_info[camera_id][prev_track_id]['exit_time'],\
                               "checkDone":self.track_id_info[camera_id][prev_track_id]["brake_checkDone_status"],\
                               "comment": f"Front tyre check {self.track_id_info[camera_id][prev_track_id]['front_tyre_check']} and back tyre check {self.track_id_info[camera_id][prev_track_id]['back_tyre_check']}"}
                    
                    self.send_platform_kakfa_msg(message)
                    self.history_track_ids[camera_id].append(prev_track_id)
                    self.track_id_in_stage[camera_id] = None
        else:
            self.track_id_not_visible[camera_id][self.track_id_in_stage[camera_id]] = 0

    def carbon_check_in_360(self,camera_id,bbox_info,track_id):
        if len(bbox_info['top-carbon-wire']) or len(bbox_info['top-carbon-wire-connected']) or len(bbox_info['carbon-wire-connected']) or len(bbox_info['carbon-wire']):
            # for carbon_bb in bbox_info['carbon-wire']:
            carbon_wire = []
            carbon_wire.extend(bbox_info["top-carbon-wire"])
            carbon_wire.extend(bbox_info["top-carbon-wire-connected"])
            carbon_wire.extend(bbox_info["carbon-wire"])
            carbon_wire.extend(bbox_info["carbon-wire-connected"])
            for carbon_bb in carbon_wire:
                _, _, c_x, c_y, c_w, c_h = carbon_bb
                x_center = int(c_x + (c_w/2))
                y_center = int(c_y + (c_h))

                # If detection is inside the stage_roi
                if is_inside(ROIS[camera_id]['entry_roi'], (x_center, y_center)):
                    self.track_id_info[camera_id][track_id]["carbon_checking_no_frames"] += 1
                    if self.track_id_info[camera_id][track_id]["carbon_checking_no_frames"] >= self.track_id_info[camera_id][track_id]["carbon_check_threshold"]:
                        self.track_id_info[camera_id][track_id]["carbon_checkDone_status"] = True
        
    def check_carbon(self,bbox_info, timestamp, camera_id) -> None:
        '''
        Calculate service time and 360 check. 
        '''
        is_track_id_present = False
        camera_info = CAMERA_DETAILS[camera_id]
        for car_bb in bbox_info['car']:
            
            track_id, confidence, x, y, w, h = car_bb
            
            if track_id not in self.track_id_info[camera_id]:
                x_bottom_center = int(x + (w/2))
                y_bottom_center = int(y + h)

                entry_roi = ROIS[camera_id]['entry_roi']
                
                if is_inside(entry_roi, (x_bottom_center, y_bottom_center)):
                    if camera_info['previous_stage'] in self.history_track_ids and len(self.history_track_ids[camera_info['previous_stage']]) > 0:
                        previous_vehicle_lpr = self.track_id_info[camera_info['previous_stage']][self.history_track_ids[camera_info['previous_stage']][-1]]['vehicle_lpr']
                    else:
                        previous_vehicle_lpr = []
                    # print("carbon.....", previous_vehicle_lpr)

                    vehicle_uuid = str(uuid.uuid1())

                    # saving snapshots
                    filepath = f'./snapshots/{camera_info["lane_name"]}_stage2/{track_id}'
                    os.makedirs(filepath, exist_ok=True)
                    filename = f'{vehicle_uuid}.jpg'
                    fetch_snapshot(camera_id, os.path.join(filepath, filename))


                    self.track_id_info[camera_id][track_id] = {"entry_time": timestamp,"entry_status":True,"carbon_checking_no_frames":0,"carbon_check_threshold":15,\
                                                               "previous_vehicle_lpr":previous_vehicle_lpr,\
                                                               "vehicleUuid":vehicle_uuid ,"carbon_checkDone_status":False,"vehicle_lpr":[],"vehicle_conf":[]}

                    if self.track_id_in_stage[camera_id] is None:
                        self.track_id_in_stage[camera_id] = track_id
                        self.track_id_not_visible[camera_id][track_id] = 0
                    else:
                        prev_track_id = self.track_id_in_stage[camera_id]
                        self.history_track_ids[camera_id].append(prev_track_id)
                        
                        self.track_id_info[camera_id][prev_track_id]["exit_time"] = timestamp
                        vehicle_lpr,vehicle_conf = freq_lpr_conf(self.track_id_info[camera_id][prev_track_id]["vehicle_lpr"],self.track_id_info[camera_id][prev_track_id]["vehicle_conf"])
                        
                        if len(self.track_id_info[camera_id][prev_track_id]['previous_vehicle_lpr']) > 0:
                            prev_vehicle_lpr = utils.most_freq(self.track_id_info[camera_id][prev_track_id]['previous_vehicle_lpr'])
                            # if vehicle_lpr:
                            #     distance = Levenshtein.distance(prev_vehicle_lpr, vehicle_lpr)
                            #     if distance <= 3:
                            #         vehicle_lpr = prev_vehicle_lpr
                            #         self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = [prev_vehicle_lpr]
                            # else:
                            vehicle_lpr = prev_vehicle_lpr
                            self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = [prev_vehicle_lpr]

                        # send platform kafka message
                        message = {"trackId":prev_track_id,\
                               "stageId":CAMERA_DETAILS[camera_id]["stage_id"],\
                               "cameraId":CAMERA_DETAILS[camera_id]["camera_uid"],\
                               "vehicleNumberPlate":vehicle_lpr,\
                               "vehicleUuid": self.track_id_info[camera_id][prev_track_id]['vehicleUuid'],\
                               "checkStartTime": self.track_id_info[camera_id][prev_track_id]['entry_time'],\
                               "useCaseType":"stageCameraLogs",\
                               "checkEndTime": self.track_id_info[camera_id][prev_track_id]['exit_time'],\
                                "checkDone":self.track_id_info[camera_id][prev_track_id]["carbon_checkDone_status"],\
                               "comment":""}
                        self.send_platform_kakfa_msg(message)

                        self.track_id_in_stage[camera_id] = track_id
                        self.track_id_not_visible[camera_id][track_id] = 0

            if track_id == self.track_id_in_stage[camera_id]:
                # Get the vehicle bottom bbox center 
                x_bottom_center = int(x + (w/2))
                y_bottom_center = int(y + h)
                if is_inside(ROIS[camera_id]['entry_roi'], (x_bottom_center, y_bottom_center)):
                    is_track_id_present = True
                    car_coordinates = (x,y,w,h)
                    # print(car_coordinates)
                    # print(bbox_info["lp_english"])
                    lpr,lpr_conf = utils.lpr_current_car(car_coordinates,bbox_info["lp_english"])
                    if lpr !=None and lpr_conf !=None:
                        self.track_id_info[camera_id][track_id]["vehicle_lpr"].append(lpr)
                        self.track_id_info[camera_id][track_id]["vehicle_conf"].append(lpr_conf)
                 
                if self.track_id_info[camera_id][track_id]["entry_status"] == True:
                    if len(bbox_info['operator']) >0:
                        # for carbon_bb in bbox_info['carbon-wire']:
                        for carbon_bb in bbox_info['operator']:
                            _, _, c_x, c_y, c_w, c_h = carbon_bb
                            x_center = int(c_x + (c_w/2))
                            y_center = int(c_y + (c_h))

                            # If detection is inside the stage_roi
                            if is_inside(ROIS[camera_id]['entry_roi'], (x_center, y_center)):
                                self.track_id_info[camera_id][track_id]["carbon_checking_no_frames"] += 1
                                if self.track_id_info[camera_id][track_id]["carbon_checking_no_frames"] >= self.track_id_info[camera_id][track_id]["carbon_check_threshold"]:
                                    self.track_id_info[camera_id][track_id]["carbon_checkDone_status"] = True
        
        # If vehicle not present in the ROI
        if not is_track_id_present:
            if self.track_id_in_stage[camera_id] in self.track_id_not_visible[camera_id]:
                self.track_id_not_visible[camera_id][self.track_id_in_stage[camera_id]] += 1 
                if self.track_id_not_visible[camera_id][self.track_id_in_stage[camera_id]] > EXIT_THRESHOLD:
                    self.track_id_info[camera_id][self.track_id_in_stage[camera_id]]["exit_time"] = timestamp
                    # send platform kafka message
                    prev_track_id = self.track_id_in_stage[camera_id]
                    vehicle_lpr,vehicle_conf = freq_lpr_conf(self.track_id_info[camera_id][prev_track_id]["vehicle_lpr"],self.track_id_info[camera_id][prev_track_id]["vehicle_conf"])
                    
                    if len(self.track_id_info[camera_id][prev_track_id]['previous_vehicle_lpr']) > 0:
                            prev_vehicle_lpr = utils.most_freq(self.track_id_info[camera_id][prev_track_id]['previous_vehicle_lpr'])
                            # if vehicle_lpr:
                            #     distance = Levenshtein.distance(prev_vehicle_lpr, vehicle_lpr)
                            #     if distance <= 3:
                            #         vehicle_lpr = prev_vehicle_lpr
                            #         self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = [prev_vehicle_lpr]
                            # else:
                            vehicle_lpr = prev_vehicle_lpr
                            self.track_id_info[camera_id][prev_track_id]['vehicle_lpr'] = [prev_vehicle_lpr]

                    message = {"trackId":prev_track_id,\
                               "stageId":CAMERA_DETAILS[camera_id]["stage_id"],\
                               "cameraId":CAMERA_DETAILS[camera_id]["camera_uid"],\
                               "vehicleNumberPlate":vehicle_lpr,\
                               "vehicleUuid": self.track_id_info[camera_id][prev_track_id]['vehicleUuid'],\
                               "checkStartTime": self.track_id_info[camera_id][prev_track_id]['entry_time'],\
                               "useCaseType":"stageCameraLogs",\
                               "checkEndTime": self.track_id_info[camera_id][prev_track_id]['exit_time'],\
                               "checkDone":self.track_id_info[camera_id][prev_track_id]["carbon_checkDone_status"]}
                    
                    self.send_platform_kakfa_msg(message)
                    self.history_track_ids[camera_id].append(self.track_id_in_stage[camera_id])
                    self.track_id_in_stage[camera_id] = None
        else:
            self.track_id_not_visible[camera_id][self.track_id_in_stage[camera_id]] = 0
            
    def update_ub_tracker(self, track_id, car_coordinates, camera_id, x_bottom_center, y_bottom_center, entry_roi, bbox_info, timestamp):
        
        # Check if the car is inside the entry ROI
        if is_inside(entry_roi, (x_bottom_center, y_bottom_center)):
            
            camera_info = CAMERA_DETAILS[camera_id]
            stage = camera_info['stage']

            
            
            # Convert car coordinates to bounding box format
            x1, y1, x2, y2 = utils.convert_coordinates(car_coordinates)
            bbox = [x1, y1, x2, y2]
            
            # Perform license plate recognition
            lpr, lpr_conf = utils.lpr_current_car(car_coordinates, bbox_info["lp_english"])
            
            # Extract and find the subregion in the camera's ROI
            subregions = utils.extract_subregions(ROIS[camera_id])
            subregion = utils.find_subregion(bbox, subregions)
            # print(f"Subregion : {subregion}")
            # current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # current_time = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")
            
            
            # Retrieve camera information
            camera_info = CAMERA_DETAILS[camera_id]
            stage = camera_info['stage']
            
            # Update existing tracker information if track_id exists
            if track_id in self.track_id_info[camera_id]:
                car = self.track_id_info[camera_id][track_id]
                entry_time = car['entry_time']
                elapsed_time = int((timestamp - entry_time).total_seconds())
                car['elapsed_time'] = elapsed_time
                car['current_time'] = timestamp
                car['bbox'] = bbox
                car['subregion'] = subregion

            else:
                # common_logger.debug(f"New Tracker {track_id} Added at {current_time} - {CAMERA_DETAILS[camera_id]['stage']}")
                print(f"New Tracker {track_id} Added at {timestamp} - {stage}")

                vehicle_uuid = str(uuid.uuid1())

                # saving snapshots
                filepath = f'./snapshots/{camera_info["lane_name"]}_stage3/{track_id}'
                os.makedirs(filepath, exist_ok=True)
                filename = f'{vehicle_uuid}.jpg'
                fetch_snapshot(camera_id, os.path.join(filepath, filename))
                # common_logger.debug(f"New Tracker {track_id} Added at {timestamp} - {stage}")
                # Add new tracker information
                # print(f"New Tracker {track_id} Added at {timestamp} - {stage}")
                car = utils.create_vehicle_details()
                car['entry_time'] = timestamp
                car["current_time"] = timestamp
                car['bbox'] = bbox
                car['subregion'] = subregion
                car["vehicle_uuid"] = vehicle_uuid
                
                # common_logger.debug(f' ub track id {track_id} {car}')
                
            # Update vehicle ID if LPR is available
            if lpr is not None and lpr_conf is not None:
                car["vehicle_ID_deque"].append(lpr)
                
            if car["vehicle_ID_deque"]:
                counter = Counter(car["vehicle_ID_deque"])
                most_common = counter.most_common(1)
                element, _ = most_common[0]
                car["vehicle_id"] = element
            
            # Store updated tracker info
            self.track_id_info[camera_id][track_id] = car
    
    def associate_cars(self, lane_name):
        # Associate cars based on matching license plate characters
        stage_ubtopfront = lane_name + '_ubtopfront'
        stage_ubtopback = lane_name + '_ubtopback'
        
        if stage_ubtopfront in self.track_id_info:
            for track_id1, car1 in self.track_id_info[stage_ubtopfront].items():
                if self.track_id_info[stage_ubtopfront][track_id1]["associated_tracking_id"] is None:
                    if stage_ubtopback in self.track_id_info:
                        for track_id2, car2 in self.track_id_info[stage_ubtopback].items():
                            # Check if at least 3 characters match in vehicle IDs
                            if sum(1 for a, b in zip(car1["vehicle_id"], car2["vehicle_id"]) if a == b) >= 3:
                                # print(f'-------------{track_id1} and {track_id2} are associated--------------')
                                self.track_id_info[stage_ubtopfront][track_id1]["associated_tracking_id"] = track_id2
                                self.track_id_info[stage_ubtopback][track_id2]["associated_tracking_id"] = track_id1
                                self.track_id_info[stage_ubtopfront][track_id1]["entry_time"] = self.track_id_info[stage_ubtopback][track_id2]["entry_time"]
                                self.track_id_info[stage_ubtopfront][track_id1]["checking_status"] = self.track_id_info[stage_ubtopback][track_id2]["checking_status"]
                                break
                
    def ub_entry_exit(self, bbox_info, camera_id, timestamp):
        # Process entry and exit of vehicles
        if bbox_info['car']:
            for car_bb in bbox_info['car']:
                track_id, confidence, x, y, w, h = car_bb
                x_bottom_center = int(x + (w/2))
                y_bottom_center = int(y + h)
                entry_roi = ROIS[camera_id]['entry_roi']
                car_coordinates = [x, y, w, h]
                
                self.update_ub_tracker(track_id, car_coordinates, camera_id, x_bottom_center, y_bottom_center, entry_roi, bbox_info, timestamp)
                
    def ub_check_status(self, bbox_info, subregions, car_data):
    
        # Retrieve the current count of frames where the condition was met
        count = car_data["check_frames_count"]

        # Check if there are detected 'ub_operator' bounding boxes
        if bbox_info['ub_operator']:
            for ub_operator_bb in bbox_info['ub_operator']:
                # Extract bounding box data: track_id, confidence, and coordinates
                track_id, confidence, x, y, w, h = ub_operator_bb

                # Convert bounding box format from (x, y, w, h) to (x_min, y_min, x_max, y_max)
                bbox = [x, y, w, h]
                x1, y1, x2, y2 = utils.convert_coordinates(bbox)
                bbox = [x1, y1, x2, y2]

                # Calculate the bottom-center position of the bounding box
                x_bottom = int(x + (w / 2))
                y_bottom = int(y + h)

                # Determine which subregion the operator belongs to based on bottom-center position
                operator_subregion = utils.find_operator_subregion((x_bottom, y_bottom), subregions)

                # Check conditions based on car's assigned subregion
                if car_data['subregion'] == 1:
                    # If car is in subregion 1, operator should be in subregions 1 or 2
                    if operator_subregion in [1, 2]:
                        count += 1  # Increment frame count
                        if count >= config.UB_CHECK_FRAME_THRESHOLD:
                            return 1, count  # Condition met, return status 1
                else:
                    # For other subregions, operator should be in subregions 2 or 3
                    if operator_subregion in [2, 3]:
                        count += 1  # Increment frame count
                        if count >= config.UB_CHECK_FRAME_THRESHOLD:
                            return 1, count  # Condition met, return status 1

        # If condition is not met, return status 0 and updated count
        return 0, count
    
    def check_under_body(self, bbox_info, timestamp, camera_id) -> None:
        
        camera_info = CAMERA_DETAILS[camera_id]
        stage = camera_info['stage']
        lane_name = camera_info['lane_name']

        fmt = "%Y-%m-%d %H:%M:%S.%f"
        timestamp = datetime.strptime(timestamp, fmt)
        
        # Create identifiers for different stages of vehicle tracking
        stage_ubtopfront = lane_name + '_ubtopfront'
        stage_ubtopback = lane_name + '_ubtopback'

        # If the current camera stage is 'ubtopback' or 'ubtopfront', perform entry/exit tracking
        if stage in ["ubtopback", "ubtopfront"]:
            self.ub_entry_exit(bbox_info, camera_id, timestamp)

        # Associate cars based on their lane name
        self.associate_cars(lane_name)

        # Check if there are any tracked vehicles in the 'ubtopback' stage
        if stage_ubtopback in self.track_id_info:
            for tracking_id, data in self.track_id_info[stage_ubtopback].items():
                # If checking status is False, the car has not been processed yet
                if self.track_id_info[stage_ubtopback][tracking_id]["checking_status"] is False:
                    
                    check_status = 0  # Default check status
                    
                    # If no associated tracking ID, use current tracking ID; otherwise, fetch associated one
                    if self.track_id_info[stage_ubtopback][tracking_id]["associated_tracking_id"] is None:
                        car_data = data
                        check_stage = lane_name + '_ubtopback'
                        check_tracking_id = tracking_id
                    else:
                        check_tracking_id = self.track_id_info[stage_ubtopback][tracking_id]["associated_tracking_id"]
                        car_data = self.track_id_info[stage_ubtopfront][check_tracking_id]
                        check_stage = lane_name + '_ubtopfront'

                    # Perform under-body check for bottom front cameras in subregion 1
                    if stage == "ubbottomfront" and data['subregion'] == 1:
                        subregions = utils.extract_subregions(ROIS[camera_id])
                        check_status, count = self.ub_check_status(bbox_info, subregions, car_data)
                        self.track_id_info[check_stage][check_tracking_id]["check_frames_count"] = count
                        # print(f'Count : {count}')

                    # Perform under-body check for bottom back cameras in subregions 2 or 3
                    elif stage == "ubbottomback" and data['subregion'] in [2,3]:
                        subregions = utils.extract_subregions(ROIS[camera_id])
                        check_status, count = self.ub_check_status(bbox_info, subregions, car_data)
                        self.track_id_info[check_stage][check_tracking_id]["check_frames_count"] = count
                        # print(f'Count : {count}')

                    # If check_status is positive, update the checking status for the vehicle
                    if check_status == 1:
                        # common_logger.debug(f"Check performed for car {check_tracking_id}")
                        # print(f"Check performed for car {check_tracking_id}")
                        
                        # Mark the vehicle as checked in the tracking information
                        self.track_id_info[stage_ubtopback][tracking_id]["checking_status"] = True
                        
                        # If there is an associated tracking ID, mark it as checked in the front stage as well
                        if self.track_id_info[stage_ubtopback][tracking_id]["associated_tracking_id"] is not None:
                            tracking_id_front = self.track_id_info[stage_ubtopback][tracking_id]["associated_tracking_id"]
                            self.track_id_info[stage_ubtopfront][tracking_id_front]["checking_status"] = True
                            
    
    def delete_tracker(self, camera_id, timestamp):
        # current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # current_time = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")
        lp_details = {}
        fmt = "%Y-%m-%d %H:%M:%S.%f"
        # common_logger.debug(f"type of timestmp {type(timestamp)}")
        timestamp_st = timestamp
        timestamp = datetime.strptime(timestamp, fmt)
        # common_logger.debug(f"type of timestmp_st {type(timestamp_st)}")
        camera_info = CAMERA_DETAILS[camera_id]
        lane_name = camera_info['lane_name']
        stage = camera_info['stage']
        lpr_camera_id = lane_name+"_ub"
        
        # If the camera stage is 'ubtopfront'
        if stage == "ubtopfront":
            if camera_id in self.track_id_info:
                for tracking_id in self.track_id_info[camera_id]:
                    car = self.track_id_info[camera_id][tracking_id]
                    lp_details["track_id"] = tracking_id
                    if int((timestamp - car['current_time']).total_seconds()) > 2:
                        car['exit_time'] = car['current_time']

                        previous_stage_message = []
                        if camera_info['previous_stage'] in self.history_track_ids and len(self.history_track_ids[camera_info['previous_stage']]):
                            for i in range(-1,max(-(len(self.history_track_ids[camera_info['previous_stage']])+1), -20),-1):
                                # print("Appending prev vehicle lprr ",self.track_id_info[camera_info['previous_stage']][self.history_track_ids[camera_info['previous_stage']][i]] )
                                previous_stage_message.append(self.track_id_info[camera_info['previous_stage']][self.history_track_ids[camera_info['previous_stage']][i]]   )
                            # previous_stage_message = self.track_id_info[camera_info['previous_stage']][self.history_track_ids[camera_info['previous_stage']][-1]]
                            
                        car["previous_stage_message"] = previous_stage_message
                        self.track_id_info[camera_id][tracking_id] = car
                        # common_logger.debug(f"Deleting Tracker: {tracking_id} Lane: {lane_no}")
                        print(f"Deleting Tracker: {tracking_id} at {timestamp}")                    
                        
                        
                        # Check lp matching with current stage prev exit
                        vehicle_lpr = car["vehicle_id"]
                        lp_details["curr_lpr"] = car["vehicle_id"]
                        # common_logger.debug(f"Before accessing : {self.lpr_info}")
                        # common_logger.debug(f"Before accessing ubtopfront:  {lpr_camera_id} {self.lpr_info[lpr_camera_id]}")
                        # common_logger.debug(f"history tracking id {self.history_track_ids[camera_id]}")
                        if len(self.history_track_ids[camera_id]):
                            previous_vehicle_lpr = self.lpr_info[lpr_camera_id][self.history_track_ids[camera_id][-1]]['vehicle_lpr'] 
                            lp_details["prev_veh_lpr"] = previous_vehicle_lpr
                            time_diff_in_seconds = get_time_diff_in_seconds(self.lpr_info[lpr_camera_id][self.history_track_ids[camera_id][-1]]["exit_time"],timestamp_st)
                            print("Time diff in ub ",time_diff_in_seconds)
                            if previous_vehicle_lpr and vehicle_lpr and time_diff_in_seconds < LPR_MATCHING_SECONDS:
                                distance = Levenshtein.distance(previous_vehicle_lpr, vehicle_lpr)
                                if distance <= 3:
                                    vehicle_lpr = previous_vehicle_lpr
                                    car["vehicle_id"] = previous_vehicle_lpr
                                    print("Matched with prev event in same stage ",previous_vehicle_lpr," curr ",vehicle_lpr)
                            
                        # Check previous stage lp matching for 5 entries
                        
                        print("Len of prev stage message ",len(self.track_id_info[camera_id][tracking_id]['previous_stage_message']))
                        lp_details["prev_stage_lpr"]=[]
                        if len(self.track_id_info[camera_id][tracking_id]['previous_stage_message']) > 0:
                            
                            match_found = False
                            for i in range(len(self.track_id_info[camera_id][tracking_id]['previous_stage_message'])):
                                
                                previous_vehicle_lpr = self.track_id_info[camera_id][tracking_id]['previous_stage_message'][i]['vehicle_lpr'] 
                                time_diff_in_seconds = get_time_diff_in_seconds(self.track_id_info[camera_id][tracking_id]['previous_stage_message'][i]["exit_time"],timestamp_st)
                                lp_details["prev_stage_lpr"].append(previous_vehicle_lpr)
                                print("Time diff in ub ",time_diff_in_seconds)
                                print("Checking prev lpr ",previous_vehicle_lpr ," with current lpr ",vehicle_lpr)
                                if previous_vehicle_lpr and time_diff_in_seconds < LPR_MATCHING_SECONDS :
                                    if vehicle_lpr:
                                        distance = Levenshtein.distance(previous_vehicle_lpr, vehicle_lpr)
                                        if distance <= 3:
                                            vehicle_lpr = previous_vehicle_lpr
                                            car["vehicle_id"] = previous_vehicle_lpr
                                            print("Found matching lpr and breaking")
                                            match_found=True
                                            break
                                    # else:
                                    #     vehicle_lpr = previous_vehicle_lpr
                                    #     car["vehicle_id"] = previous_vehicle_lpr
                                    #     print("No current lpr found so taking prev lpr  ",previous_vehicle_lpr)
                                    #     match_found=True
                                    #     break    
                        lp_details["updated_lpr"] = car["vehicle_id"]
                        # Push data to platform 
                        message_exit = {}
                        message_exit["trackId"]= tracking_id
                        message_exit["cameraId"]= camera_info["camera_uid"]
                        message_exit['stageId'] = camera_info["stage_id"]
                        # if car["vehicle_id"] == "":
                        #     car["vehicle_id"] = str(car["vehicle_uuid"])
                        message_exit['vehicleNumberPlate'] = car["vehicle_id"]
                        message_exit['checkStartTime'] = car["entry_time"].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        message_exit['checkEndTime'] = car['exit_time'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        message_exit['checkDone'] = car["checking_status"]
                        message_exit['useCaseType'] = "stageCameraLogs"
                        message_exit['vehicleUuid']= str(car["vehicle_uuid"])
                        message_exit['comment']= "UB check done" if message_exit['checkDone'] else "UB check not done"
                        
                        
                        # Check end time and start time > 7 sec
                        t1 = car["entry_time"]
                        t2 = car["exit_time"]
                        
                    
                    # Delete tracker if it's inactive for more than 2 seconds
                    if int((timestamp - car['current_time']).total_seconds()) > 2:
                        car['exit_time'] = car['current_time']
                        # print(f"Deleting Tracker: {tracking_id} at {timestamp}")

                        # Prepare exit message to be sent to the platform
                        message_exit = {
                            "trackId": tracking_id,
                            "cameraId": camera_info["camera_uid"],
                            "stageId": camera_info["stage_id"],
                            "vehicleNumberPlate": car["vehicle_id"],
                            "checkStartTime": car["entry_time"].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                            "checkEndTime": car['exit_time'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                            "checkDone": car["checking_status"],
                            "useCaseType": "stageCameraLogs",
                            "vehicleUuid": str(car["vehicle_uuid"]),
                            "comment": "UB check done" if car["checking_status"] else "UB check not done"
                        }

                        # Check if vehicle was processed for at least 4 seconds and associated with another tracker
                        if (car['exit_time'] - car["entry_time"]).total_seconds() >= 4 and car["associated_tracking_id"] is not None:
                            self.send_platform_kakfa_msg(message_exit)
                            common_logger.debug(f"UB front {lp_details}")
                            self.history_track_ids[camera_id].append(tracking_id)
                            
                            print(f"Sent vehicle stage exit log for {lane_name} and stage {stage} : {message_exit}")
                            # common_logger.debug(f"Sent vehicle stage exit log for {lane_name} and stage {camera_id} : {message_exit}")
                            # common_logger.debug(f"Elapsed time: {car['elapsed_time']}")
                        
                        
                            self.lpr_info[lpr_camera_id][tracking_id]={}
                            self.lpr_info[lpr_camera_id][tracking_id]["vehicle_lpr"] = car["vehicle_id"]
                            self.lpr_info[lpr_camera_id][tracking_id]["exit_time"] = car['exit_time'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                            # common_logger.debug(f"ubtopfront:  {self.lpr_info}")
                            # common_logger.debug(f"Appending ubtopfront: {lpr_camera_id} {self.lpr_info[lpr_camera_id]}")
                        back_track_id = self.track_id_info[camera_id][tracking_id]["associated_tracking_id"]
                            # print(f"Sent vehicle stage exit log for {lane_name} and stage {stage}: {message_exit}")
                        
                        # Delete tracking data from both front and back cameras
                        back_track_id = car["associated_tracking_id"]
                        del self.track_id_info[camera_id][tracking_id]
                        if back_track_id is not None:
                            back_stage = lane_name + '_ubtopback'
                            if back_track_id in self.track_id_info[back_stage]:
                                del self.track_id_info[back_stage][back_track_id]
                        break

        # If the camera stage is 'ubtopback'
        if stage == "ubtopback":
            if camera_id in self.track_id_info:
                for tracking_id in self.track_id_info[camera_id]:
                    car = self.track_id_info[camera_id][tracking_id]
                    lp_details["track_id"] = tracking_id 
                    front_track_id = self.track_id_info[camera_id][tracking_id]["associated_tracking_id"]
                    front_track_id = car["associated_tracking_id"]

                    # Delete unassociated trackers after 4 minutes
                    if front_track_id is None:
                        car['exit_time'] = car['current_time']
                        time_diff = int((timestamp - car['current_time']).total_seconds())
                        #delete car from back car after 2 mins if no association happened
                        previous_stage_message = []
                        if camera_info['previous_stage'] in self.history_track_ids and len(self.history_track_ids[camera_info['previous_stage']]):
                            for i in range(-1,max(-(len(self.history_track_ids[camera_info['previous_stage']])+1), -20),-1):
                                # print("Appending prev vehicle lprr ",self.track_id_info[camera_info['previous_stage']][self.history_track_ids[camera_info['previous_stage']][i]] )
                                previous_stage_message.append(self.track_id_info[camera_info['previous_stage']][self.history_track_ids[camera_info['previous_stage']][i]]   )
                            # previous_stage_message = self.track_id_info[camera_info['previous_stage']][self.history_track_ids[camera_info['previous_stage']][-1]]
                            
                        car["previous_stage_message"] = previous_stage_message
                        self.track_id_info[camera_id][tracking_id] = car
                        
                        if time_diff >= 240:
                            print(f"Back car {tracking_id} didn't associate hence being deleted")                            
                            
                            if car['exit_time'] - car["entry_time"]).total_seconds() > 4:
                                # Push data to platform 
                                vehicle_lpr = car["vehicle_id"]
                                lp_details["curr_lpr"] = car["vehicle_id"]
                                # Check lp matching with current stage prev exit
                                # common_logger.debug(f"Before accessing: {self.lpr_info[lpr_camera_id]}")
                                if len(self.history_track_ids[camera_id]):
                                    previous_vehicle_lpr = self.lpr_info[lpr_camera_id][self.history_track_ids[camera_id][-1]]['vehicle_lpr'] 
                                    lp_details["prev_veh_lpr"]=previous_vehicle_lpr
                                    time_diff_in_seconds = get_time_diff_in_seconds(self.lpr_info[lpr_camera_id][self.history_track_ids[camera_id][-1]]["exit_time"],timestamp_st)
                                    print("Time diff in ub ",time_diff_in_seconds)
                                    if previous_vehicle_lpr and vehicle_lpr and time_diff_in_seconds < LPR_MATCHING_SECONDS:
                                        distance = Levenshtein.distance(previous_vehicle_lpr, vehicle_lpr)
                                        if distance <= 3:
                                            vehicle_lpr = previous_vehicle_lpr
                                            car["vehicle_id"] = previous_vehicle_lpr
                                            print("Matched with prev event in same stage ",previous_vehicle_lpr," curr ",vehicle_lpr)
                            
                                # Check previous stage lp matching for 5 entries
                                lp_details["prev_stage_lpr"]=[]
                                print("Len of prev stage message ",len(self.track_id_info[camera_id][tracking_id]['previous_stage_message']))
                                if len(self.track_id_info[camera_id][tracking_id]['previous_stage_message']) > 0:
                                    
                                    match_found = False
                                    for i in range(len(self.track_id_info[camera_id][tracking_id]['previous_stage_message'])):
                                        
                                        previous_vehicle_lpr = self.track_id_info[camera_id][tracking_id]['previous_stage_message'][i]['vehicle_lpr'] 
                                        time_diff_in_seconds = get_time_diff_in_seconds(self.track_id_info[camera_id][tracking_id]['previous_stage_message'][i]["exit_time"],timestamp_st)
                                        lp_details["prev_stage_lpr"].append(previous_vehicle_lpr)
                                        print("Time diff in ub ",time_diff_in_seconds)
                                        print("Checking prev lpr ",previous_vehicle_lpr ," with current lpr ",vehicle_lpr)
                                        if previous_vehicle_lpr and time_diff_in_seconds < LPR_MATCHING_SECONDS :
                                            if vehicle_lpr:
                                                distance = Levenshtein.distance(previous_vehicle_lpr, vehicle_lpr)
                                                if distance <= 3:
                                                    vehicle_lpr = previous_vehicle_lpr
                                                    car["vehicle_id"] = previous_vehicle_lpr
                                                    print("Found matching lpr and breaking")
                                                    match_found=True
                                                    break
                                            # else:
                                            #     vehicle_lpr = previous_vehicle_lpr
                                            #     car["vehicle_id"] = previous_vehicle_lpr
                                            #     print("No current lpr found so taking prev lpr  ",previous_vehicle_lpr)
                                            #     match_found=True
                                            #     break    
                                

                                lp_details["updated_lpr"] = car["vehicle_id"]
                                message_exit = {}
                                message_exit["trackId"]= tracking_id
                                message_exit["cameraId"]= camera_info["camera_uid"]
                                message_exit['stageId'] = camera_info["stage_id"]
                                # if car["vehicle_id"] == "":
                                #     car["vehicle_id"] = str(car["vehicle_uuid"])
                                message_exit['vehicleNumberPlate'] = car["vehicle_id"]
                                message_exit['checkStartTime'] = car["entry_time"].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                                message_exit['checkEndTime'] = car['exit_time'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                                message_exit['checkDone'] = car["checking_status"]
                                message_exit['useCaseType'] = "stageCameraLogs"
                                message_exit['vehicleUuid']= str(car["vehicle_uuid"])
                                message_exit['comment']= "UB check done" if message_exit['checkDone'] else "UB check not done"
                                self.send_platform_kakfa_msg(message_exit)
                                common_logger.debug(f"UB back {lp_details}")
                                self.history_track_ids[camera_id].append(tracking_id)
                                
                                
                                print(f"Sent vehicle stage exit log for {lane_name} and stage {camera_id} : {message_exit}")
                                # common_logger.debug(f"Sent vehicle stage exit log for {lane_name} and stage {camera_id} : {message_exit}")
                                # common_logger.debug(f"Elapsed time: {car['elapsed_time']}")

                        if time_diff >= 240:
                            # print(f"Back car {tracking_id} didn't associate hence being deleted")

                            # If car was present for at least 4 seconds, send exit message
                            if (car['exit_time'] - car["entry_time"]).total_seconds() > 4:
                                message_exit = {
                                    "trackId": tracking_id,
                                    "cameraId": camera_info["camera_uid"],
                                    "stageId": camera_info["stage_id"],
                                    "vehicleNumberPlate": car["vehicle_id"],
                                    "checkStartTime": car["entry_time"].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                                    "checkEndTime": car['exit_time'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                                    "checkDone": car["checking_status"],
                                    "useCaseType": "stageCameraLogs",
                                    "vehicleUuid": str(car["vehicle_uuid"]),
                                    "comment": "UB check done" if car["checking_status"] else "UB check not done"
                                }
                                self.send_platform_kakfa_msg(message_exit)
                                # print(f"Sent vehicle stage exit log for {lane_name} and stage {camera_id}: {message_exit}")

                            self.lpr_info[lpr_camera_id][tracking_id] = {}
                            self.lpr_info[lpr_camera_id][tracking_id]["vehicle_lpr"] = car["vehicle_id"]
                            self.lpr_info[lpr_camera_id][tracking_id]["exit_time"] = car['exit_time'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                            # common_logger.debug(f"Appending ubtopbaack: {self.lpr_info[lpr_camera_id]}")
                            del self.track_id_info[camera_id][tracking_id]
                            break
 
        
    def update(self, message: dict) -> None:

        camera_id = message['camera_id']
        camera_info = CAMERA_DETAILS[camera_id]

        
        # print("updating")
        if camera_id not in self.track_id_info :
            self.track_id_info[camera_id] = {}
            
        if camera_info['lane_name']+"_ub" not in self.lpr_info :
            if "ub" in camera_id: 
                self.lpr_info[camera_info['lane_name']+"_ub"] = {}
           
        if camera_id not in self.track_id_in_stage :
            self.track_id_in_stage[camera_id] = None
           
        if camera_id not in self.history_track_ids :
            self.history_track_ids[camera_id] = []
           
        if camera_id not in self.track_id_not_visible :
            self.track_id_not_visible[camera_id] = {}

        while len(self.history_track_ids[camera_id]) > 10:
            oldest_tid = self.history_track_ids[camera_id].pop(0)
            
            if "ub" in camera_id:
                del self.lpr_info[camera_info['lane_name']+"_ub"][oldest_tid]

            else:
                del self.track_id_info[camera_id][oldest_tid]
                del self.track_id_not_visible[camera_id][oldest_tid]
       
        # print(f"Processing stage {
        # } and type {camera_info['type']}")
        # print(camera_info)
        
        if camera_info['stage'] == '360' and camera_info['type'] == 'check':
            self.check_360(message['bbox_info'], message['timestamp'], camera_id)
        elif camera_info['stage'] == '360' and camera_info['type'] == 'lpr':
            self.lpr_360(message['bbox_info'], message['timestamp'], camera_id)
        elif camera_info['stage'] == 'brake':
            self.check_brake(message['bbox_info'], message['timestamp'], camera_id)
        # elif camera_info['stage'] == '360' and camera_info['type'] == 'check':
        #     self.check_carbon(message['bbox_info'], message['timestamp'], camera_id)
        elif camera_info['stage'] in ["ubtopfront", "ubtopback", "ubbottomfront", "ubbottomback"]:
            # print("Calling check ub----------------", camera_id)
            self.check_under_body(message['bbox_info'], message['timestamp'], camera_id)
        
        if camera_info['stage'] in ["ubtopfront", "ubtopback"]:
            self.delete_tracker(camera_id, message['timestamp'])

        
class Main:
    def __init__(self) -> None:
        self.camera_details = {}
         # Create Kafka producer
        
        self.lane_instances = {}
        self.lane_instances[LANE_TO_USE] = LaneBase(LANE_TO_USE)


        # for lane_name in LANES_TO_USE2:
        #     self.lane_instances[lane_name] = LaneBase(lane_name)

    def parse(self,message):
        '''
        parse message and update lane wise
        '''
        camera_id = message['camera_id']
        # print(f'Processing {camera_id}')
        # print (camera_id)

        camera_info = CAMERA_DETAILS[camera_id]

        self.lane_instances[camera_info['lane_name']].update(message)

    def kafka_consumer(self):
        consumer = Consumer(KAFKA_CONSUMER_CONF)
        consumer.subscribe(CONSUMER_TOPICS)

        try:
            print(f"Subscribed to topic: saso-inference")
            while True:
                # Poll for new messages
                msg = consumer.poll(timeout=1.0)
                
                # print(msg)
                if msg is None:
                    print(msg)
                    continue


                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, the consumer reached the end of the partition
                        print(f"Reached end of partition {msg.partition()}")
                    elif msg.error():
                        # Error, handle the error
                        print(f"Error: {msg.error()}")
                    
                else:
                    # Message was successfully received
                    message_value = json.loads(msg.value().decode('utf-8'))
                    # print(f"----------------- \n{message_value}\n--------------")
                    try:
                        self.parse(message_value)
                    except Exception as e:
                        print(f'Error: {e}')
                        traceback.print_exc()
                        # continue

        except KeyboardInterrupt:
            print("Consumer interrupted")

        finally:
            # Close the consumer
            consumer.close()


if __name__ == "__main__":
    station17 = Main()
    print('Started')
    station17.kafka_consumer()
    print('Done.')




