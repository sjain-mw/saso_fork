import common_vars
from collections import  deque
from logger_config import common_logger

def default_lane_details(lane,stage):
    common_vars.lane_stage_details[lane][stage]["STATUS"]="Waiting"
    common_vars.lane_stage_details[lane][stage]["checking_status"]=False
    common_vars.lane_stage_details[lane][stage]["checked"]=""
    common_vars.lane_stage_details[lane][stage]["entry_time"]=0
    common_vars.lane_stage_details[lane][stage]["exit_time"]=0
    common_vars.lane_stage_details[lane][stage]["no_obj_frames"] = 0
    common_vars.lane_stage_details[lane][stage]["vehicle_in_roi"]=[]
    common_vars.lane_stage_details[lane][stage]["object_id"] = ""
    common_vars.lane_stage_details[lane][stage]["vehicle_id"]=""
    common_vars.lane_stage_details[lane][stage]["vehicle_id_conf"] = 0
    common_vars.lane_stage_details[lane][stage]["checking_no_frames"] = 0
    common_vars.lane_stage_details[lane][stage]["vehicle_uuid"] = ""
    common_vars.lane_stage_details[lane][stage]["inspection_id"] = None
    common_vars.lane_stage_details[lane][stage]["vehicle_ID_deque"] = deque(maxlen=common_vars.max_length)
    common_vars.lane_stage_details[lane][stage]["vehicle_ID_deque_conf"] = deque(maxlen=common_vars.max_length)
    common_vars.lane_stage_details[lane][stage]["car_ids"] = []
    common_vars.lane_stage_details[lane][stage]["operator_ids"] = {}
    common_vars.lane_stage_details[lane][stage]["grid_coordinates"] = []
    common_vars.lane_stage_details[lane][stage]["carbon_checking"]=False
    common_vars.lane_stage_details[lane][stage]["carbon_checking_vehicle_uuid"]=""
    common_vars.lane_stage_details[lane][stage]["vehicleColor"] = None
    common_vars.lane_stage_details[lane][stage]["vehicleType"] = None
    common_vars.lane_stage_details[lane][stage]["previous_stage_LP"] = ""
    common_vars.lane_stage_details[lane][stage]["previous_stage_LP_conf"] = 0
    common_vars.lane_stage_details[lane][stage]["previous_stage_LP_inspection_id"]=None

def create_vehicle_details():
        return {
            "STATUS": "Waiting",
            "checking_status": "Check Not Performed",
            "checked": "",
            "entry_time": 0,
            "current_time":0,
            "elapsed_time":0,
            "exit_time": 0,
            "no_obj_frames": 0,
            "vehicle_in_roi": [],
            "object_id": "",
            "vehicle_id": "",
            "checking_no_frames": 0,
            "vehicle_uuid": "",
            "bbox":0,
            "subregion":0,
            "associated_tracking_id":None,
            "vehicle_ID_deque": deque(maxlen=common_vars.max_length)
        }

def create_vehicle_details_back():
    return {
        "entry_time":0,
        "bbox":0
    }

def create_lane_stage_dictionary():
    for lane in common_vars.lanes_details.keys():
        common_vars.lane_stage_details[str(lane)]={}
        for stage in common_vars.lanes_details[lane]:
            common_vars.lane_stage_details[str(lane)][stage] = {}
            default_lane_details(str(lane),stage)

def create_lane_stage_dictionary_ub():
    for lane in common_vars.lanes_details.keys():
        common_vars.ub_top_back_details[int(lane)] = {}
        common_vars.lane_stage_details[int(lane)] = {}
        for stage in common_vars.lanes_details[lane]:
            common_vars.lane_stage_details[int(lane)][stage] = {}
            # default_lane_details(str(lane),stage)

def default_check_details_360(lane,stage):
    common_vars.check_details_360[lane][stage]['car_position'] = None
    common_vars.check_details_360[lane][stage]['stationary_frame_count'] = 0
    common_vars.check_details_360[lane][stage]['car_stationary'] = False
    common_vars.check_details_360[lane][stage]['new_car_count'] = 0

def create_dictionary_360_check():
    for lane in common_vars.lanes_details.keys():
        common_vars.check_details_360[str(lane)]={}
        common_vars.check_details_360[str(lane)]["360"]={}
        default_check_details_360(str(lane),"360")
        default_check_details_360_truck(str(lane),"360")

def default_check_details_break(lane,stage):
    common_vars.brake_check_details[lane][stage]['brake_check_status'] = False
    common_vars.brake_check_details[lane][stage]['brake_check_count'] = 0

def create_dictionary_brake_check():
    for lane in common_vars.lanes_details.keys():
        common_vars.brake_check_details[str(lane)]={}
        common_vars.brake_check_details[str(lane)]["break"]={}
        default_check_details_break(str(lane),"break")


def default_check_details_360_truck(lane,stage):
    common_vars.check_details_360[lane][stage]['truck_left'] = False
    common_vars.check_details_360[lane][stage]['truck_right'] = False
    common_vars.check_details_360[lane][stage]['truck_front'] = False
    common_vars.check_details_360[lane][stage]['truck_back'] = False
