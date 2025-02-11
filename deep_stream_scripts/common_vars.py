import json
import os
DEBUG=0
TEST=int(os.getenv('test'))

MUXER_OUTPUT_WIDTH=1280
MUXER_OUTPUT_HEIGHT=720
MUXER_BATCH_TIMEOUT_USEC = 40000
TILED_OUTPUT_WIDTH=1280
TILED_OUTPUT_HEIGHT=720
GST_CAPS_FEATURES_NVMM="memory:NVMM"
OSD_PROCESS_MODE= 0
OSD_DISPLAY_TEXT= 1
# ---------------------------------------------------
bitrate = 4000000
codec = "H264"
# one min please
# Define the classes
PGIE_CLASS_ID_VEHICLE = 0
PGIE_CLASS_ID_TRUCK = 6
PGIE_CLASS_ID_PERSON =1
PGIE_CLASS_ID_LP_ARABIC =2
PGIE_CLASS_ID_LP_ENGLISH =3
PGIE_CLASS_ID_TYRE =7
PGIE_CLASS_ID_NON_OPERATOR=5
PGIE_CLASS_ID_UB_OPERATOR =8
PGIE_CLASS_ID_CARBON_WIRE = 4
PGIE_CLASS_ID_TOP_TRUCK = 9
PGIE_CLASS_ID_TOP_CAR = 10
STREAM_WIDTH=1920
STREAM_HEIGHT=1080
CONFIG_WIDTH=0
CONFIG_HEIGHT=0
# Variables for underbody check
EVENT_THRESHOLD = 5
EXIT_EVENT_THRESHOLD = 5
UB_CHECK_FRAME_THRESHOLD = 10
NO_SECONDS_THRESHOLD=2
#variables for carbon check
CARBON_CHECK_FRAME_THRESHOLD = 5
#variables for 360
PIXEL_DISTANCE =3
STATIOANRY_CAR_THRESHOLD= 3
EXIT_17_STATION_ID= "06a7d884-8e6c-4f7a-95a4-043506d2be13" 
STATION_1_STATION_ID = "1c52645e-caa2-4d4d-8a37-e6a22350bf0b"

# lanes_details = {1:["break","360","carbon","ub"],6:["break","360","carbon","ub"],6:["break","360","carbon","ub"], 9:["break","360","carbon","ub"]}
lanes_details = {key: ["break","360","carbon","ub"] for key in range(1, 11)}
lanes_details['left'] = ["main"]
lanes_details['right'] = ["main"]
# lanes_details = {6:["break","360","carbon","ub"]}
# grid_coordinates = [[[2548.0, 1670.0], [2505.3333333333335, 1434.3333333333333], [2254.666666666667, 1436.0], [2268.0, 1673.6666666666667], [2548.0, 1670.0]], [[2268.0, 1673.6666666666667], [2254.666666666667, 1436.0], [2004.0, 1437.6666666666665], [1988.0, 1677.3333333333333], [2268.0, 1673.6666666666667]], [[1988.0, 1677.3333333333333], [2004.0, 1437.6666666666665], [1753.3333333333333, 1439.3333333333333], [1708.0, 1681.0], [1988.0, 1677.3333333333333]], [[2505.3333333333335, 1434.3333333333333], [2462.6666666666665, 1198.6666666666667], [2241.333333333333, 1198.3333333333335], [2254.666666666667, 1436.0], [2505.3333333333335, 1434.3333333333333]], [[2254.666666666667, 1436.0], [2241.333333333333, 1198.3333333333335], [2020.0, 1198.0], [2004.0, 1437.6666666666665], [2254.666666666667, 1436.0]], [[2004.0, 1437.6666666666665], [2020.0, 1198.0], [1798.6666666666667, 1197.6666666666667], [1753.3333333333333, 1439.3333333333333], [2004.0, 1437.6666666666665]], [[2462.6666666666665, 1198.6666666666667], [2420.0, 963.0], [2228.0, 960.6666666666666], [2241.333333333333, 1198.3333333333335], [2462.6666666666665, 1198.6666666666667]], [[2241.333333333333, 1198.3333333333335], [2228.0, 960.6666666666666], [2036.0, 958.3333333333334], [2020.0, 1198.0], [2241.333333333333, 1198.3333333333335]], [[2020.0, 1198.0], [2036.0, 958.3333333333334], [1844.0, 956.0], [1798.6666666666667, 1197.6666666666667], [2020.0, 1198.0]]]
tyre_roi = []
input_config_directory = "../configs/input_configs"
saso_config_directory = "../configs/saso_test_configs"
input_config_file = os.getenv('input_config')

# file =open(input_config_file) 
input_config = None
output_video_caps = []
entry_count=0
exit_count=0
max_length =1000

lane_stage_details={}
lane_id= {}
file_path =  os.getenv('saso_config')
roi_coordinates={}
tyre_xy=[]
stage_sequence={"360":1,"break":2,"carbon":3,"ub":4}
#360 check vars
check_details_360 = {}

# car_position = None
# stationary_frame_count = 0
# car_stationary = False
# new_car_count = 0

#break check vars
BRAKE_CHECK_FRAME = 4

brake_check_details = {}

# Exit 17 UB check vars
lane_stage_details = {}
ub_top_back_details = {}

classes =  {
	    0: "car",
	    1: "operator",
	    2: "lp_arabic",
	    3: "lp_english",
	    4: "carbon-wire",
	    5: "non operator",
	    6: "truck",
	    7: "tyre",
	    8: "ub_operator"
    }

subregions = [
        [
            [
                [1010, 282], 
                [1186, 292], 
                [1273, 383], 
                [1049, 372]
            ],
            [
                [1049, 372], 
                [1273, 383], 
                [1446, 570], 
                [1142, 580]
            ],
            [
                [1142, 580], 
                [1446, 570], 
                [1836, 994], 
                [1344, 1060]
            ]
        ],
        [
            [
                [885, 726],
                [1022, 721],
                [1065, 823],
                [854, 823]
            ],
            [
                [854, 823],
                [1065, 823],
                [1117, 953],
                [817, 948]
            ],
            [
                [817, 948],
                [1117, 953],
                [1168, 1079],
                [773, 1079]
            ]
        ],
        [
            [
                [895, 738], 
                [1023, 741], 
                [1070, 845], 
                [865, 845]
            ],
            [
                [865, 845], 
                [1070, 845], 
                [1181, 1078], 
                [799, 1078]
            ]    
            
        ]
    ]

for lane in lanes_details.keys():
    # lane = str(lane)
    roi_coordinates[str(lane)]={}
    # for stage in lanes_details[lane]:
    #     if stage == "360":
    #         roi_coordinates[str(lane)][stage] = {}

brake_roi_coordinates = {}
for lane in lanes_details.keys():
    # lane = str(lane)
    brake_roi_coordinates[str(lane)]={}

top_360_roi_coordinates = {}
for lane in lanes_details.keys():
    # lane = str(lane)
    top_360_roi_coordinates[str(lane)]={}
