import json
import os

# export LANE_TO_USE='lane1'
# export test=0
config = {}
with open('../configs/local_config.json', 'r') as file:
    config = json.load(file)

LANE_TO_USE = os.getenv("LANE_TO_USE") #config['LANES_TO_USE']

STREAMS_TO_USE = config['RTSP_LINKS'][LANE_TO_USE]
# for lane in LANES_TO_USE:
#     STREAMS_TO_USE.update(config['RTSP_LINKS'][lane])

# LANES_TO_USE2 = config['LANES_TO_USE2']
# STREAMS_TO_USE2 = {}
# for lane in LANES_TO_USE2:
#     STREAMS_TO_USE2.update(config['RTSP_LINKS'][lane])
# LANES_TO_USE2 = config['LANES_TO_USE2']
# STREAMS_TO_USE2 = {}
# for lane in LANES_TO_USE2:
#     STREAMS_TO_USE2.update(config['RTSP_LINKS'][lane])


KAFKA_PRODUCER_CONF = {
            'bootstrap.servers': 'localhost:9094',
            'client.id': 'logic_module'
        }
TEST_KAFKA_PRODUCER_CONF = {
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'test_logic_module'
        }

BBOXES_KAFKA_PRODUCER_CONF = {
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'bboxes'
        }

INFERENCE_KAFKA_PRODUCER_CONF = {
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'inference_module'
        }

KAFKA_CONSUMER_CONF = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'python-consumer',
            'auto.offset.reset': 'latest'
        }

INFERENCE_TOPIC = f'saso-inference-{LANE_TO_USE}'
CONSUMER_TOPICS = [f'saso-inference-{LANE_TO_USE}']
PLATFORM_PRODUCER_TOPIC = 'exit-17'
BBOXES_PRODUCER_TOPIC = 'bboxes'
TEST_PLATFORM_PRODUCER_TOPIC = 'saso-platform'



## Logic configurations
logic_config = {}
with open('../configs/logic_config.json', 'r') as file:
    logic_config = json.load(file)

CAMERA_DETAILS = logic_config['CAMERA_DETAILS']

ROIS = logic_config['ROIS']

max_length = 1000
UB_CHECK_FRAME_THRESHOLD = 5

# CONSTANT PARAMETERS (THRESHOLDS, ETC)
EXIT_THRESHOLD = 10
TEST_MODE = int(os.getenv('test'))
LPR_MATCHING_SECONDS =600
