import os
import re
import cv2
import numpy as np
import configparser
import json
import argparse

def parse_comma_separated_ints(value):
    try:
        return [int(item) for item in value.split(',')]
    except ValueError:
        raise argparse.ArgumentTypeError("Comma-separated integers are required.")

def extract_number(file_name):
    # This will extract the number from the filename, assuming it's at the end before the extension
    match = re.search(r'lane(\d+)', file_name)
    return int(match.group(1)) if match else float('inf')

def find_corr_input_config(lane_number, input_configs):
    try:
        for input_config in input_configs:
            # Extract lane number from the input_config filename
            input_lane_number = int(input_config.split('_')[-1].split('.')[0][4:])
            if input_lane_number == lane_number:
                return input_config
    except (ValueError, IndexError) as e:
        print(f"Error finding corresponding input config for lane {lane_number}: {e}")
    return None

def select_configs_to_merge(lanes, saso_configs, input_configs):
    saso_configs_to_merge = []
    input_configs_to_merge = []

    for file in saso_configs:
        try:
            lane_number = int(file.split('_')[-1].split('.')[0][4:])
            # print(f'Lane Number = {lane_number}')
            if lane_number in lanes:
                print(f'Lane {lane_number} usable')
                saso_config = configparser.ConfigParser()
                # Read the config file
                saso_config.read(file)
                saso_configs_to_merge.append(saso_config)
                
                # Find corresponding input config
                corr_input_config = find_corr_input_config(lane_number, input_configs)
                
                # Only append if a corresponding input config was found
                if corr_input_config:
                    input_configs_to_merge.append(corr_input_config)
                else:
                    print(f"No corresponding input config found for lane {lane_number}.")
        except (ValueError, IndexError) as e:
            print(f"Error processing SASO config file {file}: {e}")
            
    return saso_configs_to_merge, input_configs_to_merge

def parse_points(point_str):
    points = [int(p.strip()) for p in point_str.split(';') if p.strip()]
    return [(points[i], points[i+1]) for i in range(0, len(points), 2)]
    

def draw_polygon(frame, points, color=(0, 255, 0), thickness=4):
    pts = np.array(points, np.int32)

    if len(points) == 2:
        # print(tuple(pts))
        cv2.line(frame, tuple(pts[0]), tuple(pts[1]), color=color, thickness=thickness)
    else:
        pts = pts.reshape((-1, 1, 2))
        cv2.polylines(frame, [pts], isClosed=True, color=(0, 255, 255), thickness=thickness)
    

def get_frame(rtsp_url):
    try:
        cap = cv2.VideoCapture(rtsp_url)
        
        if not cap.isOpened():
            print(f"Error: Cannot open video stream : {rtsp_url}")
            exit()

        frame_count = 0
        while frame_count < 5:
            ret, frame = cap.read()
            if not ret:
                print("Error: Cannot read frame from video stream")
                break
            frame_count += 1
        
        cap.release()
        print(frame.shape)
        frame = cv2.resize(frame,(3840,2160))
        return frame

    except Exception as e:
        print(f"An error occurred: {e}")
        return False
    # return True
    
saso_config_path = '/home/saso_exit1/saso-cv/configs/saso_test_configs'
input_config_path = '/home/saso_exit1/saso-cv/configs/input_configs'
parser = argparse.ArgumentParser()
parser.add_argument('--lanes', type=parse_comma_separated_ints, default=[1,2,3,4,5,6,7,8,9,10] ,help='Comma-separated list of integers')
args = parser.parse_args()
lanes = args.lanes
# print(lanes)

all_saso_configs = os.listdir(saso_config_path)
all_input_configs = os.listdir(input_config_path)

saso_lane_configs = [os.path.join(saso_config_path, file) for file in all_saso_configs if re.search(r'lane\d+\.txt$', file)]
input_lane_configs = [os.path.join(input_config_path, file) for file in all_input_configs if re.search(r'lane\d+\.json$', file)]

# saso_config_main_entrance = [os.path.join(saso_config_path, file) for file in all_saso_configs if 'main_entrance' in file][0]
# input_config_main_entrance = [os.path.join(input_config_path, file) for file in all_input_configs if 'main_entrance' in file][0]

saso_configs = sorted(saso_lane_configs, key=extract_number)
input_configs = sorted(input_lane_configs, key=extract_number)

saso_configs_to_merge, input_configs_to_merge = select_configs_to_merge(lanes, saso_configs, input_configs)

output_dir = f'roi_images/{str(lanes)}'

# Check if the directory exists, if not, create it
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# print(input_configs_to_merge)

for i, input_config in enumerate(input_configs_to_merge):
    saso_config = saso_configs_to_merge[i]
    
    with open(input_config, 'r') as file:
        input_config_data = json.load(file)
        
    for key, value in input_config_data.items():
        try:
            frame = get_frame(value['uri'])
        except:
            continue
        print("-------------------------")
        print(f"{key} -> {value['uri']}")
        for section in saso_config.sections():
            if section.startswith('roi-filtering-stream-') or section.startswith('line-crossing-stream-'):
                # print("Code reached here")
                stream_number = int(section.split('-')[-1])
                if stream_number == int(key):
                    item = saso_config[section]
                    for option, val in item.items():
                        if option.startswith('roi'):
                            # print(f"{option} -> {val}")
                            points = parse_points(val)
                            # draw_polygon(frame, points)
                        elif option.startswith("line"):
                            values = [value.strip() for value in val.split(';')]
                            last_four_values = values[-4:]
                            first_four_values = values[:4]
                            result = ' ; '.join(last_four_values)
                            arrow =  ' ; '.join(first_four_values)
                            # print(f"{option} -> {result}")
                            points = parse_points(result)
                            arrow_points = parse_points(arrow)
                            # print(points)
                            # draw_polygon(frame, points)
                            # draw_polygon(frame, arrow_points)
                            
        filename = f"{output_dir}/{value['uri'].split('/')[-4].split('.')[-1]}.jpg"
        cv2.imwrite(filename, frame)
cv2.waitKey(0)
cv2.destroyAllWindows()
                        