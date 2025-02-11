import configparser
import os
import argparse
import json
import cv2
import re
import subprocess
from logger_config import common_logger

def parse_comma_separated_ints(value):
    try:
        return [int(item) for item in value.split(',')]
    except ValueError:
        raise argparse.ArgumentTypeError("Comma-separated integers are required.")

def extract_number(file_name):
    # This will extract the number from the filename, assuming it's at the end before the extension
    match = re.search(r'lane(\d+)', file_name)
    return int(match.group(1)) if match else float('inf')

def check_corresponding_configs(input_configs, saso_configs):
    
    # Extract lane numbers from input and SASO config files
    input_lanes = {extract_number(file): file for file in input_configs}
    saso_lanes = {extract_number(file): file for file in saso_configs}
    
    # Identify missing corresponding configs
    missing_in_saso = [lane for lane in input_lanes if lane not in saso_lanes]
    missing_in_input = [lane for lane in saso_lanes if lane not in input_lanes]
    
    if missing_in_saso or missing_in_input:
        error_message = ""
        if missing_in_saso:
            error_message += f"Missing corresponding SASO configs for input lanes: {missing_in_saso}\n"
        if missing_in_input:
            error_message += f"Missing corresponding input configs for SASO lanes: {missing_in_input}\n"
        
        # Raise an exception to stop execution
        raise Exception(f"Configuration Error:\n{error_message}")
     
    common_logger.debug("All input configs have corresponding SASO configs, and vice versa.")
    

def check_rtsp_stream(rtsp_url):
    try:
        # Open the RTSP stream
        cap = cv2.VideoCapture(rtsp_url)

        # Check if the stream is opened successfully
        if not cap.isOpened():
            common_logger.debug(f"Unable to open the stream.... {rtsp_url}")
            return False

        # Try to read the first frame
        ret, frame = cap.read()
        
        # Release the capture object
        cap.release()

        # If the frame was read successfully, the stream is running
        if ret:
            return True
        else:
            common_logger.debug(f"Failed to read a frame from the stream.... {rtsp_url}")
            return False

    except Exception as e:
        common_logger.debug(f"An error occurred: {e}")
        return False
    # return True

def reindex_sections(saso_config, start_index, running_sections):
    new_sections = {}
    for section in saso_config.sections():
        # print(section)
        if section.startswith('roi-filtering-stream-') or section.startswith('line-crossing-stream-'):
            # print("Code reached here")
            stream_number = int(section.split('-')[-1])
            
            new_stream_number = start_index + stream_number
            # print(f"saso old {stream_number} new {new_stream_number} start {start_index}")
            if new_stream_number in running_sections:
                new_section_name = section.rsplit('-', 1)[0] + f'-{running_sections.index(new_stream_number)}'
                new_sections[new_section_name] = saso_config[section]
    return new_sections, new_stream_number

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
            common_logger.error(f"Error processing SASO config file {file}: {e}")
            
    return saso_configs_to_merge, input_configs_to_merge

def reindex_input_config_sections(input_config, start_index, running_sections):
    new_sections = {}
    # urls = ['rtsp://admin:Q12345678q@192.168.0.124:554/Streaming/Channels/3601', 'rtsp://admin:Q12345678q@192.168.0.124:554/Streaming/Channels/4401']
    for section, data in input_config.items():
        rtsp_url = data['uri']
        if check_rtsp_stream(rtsp_url):
            # print(section)
            running_sections.append(start_index + int(section))
            new_section_number = str(running_sections.index(start_index + int(section)))
            # print("Running sections ",running_sections)
            # print(f"input old {section} new {new_section_number} start {start_index}")
            
            new_sections[new_section_number] = data
    return new_sections, running_sections

def merge_input_configs(input_configs_to_merge, input_config_main_entrance, output_file, running_sections):
    merged_input_config = {}
    if input_config_main_entrance is not None:
        with open(input_config_main_entrance, 'r') as file:
            main_entrance_data = json.load(file)
    max_stream = 0
    for input_config in input_configs_to_merge:
        with open(input_config, 'r') as file:
            data = json.load(file)
            new_sections, running_sections = reindex_input_config_sections(data, max_stream, running_sections)
            merged_input_config.update(new_sections)
            # Handle case when merged_input_config is empty
            if merged_input_config:
                max_stream += len(data.keys())
            else:
                max_stream = 0
    
    if input_config_main_entrance is not None:
        new_sections, running_sections = reindex_input_config_sections(main_entrance_data, max_stream, running_sections)
        merged_input_config.update(new_sections)

    # Write the merged config to the output JSON file
    with open(output_file, 'w') as jsonfile:
        json.dump(merged_input_config, jsonfile, indent=4)
    
    return running_sections
    

def merge_saso_configs(saso_configs_to_merge, saso_config_main_entrance, output_file, running_sections):
    merged_saso_config = configparser.ConfigParser()
    max_stream = 0
    property_dict = {}
    # output_file = 'output.txt'
    for section in saso_configs_to_merge[0].sections():
        if section == 'property':
            property_dict['property'] = saso_configs_to_merge[0]['property']
            merged_saso_config.read_dict(property_dict)
            break
    
    for saso_config in saso_configs_to_merge:
        # print(saso_config.sections())
        new_sections, new_stream_number = reindex_sections(saso_config, max_stream, running_sections)
        merged_saso_config.read_dict(new_sections)
        max_stream = new_stream_number + 1
        
    # Load and reindex the main entrance SASO config
    if saso_config_main_entrance is not None:
        main_entrance_saso_config = configparser.ConfigParser()
        main_entrance_saso_config.read(saso_config_main_entrance)
        new_sections, new_stream_number = reindex_sections(main_entrance_saso_config, max_stream, running_sections)
        merged_saso_config.read_dict(new_sections)
    
    # Write the merged config to the output file
    with open(output_file, 'w') as configfile:
        merged_saso_config.write(configfile)
        
def run_merge_configs(saso_config_path, input_config_path, lanes, saso_config_output_path, input_config_output_path):
    # saso_configs = [os.path.join(saso_config_path, file) for file in os.listdir(saso_config_path)]
    # input_configs = [os.path.join(input_config_path, file) for file in os.listdir(input_config_path)]
    all_saso_configs = os.listdir(saso_config_path)
    all_input_configs = os.listdir(input_config_path)

    saso_lane_configs = [os.path.join(saso_config_path, file) for file in all_saso_configs if re.search(r'lane\d+\.txt$', file)]
    input_lane_configs = [os.path.join(input_config_path, file) for file in all_input_configs if re.search(r'lane\d+\.json$', file)]

    try:
        saso_config_main_entrance = [os.path.join(saso_config_path, file) for file in all_saso_configs if 'main_entrance' in file][0]
        input_config_main_entrance = [os.path.join(input_config_path, file) for file in all_input_configs if 'main_entrance' in file][0]        
    except IndexError:
        print("No 'main_entrance' file found, skipping this step.")
        saso_config_main_entrance = None  # or handle the case in any other way
        input_config_main_entrance = None

    saso_configs = sorted(saso_lane_configs, key=extract_number)
    input_configs = sorted(input_lane_configs, key=extract_number)

    check_corresponding_configs(input_configs, saso_configs)
    
    running_sections = []
    saso_configs_to_merge, input_configs_to_merge = select_configs_to_merge(lanes, saso_configs, input_configs)
    running_sections = merge_input_configs(input_configs_to_merge, input_config_main_entrance, input_config_output_path, running_sections)
    common_logger.debug(f"Running sections: {running_sections}")
    merge_saso_configs(saso_configs_to_merge, saso_config_main_entrance, saso_config_output_path, running_sections)