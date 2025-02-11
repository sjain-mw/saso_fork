#!/usr/bin/env python3

################################################################################
# SPDX-FileCopyrightText: Copyright (c) 2020-2023 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################


import gi
import configparser
gi.require_version('Gst', '1.0')
from gi.repository import GLib, Gst
import pyds
import sys
sys.path.append('../')
sys.path.append('/opt/nvidia/deepstream/deepstream-6.4/sources/deepstream_python_apps/apps')
from common.is_aarch_64 import is_aarch64
from common.bus_call import bus_call
from ctypes import *
import time
import sys
import math
import platform
import re
import uuid
import requests
import random
import json
from datetime import datetime
import pprint
pp = pprint.PrettyPrinter(indent=4)
import cv2
from json import dumps
import numpy as np

import utils
import common_vars
import entry_exit_utils
import check_utils
import lane_stage_utils
from kafka import KafkaProducer

from common.is_aarch_64 import is_aarch64
from common.bus_call import bus_call
from common.FPS import GETFPS
from logger_config import common_logger

import merge_configs
import argparse
import os

# Intialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

lane_stage_utils.create_lane_stage_dictionary()
lane_stage_utils.create_dictionary_360_check()
lane_stage_utils.create_dictionary_brake_check()

# if common_vars.TEST:
fps_streams = {}
folder_name = os.getenv("images_folder")

# nvanlytics_src_pad_buffer_probe  will extract metadata received on nvtiler sink pad
# and update params for drawing rectangle, object information etc.
def nvanalytics_src_pad_buffer_probe(pad,info,u_data):
    
    msg_frame_meta = []
    gst_buffer = info.get_buffer()
    if not gst_buffer:
        common_logger.debug("Unable to get GstBuffer ")
        return

    # Retrieve batch metadata from the gst_buffer
    # Note that pyds.gst_buffer_get_nvds_batch_meta() expects the
    # C address of gst_buffer as input, which is obtained with hash(gst_buffer)
    batch_meta = pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))
    l_frame = batch_meta.frame_meta_list
    while l_frame:
        try:
            # Note that l_frame.data needs a cast to pyds.NvDsFrameMeta
            # The casting is done by pyds.NvDsFrameMeta.cast()
            # The casting also keeps ownership of the underlying memory
            # in the C code, so the Python garbage collector will leave
            # it alone.
            frame_meta = pyds.NvDsFrameMeta.cast(l_frame.data)
        except StopIteration:
            break
        
        lp_objects = []
        tyre_objects = []
        vehicle_points=[]
        vehicle_points_360 = []
        stage=""
        lane_no=0
        operator_xyxy= []
        operator_ids=[]
        frontal_tyre=False
        
        frame_number=frame_meta.frame_num
        l_obj=frame_meta.obj_meta_list
        if int(frame_meta.pad_index)==0:
            print("Frame number ",frame_number)
        # Get meta data from NvDsAnalyticsFrameMeta
        l_user = frame_meta.frame_user_meta_list

        

        # # For every 60th frame send the health check status for each camera
        # if frame_number%60==0:
        #     now = datetime.now()
        #     message_data = {"status": "ok", "timestamp":now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], "source_id":str(frame_meta.pad_index)}
        #     producer.send('health-check', value=message_data)

        while l_user:
            try:
                user_meta = pyds.NvDsUserMeta.cast(l_user.data)
                
                if user_meta.base_meta.meta_type == pyds.nvds_get_user_meta_type("NVIDIA.DSANALYTICSFRAME.USER_META"):
                    user_meta_data = pyds.NvDsAnalyticsFrameMeta.cast(user_meta.user_meta_data)
                    stage ,lane_no = list(user_meta_data.objInROIcnt.keys())[0].split("_")[1:]
                    common_vars.lane_stage_details[lane_no][stage]["vehicle_in_roi"]=[]

                    if common_vars.DEBUG:
                        common_logger.debug(f"Stage {stage} Lane no {lane_no}")

            except StopIteration:
                break
            try:
                l_user = l_user.next
            except StopIteration:
                break
            
        is_saved = False

        while l_obj:
            try: 
                # Note that l_obj.data needs a cast to pyds.NvDsObjectMeta
                # The casting is done by pyds.NvDsObjectMeta.cast()
                obj_meta=pyds.NvDsObjectMeta.cast(l_obj.data)
                
            except StopIteration:
                break

            l_user_meta = obj_meta.obj_user_meta_list
            # point_in_polygon = utils.is_point_inside_polygon([obj_meta.rect_params.left+obj_meta.rect_params.width/2 ,\
            #                                     obj_meta.rect_params.top+obj_meta.rect_params.height/2],\
            #                                     common_vars.top_360_roi_coordinates[lane_no])
            # stage_name = common_vars.input_config[str(frame_meta.pad_index)]["name"]
            # print(f"Check vehicle points { point_in_polygon} {common_vars.roi_coordinates[lane_no]} {stage_name}")
            if utils.is_point_inside_polygon([obj_meta.rect_params.left+obj_meta.rect_params.width/2 ,\
                                                obj_meta.rect_params.top+obj_meta.rect_params.height/2],\
                                                common_vars.top_360_roi_coordinates[lane_no]) and \
                                                 (obj_meta.class_id == common_vars.PGIE_CLASS_ID_VEHICLE or \
                                                obj_meta.class_id == common_vars.PGIE_CLASS_ID_TRUCK or \
                                                obj_meta.class_id == common_vars.PGIE_CLASS_ID_TOP_CAR or \
                                                obj_meta.class_id == common_vars.PGIE_CLASS_ID_TOP_TRUCK) and \
                                                "top_360" in common_vars.input_config[str(frame_meta.pad_index)]["name"] : 
                print("Appending vehicle points ",len(vehicle_points_360) , " ",stage)
                vehicle_points_360.append([obj_meta.rect_params.top,obj_meta.rect_params.left, \
                                obj_meta.rect_params.width,obj_meta.rect_params.height])

            # if obj_meta.class_id == common_vars.PGIE_CLASS_ID_VEHICLE or obj_meta.class_id == common_vars.PGIE_CLASS_ID_TRUCK or obj_meta.class_id == common_vars.PGIE_CLASS_ID_TOP_CAR or obj_meta.class_id == common_vars.PGIE_CLASS_ID_TOP_TRUCK and frame_number % 5==0 and not is_saved:
            #     n_frame = pyds.get_nvds_buf_surface(hash(gst_buffer), frame_meta.batch_id)
            #     frame_copy = np.array(n_frame, copy=True, order='C')
            #     frame_copy = cv2.cvtColor(frame_copy, cv2.COLOR_RGBA2BGRA)

            #     folder_path = f"{folder_name}/lane_{lane_no}_{stage}/"
            #     os.makedirs(folder_path, exist_ok=True)

            #     img_path = f"{folder_path}/frame_{frame_number}_{datetime.now()}_{str(uuid.uuid4())}.jpg"
            #     cv2.imwrite(img_path, frame_copy)
            #     is_saved = True
            # Extract object level meta data from NvDsAnalyticsObjInfo
            if obj_meta.class_id == common_vars.PGIE_CLASS_ID_VEHICLE or \
                obj_meta.class_id == common_vars.PGIE_CLASS_ID_TRUCK or \
                obj_meta.class_id == common_vars.PGIE_CLASS_ID_TOP_CAR or \
                obj_meta.class_id == common_vars.PGIE_CLASS_ID_TOP_TRUCK or \
                obj_meta.class_id == common_vars.PGIE_CLASS_ID_PERSON or \
                obj_meta.class_id == common_vars.PGIE_CLASS_ID_UB_OPERATOR :
                if "entry_exit_360_update" in common_vars.input_config[str(frame_meta.pad_index)]["functionName"] :

                    if int(lane_no) in [1,2,3,4]:
                        coordinates_pos  = "bottom"
                    else:
                        coordinates_pos  = "centre"
                    
                    eval(common_vars.input_config[str(frame_meta.pad_index)]["functionName"]+"(user_meta_data,obj_meta,lane_no,stage,frame_meta.pad_index,coordinates_pos)")
                if "top_360" in common_vars.input_config[str(frame_meta.pad_index)]["name"] :
                    if obj_meta.class_id == common_vars.PGIE_CLASS_ID_PERSON or obj_meta.class_id == common_vars.PGIE_CLASS_ID_NON_OPERATOR  :
                    
                        operator_xyxy.append([obj_meta.rect_params.left , \
                                                obj_meta.rect_params.top ,\
                                                obj_meta.rect_params.left+obj_meta.rect_params.width ,\
                                                obj_meta.rect_params.top+obj_meta.rect_params.height])
                        operator_ids.append(obj_meta.object_id)
                    
                if "entry_exit_brake_update" in common_vars.input_config[str(frame_meta.pad_index)]["functionName"] :
                    
                    eval(common_vars.input_config[str(frame_meta.pad_index)]["functionName"]+"(user_meta_data,obj_meta,lane_no,stage,frame_meta.pad_index)")
                        
                    
            if obj_meta.class_id == common_vars.PGIE_CLASS_ID_LP_ENGLISH :
                lp_objects.append(obj_meta)
                if common_vars.DEBUG:
                    common_logger.debug(f"sgie stage {stage} {obj_meta.classifier_meta_list}")

            point_in_polygon=False

            if "brake" in common_vars.input_config[str(frame_meta.pad_index)]["name"]  and int(lane_no) in [1,2,3,4]:
                point_in_polygon = utils.is_point_inside_polygon([obj_meta.rect_params.left+obj_meta.rect_params.width/2 ,\
                                                obj_meta.rect_params.top+obj_meta.rect_params.height-100],\
                                                common_vars.brake_roi_coordinates[lane_no])
            if "top_360" in common_vars.input_config[str(frame_meta.pad_index)]["name"]:
                point_in_polygon = utils.is_point_inside_polygon([obj_meta.rect_params.left+obj_meta.rect_params.width/2 ,\
                                                obj_meta.rect_params.top+obj_meta.rect_params.height/2],\
                                                common_vars.top_360_roi_coordinates[lane_no])

            # Append the bbox data only if point is in polygon
            if point_in_polygon or obj_meta.class_id == common_vars.PGIE_CLASS_ID_LP_ENGLISH or\
             ("top_360" in common_vars.input_config[str(frame_meta.pad_index)]["name"] and (obj_meta.class_id == common_vars.PGIE_CLASS_ID_PERSON or obj_meta.class_id == common_vars.PGIE_CLASS_ID_NON_OPERATOR)) :
                now = datetime.now()
                msg_meta = {}
                msg_meta["class_id"]=obj_meta.class_id
                msg_meta["bbox_top"]=  obj_meta.rect_params.top
                msg_meta["bbox_left"] =  obj_meta.rect_params.left
                msg_meta["bbox_width"] = obj_meta.rect_params.width
                msg_meta["bbox_height"] = obj_meta.rect_params.height
                msg_meta["timestamp"] = now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                msg_frame_meta.append(msg_meta)
            
            while l_user_meta:
                try:
                    
                    user_meta = pyds.NvDsUserMeta.cast(l_user_meta.data)
                    

                    if user_meta.base_meta.meta_type == pyds.nvds_get_user_meta_type("NVIDIA.DSANALYTICSOBJ.USER_META"):   
                                 
                        user_meta_data = pyds.NvDsAnalyticsObjInfo.cast(user_meta.user_meta_data)
                        # Sppendinf objects that satisfy nvanalytics conditions
                        now = datetime.now()
                        msg_meta = {}
                        msg_meta["class_id"]=obj_meta.class_id
                        msg_meta["bbox_top"]=  obj_meta.rect_params.top
                        msg_meta["bbox_left"] =  obj_meta.rect_params.left
                        msg_meta["bbox_width"] = obj_meta.rect_params.width
                        msg_meta["bbox_height"] = obj_meta.rect_params.height
                        msg_meta["timestamp"] = now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        msg_frame_meta.append(msg_meta)
                        # Calling functions for appropritate actions to be taken according to stream id
                        
                        if obj_meta.class_id == common_vars.PGIE_CLASS_ID_VEHICLE or \
                            obj_meta.class_id == common_vars.PGIE_CLASS_ID_TRUCK or \
                            obj_meta.class_id == common_vars.PGIE_CLASS_ID_TOP_CAR or \
                            obj_meta.class_id == common_vars.PGIE_CLASS_ID_TOP_TRUCK or \
                            obj_meta.class_id == common_vars.PGIE_CLASS_ID_PERSON or \
                            obj_meta.class_id == common_vars.PGIE_CLASS_ID_CARBON_WIRE or \
                            obj_meta.class_id == common_vars.PGIE_CLASS_ID_UB_OPERATOR :
                            # if common_vars.input_config[str(frame_meta.pad_index)]["functionName"]!="":
                            if common_vars.input_config[str(frame_meta.pad_index)]["functionName"]!="" and not "entry_exit_360_update" in common_vars.input_config[str(frame_meta.pad_index)]["functionName"]:
                                if common_vars.DEBUG:
                                    common_logger.debug("Calling the function")
                                
                                eval(common_vars.input_config[str(frame_meta.pad_index)]["functionName"]+"(user_meta_data,obj_meta,lane_no,stage,frame_meta.pad_index)")
                        if user_meta_data.roiStatus and \
                            (obj_meta.class_id == common_vars.PGIE_CLASS_ID_VEHICLE or obj_meta.class_id == common_vars.PGIE_CLASS_ID_TRUCK):
                            # now = datetime.now()
                            # print("Object {0} roi status: {1}".format(obj_meta.object_id, user_meta_data.roiStatus))
                            # msg_meta = {}
                            # msg_meta["class_id"]=obj_meta.class_id
                            # msg_meta["bbox_top"]=  obj_meta.rect_params.top
                            # msg_meta["bbox_left"] =  obj_meta.rect_params.left
                            # msg_meta["bbox_width"] = obj_meta.rect_params.width
                            # msg_meta["bbox_height"] = obj_meta.rect_params.height
                            # msg_meta["cam_id"] = common_vars.input_config[str(frame_meta.pad_index)]["cameraId"]
                            # msg_meta["timestamp"] = now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                            # msg_frame_meta.append(msg_meta)
                            frontal_tyre = not any("front" in key for key in user_meta_data.roiStatus)
                            vehicle_points.append([obj_meta.rect_params.top,obj_meta.rect_params.left, \
                                obj_meta.rect_params.width,obj_meta.rect_params.height,frontal_tyre])
                            vehicle_points.append(user_meta_data.roiStatus)  
                            
                        
                        if obj_meta.class_id == common_vars.PGIE_CLASS_ID_TYRE:
                            tyre_objects.append((obj_meta,user_meta_data))
                            
                        
                except StopIteration:
                    break
                try:
                    l_user_meta = l_user_meta.next
                except StopIteration:
                    break
            try: 
                l_obj=l_obj.next
            except StopIteration:
                break
        # if common_vars.TEST:
        if True:
            # Get frame rate through this probe
            fps_streams[f"stream_{frame_meta.pad_index}"].get_fps()
        
        entry_exit_utils.update_exit_in_empty_roi(frame_meta.pad_index,lane_no,stage)
            
        if common_vars.DEBUG:
            common_logger.debug(f"No of vehicles {len(vehicle_points)}")
            common_logger.debug(f"No of license plates {len(lp_objects)}")
            
        # find the lpr data to the corresponding vehicle in the roi and update common_vars.lane_stage_details
        check_utils.decode_lpr_data(lp_objects,vehicle_points,lane_no,stage)
                                        
        # check whether break check has happened and update lane_stage_details
        if "break" in common_vars.input_config[str(frame_meta.pad_index)]["name"]:
            check_utils.break_check(vehicle_points,tyre_objects,frontal_tyre,lane_no,stage)
        
        # check whether 360 check has happened and update lane_stage_details
        if "top_360" in common_vars.input_config[str(frame_meta.pad_index)]["name"]:
             if int(lane_no) in [1,2,3,4]:
                print("cAlling 360 chcek")
                check_utils.check_360_truck(vehicle_points_360,operator_xyxy,lane_no,stage)
                pass
             else:
                check_utils.check_360(vehicle_points_360,operator_xyxy,operator_ids,lane_no,stage)

        # Update frame rate through this probe
        # stream_index = "stream{0}".format(frame_meta.pad_index)
        # global perf_data
        # perf_data.update_fps(stream_index)

        # send bounding box details at frame level
        bbox_metadata = {}
        bbox_metadata["useCaseType"] = "boundingBox"
        now = datetime.now()
        bbox_metadata["timestamp"] = now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        bbox_metadata["boundingBoxes"] = msg_frame_meta
        bbox_metadata["frameHeight"] = common_vars.STREAM_HEIGHT
        bbox_metadata["frameWidth"] = common_vars.STREAM_WIDTH
        bbox_metadata["cam_id"] = common_vars.input_config[str(frame_meta.pad_index)]["cameraId"]
        
        if len(msg_frame_meta) > 0:
            producer.send('testingsaso-v2', value=bbox_metadata)
        if common_vars.DEBUG:
            common_logger.debug(f"bbox data sent{bbox_metadata}")
        msg_frame_meta=[]
        try:
            l_frame=l_frame.next
        except StopIteration:
            break

    return Gst.PadProbeReturn.OK

def sgie_sink_probe(pad, info, udata):

    gst_buffer = info.get_buffer()
    # Retrieve batch metadata from the gst_buffer
    # Note that pyds.gst_buffer_get_nvds_batch_meta() expects the
    # C address of gst_buffer as input, which is obtained with hash(gst_buffer)

    batch_meta = pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))
    l_frame = batch_meta.frame_meta_list
    while l_frame is not None:
        try:
            # Note that l_frame.data needs a cast to pyds.NvDsFrameMeta
            # The casting is done by pyds.NvDsFrameMeta.cast()
            # The casting also keeps ownership of the underlying memory
            # in the C code, so the Python garbage collector will leave
            # it alone.
            frame_meta = pyds.NvDsFrameMeta.cast(l_frame.data)
        except StopIteration:
            break

        # Iterate through object metadata (bounding boxes and object details)
        l_obj = frame_meta.obj_meta_list
        while l_obj:
            try:
                obj_meta = pyds.NvDsObjectMeta.cast(l_obj.data)
            except StopIteration:
                break

            # If the object's class ID matches the class to be skipped
            if obj_meta.class_id == common_vars.PGIE_CLASS_ID_LP_ENGLISH:
                stream_id = frame_meta.pad_index
                cam_type = common_vars.input_config[str(stream_id)]["type"]
                if cam_type != "lpr":
                    print(f"Removing bbox for class ID {common_vars.PGIE_CLASS_ID_LP_ENGLISH} on stream ID {frame_meta.pad_index}")
                    # Remove the object (and its bbox) from the frame
                    pyds.nvds_remove_obj_meta_from_frame(frame_meta, obj_meta)

            try:
                l_obj = l_obj.next
            except StopIteration:
                break

        try:
            l_frame = l_frame.next
        except StopIteration:
            break
        
    # Otherwise, allow the buffer to proceed to SGIE
    return Gst.PadProbeReturn.OK



def pgie_sink_pad_buffer_probe(pad, info, u_data):

    gst_buffer = info.get_buffer()
    if not gst_buffer:
        common_logger.debug("Unable to get GstBuffer ")
        return
    
    batch_meta = pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))
    l_frame = batch_meta.frame_meta_list
    while l_frame is not None:
        try:
            frame_meta = pyds.NvDsFrameMeta.cast(l_frame.data)
        except StopIteration:
            break

        l_obj = frame_meta.obj_meta_list
        obj_meta_list = []
        while l_obj is not None:
            try:
                obj_meta = pyds.NvDsObjectMeta.cast(l_obj.data)
            except StopIteration:
                break

            # If the object's class ID matches the class to be skipped
            if obj_meta.class_id == common_vars.PGIE_CLASS_ID_LP_ENGLISH:
                stream_id = frame_meta.pad_index
                if common_vars.input_config[str(stream_id)]["type"] == "lpr":
                    # Get the bounding box coordinates
                    rect_params = obj_meta.rect_params
                    x, y, w, h = rect_params.left, rect_params.top, rect_params.width, rect_params.height
                    
                    # Warp and extend the bounding box
                    # image_width, image_height = frame_meta.source_frame_width, frame_meta.source_frame_height
                    x, y, w, h = x-10,y,w+20,h
                    
                    # Update the bounding box coordinates in metadata
                    rect_params.left = x
                    rect_params.top = y
                    rect_params.width = w
                    rect_params.height = h
                else:
                    print(f"Removing bbox for class ID {common_vars.PGIE_CLASS_ID_LP_ENGLISH} on stream ID {frame_meta.pad_index}")
                    # Remove the object (and its bbox) from the frame
                    # pyds.nvds_remove_obj_meta_from_frame(frame_meta, obj_meta)
                    obj_meta_list.append(obj_meta)
                
            try:
                l_obj = l_obj.next
            except StopIteration:
                break
        # Remove all object meta to avoid drawing. Do this outside while since we're modifying list
        for obj_meta in obj_meta_list:
            # Remove this to avoid drawing label texts
            pyds.nvds_remove_obj_meta_from_frame(frame_meta, obj_meta)
        obj_meta_list = None

        try:
            l_frame = l_frame.next
        except StopIteration:
            break

    return Gst.PadProbeReturn.OK

def cb_newpad(decodebin, decoder_src_pad,data):
    
    if common_vars.DEBUG:
        common_logger.debug("In cb_newpad\n")
        
    caps=decoder_src_pad.get_current_caps()
    gststruct=caps.get_structure(0)
    gstname=gststruct.get_name()
    source_bin=data
    features=caps.get_features(0)

    # Need to check if the pad created by the decodebin is for video and not
    # audio.
    if common_vars.DEBUG:
        common_logger.debug(f"gstname={gstname}")
    if(gstname.find("video")!=-1):
        # Link the decodebin pad only if decodebin has picked nvidia
        # decoder plugin nvdec_*. We do this by checking if the pad caps contain
        # NVMM memory features.
        if common_vars.DEBUG:
            common_logger.debug(f"features={features}")
        if features.contains("memory:NVMM"):
            # Get the source bin ghost pad
            bin_ghost_pad=source_bin.get_static_pad("src")
            if not bin_ghost_pad.set_target(decoder_src_pad):
                sys.stderr.write("Failed to link decoder src pad to source bin ghost pad\n")
        else:
            sys.stderr.write(" Error: Decodebin did not pick nvidia decoder plugin.\n")

def new_cb_newpad(decodebin, decoder_src_pad, data):
    source_bin = data
    bin_ghost_pad = source_bin.get_static_pad("src")
    if not bin_ghost_pad.set_target(decoder_src_pad):
                common_logger.critical("Failed to link decoder src pad to source bin ghost pad")
                sys.stderr.write("Failed to link decoder src pad to source bin ghost pad\n")
    
 
 
def decodebin_child_added(child_proxy, Object, name, user_data):
    print("Decodebin child added:", name, "\n")
    if name.find("decodebin") != -1:
        Object.connect("child-added", decodebin_child_added, user_data)
    #if is_aarch64() and name.find("nvv4l2decoder") != -1:
    #    print("Seting bufapi_version\n")
    #    Object.set_property("bufapi-version", True)
 
 
def create_source_bin(index, uri):
    print("Creating source bin")
 
    # Create a source GstBin to abstract this bin's content from the rest of the
    # pipeline
    bin_name = "source-bin-%02d" % index
    print(bin_name)
    nbin = Gst.Bin.new(bin_name)
    if not nbin:
        sys.stderr.write(" Unable to create source bin \n")
        common_logger.error("Unable to create source bin")
 
    # Source element for reading from the uri.
    # We will use decodebin and let it figure out the container format of the
    # stream and the codec and plug the appropriate demux and decode plugins.
    uri_decode_bin = Gst.ElementFactory.make("nvurisrcbin", "uri-decode-bin")
    
    if not uri_decode_bin:
        sys.stderr.write(" Unable to create uri decode bin \n")
    # We set the input uri to the source element
    uri_decode_bin.set_property("uri", uri)
    # uri_decode_bin.set_property("select-rtp-protocol", 4)
    uri_decode_bin.set_property("rtsp-reconnect-interval", 120)
    uri_decode_bin.set_property("rtsp-reconnect-attempts", -1)
    uri_decode_bin.set_property("cudadec-memtype", 2)
    uri_decode_bin.set_property("drop-frame-interval", 6)
 
    # if "source" in bin_name:
    #     source_element = child_proxy.get_by_name("source")
    #     if source_element.find_property('drop-on-latency') != None:
    #         Object.set_property("drop-on-latency", True)

 
    # uri_decode_bin.set_property("rtsp-reconnect-attempts", -1)
    # uri_decode_bin.set_property("rtsp-reconnect-interval-sec", 60)
    # Connect to the "pad-added" signal of the decodebin which generates a
    # callback once a new pad for raw data has beed created by the decodebin
    uri_decode_bin.connect("pad-added", new_cb_newpad, nbin)
    uri_decode_bin.connect("child-added", decodebin_child_added, nbin)
 
    # We need to create a ghost pad for the source bin which will act as a proxy
    # for the video decoder src pad. The ghost pad will not have a target right
    # now. Once the decode bin creates the video decoder and generates the
    # cb_newpad callback, we will set the ghost pad target to the video decoder
    # src pad.
    Gst.Bin.add(nbin, uri_decode_bin)
    bin_pad = nbin.add_pad(Gst.GhostPad.new_no_target("src", Gst.PadDirection.SRC))
    if not bin_pad:
        sys.stderr.write(" Failed to add ghost pad in source bin \n")
        common_logger.error("Failed to add ghost pad in source bin")
        return None
    return nbin

def make_element(element_name, i):
    """
    Creates a Gstreamer element with unique name
    Unique name is created by adding element type and index e.g. `element_name-i`
    Unique name is essential for all the element in pipeline otherwise gstreamer will throw exception.
    :param element_name: The name of the element to create
    :param i: the index of the element in the pipeline
    :return: A Gst.Element object
    """
    element = Gst.ElementFactory.make(element_name, element_name)
    if not element:
        sys.stderr.write(" Unable to create {0}".format(element_name))
    element.set_property("name", "{0}-{1}".format(element_name, str(i)))
    return element

def osd_sink_pad_buffer_probe(pad, info,u_data):
    
    frame_number = 0

    gst_buffer = info.get_buffer()
    if not gst_buffer:
        common_logger.debug("Unable to get GstBuffer ")
        return

    # Retrieve batch metadata from the gst_buffer
    # Note that pyds.gst_buffer_get_nvds_batch_meta() expects the
    # C address of gst_buffer as input, which is obtained with hash(gst_buffer)
    batch_meta = pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))
    l_frame = batch_meta.frame_meta_list

    while l_frame is not None:
        try:
            # Note that l_frame.data needs a cast to pyds.NvDsFrameMeta
            # The casting also keeps ownership of the underlying memory
            # in the C code, so the Python garbage collector will leave
            # it alone.
            frame_meta = pyds.NvDsFrameMeta.cast(l_frame.data)
        except StopIteration:
            break
        
        # print("Batch is",str(frame_meta.batch_id))
        frame_number = frame_meta.frame_num
        num_rects = frame_meta.num_obj_meta
        l_obj = frame_meta.obj_meta_list

        while l_obj is not None:
            try:
                
                # Casting l_obj.data to pyds.NvDsObjectMeta
                obj_meta = pyds.NvDsObjectMeta.cast(l_obj.data)
            
                # if "top" in lanes_stream_id.get(str(frame_meta.pad_index)) and (obj_meta.class_id==PGIE_CLASS_ID_PERSON):
                #     obj_meta.rect_params.border_width=0
                #     obj_meta.text_params.display_text=""
                #     obj_meta.text_params.set_bg_clr=0
                    
                # if "bottom" in lanes_stream_id.get(str(frame_meta.pad_index)) and obj_meta.class_id!=PGIE_CLASS_ID_PERSON:
                #     obj_meta.rect_params.border_width=0
                #     obj_meta.text_params.display_text=""
                #     obj_meta.text_params.set_bg_clr=0
                    
            except StopIteration:
                break

            try:
                l_obj = l_obj.next
            except StopIteration:
                break

        # Acquiring a display meta object. The memory ownership remains in
        # the C code so downstream plugins can still access it. Otherwise
        # the garbage collector will claim it when this probe function exits.
        display_meta=pyds.nvds_acquire_display_meta_from_pool(batch_meta)
        display_meta.num_labels = 1
        py_nvosd_text_params = display_meta.text_params[0]
        
        # Setting display text to be shown on screen
        # Note that the pyds module allocates a buffer for the string, and the
        # memory will not be claimed by the garbage collector.
        # Reading the display_text field here will return the C address of the
        # allocated string. Use pyds.get_string() to get the string content.
        # py_nvosd_text_params.display_text = "Frame Number={} Number of Objects={} Vehicle_count={} Person_count={}\nEmployee Checking {}".format(frame_number, num_rects, obj_counter[PGIE_CLASS_ID_VEHICLE], obj_counter[PGIE_CLASS_ID_PERSON],employee_checking )
        
        stage,lane_no = common_vars.input_config[str(frame_meta.pad_index)]["name"].split("_")[1:]
        py_nvosd_text_params.display_text ="Frame Number={} \nLane Status = {}\nEntry time={}\n Check status={}\nVehicle id={}" \
            .format(frame_number,common_vars.lane_stage_details[lane_no][stage]["STATUS"],\
            common_vars.lane_stage_details[lane_no][stage]["entry_time"], \
            common_vars.lane_stage_details[lane_no][stage]["checking_status"], \
            common_vars.lane_stage_details[lane_no][stage]["vehicle_id"])
        
        # Now set the offsets where the string should appear
        py_nvosd_text_params.x_offset = 10
        py_nvosd_text_params.y_offset = 12

        # Font , font-color and font-size
        py_nvosd_text_params.font_params.font_name = "Serif"
        py_nvosd_text_params.font_params.font_size = 20
        # set(red, green, blue, alpha); set to White
        py_nvosd_text_params.font_params.font_color.set(1.0, 1.0, 1.0, 1.0)

        # Text background color
        py_nvosd_text_params.set_bg_clr = 1
        # set(red, green, blue, alpha); set to Black
        py_nvosd_text_params.text_bg_clr.set(0.0, 0.0, 0.0, 1.0)
        # Using pyds.get_string() to get display_text as string
        # print(pyds.get_string(py_nvosd_text_params.display_text))
        pyds.nvds_add_display_meta_to_frame(frame_meta, display_meta)

        try:
            l_frame = l_frame.next
        except StopIteration:
            break

    return Gst.PadProbeReturn.OK


def main(args):

    # Check input arguments
    # if len(args) < 2:
    #     sys.stderr.write("usage: %s <uri1> [uri2] ... [uriN]\n" % args[0])
    #     sys.exit(1)
    
    # # ################################################################################
    # #merging configs
    parser = argparse.ArgumentParser()
    parser.add_argument('--lanes', type=merge_configs.parse_comma_separated_ints, default=[1,2] ,help='Comma-separated list of integers')
    # parser.add_argument('--output','-o', type=str, default='output.txt', help='output config file path')
    args = parser.parse_args()
    lanes = args.lanes
    merge_configs.run_merge_configs(common_vars.saso_config_directory,
                                    common_vars.input_config_directory,
                                    lanes,
                                    common_vars.file_path,
                                    common_vars.input_config_file
                                    )
    
    file =open(common_vars.input_config_file) 
    common_vars.input_config = json.load(file)

    for key in common_vars.input_config.keys():
        if "bottom_360" in common_vars.input_config[key]["name"]:
            
            lane = common_vars.input_config[key]["name"].split("_")[-1]
            common_vars.roi_coordinates[str(lane)]=utils.parse_coordinates_from_config(common_vars.file_path, \
                                    'roi-filtering-stream-'+str(key),'roi-lpr_360_'+str(lane))


        if "top_break" in common_vars.input_config[key]["name"]:
            
            lane = common_vars.input_config[key]["name"].split("_")[-1]
            common_vars.brake_roi_coordinates[str(lane)]=utils.parse_coordinates_from_config(common_vars.file_path, \
                                    'roi-filtering-stream-'+str(key),'roi-lane_break_'+str(lane))

        if "top_360" in common_vars.input_config[key]["name"]:
            
            lane = common_vars.input_config[key]["name"].split("_")[-1]
            common_vars.top_360_roi_coordinates[str(lane)]=utils.parse_coordinates_from_config(common_vars.file_path, \
                                    'roi-filtering-stream-'+str(key),'roi-lane_360_'+str(lane))
        
    # # ################################################################################
        common_vars.lane_id[key] = common_vars.input_config[key]["laneId"]
        
    
    args = []
    args.append("streams_uri")
    # input_config_path  = os.getenv()

    for key in common_vars.input_config.keys():
        #print("Keys ",key)
        args.append(common_vars.input_config[key]["uri"])
    # global perf_data
    # perf_data = PERF_DATA(len(args) - 1)
    number_sources=len(args)-1

    # Standard GStreamer initialization
    Gst.init(None)

    # Create gstreamer elements */
    # Create Pipeline element that will form a connection of other elements
    common_logger.debug("Creating Pipeline \n ")
    pipeline = Gst.Pipeline()
    is_live = True

    if not pipeline:
        sys.stderr.write(" Unable to create Pipeline \n")
    common_logger.debug("Creating streamux \n ")

    # Create nvstreammux instance to form batches from one or more sources.
    streammux = Gst.ElementFactory.make("nvstreammux", "Stream-muxer")
    if not streammux:
        sys.stderr.write(" Unable to create NvStreamMux \n")

    pipeline.add(streammux)
    
    # if common_vars.TEST:
    if True:
        for i in range(0,number_sources):
            fps_streams[f"stream_{i}"] = GETFPS(i)

    for i in range(number_sources):
        common_logger.debug("Creating source_bin ",str(i)," \n ")
        uri_name=args[i+1] 
        if uri_name.find("rtsp://") == 0 :
            is_live = True
        source_bin=create_source_bin(i, uri_name)
        if not source_bin:
            sys.stderr.write("Unable to create source bin \n")
        pipeline.add(source_bin)
        padname="sink_%u" %i
        sinkpad= streammux.get_request_pad(padname) 
        if not sinkpad:
            sys.stderr.write("Unable to create sink pad bin \n")
        srcpad=source_bin.get_static_pad("src")
        if not srcpad:
            sys.stderr.write("Unable to create src pad bin \n")
        srcpad.link(sinkpad)

    queue1=Gst.ElementFactory.make("queue","queue1")
    queue2=Gst.ElementFactory.make("queue","queue2")
    queue3=Gst.ElementFactory.make("queue","queue3")
    queue4=Gst.ElementFactory.make("queue","queue4")
    queue5=Gst.ElementFactory.make("queue","queue5")
    
    pipeline.add(queue1)
    pipeline.add(queue2)
    pipeline.add(queue3)
    pipeline.add(queue4)
    pipeline.add(queue5)

    common_logger.debug("Creating Pgie \n ")
    pgie = Gst.ElementFactory.make("nvinfer", "primary-inference")
    if not pgie:
        sys.stderr.write(" Unable to create pgie \n")

    sgie = Gst.ElementFactory.make("nvinfer", "secondary-nvinference-engine")
    if not sgie:
        sys.stderr.write(" Unable to make sgie \n")

    common_logger.debug("Creating nvtracker \n ")
    tracker = Gst.ElementFactory.make("nvtracker", "tracker")
    if not tracker:
        sys.stderr.write(" Unable to create tracker \n")

    common_logger.debug("Creating nvdsanalytics \n ")
    nvanalytics = Gst.ElementFactory.make("nvdsanalytics", "analytics")
    if not nvanalytics:
        sys.stderr.write(" Unable to create nvanalytics \n")
    nvanalytics.set_property("config-file", common_vars.file_path)

    if is_live:
        common_logger.debug("Atleast one of the sources is live")
        streammux.set_property('live-source', 1)

    streammux.set_property('width',common_vars.STREAM_WIDTH)
    streammux.set_property('height', common_vars.STREAM_HEIGHT)
    streammux.set_property('batch-size', number_sources)
    streammux.set_property('batched-push-timeout', common_vars.MUXER_BATCH_TIMEOUT_USEC)

    pgie.set_property('config-file-path', "../configs/config_infer_yoloV8.txt")
    pgie_batch_size=pgie.get_property("batch-size")

    sgie.set_property('config-file-path', "../configs/lpr_config_sgie_us.txt") 
    sgie.set_property('process-mode', 2)

    if(pgie_batch_size != number_sources):
        common_logger.debug("WARNING: Overriding infer-config batch-size",pgie_batch_size," with number of sources ", number_sources," \n")
        pgie.set_property("batch-size",number_sources)
    
    #Set properties of tracker
    config = configparser.ConfigParser()
    config.read('../configs/dsnvanalytics_tracker_config.txt')
    config.sections()

    for key in config['tracker']:
        if key == 'tracker-width' :
            tracker_width = config.getint('tracker', key)
            tracker.set_property('tracker-width', tracker_width)
        if key == 'tracker-height' :
            tracker_height = config.getint('tracker', key)
            tracker.set_property('tracker-height', tracker_height)
        if key == 'gpu-id' :
            tracker_gpu_id = config.getint('tracker', key)
            tracker.set_property('gpu_id', tracker_gpu_id)
        if key == 'll-lib-file' :
            tracker_ll_lib_file = config.get('tracker', key)
            tracker.set_property('ll-lib-file', tracker_ll_lib_file)
        if key == 'll-config-file' :
            tracker_ll_config_file = config.get('tracker', key)
            tracker.set_property('ll-config-file', tracker_ll_config_file)
    if not is_aarch64():
        # Use CUDA unified memory in the pipeline so frames
        # can be easily accessed on CPU in Python.
        mem_type = int(pyds.NVBUF_MEM_CUDA_UNIFIED)
        streammux.set_property("nvbuf-memory-type", mem_type)
        # nvvidconv.set_property("nvbuf-memory-type", mem_type)
        # nvvidconv1.set_property("nvbuf-memory-type", mem_type)


    nvvidconv1 = Gst.ElementFactory.make("nvvideoconvert", "convertor1")
    if not nvvidconv1:
        sys.stderr.write(" Unable to create nvvidconv1 \n")
        ai_stream_logger.critical("Unable to create nvvidconv1")
    print("Creating filter1 \n ")
    caps1 = Gst.Caps.from_string("video/x-raw(memory:NVMM), format=RGBA")
    filter1 = Gst.ElementFactory.make("capsfilter", "filter1")
    if not filter1:
        sys.stderr.write(" Unable to get the caps filter1 \n")
        ai_stream_logger.critical("Unable to get the caps filter1")
    filter1.set_property("caps", caps1)

    mem_type = int(pyds.NVBUF_MEM_CUDA_UNIFIED)
    nvvidconv1.set_property("nvbuf-memory-type", mem_type)

    common_logger.debug("Adding elements to Pipeline \n")
    pipeline.add(pgie)
    pipeline.add(tracker)
    pipeline.add(nvanalytics)
    pipeline.add(sgie)
    pipeline.add(nvvidconv1)
    pipeline.add(filter1)


    common_logger.debug("Linking elements in the Pipeline \n")
    streammux.link(queue1)
    queue1.link(pgie)
    pgie.link(queue2)
    queue2.link(tracker)
    tracker.link(queue3)
    queue3.link(sgie)
    sgie.link(queue4)
    queue4.link(nvvidconv1)
    nvvidconv1.link(filter1)
    filter1.link(nvanalytics)
    nvanalytics.link(queue5)


    if common_vars.TEST:
        common_logger.debug("Creating nvstreamdemux \n ")
        nvstreamdemux = Gst.ElementFactory.make("nvstreamdemux", "nvstreamdemux")
        if not nvstreamdemux:
            sys.stderr.write(" Unable to create nvstreamdemux \n")

        pipeline.add(nvstreamdemux)
        queue5.link(nvstreamdemux)
        #########################################################################################################
        for i in range(number_sources):

            # creating queue
            queue = make_element("queue", i)
            pipeline.add(queue) # 

            common_logger.debug("Creating nvvidconv \n ")
            nvvidconv = make_element("nvvideoconvert", i+1)
            if not nvvidconv:
                common_logger.debug("nvvidcon not created",i)
                sys.stderr.write(" Unable to create nvvidconv \n")
                
            if not is_aarch64():
                # Use CUDA unified memory in the pipeline so frames
                # can be easily accessed on CPU in Python.
                mem_type = int(pyds.NVBUF_MEM_CUDA_UNIFIED)
                nvvidconv.set_property("nvbuf-memory-type", mem_type)

            pipeline.add(nvvidconv)

            common_logger.debug("Creating nvosd \n ")
            nvosd = make_element("nvdsosd", i)
            if not nvosd:
                sys.stderr.write(" Unable to create nvosd \n")
            # nvosd.set_property('process-mode',common_vars.OSD_PROCESS_MODE)
            nvosd.set_property('display-text',common_vars.OSD_DISPLAY_TEXT)
            pipeline.add(nvosd)

            # if i < 4 or i == 10:

            #     # Make converter for post NVOSD
            #     nvvidconv_postosd = make_element("nvvideoconvert", i+13)
            #     if not nvvidconv_postosd:
            #         sys.stderr.write(" Unable to create nvvidconv_postosd \n")

            #     # Create a caps filter
            #     caps = make_element("capsfilter", i)
            #     caps.set_property(
            #         "caps", Gst.Caps.from_string("video/x-raw(memory:NVMM), format=I420")
            #     )

            #     # Make the encoder
            #     if codec == "H264":
            #         encoder = make_element("nvv4l2h264enc", i)
            #         print("Creating H264 Encoder")
            #     elif codec == "H265":
            #         encoder = make_element("nvv4l2h265enc", i)
            #         print("Creating H265 Encoder")
            #     if not encoder:
            #         sys.stderr.write(" Unable to create encoder")
            #     encoder.set_property("bitrate", bitrate)
            #     if is_aarch64():
            #         encoder.set_property("preset-level", 1)
            #         encoder.set_property("insert-sps-pps", 1)
            #         #encoder.set_property("bufapi-version", 1)

            #     # ------------- code added for the hls sink ------------------------------
            #     h264parse = make_element("h264parse", i)
            #     if not h264parse:
            #         sys.stderr.write(" Unable to create h264parse")
            #     else:
            #         print("Creating the h264 Parser")
                
            #     mpegtsmux = make_element("mpegtsmux", i)
            #     if not mpegtsmux:
            #         sys.stderr.write(" Unable to create mpegtsmux")
            #     else:
            #         print("Creating the mpegmuxer")
                
            #     hlssink = make_element("hlssink", i)
            #     if not hlssink:
            #         sys.stderr.write(" Unable to create hlssink")
            #     else:
            #         print("Creating the hlssink")

            #     hlssink.set_property("target-duration",5)
            #     hlssink.set_property("location","/opt/nvidia/deepstream/deepstream-6.4/sources/deepstream_python_apps/apps/deepstream-nvdsanalytics/kfca/output/"+ts_file_stage_id_map[i]+"/"+ts_file_cam_id_map[i]+"/"+f"hlssink%02d.ts")
            #     hlssink.set_property("playlist-location", "/opt/nvidia/deepstream/deepstream-6.4/sources/deepstream_python_apps/apps/deepstream-nvdsanalytics/kfca/output/"+ts_file_stage_id_map[i]+"/"+ts_file_cam_id_map[i]+"/"+"output_file.m3u8")
            #     hlssink.set_property("max-files", 15)
            #     # hlssink.set_property("playlist-length",10)
                
            #     pipeline.add(nvvidconv_postosd)
            #     pipeline.add(caps)
            #     pipeline.add(encoder)
            #     pipeline.add(h264parse)
            #     pipeline.add(mpegtsmux)
            #     pipeline.add(hlssink)

            #     # connect nvstreamdemux -> queue
            #     padname = "src_%u" % i
            #     demuxsrcpad = nvstreamdemux.get_request_pad(padname)
            #     if not demuxsrcpad:
            #         sys.stderr.write("Unable to create demux src pad \n")

            #     queuesinkpad = queue.get_static_pad("sink")
            #     if not queuesinkpad:
            #         sys.stderr.write("Unable to create queue sink pad \n")
            #     demuxsrcpad.link(queuesinkpad)
            #     queue.link(nvvidconv)
            #     nvvidconv.link(nvosd)
            #     # ---------------------------------
            #     nvosd.link(nvvidconv_postosd)
            #     nvvidconv_postosd.link(caps)
            #     caps.link(encoder)
            #     encoder.link(h264parse)
            #     h264parse.link(mpegtsmux)
            #     mpegtsmux.link(hlssink)

            # else:

            # connect nvstreamdemux -> queue
            padname = "src_%u" % i
            demuxsrcpad = nvstreamdemux.get_request_pad(padname)
            if not demuxsrcpad:
                sys.stderr.write("Unable to create demux src pad \n")
            queuesinkpad = queue.get_static_pad("sink")
            if not queuesinkpad:
                sys.stderr.write("Unable to create queue sink pad \n")
            demuxsrcpad.link(queuesinkpad)
            queue.link(nvvidconv)
            nvvidconv.link(nvosd)



            common_logger.debug("Creating Fakesink \n")
            # sink =make_element("fakesink", i)
            # sink.set_property('enable-last-sample', 0)
            # sink.set_property('sync', 0)
            sink = Gst.ElementFactory.make("nveglglessink", "nvvideo-renderer"+str(i))
            sink.set_property('sync', 0)
            sink.set_property('enable-last-sample', 0)
            
            pipeline.add(sink)
            # queue5=make_element("queue",i+13)
            # pipeline.add(queue5)

            nvosd.link(sink)
            osdsinkpad = nvosd.get_static_pad("sink")
            if not osdsinkpad:
                sys.stderr.write(" Unable to get sink pad of nvosd \n")

            osdsinkpad.add_probe(Gst.PadProbeType.BUFFER, osd_sink_pad_buffer_probe, None) 

    else:

        # queue5.link(nvstreamdemux)
        common_logger.debug("Creating Fakesink \n")
        sink =make_element("fakesink", "fake-sink")
        sink.set_property('enable-last-sample', 1)
        # sink.set_property('sync', 0)
        # sink = Gst.ElementFactory.make("nveglglessink", "nvvideo-renderer"+str(i))
        sink.set_property('sync', 0)
        
        pipeline.add(sink)
        queue5.link(sink)
    # #########################################################################################################
    # for i in range(number_sources):

    #     # creating queue
    #     queue = make_element("queue", i)
    #     pipeline.add(queue) # 

    #     print("Creating nvvidconv \n ")
    #     nvvidconv = make_element("nvvideoconvert", i+1)
    #     if not nvvidconv:
    #         print("nvvidcon not created",i)
    #         sys.stderr.write(" Unable to create nvvidconv \n")
            
    #     if not is_aarch64():
    #         # Use CUDA unified memory in the pipeline so frames
    #         # can be easily accessed on CPU in Python.
    #         mem_type = int(pyds.NVBUF_MEM_CUDA_UNIFIED)

    #         nvvidconv.set_property("nvbuf-memory-type", mem_type)

    #     pipeline.add(nvvidconv)

    #     print("Creating nvosd \n ")
    #     nvosd = make_element("nvdsosd", i)
    #     if not nvosd:
    #         sys.stderr.write(" Unable to create nvosd \n")
    #     # nvosd.set_property('process-mode',common_vars.OSD_PROCESS_MODE)
    #     nvosd.set_property('display-text',common_vars.OSD_DISPLAY_TEXT)
    #     pipeline.add(nvosd)

    #     # connect nvstreamdemux -> queue
    #     padname = "src_%u" % i
    #     demuxsrcpad = nvstreamdemux.get_request_pad(padname)
    #     if not demuxsrcpad:
    #         sys.stderr.write("Unable to create demux src pad \n")
    #     queuesinkpad = queue.get_static_pad("sink")
    #     if not queuesinkpad:
    #         sys.stderr.write("Unable to create queue sink pad \n")
    #     demuxsrcpad.link(queuesinkpad)
    #     queue.link(nvvidconv)
    #     nvvidconv.link(nvosd)


    #     print("Creating Fakesink \n")
    #     sink =make_element("fakesink", i)
    #     sink.set_property('enable-last-sample', 1)
    #     # sink.set_property('sync', 0)
    #     # sink = Gst.ElementFactory.make("nveglglessink", "nvvideo-renderer"+str(i))
    #     sink.set_property('sync', 0)
        
    #     pipeline.add(sink)
    #     nvosd.link(sink)
    #     # ----------------------------------------------------------
    #     # Lets add probe to get informed of the meta data generated, we add probe to
    #     # the sink pad of the osd element, since by that time, the buffer would have
    #     # had got all the metadata.
    #     osdsinkpad = nvosd.get_static_pad("sink")
    #     if not osdsinkpad:
    #         sys.stderr.write(" Unable to get sink pad of nvosd \n")

    #     osdsinkpad.add_probe(Gst.PadProbeType.BUFFER, osd_sink_pad_buffer_probe, None) 
    ##########################################################################################################
    ##########################################################################################################
    # create an event loop and feed gstreamer bus mesages to it
    loop = GLib.MainLoop()
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect ("message", bus_call, loop)
    # ----------------------------------------------------
    nvanalytics_src_pad=nvanalytics.get_static_pad("src")
    if not nvanalytics_src_pad:
        sys.stderr.write(" Unable to get src pad \n")
    else:
        nvanalytics_src_pad.add_probe(Gst.PadProbeType.BUFFER, nvanalytics_src_pad_buffer_probe, 0)

    # Add probe to preprocess PGIE results
    pgie_src_pad = pgie.get_static_pad("src")
    pgie_src_pad.add_probe(Gst.PadProbeType.BUFFER, pgie_sink_pad_buffer_probe, 0)

    # Add probe to SGIE results
    # Add the probe to the SGIE sink pad
    # sgie_sink_pad = sgie.get_static_pad("sink")
    # sgie_sink_pad.add_probe(Gst.PadProbeType.BUFFER, sgie_sink_probe, None)

    # List the sources
    common_logger.debug("Now playing...")
    for i, source in enumerate(args):
        if (i != 0):
            common_logger.debug(f"{i}, :  {source}")
    
    common_logger.debug("Starting pipeline \n")
    # start play back and listed to events		
    pipeline.set_state(Gst.State.PLAYING)
    try:
        loop.run()
    except:
        pass
    # cleanup


    common_logger.debug(f"Entry count {common_vars.entry_count}")
    common_logger.debug(f"Exit count {common_vars.exit_count}")
    common_logger.debug(f"Exiting app\n")
    pipeline.set_state(Gst.State.NULL)
    # for otpt_cap in output_video_caps:
    #     otpt_cap.release()

if __name__ == '__main__':
    sys.exit(main(sys.argv))
