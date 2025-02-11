import sys

sys.path.append('../')
import gi
import configparser

gi.require_version('Gst', '1.0')
gi.require_version('GstRtspServer', '1.0')
from gi.repository import GObject, Gst, GstRtspServer
from gi.repository import GLib
from ctypes import *

import sys
import math
from common.is_aarch_64 import is_aarch64
from common.bus_call import bus_call

from common.FPS import GETFPS
import numpy as np
import pyds
import cv2
import os
import os.path
import uuid


from confluent_kafka import Producer
import json

from datetime import datetime

from config import INFERENCE_KAFKA_PRODUCER_CONF, INFERENCE_TOPIC, STREAMS_TO_USE, \
    TEST_MODE, CAMERA_DETAILS, BBOXES_PRODUCER_TOPIC, BBOXES_KAFKA_PRODUCER_CONF

import logging
from logging.handlers import RotatingFileHandler
import os



# Create Kafka producer
producer = Producer(INFERENCE_KAFKA_PRODUCER_CONF)
platform_producer = Producer(BBOXES_KAFKA_PRODUCER_CONF)

# Function to send message to Kafka topic
def send_message(message):
    producer.produce(INFERENCE_TOPIC, value=json.dumps(message))
    producer.flush()
def send_message_bbox(message):
    platform_producer.produce(BBOXES_PRODUCER_TOPIC, value=json.dumps(message))
    platform_producer.flush()

index_to_camera_id = {}
fps_streams = {}
frame_count = {}
saved_count = {}
global PGIE_CLASS_ID_PERSON
PGIE_CLASS_ID_PERSON = 0
PGIE_CLASS_ID_BAG = 1
global PGIE_CLASS_ID_FACE
PGIE_CLASS_ID_FACE = 2

MAX_DISPLAY_LEN = 64

MUXER_OUTPUT_WIDTH = 1920
MUXER_OUTPUT_HEIGHT = 1080
MUXER_BATCH_TIMEOUT_USEC = 4000
TILED_OUTPUT_WIDTH = 1280  
TILED_OUTPUT_HEIGHT = 720
GST_CAPS_FEATURES_NVMM = "memory:NVMM"
pgie_classes_str = ["car", "operator", "lp_arabic", "lp_english", "carbon-wire", "non operator", "truck", "tyre", "ub_operator", "top_truck", "top_car", "carbon-wire-connected","top-carbon-wire","top-carbon-wire-connected","top-person"]

MIN_CONFIDENCE = 0.3
MAX_CONFIDENCE = 0.4
low_fps_count = 0
is_low_fps_mail_sent = False


# tiler_sink_pad_buffer_probe  will extract metadata received on tiler sink pad
# and update params for drawing rectangle, object information etc.
def tiler_sink_pad_buffer_probe(pad, info, u_data):
    # print('--------------------here--------------------')
    global low_fps_count, is_low_fps_mail_sent
    frame_number = 0
    num_rects = 0
    gst_buffer = info.get_buffer()
    if not gst_buffer:
        #print("Unable to get GstBuffer ")
        return

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

        frame_number = frame_meta.frame_num
        l_obj = frame_meta.obj_meta_list
        num_rects = frame_meta.num_obj_meta
    
        obj_counter = {
            PGIE_CLASS_ID_PERSON: 0,
            PGIE_CLASS_ID_BAG: 0,
            PGIE_CLASS_ID_FACE: 0
        }
        # n_frame = pyds.get_nvds_buf_surface(hash(gst_buffer), frame_meta.batch_id)
        bbox_info = {
            "car": [],
            "operator": [],
            "lp_arabic": [],
            "lp_english": [],
            "carbon-wire": [],
            "non operator": [],
            "truck": [],
            "tyre": [],
            "ub_operator": [],
            "top_truck": [],
            "top_car": [],
             "carbon-wire-connected":[],
            "top-carbon-wire":[],
            "top-carbon-wire-connected":[],
            "top-person":[]
        }
        display_platform_info = []
        camera_id = index_to_camera_id[frame_meta.pad_index]
        
        is_kafka_produce = False
        while l_obj is not None:
            try:
                # Casting l_obj.data to pyds.NvDsObjectMeta
                obj_meta = pyds.NvDsObjectMeta.cast(l_obj.data)
            except StopIteration:
                break
            # obj_counter[obj_meta.class_id] += 1

             # osd_rect_params =  pyds.NvOSD_RectParams.cast(obj_meta.rect_params)
             # Draw black patch to cover faces (class_id = 2), can change to other colors 
            if frame_count[f"stream_{camera_id}"] % 1 == 0:
                is_kafka_produce = True
                
                rect_params = obj_meta.rect_params
                top = int(rect_params.top)
                left = int(rect_params.left)
                width = int(rect_params.width)
                height = int(rect_params.height)
                obj_name = pgie_classes_str[obj_meta.class_id]

                track_id = obj_meta.object_id
                confidence = obj_meta.confidence

                if obj_meta.class_id == 3 and obj_meta.classifier_meta_list:
                    result_label = pyds.NvDsLabelInfo.cast(pyds.NvDsClassifierMeta.cast(obj_meta.classifier_meta_list.data).label_info_list.data).result_label
                    
                    # print(result_label)

                    bbox_info[obj_name].append([track_id, confidence, left, top, width, height,result_label])
                    
                else:   

                    bbox_info[obj_name].append([track_id, confidence, left, top, width, height])
                
                display_platform_info.append(
                    {"class_id":obj_meta.class_id,
                     "bbox_top": obj_meta.rect_params.top,
                     "bbox_left":obj_meta.rect_params.left,
                     "bbox_width": obj_meta.rect_params.width,
                     "bbox_height": obj_meta.rect_params.height,
                     "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                     }
                )


                obj_meta.rect_params.border_width = 1
                obj_meta.rect_params.has_bg_color = 0
                obj_meta.rect_params.bg_color.red = 0.0
                obj_meta.rect_params.bg_color.green = 0.0
                obj_meta.rect_params.bg_color.blue = 0.0
                obj_meta.rect_params.bg_color.alpha = 0.5
                
                

                   
            try:
                l_obj = l_obj.next
            except StopIteration:
                break

        if is_kafka_produce:
            kafka_producer(bbox_info, index_to_camera_id[frame_meta.pad_index], frame_number, display_platform_info)

        #print("Frame Number=", frame_number, "Number of Objects=", num_rects, "Face_count=",
        #      obj_counter[PGIE_CLASS_ID_FACE], "Person_count=", obj_counter[PGIE_CLASS_ID_PERSON], "Bag_count=", obj_counter[PGIE_CLASS_ID_BAG])
        # Get frame rate through this probe
        fps_streams[f"stream_{index_to_camera_id[frame_meta.pad_index]}"].get_fps()
        

        # if save_image:
        #     img_path = "{}/stream_{}/frame_{}.jpg".format(folder_name, frame_meta.pad_index, frame_number)
        #     cv2.imwrite(img_path, frame_copy)
        # saved_count[f"stream_{index_to_camera_id[frame_meta.pad_index]}"] += 1
        frame_count[f"stream_{index_to_camera_id[frame_meta.pad_index]}"] += 1

        try:
            l_frame = l_frame.next
        except StopIteration:
            break

    return Gst.PadProbeReturn.OK

# def crop_object(image, obj_meta):
#     rect_params = obj_meta.rect_params
#     top = max(0, int(rect_params.top) - 40)
#     left = max(0, int(rect_params.left) - 40)
#     width = max(0, int(rect_params.width) + 80)
#     height = max(0, int(rect_params.height) + 80)
#     obj_name = pgie_classes_str[obj_meta.class_id]

#     crop_img = image[top:top+height, left:left+width]
	
#     return crop_img

def kafka_producer(bbox_info, camera_id, frame_number, display_platform_info):
    # Get the current system time
    print('sent')
    now = datetime.now()
    current_time = now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    # Format the time in the desired format
    formatted_time = current_time

    message = {
        "camera_id": camera_id, 
        "timestamp": formatted_time,
        "bbox_info": bbox_info, 
        "frame_number": frame_number
    }

    send_message(message)

    bbox_metadata = {}
    bbox_metadata["useCaseType"] = "boundingBox"
    bbox_metadata["timestamp"] = formatted_time
    bbox_metadata["boundingBoxes"] = display_platform_info
    bbox_metadata["frameHeight"] = MUXER_OUTPUT_HEIGHT
    bbox_metadata["frameWidth"] = MUXER_OUTPUT_WIDTH
    bbox_metadata["cam_id"] = CAMERA_DETAILS[camera_id]['camera_uid']

    send_message_bbox(bbox_metadata)


def cb_newpad(decodebin, decoder_src_pad, data):
    print("In cb_newpad\n")
    caps = decoder_src_pad.get_current_caps()
    gststruct = caps.get_structure(0)
    gstname = gststruct.get_name()
    source_bin = data
    features = caps.get_features(0)

    # Need to check if the pad created by the decodebin is for video and not
    # audio.
    if (gstname.find("video") != -1):
        # Link the decodebin pad only if decodebin has picked nvidia
        # decoder plugin nvdec_*. We do this by checking if the pad caps contain
        # NVMM memory features.
        if features.contains("memory:NVMM"):
            # Get the source bin ghost pad
            bin_ghost_pad = source_bin.get_static_pad("src")
            if not bin_ghost_pad.set_target(decoder_src_pad):
                sys.stderr.write("Failed to link decoder src pad to source bin ghost pad\n")
        else:
            sys.stderr.write(" Error: Decodebin did not pick nvidia decoder plugin.\n")

def new_cb_newpad(decodebin, decoder_src_pad, data):
    source_bin = data
    bin_ghost_pad = source_bin.get_static_pad("src")
    if not bin_ghost_pad.set_target(decoder_src_pad):
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

    # Source element for reading from the uri.
    # We will use decodebin and let it figure out the container format of the
    # stream and the codec and plug the appropriate demux and decode plugins.
    uri_decode_bin = Gst.ElementFactory.make("nvurisrcbin", "uri-decode-bin")
    
    if not uri_decode_bin:
        sys.stderr.write(" Unable to create uri decode bin \n")
    # We set the input uri to the source element
    uri_decode_bin.set_property("uri", uri)
    uri_decode_bin.set_property("rtsp-reconnect-interval", 120)
    uri_decode_bin.set_property("rtsp-reconnect-attempts", -1)
    uri_decode_bin.set_property("cudadec-memtype", 2)
    uri_decode_bin.set_property("drop-frame-interval", 3)

    

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
        return None
    return nbin


def main(uri_inputs,codec,bitrate):
    global pipeline, streammux
    # Check input arguments
    number_sources = len(uri_inputs)
    for i in range(0, number_sources ):
        fps_streams[f"stream_{index_to_camera_id[i]}"] = GETFPS(i)
    

    # Standard GStreamer initialization
    GObject.threads_init()
    Gst.init(None)

    # Create gstreamer elements */
    # Create Pipeline element that will form a connection of other elements
    print("Creating Pipeline \n ")
    pipeline = Gst.Pipeline()

    if not pipeline:
        sys.stderr.write(" Unable to create Pipeline \n")
    print("Creating streamux \n ")

    # Create nvstreammux instance to form batches from one or more sources.
    streammux = Gst.ElementFactory.make("nvstreammux", "Stream-muxer")
    if not streammux:
        sys.stderr.write(" Unable to create NvStreamMux \n")

    pipeline.add(streammux)
    for i in range(number_sources):
        #os.mkdir(folder_name + "/stream_" + str(i))
        frame_count[f"stream_{index_to_camera_id[i]}"] = 0
        # saved_count[f"stream_{index_to_camera_id[i]}"] = 0
        print("Creating source_bin ", i, " \n ")
        uri_name = uri_inputs[i]
        if uri_name.find("rtsp://") == 0:
            is_live = True
        source_bin = create_source_bin(i, uri_name)
        if not source_bin:
            sys.stderr.write("Unable to create source bin \n")
        pipeline.add(source_bin)
        padname = "sink_%u" % i
        sinkpad = streammux.get_request_pad(padname)
        if not sinkpad:
            sys.stderr.write("Unable to create sink pad bin \n")
        srcpad = source_bin.get_static_pad("src")
        if not srcpad:
            sys.stderr.write("Unable to create src pad bin \n")
        srcpad.link(sinkpad)
    print("Creating Pgie \n ")
    pgie = Gst.ElementFactory.make("nvinfer", "primary-inference")
    if not pgie:
        sys.stderr.write(" Unable to create pgie \n")
    print("Creating nvtracker \n ")
    tracker = Gst.ElementFactory.make("nvtracker", "tracker")
    if not tracker:
        sys.stderr.write(" Unable to create tracker \n")

    # face_classifier = Gst.ElementFactory.make("nvinfer", "secondary-inference face_classifier")
    # if not face_classifier:
    #     sys.stderr.write(" Unable to create face_classifier \n")
    
    # Add nvvidconv1 and filter1 to convert the frames to RGBA
    # which is easier to work with in Python.
    print("Creating nvvidconv1 \n ")
    nvvidconv1 = Gst.ElementFactory.make("nvvideoconvert", "convertor1")
    if not nvvidconv1:
        sys.stderr.write(" Unable to create nvvidconv1 \n")
    print("Creating filter1 \n ")
    caps1 = Gst.Caps.from_string("video/x-raw(memory:NVMM), format=RGBA")
    filter1 = Gst.ElementFactory.make("capsfilter", "filter1")
    if not filter1:
        sys.stderr.write(" Unable to get the caps filter1 \n")
    filter1.set_property("caps", caps1)
    print("Creating tiler \n ")
    tiler = Gst.ElementFactory.make("nvmultistreamtiler", "nvtiler")
    if not tiler:
        sys.stderr.write(" Unable to create tiler \n")
    print("Creating nvvidconv \n ")
    nvvidconv = Gst.ElementFactory.make("nvvideoconvert", "convertor")
    if not nvvidconv:
        sys.stderr.write(" Unable to create nvvidconv \n")

    nvvidconv2 = Gst.ElementFactory.make("nvvideoconvert", "convertor2")
    if not nvvidconv2:
        sys.stderr.write(" Unable to create nvvidconv \n")


    print("Creating nvosd \n ")
    nvosd = Gst.ElementFactory.make("nvdsosd", "onscreendisplay")
    if not nvosd:
        sys.stderr.write(" Unable to create nvosd \n")
    nvosd.set_property("process-mode", 1)
    # nvvidconv_postosd = Gst.ElementFactory.make("nvvideoconvert", "convertor_postosd")
    # if not nvvidconv_postosd:
    #     sys.stderr.write(" Unable to create nvvidconv_postosd \n")
    
    # Create a caps filter
    # caps = Gst.ElementFactory.make("capsfilter", "filter")
    # caps.set_property("caps", Gst.Caps.from_string("video/x-raw(memory:NVMM), format=I420"))
    
    # Make the encoder
    if codec == "H264":
        encoder = Gst.ElementFactory.make("nvv4l2h264enc", "encoder")
        print("Creating H264 Encoder")
    elif codec == "H265":
        encoder = Gst.ElementFactory.make("nvv4l2h265enc", "encoder")
        print("Creating H265 Encoder")
    if not encoder:
        sys.stderr.write(" Unable to create encoder")
    encoder.set_property('bitrate', bitrate)
    if is_aarch64():
        encoder.set_property('preset-level', 1)
        encoder.set_property('insert-sps-pps', 1)
        encoder.set_property('bufapi-version', 1)
    
    # Make the payload-encode video into RTP packets
    if codec == "H264":
        rtppay = Gst.ElementFactory.make("rtph264pay", "rtppay")
        print("Creating H264 rtppay")
    elif codec == "H265":
        rtppay = Gst.ElementFactory.make("rtph265pay", "rtppay")
        print("Creating H265 rtppay")
    if not rtppay:
        sys.stderr.write(" Unable to create rtppay")
    
    # Make the UDP sink
    updsink_port_num = 5400
    # sink = Gst.ElementFactory.make("udpsink", "udpsink")
    # if not sink:
    #     sys.stderr.write(" Unable to create udpsink")
    
    # sink.set_property('host', '224.224.255.255')
    # sink.set_property('port', updsink_port_num)
    # sink.set_property('async', False)
    # sink.set_property('sync', 1)

    sink = Gst.ElementFactory.make("nveglglessink", "nveglglessink")
    sink.set_property("sync", 0)
    sink.set_property("qos",0)

    fake_sink = Gst.ElementFactory.make("fakesink", "fakesink")
    
    print("Playing file {} ".format(uri_inputs))
    
    streammux.set_property('width', MUXER_OUTPUT_WIDTH)
    streammux.set_property('height', MUXER_OUTPUT_HEIGHT)
    streammux.set_property('batch-size', number_sources)
    streammux.set_property('batched-push-timeout', 4000)
    #streammux.set_property("gpu-id", 1)
    pgie.set_property('config-file-path', "../configs/config_infer_yoloV8.txt")
    pgie_batch_size = pgie.get_property("batch-size")
    if (pgie_batch_size != number_sources):
        print("WARNING: Overriding infer-config batch-size", pgie_batch_size, " with number of sources ",
              number_sources, " \n")
        pgie.set_property("batch-size", number_sources)

    sgie = Gst.ElementFactory.make("nvinfer", "secondary-nvinference-engine")
    if not sgie:
        sys.stderr.write(" Unable to make sgie \n")
    sgie.set_property('config-file-path', "../configs/lpr_config_sgie_us.txt") 
    sgie.set_property('process-mode', 2)

    tiler_rows = int(math.sqrt(number_sources))
    tiler_columns = int(math.ceil((1.0 * number_sources) / tiler_rows))
    tiler.set_property("rows", tiler_rows)
    tiler.set_property("columns", tiler_columns)
    tiler.set_property("width", TILED_OUTPUT_WIDTH)
    tiler.set_property("height", TILED_OUTPUT_HEIGHT)

    #Set properties of tracker
    config = configparser.ConfigParser()
    config.read('../configs/tracker_config.txt')
    config.sections()

    for key in config['tracker']:
        if key == 'tracker-width':
            tracker_width = config.getint('tracker', key)
            tracker.set_property('tracker-width', tracker_width)
        if key == 'tracker-height':
            tracker_height = config.getint('tracker', key)
            tracker.set_property('tracker-height', tracker_height)
        if key == 'gpu-id':
            tracker_gpu_id = config.getint('tracker', key)
            tracker.set_property('gpu_id', tracker_gpu_id)
        if key == 'll-lib-file':
            tracker_ll_lib_file = config.get('tracker', key)
            tracker.set_property('ll-lib-file', tracker_ll_lib_file)
        if key == 'll-config-file':
            tracker_ll_config_file = config.get('tracker', key)
            tracker.set_property('ll-config-file', tracker_ll_config_file)


    if not is_aarch64():
        # Use CUDA unified memory in the pipeline so frames
        # can be easily accessed on CPU in Python.
        mem_type = int(pyds.NVBUF_MEM_CUDA_UNIFIED)
        streammux.set_property("nvbuf-memory-type", mem_type)
        nvvidconv.set_property("nvbuf-memory-type", mem_type)
        nvvidconv1.set_property("nvbuf-memory-type", mem_type)
        tiler.set_property("nvbuf-memory-type", mem_type)

    print("Adding elements to Pipeline \n")
    pipeline.add(pgie)
    pipeline.add(sgie)
    pipeline.add(tracker)


    print("Linking elements in the Pipeline \n")
    streammux.link(pgie)
    pgie.link(tracker)
    tracker.link(sgie)

    if TEST_MODE:
        pipeline.add(tiler)
        pipeline.add(nvvidconv)
        pipeline.add(nvosd)
        pipeline.add(sink)

        sgie.link(tiler)
        tiler.link(nvvidconv)
        nvvidconv.link(nvosd)
        nvosd.link(sink)
    else:
        pipeline.add(fake_sink)
        sgie.link(fake_sink)
    

    
    # create an event loop and feed gstreamer bus mesages to it
    loop = GObject.MainLoop()
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", bus_call, loop)
    
    # Start streaming
    # rtsp_port_num = 8554
    
    # server = GstRtspServer.RTSPServer.new()
    # server.props.service = "%d" % rtsp_port_num
    # server.attach(None)
    
    # factory = GstRtspServer.RTSPMediaFactory.new()
    # factory.set_launch( "( udpsrc name=pay0 port=%d buffer-size=524288 caps=\"application/x-rtp, media=video, clock-rate=90000, encoding-name=(string)%s, payload=96 \" )" % (updsink_port_num, codec))
    # factory.set_shared(True)
    # server.get_mount_points().add_factory("/inference-f", factory)
    
    # print("\n *** DeepStream: Launched RTSP Streaming at rtsp://localhost:%d/inference-f ***\n\n" % rtsp_port_num)
    if TEST_MODE:
        tiler_sink_pad = tiler.get_static_pad("sink")
        if not tiler_sink_pad:
            sys.stderr.write("Unable to get sink pad \n")
        else:
            tiler_sink_pad.add_probe(Gst.PadProbeType.BUFFER, tiler_sink_pad_buffer_probe, 0)
    else:
        fake_sink_sink_pad = fake_sink.get_static_pad("sink")
        if not fake_sink_sink_pad:
            sys.stderr.write("Unable to get sink pad \n")
        else:
            fake_sink_sink_pad.add_probe(Gst.PadProbeType.BUFFER, tiler_sink_pad_buffer_probe, 0)

    print("Starting pipeline \n")
    # start play back and listed to events		
    pipeline.set_state(Gst.State.PLAYING)
    try:
        loop.run()
    except:
        pass
    # cleanup
    print("Exiting app\n")
    pipeline.set_state(Gst.State.NULL)


if __name__ == '__main__':
    # send_email() # testing
    uri_inputs = []
    out_codec =  'H265'
    out_bitrate = 4000
    
    for index,camera_id in enumerate(STREAMS_TO_USE):
        uri_inputs.append(STREAMS_TO_USE[camera_id])
        index_to_camera_id[index] = camera_id

    sys.exit(main(uri_inputs, out_codec, out_bitrate ))
