from shapely.geometry import Polygon, LineString
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
import pprint
pp = pprint.PrettyPrinter(indent=4)
import cv2
import numpy as np
# from kafka import KafkaProducer


import common_vars

def convert_coordinates(vehicle_points):
    
    x1 = vehicle_points[1]
    y1 = vehicle_points[0]
    x2 = vehicle_points[1]+vehicle_points[2]
    y2 = vehicle_points[0]+vehicle_points[3]
    
    return x1, y1, x2, y2

def convert_grid_coordinates(grid_coord):
    
    for ind,grid in enumerate(grid_coord):
        for point_ind,point in enumerate(grid):
            grid_coord[ind][point_ind][0] = (point[0]/common_vars.CONFIG_WIDTH) * common_vars.STREAM_WIDTH
            grid_coord[ind][point_ind][1] = (point[1]/common_vars.CONFIG_HEIGHT) * common_vars.STREAM_HEIGHT
    
    return grid_coord

def convert_detection_points(vehicle_points):
    
    top_left = (vehicle_points[1], vehicle_points[0])
    top_right = (vehicle_points[1]+vehicle_points[2], vehicle_points[0])
    bottom_left = (vehicle_points[1], vehicle_points[0]+vehicle_points[3])
    bottom_right = (vehicle_points[1]+vehicle_points[2], vehicle_points[0]+vehicle_points[3])
    
    return [top_left,top_right,bottom_right,bottom_left]

def parse_coordinates_from_config(file_path, section_name,field_name):

    coordinates = []
    config = configparser.ConfigParser()
    config.read(file_path)
    print("File path ",file_path)
    # print(config["property"])
    for key,val in config.items():
        print(key, " ",val)
    common_vars.CONFIG_WIDTH = int(config["property"].get('config-width',''))
    common_vars.CONFIG_HEIGHT = int(config["property"].get('config-height',''))
    
    print("CONFIG DETAILS ",common_vars.CONFIG_HEIGHT," ",common_vars.CONFIG_WIDTH)
    
    if section_name in config:
        roi_value = config[section_name].get(field_name, '')
        split_roi = [int(val) for val in roi_value.split(";")]
        # print(split_roi)
        coordinates = [[(split_roi[i*2]/common_vars.CONFIG_WIDTH)*common_vars.STREAM_WIDTH, \
            (split_roi[(i*2)+1]/common_vars.CONFIG_HEIGHT)*common_vars.STREAM_HEIGHT] for i in range(int(len(split_roi)/2))]

    return coordinates


# def is_point_inside_polygon(x, y, polygon):
#     """
#     Check if a point (x, y) is inside a polygon.

#     :param x: x-coordinate of the point
#     :param y: y-coordinate of the point
#     :param polygon: list of (x, y) tuples representing the vertices of the polygon
#     :return: True if the point is inside the polygon, False otherwise
#     """
    
#     n = len(polygon)
#     inside = False
#     p1x, p1y = polygon[0]
    
#     for i in range(n + 1):
#         p2x, p2y = polygon[i % n]
#         if y > min(p1y, p2y) and y <= max(p1y, p2y) and x <= max(p1x, p2x):
#             if p1y != p2y:
#                 xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
#             if p1x == p2x or x <= xinters:
#                 inside = not inside
#         p1x, p1y = p2x, p2y

#     return inside



def is_point_inside_polygon(point, polygon):
  """
  Checks if a point is inside a polygon using ray casting.

  Args:
      point: A tuple representing the point coordinates (x, y).
      polygon: A list of tuples representing the polygon vertices.

  Returns:
      True if the point is inside the polygon, False otherwise.
  """
  
  count = 0
  
  for i in range(len(polygon)):
    start_x, start_y = polygon[i]
    end_x, end_y = polygon[(i + 1) % len(polygon)]  # Wrap around for last point
    # Check if the ray intersects with the edge
    if (start_y > point[1] >= end_y) or (start_y <= point[1] < end_y):
      # Check for x-axis intersection to avoid collinear points
      if (start_x - (end_x - start_x) * (start_y - point[1]) / (end_y - start_y)) < point[0]:
        count += 1
        
  return count % 2 == 1  # Odd intersections means inside


def rectangle_to_coordinates(x1, y1, x2, y2):
    """
    Convert a rectangle defined by top-left and bottom-right corners
    into a list of coordinates for all four corners.

    Parameters:
    - x1, y1: Coordinates of the top-left corner.
    - x2, y2: Coordinates of the bottom-right corner.

    Returns:
    - List of tuples, each representing a coordinate (x, y) of the rectangle's corners.
    """
    # Ensure the rectangle definition is correct
    if x1 > x2 or y1 > y2:
        raise ValueError("Invalid coordinates: Ensure that x1 <= x2 and y1 <= y2.")

    # Define all four corners
    top_left = (x1, y1)
    top_right = (x2, y1)
    bottom_right = (x2, y2)
    bottom_left = (x1, y2)

    # List of coordinates in clockwise order starting from the top-left
    coordinates = [top_left, top_right, bottom_right, bottom_left]

    return coordinates

def calculate_iou(coords_A, coords_B):
    """
    Calculate the Intersection over Union (IoU) of two polygons given their coordinates.
    
    Parameters:
    - coords_A: List of tuples, where each tuple contains the (x, y) coordinates of polygon A's vertices.
    - coords_B: List of tuples, where each tuple contains the (x, y) coordinates of polygon B's vertices.
    
    Returns:
    - iou: The Intersection over Union (IoU) of the two polygons.
    """
    # Create polygons from coordinates
    polygon_A = Polygon(coords_A)
    polygon_B = Polygon(coords_B)

    # Check if the polygons are valid
    if not polygon_A.is_valid or not polygon_B.is_valid:
        raise ValueError("One of the polygons is invalid. Please check the coordinates.")

    # Calculate the intersection and union areas
    intersection = polygon_A.intersection(polygon_B).area
    union = polygon_A.union(polygon_B).area

    # Calculate IoU
    iou = intersection / union if union != 0 else 0

    return iou

def check_polygon_intersection(coords_A, coords_B):
    """
    Check if two polygons defined by their coordinates intersect.
    
    Parameters:
    - coords_A: List of tuples, where each tuple contains the (x, y) coordinates of polygon A's vertices.
    - coords_B: List of tuples, where each tuple contains the (x, y) coordinates of polygon B's vertices.
    
    Returns:
    - bool: True if the polygons intersect, False otherwise.
    """
    # Create polygons from coordinates
    polygon_A = Polygon(coords_A)
    polygon_B = Polygon(coords_B)

    # Check if the polygons are valid
    if not polygon_A.is_valid or not polygon_B.is_valid:
        raise ValueError("One of the polygons is invalid. Please check the coordinates.")

    # Check for intersection
    return polygon_A.intersects(polygon_B)

def check_polygon_inside(inner_coords, outer_coords):
    """
    Check if the polygon defined by 'inner_coords' is completely inside the 
    polygon defined by 'outer_coords'.
    
    Parameters:
    inner_coords (list of tuples): Coordinates for the inner polygon.
    outer_coords (list of tuples): Coordinates for the outer polygon.
    
    Returns:
    bool: True if the inner polygon is inside the outer polygon, False otherwise.
    """
    # Create the polygon objects
    outer_polygon = Polygon(outer_coords)
    inner_polygon = Polygon(inner_coords)
    
    # Use the 'within' method to check if the inner polygon is inside the outer polygon
    return inner_polygon.within(outer_polygon)

def interpolate_points(p1, p2, divisions):
    """Interpolates points between two endpoints p1 and p2, including endpoints."""
    line = LineString([p1, p2])
    return [line.interpolate(float(i) / (divisions - 1), normalized=True) for i in range(divisions)]

def draw_grid(image, polygon_points, rows, columns):
    """Draws a grid on the image based on the provided polygon points with specified rows and columns, and prints the grid coordinates."""
    poly = Polygon(polygon_points)
    vertical_divisions = columns + 1
    horizontal_divisions = rows + 1
    grid_coordinates = []

    # Interpolate points for vertical divisions (along the top and bottom of the quadrilateral)
    points_top = interpolate_points(poly.exterior.coords[0], poly.exterior.coords[3], vertical_divisions)
    points_bottom = interpolate_points(poly.exterior.coords[1], poly.exterior.coords[2], vertical_divisions)

    # Prepare to draw grid lines by connecting corresponding interpolated points vertically and horizontally
    for column in range(columns):
        for row in range(rows):
            grid_cell = Polygon([
                points_top[column].coords[0], points_top[column + 1].coords[0],
                points_bottom[column + 1].coords[0], points_bottom[column].coords[0]
            ])

            # Interpolate horizontal points within this cell to create rows
            left_vertical = LineString([points_bottom[column].coords[0], points_top[column].coords[0]])
            right_vertical = LineString([points_bottom[column + 1].coords[0], points_top[column + 1].coords[0]])
            left_horizontal_points = interpolate_points(left_vertical.coords[0], left_vertical.coords[1], horizontal_divisions)
            right_horizontal_points = interpolate_points(right_vertical.coords[0], right_vertical.coords[1], horizontal_divisions)

            sub_grid_cell = Polygon([
                left_horizontal_points[row].coords[0], right_horizontal_points[row].coords[0],
                right_horizontal_points[row + 1].coords[0], left_horizontal_points[row + 1].coords[0]
            ])
            pts = np.array(sub_grid_cell.exterior.coords, np.int32)
            cv2.polylines(image, [pts], isClosed=True, color=(0, 255, 0), thickness=2)
            
            # Collecting grid cell coordinates
            grid_coordinates.append(list(sub_grid_cell.exterior.coords))

    return grid_coordinates


# def divide_bounding_box_with_all_corners(x_min, y_min, x_max, y_max, m, n):
#     grid_coordinates_div = []
#     width = x_max - x_min
#     height = y_max - y_min
    
#     cell_width = width / m
#     cell_height = height / n
    
#     for i in range(m):
#         for j in range(n):
#             if i == 0 or i == m-1 or j == 0 or j == n-1:
#                 top_left = (x_min + i * cell_width, y_min + j * cell_height)
#                 top_right = (top_left[0] + cell_width, top_left[1])
#                 bottom_right = (top_right[0], top_right[1] + cell_height)
#                 bottom_left = (top_left[0], top_left[1] + cell_height)
#                 grid_coordinates_div.append((top_left, top_right, bottom_right, bottom_left))
    
#     return grid_coordinates_div

# def divide_bounding_box_with_all_corners(x_min, y_min, x_max, y_max, m, n):
#     grid_coordinates_div = []
#     width = x_max - x_min
#     height = y_max - y_min
    
#     cell_width = width / m
#     cell_height = height / n
    
#     for i in range(m):
#         for j in range(n):
#             if (i == 0 or i == m-1 or j == 0) and not (j == n-1):
#                 top_left = (x_min + i * cell_width, y_min + j * cell_height)
#                 top_right = (top_left[0] + cell_width, top_left[1])
#                 bottom_right = (top_right[0], top_right[1] + cell_height)
#                 bottom_left = (top_left[0], top_left[1] + cell_height)
#                 grid_coordinates_div.append((top_left, top_right, bottom_right, bottom_left))
    
#     return grid_coordinates_div


def divide_bounding_box_with_all_corners(x_min, y_min, x_max, y_max, m, n, u_grid):
    grid_coordinates_div = []
    width = x_max - x_min
    height = y_max - y_min
    
    cell_width = width / m
    cell_height = height / n
    

    if u_grid:
        for i in range(m):
            for j in range(n):
                if (i == 0 or i == m-1 or j == n-1) and not (j == 0):
                    top_left = (x_min + i * cell_width, y_min + j * cell_height)
                    top_right = (top_left[0] + cell_width, top_left[1])
                    bottom_right = (top_right[0], top_right[1] + cell_height)
                    bottom_left = (top_left[0], top_left[1] + cell_height)
                    grid_coordinates_div.append((top_left, top_right, bottom_right, bottom_left))

    else:
        for i in range(m):
            for j in range(n):
                if (i == 0 or i == m-1 or j == 0) and not (j == n-1):
                    top_left = (x_min + i * cell_width, y_min + j * cell_height)
                    top_right = (top_left[0] + cell_width, top_left[1])
                    bottom_right = (top_right[0], top_right[1] + cell_height)
                    bottom_left = (top_left[0], top_left[1] + cell_height)
                    grid_coordinates_div.append((top_left, top_right, bottom_right, bottom_left))

    
    return grid_coordinates_div


def expand_rectangle(x1, y1, x2, y2, d):
    #print(x1, y1, x2, y2)
    # Expand the rectangle uniformly by d pixels
    # new_x1 = x1 - d - 40
    # new_y1 = y1-d -20
    # new_x2 = x2 + d+ 120
    # new_y2 = y2+d
    
    # Expand the rectangle uniformly by d pixels
    new_x1 = x1 - d - 80
    new_y1 = max(y1-d -50,0)
    new_x2 = x2 + d+ 80
    new_y2 = min(y2+d,common_vars.STREAM_HEIGHT)
    
    
    return new_x1, new_y1, new_x2, new_y2

# def check_360_status(car_coordinates, bboxes,ids , new_car, grid_check_threshold,status):
#     if new_car:
#         # print("new_car")
#         global operator_ids
#         global grid_coordinates
#         x1,y1,x2,y2 = car_coordinates
#         x1,y1,x2,y2 = expand_rectangle(x1, y1, x2, y2, 120)
#         grid_coordinates = divide_bounding_box_with_all_corners(x1,y1,x2,y2,3,3)
#         operator_ids = {}

#     for id in ids:
#         if id not in operator_ids:
#              operator_ids[id] = grid_coordinates[:]

#     for i, id in enumerate(ids):
#         temp_grid_coordinates = operator_ids[id]
#         x1, y1, x2, y2 = map(int, bboxes[i])
#         operator_cordinates = rectangle_to_coordinates(x1, y1, x2, y2)

#         # #debug
#         # if id==53:
#         #     print("coord:",operator_cordinates)

#         for grid_box in temp_grid_coordinates:
            
#             if check_polygon_intersection(operator_cordinates,grid_box):
#                 temp_grid_coordinates.remove(grid_box)
        
#         operator_ids[id] = temp_grid_coordinates


#         #print("operator_id",id,len(operator_ids[id]))

#         for id,grid in operator_ids.items():
#             #print("id,length:",id,len(grid))
#             if len(grid) == grid_check_threshold:
#                 status = True
#     return status
    


# def check_360_status(car_coordinates, bboxes,ids , new_car, grid_check_threshold,status):
#     if new_car:
#         #print("new_car")
#         global operator_ids
#         global grid_coordinates
#         x1,y1,x2,y2 = car_coordinates
#         x1,y1,x2,y2 = expand_rectangle(x1, y1, x2, y2, 60)
#         grid_coordinates = divide_bounding_box_with_all_corners(x1,y1,x2,y2,3,3)
#         print("New car grid_cordinates1---->",grid_coordinates) 
#         operator_ids = {}     

#     # for id in ids:
#     #     if id not in operator_ids:
#     #          operator_ids[id] = grid_coordinates[:]

#     for i, id in enumerate(ids):
#         #temp_grid_coordinates = operator_ids[id]
#         x1, y1, x2, y2 = map(int, bboxes[i])
#         operator_cordinates = rectangle_to_coordinates(x1, y1, x2, y2)
#         print("Check operator ",id," ",operator_cordinates)
#         # #debug
#         # if id==53:
#         #     print("coord:",operator_cordinates)

#         for i,grid_box in enumerate(grid_coordinates):
            
#             if check_polygon_intersection(operator_cordinates,grid_box):
#                 print("Operator ",id," in ",i)
#                 grid_coordinates.remove(grid_box)
#                 # break
        
#         #operator_ids[id] = temp_grid_coordinates


#         #print("operator_id",id,len(operator_ids[id]))

#         # for id,grid in operator_ids.items():
#             #print("id,length:",id,len(grid))
#         print("grid_cordinates---->",grid_coordinates)    
#         print("No of grid checked ",len(grid_coordinates) ,"for car",)
#         if len(grid_coordinates) == grid_check_threshold:
#             status = True
#     return status


def check_360_status(car_coordinates, bboxes,ids , new_car, grid_check_threshold,status, lane,stage):
    if new_car:
        # print("new_car")
        # global operator_ids
        # global grid_coordinates
        x1,y1,x2,y2 = car_coordinates
        if y1<120:
            u_grid = True
        else:
            u_grid = False
        x1,y1,x2,y2 = expand_rectangle(x1, y1, x2, y2, 60)
        common_vars.lane_stage_details[lane][stage]["grid_coordinates"] = divide_bounding_box_with_all_corners(x1,y1,x2,y2,3,3, u_grid)
        common_vars.lane_stage_details[lane][stage]["operator_ids"] = {}

    # for id in ids:
    #     if id not in common_vars.lane_stage_details[lane][stage]["operator_ids"]:
    #          common_vars.lane_stage_details[lane][stage]["operator_ids"][id] = common_vars.lane_stage_details[lane][stage]["grid_coordinates"][:]

    for i, id in enumerate(ids):
        #temp_grid_coordinates = common_vars.lane_stage_details[lane][stage]["operator_ids"][id]
        x1, y1, x2, y2 = map(int, bboxes[i])
        operator_cordinates = rectangle_to_coordinates(x1, y1, x2, y2)

        # #debug
        # if id==53:
        #     print("coord:",operator_cordinates)

        for grid_box in common_vars.lane_stage_details[lane][stage]["grid_coordinates"]:
            
            if check_polygon_intersection(operator_cordinates,grid_box):
                common_vars.lane_stage_details[lane][stage]["grid_coordinates"].remove(grid_box)
        
        #common_vars.lane_stage_details[lane][stage]["operator_ids"][id] = temp_grid_coordinates


        #print("operator_id",id,len(operator_ids[id]))

        # for id,grid in common_vars.lane_stage_details[lane][stage]["operator_ids"].items():
            #print("id,length:",id,len(grid))
        if len(common_vars.lane_stage_details[lane][stage]["grid_coordinates"]) == grid_check_threshold:
            status = True
    return status


# def check_360_status(car_coordinates, bboxes,ids , new_car, grid_check_threshold,status, lane,stage):
#     if new_car:
#         # print("new_car")
#         # global operator_ids
#         # global grid_coordinates
#         x1,y1,x2,y2 = car_coordinates
#         if y1<120:
#             u_grid = True
#         else:
#             u_grid = False
#         x1,y1,x2,y2 = expand_rectangle(x1, y1, x2, y2, 60)
#         common_vars.lane_stage_details[lane][stage]["grid_coordinates"] = divide_bounding_box_with_all_corners(x1,y1,x2,y2,3,3, u_grid)
#         common_vars.lane_stage_details[lane][stage]["operator_ids"] = {}

#     for id in ids:
#         if id not in common_vars.lane_stage_details[lane][stage]["operator_ids"]:
#              common_vars.lane_stage_details[lane][stage]["operator_ids"][id] = common_vars.lane_stage_details[lane][stage]["grid_coordinates"][:]

#     for i, id in enumerate(ids):
#         temp_grid_coordinates = common_vars.lane_stage_details[lane][stage]["operator_ids"][id]
#         x1, y1, x2, y2 = map(int, bboxes[i])
#         operator_cordinates = rectangle_to_coordinates(x1, y1, x2, y2)

#         # #debug
#         # if id==53:
#         #     print("coord:",operator_cordinates)

#         for grid_box in temp_grid_coordinates:
            
#             if check_polygon_intersection(operator_cordinates,grid_box):
#                 temp_grid_coordinates.remove(grid_box)
        
#         common_vars.lane_stage_details[lane][stage]["operator_ids"][id] = temp_grid_coordinates


#         #print("operator_id",id,len(operator_ids[id]))

#         for id,grid in common_vars.lane_stage_details[lane][stage]["operator_ids"].items():
#             #print("id,length:",id,len(grid))
#             if len(grid) == grid_check_threshold:
#                 status = True
#     return status








def convert_to_dict(d):
    if isinstance(d, defaultdict):
        d = {k: convert_to_dict(v) for k, v in d.items()}
    return d

def calculate_iou_ub(boxA, boxB):
    # Calculate intersection coordinates
    xA = max(boxA[0], boxB[0])
    yA = max(boxA[1], boxB[1])
    xB = min(boxA[2], boxB[2])
    yB = min(boxA[3], boxB[3])

    # Calculate intersection area
    intersection_area = max(0, xB - xA + 1) * max(0, yB - yA + 1)

    # Calculate area of each bounding box
    boxA_area = (boxA[2] - boxA[0] + 1) * (boxA[3] - boxA[1] + 1)
    boxB_area = (boxB[2] - boxB[0] + 1) * (boxB[3] - boxB[1] + 1)

    # Calculate union area
    union_area = boxA_area + boxB_area - intersection_area

    # Calculate IoU
    iou = intersection_area / union_area

    return iou

def find_subregion(car_bbox, subregions):
    max_iou = -1
    max_iou_index = -1

    for i, subregion in enumerate(subregions):
        subregion_bbox = [
            min(subregion[0][0], subregion[1][0], subregion[2][0], subregion[3][0]),  # xmin
            min(subregion[0][1], subregion[1][1], subregion[2][1], subregion[3][1]),  # ymin
            max(subregion[0][0], subregion[1][0], subregion[2][0], subregion[3][0]),  # xmax
            max(subregion[0][1], subregion[1][1], subregion[2][1], subregion[3][1])   # ymax
        ]
        
        iou = calculate_iou_ub(car_bbox, subregion_bbox)
        
        if iou > max_iou:
            max_iou = iou
            max_iou_index = i
    # print(max_iou)
    
    return max_iou_index

def find_operator_subregion(coordinate, subregions):
        x, y = coordinate
        for i, subregion in enumerate(subregions):
            if is_point_inside_polygon(x, y, subregion):
                return i
        return -1 

    