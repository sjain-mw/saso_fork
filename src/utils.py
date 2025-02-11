from shapely.geometry import Point, Polygon

from collections import  Counter, deque

import config

def is_inside(roi, center_point):
    polygon = Polygon(roi)

    point = Point(center_point)

    # Check if the point is inside the polygon
    return polygon.contains(point)

def is_point_inside_rectangle(point, rect):
  """
  Checks if a point lies within a rectangle.

  Args:
    point: A tuple (x, y) representing the point.
    rect: A tuple (x, y, w, h) representing the rectangle's top-left corner and dimensions.

  Returns:
    True if the point is inside the rectangle, False otherwise.
  """

  x, y = point
  rect_x, rect_y, rect_w, rect_h = rect

  return rect_x <= x <= rect_x + rect_w and rect_y <= y <= rect_y + rect_h

def freq_lpr_conf(lpr,lpr_conf):
    if len(lpr):
        lpr_counter = Counter(lpr)
        # print("LPR  ",lpr)
        lpr_value=lpr_counter.most_common(1)[0][0]
        indices = [i for i, x in enumerate(lpr) if x == lpr_value]
        
        values_at_indices = [lpr_conf[i] for i in indices]
        # print("LPR value ",lpr_value)
        
        # print(values_at_indices)
        if None not in values_at_indices:
            lpr_conf = sum(values_at_indices) / len(values_at_indices)
            return lpr_value,lpr_conf
    return None,None

def is_valid_plate(plate):
    if plate is None:
        return False
    # Check total length (5 or 7)
    if len(plate) not in (5, 7):
        return False
    # Check last 3 characters are alphabets
    if not plate[-3:].isalpha():
        return False
    # Check rest are numbers
    if not plate[:-3].isdigit():
        return False
    return True

def most_freq(str_list):
    if str_list is None:
        return None
    if len(str_list) > 0:
      frequency = Counter(str_list)
      sorted_plates = sorted(frequency.items(), key=lambda x: x[1], reverse=True)
      valid_plate = None
      for plate, freq in sorted_plates:
        if is_valid_plate(plate):
            valid_plate = plate
            break
      return valid_plate
    else:
        return None
  
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


def lpr_current_car(car_coordinates, lpr_info):
    lpr=""
    lpr_conf=""
    for lpr in lpr_info:
        lpr_point = (lpr[2],lpr[3])
        if is_point_inside_rectangle(lpr_point,car_coordinates):
            if len(lpr) == 7 and len(lpr[-1])>4:
                return lpr[-1],lpr[1]
    return None,None
    

def convert_coordinates(vehicle_points):
    
    x1 = vehicle_points[1]
    y1 = vehicle_points[0]
    x2 = vehicle_points[1]+vehicle_points[2]
    y2 = vehicle_points[0]+vehicle_points[3]
    
    return x1, y1, x2, y2
  
def extract_subregions(data):
    """
    Extract subregions from the input dictionary and return them in a structured format.

    Args:
        data (dict): Dictionary containing the lane data with subregion information.

    Returns:
        dict: A dictionary containing subregions as keys ('1', '2', '3') with their respective ROI values.
    """
    subregions = {}

    # Iterate over keys and values in the input dictionary
    for key, value in data.items():
        # Filter keys that match the subregion naming pattern
        if key.startswith("subregion"):
            # Extract the subregion number (last character of the key)
            subregion_number = key.split('_')[-1][-1]
            # Add the subregion number and its ROI to the result dictionary
            subregions[subregion_number] = value

    return subregions
  
def find_subregion(car_bbox, subregions):
  # Calculate the y center of the car bounding box
  y_center = (car_bbox[1] + car_bbox[3]) / 2

  for key, subregion in subregions.items():
      # Find ymin and ymax for the subregion
      subregion_ymin = min(subregion[0][1], subregion[1][1], subregion[2][1], subregion[3][1])
      subregion_ymax = max(subregion[0][1], subregion[1][1], subregion[2][1], subregion[3][1])

      # Check if y_center lies within the subregion's ymin and ymax
      if subregion_ymin <= y_center <= subregion_ymax:
          return int(key)

  return -1  # Return -1 if no subregion matches the y_center

def create_vehicle_details():
        return {
            "checking_status": False,
            "checked": "",
            "entry_time": 0,
            "current_time":0,
            "elapsed_time":0,
            "exit_time": 0,
            "no_obj_frames": 0,
            "vehicle_in_roi": 0,
            "object_id": "",
            "vehicle_id": "",
            "check_frames_count": 0,
            "vehicle_uuid": "",
            "bbox":0,
            "subregion":0,
            "associated_tracking_id":None,
            "vehicle_ID_deque": deque(maxlen=config.max_length)
        }

def is_point_inside_polygon_ub(x, y, polygon):
    
    n = len(polygon)
    inside = False
    p1x, p1y = polygon[0]
    
    for i in range(n + 1):
        p2x, p2y = polygon[i % n]
        if y > min(p1y, p2y) and y <= max(p1y, p2y) and x <= max(p1x, p2x):
            if p1y != p2y:
                xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
            if p1x == p2x or x <= xinters:
                inside = not inside
        p1x, p1y = p2x, p2y

    return inside
        
def find_operator_subregion(coordinate, subregions):
    x, y = coordinate
    for key, subregion in subregions.items():
        if is_point_inside_polygon_ub(x, y, subregion):
            return int(key)
    return -1 