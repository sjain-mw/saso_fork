import cv2
import numpy as np
import matplotlib.pyplot as plt

def draw_circle(event, x, y, flags, param):
    global point_list
    if event == cv2.EVENT_LBUTTONDOWN:
        point_list.append((x, y))
        cv2.circle(img, (x, y), 3, (0, 255, 0), -1)
        cv2.imshow("image", img)
        # print("Adding point of polygon ",(x,y))

def draw_line(event, x, y, flags, param):
    global line_list
    if event == cv2.EVENT_LBUTTONDOWN:
        line_list.append((x, y))
        cv2.circle(img, (x, y), 3, (0, 0, 255), -1)
        cv2.imshow("image", img)
        
        # print("Adding point for line ",(x,y))
        # print(line_list)
video_path = "rtsp://admin:Q12345678q@192.168.100.46:554/Streaming/Channels/101"
# Open the video file
cap = cv2.VideoCapture(video_path)
print(video_path)

# Check if the video file open
# ed successfully
if not cap.isOpened():
    print("Error: Could not open video.")

while True:
    # Read the first frame
    ret, img = cap.read()
    cv2.namedWindow("Video",cv2.WINDOW_NORMAL)
    cv2.imshow("Video", img)
    # Wait for key press
    key = cv2.waitKey(1)
    # print("Video")
    # If the key pressed is 'q', exit the loop
    if key == ord('q'):
        break

    
# img = cv2.imread("Break_Check_Line_5-6_Front.jpg")
point_list = []
line_list = []

cv2.namedWindow("image",cv2.WINDOW_NORMAL)
cv2.imshow("image", img)
print(img.shape)
cv2.setMouseCallback("image", draw_circle)

while True:
    if len(point_list) ==4:
        break
    cv2.waitKey(1)

cv2.destroyAllWindows()

# Draw Polygon
pts = np.array(point_list, np.int32)
pts = pts.reshape((-1, 1, 2))
cv2.polylines(img, [pts], True, (255, 0, 0), thickness=2)



# Save points of polygon
print("Points of polygon:")
for point in point_list:
    
    print(point[0],";",point[1],";",end='')

# For lines
cv2.namedWindow("image",cv2.WINDOW_NORMAL)
cv2.imshow("image", img)
cv2.setMouseCallback("image", draw_line)

while True:
    if len(line_list) == 4:
        break
    cv2.waitKey(1)

cv2.destroyAllWindows()

# Draw lines
cv2.line(img, line_list[0], line_list[1], (0, 255, 255), thickness=2)
cv2.line(img, line_list[2], line_list[3], (0, 255, 255), thickness=2)

print("\nPoints of lines:")
for i in range(int(len(line_list)/2)):
    # print(i)
    # print(len(line_list))
    print(line_list[i*2][0],";",line_list[i*2][1],";",line_list[(i*2)+1][0],";",line_list[(i*2)+1][1],";",end="")
    print(point_list[i*2][0],";",point_list[i*2][1],";",point_list[(i*2)+1][0],";",point_list[(i*2)+1][1])

# Show image with lines
cv2.namedWindow("image",cv2.WINDOW_NORMAL)
cv2.imshow("image", img)
cv2.waitKey(1000)
cv2.destroyAllWindows()

# Save points of lines

# Save the image with lines and polygon
# cv2.imwrite("Break_Check_Line_5-6_Front_points.jpg", img)