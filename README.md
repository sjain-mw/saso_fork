# SASO

### **Project Name :**  
SASO (Saudi Standards Metrology & Quality Organization) Vehicle Inspection Analytics

### **Description :** 
 The SASO project utilizes computer vision and analytics to optimize vehicle inspection processes at SASO sites across Saudi Arabia. The system ensures that every vehicle undergoes a comprehensive inspection, including 360-degree checks, carbon checks, brake checks, and underbody vehicle checks. Our Basser product analyzes inspection times at each stage and calculates the overall journey time of the vehicle, verifying that operators perform the required checks.

### **Requirements :**
 - Linux OS 22-04
 - Docker
 - Docker compose
 - Python 3
 - CUDA drivers
 - CUDA toolkit
 - Nvidia container toolkit
 - Kafka
 - pm2

### **Installation Steps :**
 - Change the directory : ``` cd /home ```
 - Clone the repository : ``` git clone https://github.com/masterworks-engineering/saso-cv.git ``` 
 - Pull the Kafka Docker image : ``` docker pull apache/kafka-native:3.8.0 ```
 - Pull the DeepStream 7.0 Docker image : ``` docker pull nvcr.io/nvidia/deepstream:7.0-triton-multiarch ``` 
 - Run the Deepstream docker container : ``` sudo docker run --gpus '"'device=0'"' -e DISPLAY=$DISPLAY -it --rm --net=host -w /host_data/saso/deep_stream_scripts -v /home/saso-cv:/host_data/saso  -v /etc/localtime:/etc/localtime:ro docker_name ```
 - Install the requirements file : ``` pip install -r requirements.txt ```
 - Run this command to install dependencies : ``` /opt/nvidia/deepstream/deepstream/user_deepstream_python_apps_install.sh --build-bindings -r master ```
 - Commit the changes to the docker

### **Usage Instructions :**
 - Go to the project directory : ``` cd /saso-cv ```
 - Run the Docker containers for Kafka and DeepStream : ``` docker compose up -d ```
 - To stop the docker services : ``` docker compose down ```

### **Pipeline Overview:**

![Architecture](./images/saso_arch.jpg) 

### **Important Commands:**
- docker compose up -d
- docker compose down
- docker compose ps
- docker compose logs --tail=400 -f service_name
- pm2 start <app_id>
- pm2 stop <app_id>
- pm2 restart all / <app_id>
- pm2 status

