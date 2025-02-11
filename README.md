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
 - Change the branch: ``` git checkout exit_17 ```
 - Pull the Kafka Docker image : ``` docker pull apache/kafka-native:3.8.0 ```
 - Pull the DeepStream 7.0 Docker image : ``` docker pull nvcr.io/nvidia/deepstream:7.0-triton-multiarch ``` 
 - Run the Deepstream docker container : ``` sudo docker run --gpus '"'device=0'"' -e DISPLAY=$DISPLAY -it --rm --net=host -w /host_data/saso/deep_stream_scripts -v /home/saso-cv:/host_data/saso  -v /etc/localtime:/etc/localtime:ro docker_name ```
 - Install the requirements file : ``` pip install -r requirements.txt ```
 - Run this command to install dependencies : ``` /opt/nvidia/deepstream/deepstream-7.0/user_deepstream_python_apps_install.sh -r v1.1.11 -b ```
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
/opt/nvidia/deepstream/deepstream-7.0/user_deepstream_python_apps_install.sh -r v1.1.11 -b
 
### **Data set paths:**
#### Training data for Exit17  
- Location: TUF machine, Hyderabad 
- Server Type: On premise server 
- System data path: /media/tuf/New_9997/SASO/Dataset_Exit17
 
#### Test data for Station1 
- Location: TUF machine, Hyderabad 
- Server Type: On premise server 
- System data path: /media/tuf/New_9997/SASO/Test_data
 
### LPR training data 
- Location: TUF machine, Hyderabad 
- Server Type: On premise server
- System data path: /media/tuf/New_9997/SASO/tao-experiments/lprnet/prepared_data/
 
### **Important links:**
[Station1 CV logics] (https://masterworksoutlook-my.sharepoint.com/:w:/g/personal/skumar_master-works_sa/Eds1vrLswNVCvLRep4nVbAEBvPNCNvtVNOix7mOiTvEVpg?e=D8zYy3)
 

