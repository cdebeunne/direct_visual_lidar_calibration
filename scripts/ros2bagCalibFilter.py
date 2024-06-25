import cv2
import pandas as pd
import os


import rosbag2_py
from rclpy.serialization import serialize_message
from rosbags.serde import deserialize_cdr, serialize_cdr
from cv_bridge import CvBridge
from rosbags.rosbag2 import Reader, Writer
from sensor_msgs.msg import Image
from sensor_msgs.msg import Imu
from sensor_msgs.msg import CameraInfo

pd.set_option('display.float_format', str)


def get_rosbag_options(path, storage_id, serialization_format='cdr'):
    storage_options = rosbag2_py.StorageOptions(
        uri=path, storage_id=storage_id)

    converter_options = rosbag2_py.ConverterOptions(
        input_serialization_format=serialization_format,
        output_serialization_format=serialization_format)

    return storage_options, converter_options

if __name__ == '__main__':

    bag_path = '/media/ce.debeunne/HDD/datasets/ISAE/2024-03-18-Cagnac/ros2bag/Radar2_light_run'
    storage_id = 'sqlite3'
    storage_options, converter_options = get_rosbag_options(bag_path, storage_id)

    # Set filter for topic of string type
    storage_filter = rosbag2_py.StorageFilter(topics=['/cam_0/image_raw', '/cortex/ouster/points'])
    
    # ROS2 bag writer
    os.system('rm -r /media/ce.debeunne/HDD/datasets/ISAE/2024-03-18-Cagnac/ros2bag/Radar2_light_run_filtered')
    writer = rosbag2_py.SequentialWriter()
    storage_options = rosbag2_py._storage.StorageOptions(
            uri=bag_path+'_filtered',
            storage_id='sqlite3')
    converter_options = rosbag2_py._storage.ConverterOptions('', '')
    writer.open(storage_options, converter_options)
    
    # Create topics
    topic_info = rosbag2_py._storage.TopicMetadata(
            name='/cam_0/image_raw',
            type='sensor_msgs/msg/Image',
            serialization_format='cdr')
    writer.create_topic(topic_info)
    topic_info = rosbag2_py._storage.TopicMetadata(
            name='/cam_0/camera_info',
            type='sensor_msgs/msg/CameraInfo',
            serialization_format='cdr')
    writer.create_topic(topic_info)
    topic_info = rosbag2_py._storage.TopicMetadata(
            name='/cortex/ouster/points',
            type='sensor_msgs/msg/PointCloud2',
            serialization_format='cdr')
    writer.create_topic(topic_info)

    with Reader(bag_path) as reader:
        for conn, timestamp, data in reader.messages():
            msg = deserialize_cdr(data, conn.msgtype)
            
            if (timestamp > 1710776357888979114):
            	break
            
            if (conn.topic == "/cam_0/image_raw"):
                writer.write('/cam_0/image_raw', data, timestamp)
                
                cam_info_msg = CameraInfo()
                cam_info_msg.header.stamp.sec = msg.header.stamp.sec
                cam_info_msg.header.stamp.nanosec = msg.header.stamp.nanosec
                cam_info_msg.height = 1024
                cam_info_msg.width = 1280
                cam_info_msg.distortion_model = "equidistant"
                cam_info_msg.d = [0.01879377, -0.00132283, -0.00234174, 0.00032518]
                cam_info_msg.k = [499.86967694, 0.0, 650.07989234, 0.0, 499.77443338, 531.47666637, 0.0, 0.0, 1.0]
                writer.write('/cam_0/camera_info', serialize_message(cam_info_msg), timestamp)
                print(cam_info_msg)
                
            if (conn.topic == "/cortex/ouster/points"):
                writer.write('/cortex/ouster/points', data, timestamp)
                
