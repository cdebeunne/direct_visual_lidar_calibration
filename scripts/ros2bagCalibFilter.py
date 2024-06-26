import cv2
import argparse
import yaml
import os

import rosbag2_py
from rclpy.serialization import serialize_message
from rosbags.serde import deserialize_cdr, serialize_cdr
from cv_bridge import CvBridge
from rosbags.rosbag2 import Reader, Writer
from sensor_msgs.msg import Image
from sensor_msgs.msg import Imu
from sensor_msgs.msg import CameraInfo


if __name__ == '__main__':
    
    # Parse arguments
    parser = argparse.ArgumentParser(description='Bag formatting')
    parser.add_argument('--bag_path', type=str, required=True, help="Path of the ROS2 bag")
    parser.add_argument('--config_path', type=str, required=True, help="Path of the yaml config file")
    args = parser.parse_args()
    bag_path = args.bag_path
    with open(args.config_path, 'r') as file:
        config = yaml.load(file)
    camera_topic = config['camera_topic']
    lidar_topic = config['lidar_topic']
    stop_stamp = config['stop_stamp']
    height = config["height"]
    width = config["width"]
    distortion_model = config["distortion_model"]
    distortion_coefficients = config["distortion_coefficients"]
    intrinsics = config["intrinsics"]
    
    # ROS2 bag writer
    os.system(f'rm -r {bag_path}_filtered')
    writer = rosbag2_py.SequentialWriter()
    storage_options = rosbag2_py._storage.StorageOptions(
            uri=bag_path+'_filtered',
            storage_id='sqlite3')
    converter_options = rosbag2_py._storage.ConverterOptions('', '')
    writer.open(storage_options, converter_options)
    
    # Create topics
    topic_info = rosbag2_py._storage.TopicMetadata(
            name=camera_topic,
            type='sensor_msgs/msg/Image',
            serialization_format='cdr')
    writer.create_topic(topic_info)
    topic_info = rosbag2_py._storage.TopicMetadata(
            name='/cam/camera_info',
            type='sensor_msgs/msg/CameraInfo',
            serialization_format='cdr')
    writer.create_topic(topic_info)
    topic_info = rosbag2_py._storage.TopicMetadata(
            name=lidar_topic,
            type='sensor_msgs/msg/PointCloud2',
            serialization_format='cdr')
    writer.create_topic(topic_info)

    with Reader(bag_path) as reader:
        for conn, timestamp, data in reader.messages():
            msg = deserialize_cdr(data, conn.msgtype)
            
            if ((stop_stamp != 0) & (timestamp > stop_stamp)):
            	break
            
            if (conn.topic == camera_topic,):
                writer.write(camera_topic, data, timestamp)
                
                cam_info_msg = CameraInfo()
                cam_info_msg.header.stamp.sec = msg.header.stamp.sec
                cam_info_msg.header.stamp.nanosec = msg.header.stamp.nanosec
                cam_info_msg.height = height
                cam_info_msg.width = width
                cam_info_msg.distortion_model = distortion_model
                cam_info_msg.d = distortion_coefficients
                cam_info_msg.k = intrinsics
                writer.write('/cam/camera_info', serialize_message(cam_info_msg), timestamp)
                print(cam_info_msg)
                
            if (conn.topic == lidar_topic):
                writer.write(lidar_topic, data, timestamp)
                
