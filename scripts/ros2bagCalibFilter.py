import cv2
import argparse
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
    parser.add_argument('--camera_topic', type=str, required=True, help="Topic of the image")
    parser.add_argument('--lidar_topic', type=str, required=True, help="Topic of the LiDAR")
    parser.add_argument('--stop_stamp', type=int, required=False, help="Stamp to stop bag processing", default=2000000000000000000000)
    args = parser.parse_args()
    bag_path = args.bag_path
    
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
            name=args.camera_topic,
            type='sensor_msgs/msg/Image',
            serialization_format='cdr')
    writer.create_topic(topic_info)
    topic_info = rosbag2_py._storage.TopicMetadata(
            name='/cam/camera_info',
            type='sensor_msgs/msg/CameraInfo',
            serialization_format='cdr')
    writer.create_topic(topic_info)
    topic_info = rosbag2_py._storage.TopicMetadata(
            name=args.lidar_topic,
            type='sensor_msgs/msg/PointCloud2',
            serialization_format='cdr')
    writer.create_topic(topic_info)

    with Reader(bag_path) as reader:
        for conn, timestamp, data in reader.messages():
            msg = deserialize_cdr(data, conn.msgtype)
            
            if (timestamp > args.stop_stamp):
            	break
            
            if (conn.topic == args.camera_topic,):
                writer.write(args.camera_topic, data, timestamp)
                
                cam_info_msg = CameraInfo()
                cam_info_msg.header.stamp.sec = msg.header.stamp.sec
                cam_info_msg.header.stamp.nanosec = msg.header.stamp.nanosec
                cam_info_msg.height = 1024
                cam_info_msg.width = 1280
                cam_info_msg.distortion_model = "equidistant"
                cam_info_msg.d = [0.01879377, -0.00132283, -0.00234174, 0.00032518]
                cam_info_msg.k = [499.86967694, 0.0, 650.07989234, 0.0, 499.77443338, 531.47666637, 0.0, 0.0, 1.0]
                writer.write('/cam/camera_info', serialize_message(cam_info_msg), timestamp)
                print(cam_info_msg)
                
            if (conn.topic == args.lidar_topic):
                writer.write(args.lidar_topic, data, timestamp)
                
