from __future__ import print_function

import grpc
import os
from detect import object_detect_pb2
from detect import object_detect_pb2_grpc

OD_HOST = os.environ['OD_HOST']
OD_PORT = os.environ['OD_PORT']

class ObjectDetector(object):
  def __init__(self):
    channel = grpc.insecure_channel(OD_HOST + ':' + OD_PORT)
    self.stub = object_detect_pb2_grpc.DetectStub(channel)

  def getObjects(self, file):
    with open(file, 'rb') as fid:
      image_data = fid.read()

    objects = self.stub.GetObjects(object_detect_pb2.DetectRequest(file_data=image_data))
    # for object in objects:
    #   print(object.class_name)
    #   print(object.class_code)
    #   print(object.location)
    #   arr = np.fromstring(object.feature, dtype=np.float32)
    #   print(arr)
    return objects
