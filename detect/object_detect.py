from __future__ import print_function

import grpc
import numpy as np
from detect import object_detect_pb2
from detect import object_detect_pb2_grpc

HOST = 'localhost'
PORT = '50052'

class ObjectDetector(object):
  def __init__(self):
    channel = grpc.insecure_channel(HOST + ':' + PORT)
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
