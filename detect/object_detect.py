from __future__ import print_function

from detect.object_detect_top import TopObjectDetect
from detect.object_detect_bottom import BottomObjectDetect
from detect.object_detect_full import FullObjectDetect

class ObjectDetector(object):
  def __init__(self):
    self.top_od = TopObjectDetect()
    self.bottom_od = BottomObjectDetect()
    self.full_od = FullObjectDetect()

  def getObjects(self, file):
    with open(file, 'rb') as fid:
      image_data = fid.read()
    top_objects = self.top_od.detect(image_data)
    bottom_objects = self.bottom_od.detect(image_data)
    full_objects = self.full_od.detect(image_data)

    objs = []
    objs.extend(top_objects)
    objs.extend(bottom_objects)
    objs.extend(full_objects)

    objects = []
    for obj in objs:
      object = {}
      location = {}
      location['left'] = obj['box'][0]
      location['right'] = obj['box'][1]
      location['top'] = obj['box'][2]
      location['bottom'] = obj['box'][3]
      object['class_code'] = obj.get('class_code')
      object['class_name'] = obj.get('class_name')
      object['score'] = float(obj.get('score'))
      object['feature'] = obj.get('feature')
      object['location'] = location
      objects.append(object)

    return objects
