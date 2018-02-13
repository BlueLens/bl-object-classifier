# coding: utf-8

from __future__ import absolute_import

import numpy as np
import os
from stylelens_s3 import s3
from PIL import Image
import tensorflow as tf
from object_detection.utils import visualization_utils as vis_util
from stylelens_feature.feature_extract import ExtractFeature
from bluelens_log import Logging

import io
from util import label_map_util

TMP_CROP_IMG_FILE = './tmp.jpg'

NUM_CLASSES = 1

AWS_BUCKET = 'bluelens-style-model'
AWS_BUCKET_FOLDER = 'object_detection'
MODEL_TYPE = 'bottom'
AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
RELEASE_MODE = os.environ['RELEASE_MODE']
FEATURE_GRPC_HOST = os.environ['FEATURE_GRPC_HOST']
FEATURE_GRPC_PORT = os.environ['FEATURE_GRPC_PORT']
OD_SCORE_MIN = float(os.environ['OD_SCORE_MIN'])

MODEL_FILE = 'frozen_inference_graph.pb'
LABEL_MAP_FILE = 'label_map.pbtxt'
options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-detect:BottomObjectDetect')
storage = s3.S3(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)

class BottomObjectDetect(object):
  def __init__(self):
    label_map_file = self.load_labelemap()
    label_map = label_map_util.load_labelmap(label_map_file)
    log.debug(label_map)
    categories = label_map_util.convert_label_map_to_categories(label_map, max_num_classes=NUM_CLASSES,
                                                                use_display_name=True)
    self.__category_index = label_map_util.create_category_index(categories)
    self.__detection_graph = tf.Graph()
    self.__feature_extractor = ExtractFeature(use_gpu=True)
    model_file = self.load_model()
    with self.__detection_graph.as_default():
      od_graph_def = tf.GraphDef()
      with tf.gfile.GFile(model_file, 'rb') as fid:
        serialized_graph = fid.read()
        od_graph_def.ParseFromString(serialized_graph)
        tf.import_graph_def(od_graph_def, name='')

        self.__sess = tf.Session(graph=self.__detection_graph)

    log.info('_init_ done')

  def load_labelemap(self):
    log.info('load_labelmap')
    file = os.path.join(os.getcwd(), LABEL_MAP_FILE)
    key = os.path.join(AWS_BUCKET_FOLDER, RELEASE_MODE, MODEL_TYPE, LABEL_MAP_FILE)
    print(key)
    try:
      return storage.download_file_from_bucket(AWS_BUCKET, file, key)
    except:
      log.error('download error')
      return None

  def load_model(self):
    log.info('load_model')
    file = os.path.join(os.getcwd(), MODEL_FILE)
    key = os.path.join(AWS_BUCKET_FOLDER, RELEASE_MODE, MODEL_TYPE, MODEL_FILE)
    print(key)
    try:
      return storage.download_file_from_bucket(AWS_BUCKET, file, key)
    except:
      log.error('download error')
      return None

  def detect(self, image_bytes):

    image_data = Image.open(io.BytesIO(image_bytes))
    image_np = self.load_image_into_numpy_array(image_data)

    show_box = False
    out_image, boxes, scores, classes, num_detections = self.detect_objects(image_np, self.__sess, self.__detection_graph, show_box)

    out_boxes = self.take_object(
      out_image,
      np.squeeze(boxes),
      np.squeeze(scores),
      np.squeeze(classes).astype(np.int32))

    # log.debug(out_boxes)
    return out_boxes

  def take_object(self, image_np, boxes, scores, classes):
    max_boxes_to_save = 3
    taken_boxes = []
    if not max_boxes_to_save:
      max_boxes_to_save = boxes.shape[0]
    for i in range(min(max_boxes_to_save, boxes.shape[0])):
      if scores is None or scores[i] > OD_SCORE_MIN:
        print(scores[i])
        if classes[i] in self.__category_index.keys():
          class_name = self.__category_index[classes[i]]['name']
          class_code = str(self.__category_index[classes[i]]['id'])
        else:
          class_name = 'na'
          class_code = 'na'
        ymin, xmin, ymax, xmax = tuple(boxes[i].tolist())

        use_normalized_coordinates = True
        image_pil = Image.fromarray(np.uint8(image_np)).convert('RGB')

        left, right, top, bottom = self.crop_bounding_box(
          image_pil,
          ymin,
          xmin,
          ymax,
          xmax,
          use_normalized_coordinates=use_normalized_coordinates)

        feature_vector = self.extract_feature(image_pil, left, right, top, bottom)
        item = {}

        item['box'] = [left, right, top, bottom]
        item['class_name'] = class_name
        # item['class_code'] = class_code
        item['class_code'] = '2'
        item['score'] = scores[i]
        item['feature'] = feature_vector
        taken_boxes.append(item)
    return taken_boxes

  def extract_feature(self, image, left, right, top, bottom):
    area = (left, top, left + abs(left-right), top + abs(bottom-top))
    cropped_img = image.crop(area)
    cropped_img.save(TMP_CROP_IMG_FILE)
    # cimage = io.BytesIO()
    # cropped_img.save(cimage, format='JPEG')
    # cimage.seek(0)  # rewind to the start
    # cimage = Image.open(cimage)
    feature = self.__feature_extractor.extract_feature(TMP_CROP_IMG_FILE)
    return feature

  def load_image_into_numpy_array(self, image):
    (im_width, im_height) = image.size
    return np.array(image.getdata()).reshape(
      (im_height, im_width, 3)).astype(np.uint8)

  def crop_bounding_box(self,
                        image,
                        ymin,
                        xmin,
                        ymax,
                        xmax,
                        use_normalized_coordinates=True):
    im_width, im_height = image.size
    # image.show()
    if use_normalized_coordinates:
      (left, right, top, bottom) = (xmin * im_width, xmax * im_width,
                                    ymin * im_height, ymax * im_height)
    else:
      (left, right, top, bottom) = (xmin, xmax, ymin, ymax)

    return left, right, top, bottom

  def detect_objects(self, image_np, sess, detection_graph, show_box=True):
    # Expand dimensions since the model expects images to have shape: [1, None, None, 3]
    image_np_expanded = np.expand_dims(image_np, axis=0)
    image_tensor = detection_graph.get_tensor_by_name('image_tensor:0')

    # Each box represents a part of the image where a particular object was detected.
    boxes = detection_graph.get_tensor_by_name('detection_boxes:0')

    # Each score represent how level of confidence for each of the objects.
    # Score is shown on the result image, together with the class label.
    scores = detection_graph.get_tensor_by_name('detection_scores:0')
    classes = detection_graph.get_tensor_by_name('detection_classes:0')
    num_detections = detection_graph.get_tensor_by_name('num_detections:0')

    # Actual detection.
    (boxes, scores, classes, num_detections) = sess.run(
      [boxes, scores, classes, num_detections],
      feed_dict={image_tensor: image_np_expanded})

    if show_box:
      # Visualization of the results of a detection.
      vis_util.visualize_boxes_and_labels_on_image_array(
        image_np,
        np.squeeze(boxes),
        np.squeeze(classes).astype(np.int32),
        np.squeeze(scores),
        self.__category_index,
        use_normalized_coordinates=True,
        max_boxes_to_draw=3,
        min_score_thresh=.05,
        line_thickness=8)
    # print(image_np)
    return image_np, boxes, scores, classes, num_detections
