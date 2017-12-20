from __future__ import print_function

import os
from multiprocessing import Process
from threading import Timer

from PIL import Image
import time
import numpy as np
import uuid
import urllib.request
from collections import Counter
import pickle
from bluelens_spawning_pool import spawning_pool
from detect.object_detect import ObjectDetector
from stylelens_object.objects import Objects
from stylelens_image.images import Images
from util import s3
import redis

from bluelens_log import Logging

AWS_OBJ_IMAGE_BUCKET = 'bluelens-style-object'
AWS_MOBILE_IMAGE_BUCKET = 'bluelens-style-mainimage'

OBJECT_IMAGE_WIDTH = 300
OBJECT_IMAGE_HEITH = 300
HEALTH_CHECK_TIME = 300

CLASS_NUM = 3
TMP_MOBILE_IMG = 'tmp_mobile_full.jpg'
TMP_MOBILE_THUMB_IMG = 'tmp_mobile_thumb.jpg'


SPAWN_ID = os.environ['SPAWN_ID']
REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
RELEASE_MODE = os.environ['RELEASE_MODE']
AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY'].replace('"', '')
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY'].replace('"', '')

REDIS_PRODUCT_CLASSIFY_QUEUE = 'bl:product:classify:queue'
REDIS_OBJECT_INDEX_QUEUE = 'bl:object:index:queue'
REDIS_PRODUCT_HASH = 'bl:product:hash'
REDIS_PRODUCT_IMAGE_PROCESS_QUEUE = 'bl:product:image:process:queue'
REDIS_CRAWL_VERSION = 'bl:crawl:version'
REDIS_CRAWL_VERSION_LATEST = 'latest'

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-object-classifier')
rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

storage = s3.S3(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)

heart_bit = True
object_api = None
image_api = None
version_id = None

def analyze_product(p_data):
  log.info('analyze_product')
  p_dict = pickle.loads(p_data)

  save_main_image_as_object(p_dict)
  main_class_code, main_objects = analyze_main_image(p_dict['main_image_mobile_full'])
  save_image_to_db(p_dict, main_class_code, main_objects)

  sub_class_code, sub_objects = analyze_sub_images(p_dict['sub_images_mobile'])

  save_objects = []
  for obj in sub_objects:
    if obj['class_code'] == main_class_code:
      save_objects.append(obj)

  save_objects_to_db(p_dict['id'], main_class_code, save_objects)

  # color = analyze_color(p_dict)

def get_latest_crawl_version():
  value = rconn.hget(REDIS_CRAWL_VERSION, REDIS_CRAWL_VERSION_LATEST)
  version_id = value.decode("utf-8")
  return version_id

def save_objects_to_db(product_id, class_code, objects):
  global version_id

  for obj in objects:
    object = {}
    object['product_id'] = product_id
    object['storage'] = 's3'
    object['bucket'] = AWS_OBJ_IMAGE_BUCKET
    object['class_code'] = class_code
    object['name'] = obj['name']
    object['version_id'] = version_id
    feature = np.fromstring(obj['feature'], dtype=np.float32)
    object['feature'] = feature.tolist()

    save_to_storage(object)
    save_object_to_db(object)

    push_object_to_queue(object)
  # obj_img.show()

def save_image_to_db(product, class_code, objects):
  global version_id
  log.info('save_image_to_db')

  for obj in objects:
    save_object_to_db(obj)

  image = {}
  image['main_image_mobile_full'] = product['main_image_mobile_full']
  image['main_image_mobile_thumb'] = product['main_image_mobile_thumb']
  image['product_url'] = product['product_url']
  image['price'] = product['price']
  image['host_code'] = product['host_code']
  image['host_name'] = product['host_name']
  image['product_no'] = product['product_no']
  image['class_code'] = class_code
  image['objects'] = objects
  image['version_id'] = version_id

  # image['color_code'] = ''
  # image['sex_code'] = ''
  # image['age_code'] = ''

  global image_api
  try:
    api_response = image_api.add_image(image)
    log.debug(api_response)
  except Exception as e:
    log.warn("Exception when calling add_image: %s\n" % e)

def analyze_color(product):
  log.debug('analyze_color')
  color = ''
  return color

def analyze_category(product):
  log.debug('analyze_category')
  category = 1
  return category

def analyze_main_image(image):
  log.info('analyze_main_image')

  classes = []
  objects = []

  try:
    class_code, detected_objects = object_detect(image)
    if class_code is not None:
      classes.append(class_code)
      objects.extend(detected_objects)
  except Exception as e:
    log.error(str(e))

  final_class = None
  score = 0.0
  for obj in objects:
    if obj['score'] > score:
      score = obj['score']
      final_class = obj['class_code']

  return final_class, objects

def analyze_sub_images(images):
  log.info('analyze_class')

  classes = []
  objects = []

  for image in images:
    try:
      class_code, detected_objects = object_detect(image)
      if class_code is not None:
        classes.append(class_code)
        objects.extend(detected_objects)
    except Exception as e:
      log.error(str(e))

  final_class = None
  final_objects = []
  if len(images) > 1:
    try:
      c = Counter(classes)
      k = c.most_common()
      final_class = k[0][0]
      log.debug('analyze_class: ' + final_class)
      for obj in objects:
        if obj['class_code'] == final_class:
          final_objects.append(obj)
    except Exception as e:
      print(e)
  else:
    score = 0.0
    for obj in objects:
      if obj['score'] > score:
        score = obj['score']
        final_class = obj['class_code']

  return final_class, final_objects

def object_detect(image_path):
  log.info('object_detect:start')
  start_time = time.time()
  log.info(image_path)
  try:
    f = urllib.request.urlopen(image_path)
  except Exception as e:
    log.error(str(e))
    return
  im = Image.open(f).convert('RGB')
  tmp_img = 'tmp.jpg'
  im.save(tmp_img)

  classes = []
  detected_objects = []
  try:
    obj_detector = ObjectDetector()
    objects = obj_detector.getObjects(tmp_img)
    for obj in objects:
      log.info(obj.class_name + ':' + str(obj.score))
      left = obj.location.left
      right = obj.location.right
      top = obj.location.top
      bottom = obj.location.bottom
      area = (left, top, left + abs(left-right), top + abs(bottom-top))
      obj_img = im.crop(area)
      size = OBJECT_IMAGE_WIDTH, OBJECT_IMAGE_HEITH
      obj_img.thumbnail(size, Image.ANTIALIAS)

      id = str(uuid.uuid4())
      tmp_obj_img = id + '.jpg'
      obj_img.save(tmp_obj_img)
      classes.append(obj.class_code)
      image_obj = {}
      image_obj['class_code'] = obj.class_code
      image_obj['name'] = id
      image_obj['score'] = obj.score
      image_obj['feature'] = obj.feature
      box = {}
      box['left'] = left
      box['right'] = right
      box['top'] = top
      box['bottom'] = bottom
      image_obj['box'] = box
      detected_objects.append(image_obj)

  except Exception as e:
    log.error(str(e))
    return

  final_class = None
  try:
    c = Counter(classes)
    k = c.most_common()
    final_class = k[0][0]
    print(final_class)
    log.debug('Decided class_code:' + final_class)
  except Exception as e:
    log.warn(str(e))
  elapsed_time = time.time() - start_time
  log.info('total object_detection time: ' + str(elapsed_time))
  return final_class, detected_objects

def save_main_image_as_object(product):
  log.info('save_main_image_as_object')
  try:
    f = urllib.request.urlopen(product['main_image'])
  except Exception as e:
    log.error(str(e))
    return
  im = Image.open(f).convert('RGB')
  size = OBJECT_IMAGE_WIDTH, OBJECT_IMAGE_HEITH
  im.thumbnail(size, Image.ANTIALIAS)

  object = {}
  object['product_id'] = product['id']
  object['storage'] = 's3'
  object['bucket'] = AWS_OBJ_IMAGE_BUCKET
  object['class_code'] = '0'
  id = str(uuid.uuid4())
  object['name'] = id
  im.save(id + '.jpg')
  save_to_storage(object)
  save_object_to_db(object)
  push_object_to_queue(object)

def push_object_to_queue(obj):
  log.info('push_object_to_queue')
  rconn.lpush(REDIS_OBJECT_INDEX_QUEUE, pickle.dumps(obj, protocol=2))

def save_object_to_db(obj):
  log.info('save_object_to_db')
  global object_api
  try:
    api_response = object_api.add_object(obj)
    log.debug(api_response)
  except Exception as e:
    log.warn("Exception when calling add_object: %s\n" % e)

def check_health():
  global  heart_bit
  log.info('check_health: ' + str(heart_bit))
  if heart_bit == True:
    heart_bit = False
    Timer(HEALTH_CHECK_TIME, check_health, ()).start()
  else:
    delete_pod()

def delete_pod():
  log.info('exit: ' + SPAWN_ID)

  data = {}
  data['namespace'] = RELEASE_MODE
  data['id'] = SPAWN_ID
  spawn = spawning_pool.SpawningPool()
  spawn.setServerUrl(REDIS_SERVER)
  spawn.setServerPassword(REDIS_PASSWORD)
  spawn.delete(data)

def save_to_storage(obj):
  log.debug('save_to_storage')
  file = obj['name'] + '.jpg'
  key = os.path.join(RELEASE_MODE, obj['class_code'], file)
  is_public = True
  path = storage.upload_file_to_bucket(AWS_OBJ_IMAGE_BUCKET, file, key, is_public=is_public)
  obj['image_url'] = path
  log.debug('save_to_storage done')

def dispatch_job(rconn):
  global object_api
  global image_api
  global version_id
  object_api = Objects()
  image_api = Images()
  version_id = get_latest_crawl_version()

  log.info('Start dispatch_job')
  Timer(HEALTH_CHECK_TIME, check_health, ()).start()
  while True:
    key, value = rconn.blpop([REDIS_PRODUCT_CLASSIFY_QUEUE])
    analyze_product(value)
    global  heart_bit
    heart_bit = True

if __name__ == '__main__':
  log.info('Start bl-object-classifier')
  try:
    Process(target=dispatch_job, args=(rconn,)).start()
  except Exception as e:
    log.error(str(e))
    delete_pod()
