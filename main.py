from __future__ import print_function

import os
from multiprocessing import Process
from threading import Timer
from PIL import Image
import time
import urllib.request
from collections import Counter
import pickle
from bluelens_spawning_pool import spawning_pool
from detect.object_detect import ObjectDetector
from stylelens_product import Product
from stylelens_product import Object
# from stylelens_product import Image
from stylelens_product import ProductApi
from stylelens_product import ObjectApi
from stylelens_product.rest import ApiException
from util import s3
import redis

from bluelens_log import Logging



AWS_OBJ_IMAGE_BUCKET = 'bluelens-style-object'
AWS_MOBILE_IMAGE_BUCKET = 'bluelens-style-mainimage'

OBJECT_IMAGE_WIDTH = 300
OBJECT_IMAGE_HEITH = 300
MOBILE_FULL_WIDTH = 343
MOBILE_THUMBNAIL_WIDTH = 163
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

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-classifier')
product_api = ProductApi()
object_api = ObjectApi()
rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

storage = s3.S3(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)

heart_bit = True
obj_detector = ObjectDetector()

def analyze_product(p_data):
  log.info('analyze_product')
  p_dict = pickle.loads(p_data)

  save_main_image_as_object(p_dict)
  class_code = analyze_class(p_dict)
  # color = analyze_color(p_dict)

  product = Product()
  product.id = p_dict['id']
  product.class_code = class_code
  product.is_indexed = True
  update_product_to_db(product)

def analyze_color(product):
  log.debug('analyze_color')
  color = ''
  return color

def analyze_category(product):
  log.debug('analyze_category')
  category = 1
  return category

def analyze_class(product):
  log.info('analyze_image')
  images = []

  images.append(product['main_image_mobile_full'])
  images.extend(product['sub_images_mobile'])

  classes = []
  for image in images:
    class_code = object_detect(product, image)
    classes.append(class_code)

  c = Counter(classes)
  k = c.most_common()
  # final_class = k[0][0]
  final_class = ''
  log.debug('analyze_class: ' + final_class)
  return final_class

def object_detect(product, image_path):
  log.info('object_detect:start')
  start_time = time.time()
  log.info(image_path)
  log.debug('product id = ' + product['id'])
  try:
    f = urllib.request.urlopen(image_path)
  except Exception as e:
    log.error(str(e))
    return
  im = Image.open(f).convert('RGB')
  tmp_img = product['id'] + '.jpg'
  im.save(tmp_img)

  detect_time = time.time()
  elapsed_detect_time = time.time() - detect_time
  log.info('object detection time: ' + str(elapsed_detect_time))
  classes = []
  try:
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

      object = Object()
      object.product_id = product['id']
      object.storage = 's3'
      object.bucket = AWS_OBJ_IMAGE_BUCKET
      object.class_code = obj.class_code
      classes.append(obj.class_code)
      id = save_object_to_db(object)

      object.name = id
      tmp_obj_img = id + '.jpg'
      obj_img.save(tmp_obj_img)
      save_to_storage(object)
      push_object_to_queue(object)
      # obj_img.show()
  except Exception as e:
    log.error(str(e))
    return

  c = Counter(classes)
  k = c.most_common()
  # final_class = k[0][0]
  final_class = ''
  elapsed_time = time.time() - start_time
  log.debug('Decided class_code:' + final_class)
  log.info('total object_detection time: ' + str(elapsed_time))
  return final_class

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

  object = Object()
  object.product_id = product['id']
  object.storage = 's3'
  object.bucket = AWS_OBJ_IMAGE_BUCKET
  object.class_code = '0'
  id = save_object_to_db(object)

  object.name = id
  im.save(id + '.jpg')
  save_to_storage(object)
  push_object_to_queue(object)

def push_object_to_queue(obj):
  log.info('push_object_to_queue')
  rconn.lpush(REDIS_OBJECT_INDEX_QUEUE, pickle.dumps(obj.to_dict(), protocol=2))

def save_object_to_db(obj):
  log.info('save_object_to_db')
  try:
    api_response = object_api.add_object(obj)
    log.debug(api_response)
  except ApiException as e:
    log.warn("Exception when calling ObjectApi->add_object: %s\n" % e)

  return api_response.data.object_id

def update_product_to_db(product):
  log.debug('update_product_to_db')
  try:
    api_response = product_api.update_product_by_id(product.id, product)
    log.debug(api_response)
  except ApiException as e:
    log.warn("Exception when calling ProductApi->update_product: %s\n" % e)

def check_health():
  global  heart_bit
  log.info('check_health: ' + str(heart_bit))
  if heart_bit == True:
    heart_bit = False
    Timer(HEALTH_CHECK_TIME, check_health, ()).start()
  else:
    exit()

def exit():
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
  file = obj.name + '.jpg'
  key = os.path.join(RELEASE_MODE, obj.class_code, obj.name + '.jpg')
  is_public = True
  storage.upload_file_to_bucket(AWS_OBJ_IMAGE_BUCKET, file, key, is_public=is_public)
  log.debug('save_to_storage done')

def dispatch_job(rconn):
  log.info('Start dispatch_job')
  Timer(HEALTH_CHECK_TIME, check_health, ()).start()
  while True:
    key, value = rconn.blpop([REDIS_PRODUCT_CLASSIFY_QUEUE])
    analyze_product(value)
    global  heart_bit
    heart_bit = True

if __name__ == '__main__':
  log.info('Start bl-classifier')
  try:
    Process(target=dispatch_job, args=(rconn,)).start()
  except Exception as e:
    log.error(str(e))
    exit()
