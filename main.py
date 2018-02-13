from __future__ import print_function

import os
from threading import Timer

from PIL import Image
import time
import uuid
import urllib.request
from collections import Counter
import pickle
from bluelens_spawning_pool import spawning_pool
from detect.object_detect import ObjectDetector
from stylelens_product.products import Products
from stylelens_object.objects import Objects
from stylelens_object.features import Features
from stylelens_image.images import Images
from stylelens_s3 import s3
import redis

from bluelens_log import Logging

AWS_OBJ_IMAGE_BUCKET = 'bluelens-style-object'
AWS_MOBILE_IMAGE_BUCKET = 'bluelens-style-mainimage'

OBJECT_IMAGE_WIDTH = 380
OBJECT_IMAGE_HEITH = 380
HEALTH_CHECK_TIME = 60*20

SPAWN_ID = os.environ['SPAWN_ID']
REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
RELEASE_MODE = os.environ['RELEASE_MODE']
AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY'].replace('"', '')
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY'].replace('"', '')
FEATURE_GRPC_HOST = os.environ['FEATURE_GRPC_HOST']
FEATURE_GRPC_PORT = os.environ['FEATURE_GRPC_PORT']
OD_SCORE_MIN = os.environ['OD_SCORE_MIN']

MAX_PROCESS_NUM = int(os.environ['MAX_PROCESS_NUM'])

REDIS_PRODUCT_CLASSIFY_QUEUE = 'bl_product_classify_queue'
REDIS_PRODUCT_CLASSIFY_TEXT_QUEUE = 'bl:product:classify:text:queue'
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
rconn = redis.StrictRedis(REDIS_SERVER, decode_responses=False, port=6379, password=REDIS_PASSWORD)

storage = s3.S3(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)

heart_bit = True

product_api = Products()
object_api = Objects()
feature_api = Features()
image_api = Images()
version_id = None
obj_detector = None

def analyze_product(p_data):
  # log.info('analyze_product')
  product = pickle.loads(p_data)

  try:
    main_class_code, main_objects = analyze_main_image(product)
  except Exception as e:
    log.error('analyze_product:main_image' + str(e))
    return

  #Todo Need to uncomment when we can analyze sub images
  # try:
  #   sub_class_code, sub_objects = analyze_sub_images(product['sub_images_mobile'])
  # except Exception as e:
  #   log.error('analyze_product:sub_image' + str(e))
  #   image_id, obj_ids = save_image_to_db(product, main_class_code, main_objects)
  #   update_image_id_to_object_db(obj_ids, image_id)
  #   save_main_image_as_object(product, image_id)
  #   set_product_is_classified(product)
  #   return
  # save_objects = []
  # for obj in sub_objects:
  #   if obj['class_code'] == main_class_code:
  #     save_objects.append(obj)

  image_id, obj_ids = save_image_to_db(product, main_class_code, main_objects)
  if image_id is None:
    set_product_is_classified(product)
    return

  update_image_id_to_object_db(obj_ids, image_id)
  save_main_image_as_object(product, image_id)

  #Todo Need to uncomment when we can analyze sub images
  # if len(save_objects) > 0:
  #   save_objects_to_db(str(product['_id']), image_id, main_class_code, save_objects)

  set_product_is_classified(product)

  data = {}
  data['product_id'] = str(product['_id'])
  data['image_id'] = image_id
  data['product_name'] = product.get('name')
  data['category'] = product.get('cate')
  data['tags'] = product.get('tags')
  push_image_to_queue(data)
  # color = analyze_color(p_dict)

def get_latest_crawl_version():
  value = rconn.hget(REDIS_CRAWL_VERSION, REDIS_CRAWL_VERSION_LATEST)
  return value.decode('utf-8')

def set_product_is_classified(product):
  try:
    p = {}
    p['is_classified'] = True
    p['is_available'] = True
    product_api.update_product_by_id(str(product['_id']), p)
  except Exception as e:
    log.error(str(e))

def set_product_is_unavailable(product):
  try:
    p = {}
    p['is_classified'] = True
    p['is_available'] = False
    product_api.update_product_by_id(str(product['_id']), p)
  except Exception as e:
    log.error(str(e))

def save_objects_to_db(product_id, image_id, class_code, objects):
  global version_id

  for obj in objects:
    object = {}
    object['image_id'] = image_id
    object['product_id'] = product_id
    object['storage'] = 's3'
    object['bucket'] = AWS_OBJ_IMAGE_BUCKET
    object['is_main'] = False
    object['class_code'] = class_code
    object['name'] = obj['name']
    object['version_id'] = version_id
    # object['feature'] = obj['feature']

    save_to_storage(object)

    object_id = save_object_to_db(object)
    save_feature_to_db(object_id, obj['feature'], version_id)
    # push_object_to_queue(object)
  # obj_img.show()

def save_feature_to_db(object_id, feature, version_id):
  data = {}

  data['object_id'] = object_id
  data['vector'] = feature
  data['version_id'] = version_id
  try:
    feature_id = feature_api.add_feature(data)
    if feature_id is not None:
      return feature_id
    else:
      return None
  except Exception as e:
    log.warn("Exception when calling save_feature_to_db: %s\n" % e)

def update_image_id_to_object_db(object_ids, image_id):
  # log.info('update_image_id_to_object_db')
  try:
    for id in object_ids:
      obj = {}
      obj['image_id'] = image_id
      object_api.update_object_by_id(id, obj)
  except Exception as e:
    log.warn("Exception when calling update_object_by_id: %s\n" % e)

def save_image_to_db(product, class_code, objects):
  # log.info('save_image_to_db')
  global version_id

  object_ids = []
  for obj in objects:
    save_to_storage(obj)
    obj['product_id'] = str(product['_id'])
    obj['version_id'] = version_id
    obj['host_code'] = product['host_code']
    obj['host_group'] = product['host_group']
    obj['storage'] = 's3'
    obj['is_main'] = False
    obj['bucket'] = AWS_OBJ_IMAGE_BUCKET
    feature = obj.pop('feature')
    object_id = save_object_to_db(obj)
    object_ids.append(object_id)
    save_feature_to_db(object_id, feature, version_id)

  image = {}
  image['product_id'] = str(product['_id'])
  image['main_image_mobile_full'] = product['main_image_mobile_full']
  image['main_image_mobile_thumb'] = product['main_image_mobile_thumb']
  image['product_url'] = product['product_url']
  image['price'] = product['price']
  image['host_code'] = product['host_code']
  image['host_group'] = product['host_group']
  image['host_url'] = product['host_url']
  image['host_name'] = product['host_name']
  image['product_no'] = product['product_no']
  image['product_name'] = product['name']
  image['class_code'] = class_code
  image['currency_unit'] = product.get('currency_unit', None)
  image['nation'] = product.get('nation', None)
  image['tags'] = product.get('tags', None)
  image['cate'] = product.get('cate', None)
  image['sale_price'] = product.get('sale_price', None)
  image['objects'] = object_ids
  image['version_id'] = version_id

  # image['color_code'] = ''
  # image['sex_code'] = ''
  # image['age_code'] = ''

  try:
    api_response = image_api.add_image(image)
    if api_response is not None:
      if 'upserted' in api_response:
        image_id = str(api_response['upserted'])
        return image_id, object_ids
    # log.debug(api_response)
  except Exception as e:
    log.warn("Exception when calling add_image: %s\n" % e)

  return None, None

def analyze_color(product):
  # log.debug('analyze_color')
  color = ''
  return color

def analyze_category(product):
  # log.debug('analyze_category')
  category = 1
  return category

def analyze_main_image(product):
  # log.info('analyze_main_image')

  image = product['main_image_mobile_full']
  classes = []
  objects = []

  try:
    class_code, detected_objects = object_detect(image, product)
    if class_code != None and detected_objects != None:
      classes.append(class_code)
      objects.extend(detected_objects)
  except Exception as e:
    log.error('analyze_main_image2: ' + str(e))
    return

  final_class = None
  score = 0.0
  if len(objects) > 0:
    for obj in objects:
      if obj['score'] > score:
        score = obj['score']
        final_class = obj['class_code']

  return final_class, objects

def analyze_sub_images(images):
  # log.info('analyze_sub_images')

  classes = []
  objects = []

  for image in images:
    try:
      class_code, detected_objects = object_detect(image)
      if class_code != None and detected_objects != None:
        classes.append(class_code)
        objects.extend(detected_objects)
    except Exception as e:
      log.error('analyze_sub_images: ' + str(e))
      return

  final_class = None
  final_objects = []
  if len(images) > 1:
    try:
      c = Counter(classes)
      k = c.most_common()
      final_class = k[0][0]
      #log.debug('analyze_class: ' + final_class)
      for obj in objects:
        if obj['class_code'] == final_class:
          final_objects.append(obj)
    except Exception as e:
      log.error('analyze_sub_images2: ' + str(e))
  else:
    score = 0.0
    for obj in objects:
      if obj['score'] > score:
        score = obj['score']
        final_class = obj['class_code']

  return final_class, final_objects

def object_detect(image_path, product):
  # log.info('object_detect:start')
  start_time = time.time()
  #log.info(image_path)
  try:
    f = urllib.request.urlopen(image_path)
  except Exception as e:
    log.error('object_detect urlopen: ' + str(e))
    return
  im = Image.open(f).convert('RGB')
  tmp_img = 'tmp.jpg'
  im.save(tmp_img)

  classes = []
  detected_objects = []
  global obj_detector
  try:
    objects = obj_detector.getObjects(tmp_img)
    for obj in objects:
      #log.info(obj.class_name + ':' + str(obj.score))
      location = obj.get('location')

      left =   location.get('left')
      right =  location.get('right')
      top =    location.get('top')
      bottom = location.get('bottom')
      area = (left, top, left + abs(left-right), top + abs(bottom-top))
      obj_img = im.crop(area)
      size = OBJECT_IMAGE_WIDTH, OBJECT_IMAGE_HEITH
      obj_img.thumbnail(size, Image.ANTIALIAS)

      id = str(uuid.uuid4())
      tmp_obj_img = id + '.jpg'
      obj_img.save(tmp_obj_img)
      classes.append(obj.get('class_code'))
      image_obj = {}
      image_obj['class_code'] = obj.get('class_code')
      image_obj['name'] = id
      image_obj['score'] = obj.get('score')
      image_obj['feature'] = obj.get('feature')
      box = {}
      box['left'] = left
      box['right'] = right
      box['top'] = top
      box['bottom'] = bottom
      image_obj['box'] = box
      detected_objects.append(image_obj)

  except Exception as e:
    log.error('object_detect:' + str(e))
    if 'StatusCode.UNKNOWN' in str(e):
      # delete_product_from_db(str(product['_id']))
      set_product_is_unavailable(product)
    return

  final_class = None

  if len(classes) > 0:
    try:
      c = Counter(classes)
      k = c.most_common()
      final_class = k[0][0]
      print(final_class)
      #log.debug('Decided class_code:' + final_class)
    except Exception as e:
      log.warn(str(e))
  elapsed_time = time.time() - start_time
  log.info('total object_detection time: ' + str(elapsed_time))
  return final_class, detected_objects

def delete_product_from_db(product_id):
  # log.info('delete_product_from_db: ' + product_id)
  try:
    product_api.delete_product(product_id)
  except Exception as e:
    log.error(str(e))

def save_main_image_as_object(product, image_id):
  # log.info('save_main_image_as_object')
  global version_id
  try:
    f = urllib.request.urlopen(product['main_image'])
  except Exception as e:
    log.error(str(e))
    return
  im = Image.open(f).convert('RGB')
  size = OBJECT_IMAGE_WIDTH, OBJECT_IMAGE_HEITH
  im.thumbnail(size, Image.ANTIALIAS)

  object = {}
  object['product_id'] = str(product['_id'])
  object['storage'] = 's3'
  object['bucket'] = AWS_OBJ_IMAGE_BUCKET
  object['class_code'] = '0'
  object['is_main'] = True
  object['host_code'] = product['host_code']
  object['host_group'] = product['host_group']
  object['image_id'] = image_id
  object['version_id'] = version_id
  id = str(uuid.uuid4())
  object['name'] = id
  im.save(id + '.jpg')
  save_to_storage(object)
  save_object_to_db(object)
  # push_object_to_queue(object)

def push_image_to_queue(data):
  # log.info('push_image_to_queue')
  rconn.lpush(REDIS_PRODUCT_CLASSIFY_TEXT_QUEUE, pickle.dumps(data))

def save_object_to_db(obj):
  # log.info('save_object_to_db')
  try:
    object_id = object_api.add_object(obj)
    # log.debug(object_id)
  except Exception as e:
    log.warn("Exception when calling add_object: %s\n" % e)
  return object_id

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
  data['key'] = 'SPAWN_ID'
  data['value'] = SPAWN_ID
  spawn = spawning_pool.SpawningPool()
  spawn.setServerUrl(REDIS_SERVER)
  spawn.setServerPassword(REDIS_PASSWORD)
  spawn.delete(data)

def save_to_storage(obj):
  # log.debug('save_to_storage')
  try:
    file = obj['name'] + '.jpg'
    key = os.path.join(RELEASE_MODE, obj['class_code'], file)
    is_public = True
    path = storage.upload_file_to_bucket(AWS_OBJ_IMAGE_BUCKET, file, key, is_public=is_public)
    obj['image_url'] = path
  except Exception as e:
    log.error('save_to_storage: ' + str(e))

  # log.debug('save_to_storage done')

def start(rconn):
  global version_id
  global obj_detector
  version_id = get_latest_crawl_version()
  obj_detector = ObjectDetector()

  log.info('Start dispatch_job')

  Timer(HEALTH_CHECK_TIME, check_health, ()).start()
  count = 0
  while True:
    key, value = rconn.blpop([REDIS_PRODUCT_CLASSIFY_QUEUE])
    if value is not None:
      analyze_product(value)
    global  heart_bit
    heart_bit = True

    # count = count + 1
    # if count > MAX_PROCESS_NUM:
    #   delete_pod()

if __name__ == '__main__':
  try:
    log.info('Start bl-object-classifier:4')
    start(rconn)
  except Exception as e:
    log.error('main; ' + str(e))
    delete_pod()
