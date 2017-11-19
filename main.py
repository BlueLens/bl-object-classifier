from __future__ import print_function

import os
from multiprocessing import Process
from PIL import Image
import urllib.request
import pickle
from detect.object_detect import ObjectDetector
from stylelens_product import Product
from stylelens_product import Object
from stylelens_product import ProductApi
from stylelens_product import ObjectApi
from stylelens_product.rest import ApiException
from six import integer_types, iteritems
from util import s3
import uuid
import redis

from bluelens_log import Logging


TMP_IMG = 'tmp.jpg'
TMP_OBJ_IMG = 'obj.jpg'

AWS_OBJ_IMAGE_BUCKET = 'bluelens-style-object'
AWS_MOBILE_IMAGE_BUCKET = 'bluelens-style-mainimage'

MOBILE_FULL_WIDTH = 343
MOBILE_THUMBNAIL_WIDTH = 163
TMP_MOBILE_IMG = 'tmp_mobile_full.jpg'
TMP_MOBILE_THUMB_IMG = 'tmp_mobile_thumb.jpg'

REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY'].replace('"', '')
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY'].replace('"', '')

REDIS_PRODUCT_CLASSIFY_QUEUE = 'bl:product:classify:queue'

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-classifier')
product_api = ProductApi()
object_api = ObjectApi()
rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

storage = s3.S3(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)

def analyze_product(p_data):
  product = pickle.loads(p_data)
  log.debug(product)
  analyze_image(product)


def define_class(product):
  product_api.update_product(product)

def make_mobile_image(image_name, type, image_path):

  if type == 'full':
    basewidth = MOBILE_FULL_WIDTH
  else:
    basewidth = MOBILE_THUMBNAIL_WIDTH

  f = urllib.request.urlopen(image_path)
  im = Image.open(f).convert('RGB')
  wpercent = (basewidth / float(im.size[0]))
  hsize = int((float(im.size[1]) * float(wpercent)))
  im = im.resize((basewidth, hsize), Image.ANTIALIAS)
  im.save(TMP_MOBILE_IMG)
  file_url = save_mobile_image_to_storage(image_name, type, TMP_MOBILE_IMG)
  return file_url

def make_mobile_images(product_dic):
  full_image = make_mobile_image(product_dic['id'], 'full', product_dic['main_image'])
  thumb_image = make_mobile_image(product_dic['id'], 'thumb', product_dic['main_image'])
  product = Product()
  product.id = product_dic['id']
  product.main_image_mobile_full = full_image
  product.main_image_mobile_thumb = thumb_image
  update_product_to_db(product)

def save_mobile_image_to_storage(name, path, image_file):
  log.debug('save_mobile_image_to_storage')
  key = os.path.join('mobile', path, name + '.jpg')
  is_public = True
  file_url = storage.upload_file_to_bucket(AWS_MOBILE_IMAGE_BUCKET, image_file, key, is_public=is_public)
  log.info(file_url)
  return file_url

def analyze_image(product):
  images = []

  make_mobile_images(product)

  images.append(product['main_image'])
  images.extend(product['sub_images'])
  for image in images:
    object_detect(product, image)

def object_detect(product, image_path):
  log.info(image_path)
  f = urllib.request.urlopen(image_path)
  im = Image.open(f).convert('RGB')
  size = 600, 600
  im.thumbnail(size, Image.ANTIALIAS)
  im.save(TMP_IMG)

  od = ObjectDetector()
  objects = od.getObjects(TMP_IMG)
  for obj in objects:
    log.info(obj.class_name + ':' + str(obj.score))
    left = obj.location.left
    right = obj.location.right
    top = obj.location.top
    bottom = obj.location.bottom
    area = (left, top, left + abs(left-right), top + abs(bottom-top))
    obj_img = im.crop(area)
    size = 300, 300
    obj_img.thumbnail(size, Image.ANTIALIAS)
    obj_img.save(TMP_OBJ_IMG)

    object = Object()
    object.product_id = product['id']
    object.storage = 's3'
    object.bucket = AWS_OBJ_IMAGE_BUCKET
    object.class_code = obj.class_code
    id = save_object_to_db(object)

    object.name = id
    save_to_storage(object)
    obj_img.show()

def save_object_to_db(obj):
  log.debug('save_to_db')
  try:
    api_response = object_api.add_object(obj)
    log.debug(api_response)
  except ApiException as e:
    log.warn("Exception when calling ObjectApi->add_object: %s\n" % e)

  return api_response.data.object_id

def update_product_to_db(product):
  log.debug('update_product_to_db')
  try:
    api_response = product_api.update_product(product)
    log.debug(api_response)
  except ApiException as e:
    log.warn("Exception when calling ProductApi->update_product: %s\n" % e)


def save_to_storage(obj):
  log.debug('save_to_storage')
  key = os.path.join(obj.class_code, obj.name + '.jpg')
  is_public = True
  storage.upload_file_to_bucket(AWS_OBJ_IMAGE_BUCKET, TMP_OBJ_IMG, key, is_public=is_public)
  log.debug('save_to_storage done')

def dispatch_detect_job(rconn):
  while True:
    key, value = rconn.blpop([REDIS_PRODUCT_CLASSIFY_QUEUE])
    analyze_product(value.decode('utf-8'))

def dispatch_detect_job(rconn):
  log.info('Start dispatch_detect_job')
  while True:
    key, value = rconn.blpop([REDIS_PRODUCT_CLASSIFY_QUEUE])
    analyze_product(value)

if __name__ == '__main__':
  Process(target=dispatch_detect_job, args=(rconn,)).start()
  # Process(target=dispatch_classifier, args=(rconn,)).start()
