from ckan.lib.celery_app import celery


from time import sleep
import s
@celery.task(name = "dfmp.cleaning")
def clearing( context, data ):
  reload(s)
  s.clearing(context, data)
