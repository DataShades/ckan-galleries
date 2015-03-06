from ckan.lib.celery_app import celery


from time import sleep
import dinamic
@celery.task(name = "dfmp.cleaning")
def clearing( context, data ):
  # for i in range(10000):
    reload(dinamic)
    dinamic.clearing(context, data)
    # sleep(10)
