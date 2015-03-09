from ckan.lib.celery_app import celery


import dinamic

@celery.task(name = "dfmp.cleaning")
def clearing( context, data ):
    reload(dinamic)
    dinamic.datastore_mass(context, data, 'clearing')

@celery.task(name = "dfmp.getting_tweets")
def getting_tweets( context, data ):
    reload(dinamic)
    dinamic.datastore_mass(context, data, 'getting_tweets')

@celery.task(name = "dfmp.streaming_tweets")
def streaming_tweets( context, data ):
    reload(dinamic)
    dinamic.datastore_mass(context, data, 'streaming_tweets')

@celery.task(name = "dfmp.getting_flickr")
def getting_flickr( context, data ):
    reload(dinamic)
    dinamic.datastore_mass(context, data, 'getting_flickr')

@celery.task(name = "dfmp.indexing_solr")
def indexing_solr( context, data ):
    reload(dinamic)
    dinamic.datastore_mass(context, data, 'indexing_solr')