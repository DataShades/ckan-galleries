from ckan.lib.celery_app import celery


import dinamic, os

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

@celery.task(name = "dfmp.indexing_solr")
def indexing_solr( context, data ):
    reload(dinamic)
    dinamic.datastore_mass(context, data, 'indexing_solr')

@celery.task(name = "dfmp.flickr_images")
def flickr_images( context, data ):
    reload(dinamic)
    dinamic.flickr_add_image_to_dataset(context, data)

@celery.task(name = "dfmp.revoke_listener")
def revoke_listener( context, data ):
    int(data['id'])
    reload(dinamic)
    print 'TRY TO KILL PROCESS'
    dinamic.revoke(data)