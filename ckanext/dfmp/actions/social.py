import ckan.plugins.toolkit as toolkit
from ckan.logic import side_effect_free
from ckanext.dfmp.scripts.flickr_import import flickr_group_pool_import
import logging
log = logging.getLogger(__name__)

# Imports images from Flickr group pool
@side_effect_free
def flickr_import_group_pool (context, data_dict):
    '''Imports images from Flickr group pool'''

    url = data_dict[u"url"]
    return flickr_group_pool_import(context, url)

