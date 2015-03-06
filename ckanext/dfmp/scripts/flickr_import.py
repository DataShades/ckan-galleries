import flickrapi, math
import ckan.plugins.toolkit as toolkit
import logging
from datetime import datetime
import uuid
log = logging.getLogger(__name__)

# group not found action
def group_not_found (type, message) :
	# CODE THAT SAYS USER THAT URL IS INCORRECT AND GROUP NOT FOUND
	import ckan.plugins.toolkit as toolkit

	raise getattr(toolkit, type)(message)

# creates dataset
def flickr_group_pool_create_dataset(context, dataset) :
    try:
        package = toolkit.get_action('package_create')(context, {
            'name' : u"flickr_pool_" + dataset[u"path_alias"],
            'title' : u"Flickr - " + dataset[u"name"],
            'notes' : dataset[u"description"],
            'tags' : []
        })
        try:
            toolkit.get_action('package_owner_org_update')(context, {
                'id' : package['id'],
                'organization_id' : 'brand-cbr'
            })
        except Exception:
            log.warn('Error during adding user to organization ')

    except toolkit.ValidationError, e:
        group_not_found('ValidationError', u"This group pool has been allready imported")

    return package

# adds resource to dataset
def flickr_group_pool_add_resource(context, dataset, resource) :
    # organization = _organization_from_list(context['auth_user_obj'].get_groups())[2]
    # data_dict['owner_name'] = organization.title  if organization else context['auth_user_obj'].name

    # Gets spatial coords if they exist
    if resource[u"spatial"][u"latitude"] and resource[u"spatial"][u"longitude"]:
        location =  {
            "type": "Point",
            "coordinates": [
                float(resource[u"spatial"][u"latitude"]),
                float(resource[u"spatial"][u"longitude"])
            ]
        }
    else:
        location = None

    # Gets resource license
    if resource[u"metadata"][u"license"]:
        resource['license_name'] = resource[u"metadata"][u"license"]

    # Parent dataset
    package_id = dataset['name']
    package = context['session']\
        .query(context['model'].Package)\
        .filter_by(name=package_id)\
        .first()

    # creates resource if it is not created yet
    if not package.resources:
        parent = toolkit.get_action('resource_create')(context, {
            'package_id' : package_id,
            'url' : 'http://web.actgdfmp.links.com.au',
            'name':'Asset'
        })
    else:
        group_not_found('ValidationError', u"This group pool has been allready imported")

    if parent.get('datastore_active'):
        pass
    else:
        toolkit.get_action('datastore_create')(context, {
            'resource_id' : parent['id'],
            'force': True,
            'fields':[
                {'id' : 'assetID', 'type' : 'text'},
                {'id' : 'lastModified', 'type' : 'text'},
                {'id' : 'name', 'type' : 'text'},
                {'id' : 'url', 'type' : 'text'},
                {'id' : 'spatial', 'type' : 'json'},
                {'id' : 'metadata', 'type' : 'json'},
            ],
            'primary_key' : ['assetID'],
            'indexes' : ['name', 'assetID']
        })

    datastore_item = toolkit.get_action('datastore_upsert')(context, {
        'resource_id' : parent['id'],
        'force': True,
        'records':[
            {
                'assetID' : str(unicode(uuid.uuid4())),
                'lastModified' : datetime.now().isoformat(' '),
                'name' : resource['name'],
                'url' : resource['url'],
                'spatial' : location,
                'metadata' : resource,
            }
        ],
        'method':'insert'
    })

    return datastore_item

# get existing original url
def flickr_group_pull_get_existing_original_url(resource) :
	# url priority
	urls = (u"url_o", u"url_l", u"url_c", u"url_z", u"url_n", u"url_m", u"url_q", u"url_s", u"url_t", u"url_sq")

	image_path = ''
	for path in urls :
		if (resource.get(path)) :
			image_path = resource[path]
			break

	return image_path

# get existing url
def flickr_group_pull_get_existing_thumbnail_url(resource) :
	# url priority
	urls = (u"url_sq", u"url_t", u"url_s", u"url_q", u"url_m", u"url_n", u"url_z", u"url_c", u"url_l", u"url_o")

	image_path = ''
	for path in urls :
		if (resource.get(path)) :
			image_path = resource[path]
			break

	return image_path

def flickr_group_pool_import (context, url) :
    # FlickrAPI init
    api_key = u'1903a21d2f1e99652164ad8c681e4b22'
    api_secret = u'34ed321a99cb93f6'
    flickr = flickrapi.FlickrAPI(api_key, api_secret, format='parsed-json')

    # Harvesting options
    photos_per_iteration = 100.
    group_url = url #u"https://www.flickr.com/groups/abccanberra/pool/55001756@N05/" ### SHOULD BE PROVIDED BY USER  ###

    # Group lookup
    group = flickr.urls.lookupGroup(url=group_url)
    # log.info(group)

    # Harvesting images from the group if possible
    if group[u"stat"] == u"fail" :
        group_not_found('ObjectNotFound', group[u"message"])
    else :
        # We need to get group info for dataset
        group_info = flickr.groups.getInfo(group_id=group[u"group"][u"id"])

        dataset = {
            u"group_id" : group[u"group"][u"id"],
            u"name" : group[u"group"][u"groupname"][u"_content"],
            u"path_alias" : group_info[u"group"][u"path_alias"],
            u"description" : group_info[u"group"][u"description"][u"_content"]
        }
        dataset = flickr_group_pool_create_dataset(context, dataset)

        # We get the total number of photos in the pool here
        photos = flickr.groups.pools.getPhotos(group_id=group[u"group"][u"id"], per_page=1, page=1)
        total = int(photos[u"photos"][u"total"])

        #counting the number of iterations
        number_of_iterations = math.ceil(total / photos_per_iteration)

        # reseting total for custom counter
        total = 0

        # request photo splited into pages
        for iteration in range(1, int(number_of_iterations) + 1) :
            batch = flickr.groups.pools.getPhotos(group_id=group['group']['id'], per_page=photos_per_iteration, page=iteration, extras=u"description, license, original_format, geo,tags, machine_tags, url_sq, url_t, url_s, url_q, url_m, url_n, url_z, url_c, url_l, url_o")
            total += len(batch[u"photos"][u"photo"])

            # process each photo
            for photo in batch[u"photos"][u"photo"] :
                # fetches resource data
                resource = {
                    u"name" : photo.get(u"title", photo[u"title"]),
                    # gets first available url
                    u"url" : flickr_group_pull_get_existing_original_url(photo),
                    u"spatial" : {
                        u"latitude" : photo.get(u"latitude"),
                        u"longitude" : photo.get(u"longitude"),
                    },
                    u"metadata" : {
                        u"mimetype" : u"image/" + photo.get(u"originalformat", ''),
                        # gets first available url
                        u"thumb" : flickr_group_pull_get_existing_thumbnail_url(photo),
                        u"text" : photo[u"description"][u"_content"],
                        u"time" : photo.get(u"dateadded"),
                        u"name" : photo.get(u"title"),
                        u"tags" : photo.get(u"tags"),
                        u"machine_tags" : photo.get(u"machine_tags"),
                        u"license" : photo.get(u"license"),
                        u"flickr_id" : photo.get(u"license"),
                    }
                }
                # adds resource to dataset
                flickr_group_pool_add_resource(context, dataset, resource)
        return str(total) + u" successfully imported from the pool"