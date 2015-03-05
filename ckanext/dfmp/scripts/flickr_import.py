import flickrapi, math

# group not found action
def group_not_found (message) :
	# CODE THAT SAYS USER THAT URL IS INCORRECT AND GROUP NOT FOUND
	print message
	exit()

# creates dataset
def flickr_group_pool_create_dataset(dataset) :
	# CREATES DATSET AND RETUNS IT ID OR DATASET OBJECT OR DICT
	return dict()

# adds resource to dataset
def flickr_group_pool_add_resource(dataset, resource) :
	# ADDS RESOURCE TO DATASET
	return True

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

# FlickrAPI init
api_key = u'1903a21d2f1e99652164ad8c681e4b22'
api_secret = u'34ed321a99cb93f6'
flickr = flickrapi.FlickrAPI(api_key, api_secret, format='parsed-json')

# Harvesting options
photos_per_iteration = 100.
group_url = u"https://www.flickr.com/groups/abccanberra/pool/55001756@N05/" ### SHOULD BE PROVIDED BY USER  ###

# Group lookup
group = flickr.urls.lookupGroup(url=group_url)

# Harvesting images from the group if possible
if group[u"stat"] == u"fail" :
	group_not_found(group[u"message"])
else :
	# We need to get group info for dataset
	group_info = flickr.groups.getInfo(group_id=group[u"group"][u"id"])

	dataset = {
		u"group_id" : group[u"group"][u"id"],
		u"name" : group[u"group"][u"groupname"][u"_content"],
		u"path_alias" : group_info[u"group"][u"path_alias"],
		u"description" : group_info[u"group"][u"description"][u"_content"]
	}

	dataset = flickr_group_pool_create_dataset(dataset)

	# We get the total number of photos in the pool here
	photos = flickr.groups.pools.getPhotos(group_id=group[u"group"][u"id"], per_page=1, page=1)
	total = int(photos[u"photos"][u"total"])

	#counting the number of iterations
	number_of_iterations = math.ceil(total / photos_per_iteration)

	# request photo splited into pages
	for iteration in range(1, int(number_of_iterations) + 1) :
		batch = flickr.groups.pools.getPhotos(group_id=group['group']['id'], per_page=photos_per_iteration, page=iteration, extras=u"description, license, original_format, geo,tags, machine_tags, url_sq, url_t, url_s, url_q, url_m, url_n, url_z, url_c, url_l, url_o")

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
			flickr_group_pool_add_resource(dataset, resource)
