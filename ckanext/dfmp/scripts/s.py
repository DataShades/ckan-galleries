def sentMes(api, data):
  try:
    url = data['entities']['media'][0]['url']
    name = data['user']['screen_name']
    api.update_status(status='Hi, @{name}! We\'ve just gotten photos from your tweet {url}. Thanks for your generosity!'.format(url=url, name=name))
  except Exception, e:
    print e