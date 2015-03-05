def sentMes(api, data):
  try:
    url = data['entities']['media'][0]['url']
    full_name = data['user']['name'].encode('utf-8')
    api.send_direct_message(data['user']['screen_name'], text='Hi, {name}! We\'ve just gotten photos from your tweet {url}. Thanks for your generosity!'.format(url=url, name=full_name))
  except Exception, e:
    print e