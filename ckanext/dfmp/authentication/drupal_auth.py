import Cookie
import logging
import hashlib
import ckan.lib.base as base
log = logging.getLogger(__name__)

class DrupalAuthMiddleware(object):
  '''Allows CKAN user to login via Drupal.'''

  def __init__(self, app, app_conf):
    self.app = app
  def __call__(self, environ, start_response):
    current_url = environ.get('CKAN_CURRENT_URL')
    redirects = {
      '/user/login':'/user/login',
      '/user/register':'/user/register',
      '/user/_logout':'/user/logout',
    }
    if current_url in redirects:
      base.redirect(redirects[current_url])

    # new_headers = []
    # self.do_drupal_login_logout(environ, new_headers)

    # def cookie_setting_start_response(status, headers, exc_info=None):
    #     if headers:
    #         headers.extend(new_headers)
    #     else:
    #         headers = new_headers
    #     return start_response(status, headers, exc_info)
    # new_start_response = cookie_setting_start_response

    return self.app(environ, start_response)


    # if that int() raises a ValueError then the app will not start

  def _parse_cookies(self, environ):
    is_ckan_cookie = [False]
    drupal_session_id = [False]
    drupal_apikey = [False]
    server_name = environ['SERVER_NAME']

    import pprint
    for k, v in environ.items():
      key = k.lower()

      if key  == 'http_cookie':
        is_ckan_cookie[0] = self._is_this_a_ckan_cookie(v)
        drupal_session_id[0], drupal_apikey[0] = self._drupal_cookie_parse(v, server_name)
    is_ckan_cookie = is_ckan_cookie[0]
    drupal_session_id = drupal_session_id[0]
    drupal_apikey = drupal_apikey[0]
    return is_ckan_cookie, drupal_session_id, drupal_apikey

  @staticmethod
  def _drupal_cookie_parse(cookie_string, server_name):
    '''Returns the Drupal Session ID from the cookie string.'''
    cookies = Cookie.SimpleCookie()
    apikey = [False]
    try:
      cookies.load(str(cookie_string))
    except Cookie.CookieError:
      log.error("Received invalid cookie: %s" % cookie_string)
      return False, False
    similar_cookies = []
    for cookie in cookies:
      if cookie.startswith('Drupal.visitor.apikey'):
        apikey = cookies[cookie].value
    for cookie in cookies:
      if cookie.startswith('SESS') or cookie.startswith('SSESS'):
        # Drupal 6 uses md5, Drupal 7 uses sha256
        server_hash = hashlib.sha256(server_name).hexdigest()[:32]
        if cookie == 'SESS%s' % server_hash:
          log.debug('Drupal cookie found for server request %s', server_name)
          return cookies[cookie].value, apikey
        elif cookie == 'SSESS%s' % server_hash:
          log.debug('Drupal cookie (secure) found for server request %s', server_name)
          return cookies[cookie].value, apikey
        else:
          similar_cookies.append(cookie)
    if similar_cookies:
      log.debug('Drupal cookies ignored with incorrect hash for server %r: %r',
                server_name, similar_cookies)
    return None, None

  @staticmethod
  def _is_this_a_ckan_cookie(cookie_string):
    cookies = Cookie.SimpleCookie()
    try:
      cookies.load(str(cookie_string))
    except Cookie.CookieError:
      log.warning("Received invalid cookie: %s" % cookie_string)
      return False

    if not 'auth_tkt' in cookies:
      return False
    return True

  def _log_out(self, environ, new_headers):
    # don't progress the user info for this request
    environ['REMOTE_USER'] = None
    environ['repoze.who.identity'] = None

    log.debug('Logged out Drupal user')

  def do_drupal_login_logout(self, environ, new_headers):
    is_ckan_cookie, drupal_session_id, drupal_apikey = self._parse_cookies(environ)

    if drupal_apikey:
      self._do_drupal_login_by_key(environ, drupal_apikey, new_headers)
    else:
      self._log_out(environ, new_headers)


  def _do_drupal_login_by_key(self, environ, drupal_apikey, new_headers):
    from ckan import model
    from ckan.model.meta import Session
    try:
      user = Session.query(model.User).filter_by(apikey=drupal_apikey).one()
    except Exception, e:
      log.debug('Drupal user not found in CKAN: %s', e)
      return
    log.debug('Drupal user found in CKAN: %s', user.name)

    # if user.email != user_dict['email'] or \
    #         user.fullname != user_dict['name']:
    #     user.email = user_dict['email']
    #     user.fullname = user_dict['fullname']
    #     log.debug('User details updated from Drupal: %s %s',
    #               user.email, user.fullname)
    #     model.Session.commit()

    new_headers[:] = [(key, value) for (key, value) in new_headers \
                        if (not (key=='Set-Cookie' and value.startswith('auth_tkt="INVALID"')))]

    # Tell app during this request that the user is logged in
    environ['REMOTE_USER'] = user.name
    log.debug('Set REMOTE_USER = %r', user.name)

