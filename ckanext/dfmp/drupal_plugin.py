import ckan.plugins as p
from ckanext.dfmp.authentication.drupal_auth import DrupalAuthMiddleware

class DrupalAuthPlugin(p.SingletonPlugin):
    '''Reads Drupal login cookies to log user in.'''
    p.implements(p.IMiddleware, inherit=True)

    def make_middleware(self, app, config):
        return DrupalAuthMiddleware(app, config)