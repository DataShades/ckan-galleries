from ckan.plugins import toolkit
import ckan.lib.base as base
import ckan.plugins as plugins


from pylons.controllers.util import redirect_to
from ckan.common import c, request
import ckan.model as model


import logging
log = logging.getLogger(__name__)


class DFMPController(toolkit.BaseController):
  def api_login(self):
    key = request.GET.get('api_key')
    if key:
      for item in plugins.PluginImplementations(plugins.IAuthenticator):
        item.login(key)
    return base.render('home/about.html')