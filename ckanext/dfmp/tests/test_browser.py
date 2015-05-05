import ckan.model as model
session = model.Session

import itertools
import nose.tools as nt
from pylons import config

import ckan.plugins.toolkit as toolkit

import ckanext.dfmp.actions.action as common_actions
import ckanext.dfmp.plugin as dfmp_plugin
from ckanext.dfmp.tests.test import AbstractDFMP

from selenium import webdriver as w


class AbstractBrowser(AbstractDFMP):

    def __init__(self, host=None, browser='FIREFOX'):
      super(AbstractBrowser, self).__init__()
      self.host = host or config.get('dfmp.selenium_url', 'http://127.0.0.1:4444/wd/hub')
      self.browser = getattr(w.DesiredCapabilities, browser)

      self.common_user_dict = dict(
        password='aA129kk',
        name='test_user_created_for_selenium_probably_unique_name',
        email='test.user@selenium.dfmp.test'
      )

    def setUp(self):
      self.driver = w.Remote(self.host, self.browser)
      self._create_common_user()


    def tearDown(self):
      self.driver.close()
      self._purge_common_user()

      


class TestFirefox(AbstractBrowser):
  def test_asset_edit_page(self):
    user = session.query(model.User).get(self.common_user['id'])
    context = dict(
      model=model,
      user=user.name,
      auth_user_obj=user
    )
    asset_dict = {
      'url':'http://some.image/com/a.jpg',

    }
    asset = dfmp_plugin.user_add_asset(context, asset_dict)






    dfmp_plugin.user_remove_asset(context, {'id': asset['parent_id'], 'assetID': asset['assetID']})



