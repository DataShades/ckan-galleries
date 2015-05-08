import ckan.model as model
session = model.Session
import json
import itertools
import nose.tools as nt
from pylons import config
from copy import deepcopy

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
        email='test.user@selenium.dfmp.test',
      )

    def setUp(self):
      self.driver = w.Remote(self.host, self.browser)


    def tearDown(self):
      self.driver.close()


    def _user_login(self, login_action='/login_generic', redirect_url='/', **params):
      site_url = config.get('ckan.site_url')
      url = site_url + '/about'

      self.driver.get(url)

      self.driver.execute_script('''
        var form = document.createElement("form");
        form.setAttribute("method", "post");
        form.setAttribute("action", "%s?came_from=%s");

        params = %s;

        for(var key in params) {
                var hiddenField = document.createElement("input");
                hiddenField.setAttribute("name", key);
                hiddenField.setAttribute("value", params[key]);
                form.appendChild(hiddenField);
        }

        document.body.appendChild(form);
        form.submit();
      '''%(login_action, redirect_url, params))

      


class TestFirefox(AbstractBrowser):
  def setUp(self):
    super(TestFirefox, self).setUp()
    self._create_common_user()
    user = session.query(model.User).get(self.common_user['id'])
    self.common_user_dict0 = deepcopy(self.common_user_dict)
    self.common_user_dict['name'] = 'atest_user_created_for_selenium_probably_unique_name1'

    self._create_common_user()
    user1 = session.query(model.User).get(self.common_user['id'])
    self.common_user_dict1 = deepcopy(self.common_user_dict)

    self.context = dict(
      model=model,
      user=user.name,
      auth_user_obj=user
    )
    asset_dict = {
      'url':'http://some.image/com/a.jpg',
      'metadata': json.dumps({})
    }

    self.asset = dfmp_plugin.user_add_asset(self.context, asset_dict)

  def tearDown(self):
    super(TestFirefox, self).tearDown()
    dfmp_plugin.user_remove_asset(self.context, {'id': self.asset['parent_id'], 'assetID': self.asset['assetID']})

    self._purge_dataset(self.asset['parent_id'])
    self._purge_common_user(self.common_user_dict0['name'])
    self._purge_common_user(self.common_user_dict1['name'])


  def test_asset_edit_page(self):
    site_url = config.get('ckan.site_url')
    # logins user and goes to assets edit page
    self._user_login(redirect_url=site_url + '/asset', login=self.common_user_dict0['name'], password=self.common_user_dict0['password'])
    self.driver.get(site_url + '/asset/' + self.asset['parent_id'] + '/' + self.asset['assetID'] + '/edit')

    # checks if edit form exists
    edit_form = self.driver.find_element_by_id('asset-edit-form')
    assert edit_form

    # checks if edit fields exist
    last_modified = self.driver.find_element_by_id('field-last_modified')
    name = self.driver.find_element_by_id('field-name')
    assert last_modified, name

    # updates asset
    new_date = '2015-05-06 13:00:00'
    last_modified.clear()
    last_modified.send_keys(new_date)
    new_name = 'new name'
    name.clear()
    name.send_keys(new_name)
    edit_form.submit()

    # checks updated fileds
    last_modified = self.driver.find_element_by_id('field-last_modified')
    name = self.driver.find_element_by_id('field-name')
    nt.assert_equal(last_modified.get_attribute('value'), new_date)
    nt.assert_equal(name.get_attribute('value'), new_name)


  def test_asset_edit_button(self):

    site_url = config.get('ckan.site_url')

    self._user_login(redirect_url=site_url + '/asset', login=self.common_user_dict0['name'], password=self.common_user_dict0['password'])
    asset_url_edit = '/asset/' + self.asset['parent_id'] + '/' + self.asset['assetID'] + '/edit'
    asset_url_link = '/gallery/item/' + self.asset['parent_id'] + '/' + self.asset['assetID']

    user_asset_edit = self.driver.find_elements_by_xpath('//a[@href="' + asset_url_edit + '"]')
    user_asset_link = self.driver.find_elements_by_xpath('//a[@href="' + asset_url_link + '"]')

    assert user_asset_edit, 'Edit link does not exist'
    assert user_asset_link, 'Assert link does not exist'

    self.driver.get(site_url + '/user/logout')
    self._user_login(redirect_url=site_url + '/asset', login=self.common_user_dict1['name'], password=self.common_user_dict1['password'])

    user_asset_edit = self.driver.find_elements_by_xpath('//a[@href="' + asset_url_edit + '"]')
    user_asset_link = self.driver.find_elements_by_xpath('//a[@href="' + asset_url_link + '"]')
    assert not user_asset_edit, 'Edit link exist'
    assert user_asset_link, 'Assert link does not exist'

