import ckan.model as model
session = model.Session
import json
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
        email='test.user@selenium.dfmp.test',
      )

    def setUp(self):
      self.driver = w.Remote(self.host, self.browser)
      self._create_common_user()
      user = session.query(model.User).get(self.common_user['id'])
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
      self.driver.close()
      dfmp_plugin.user_remove_asset(self.context, {'id': self.asset['parent_id'], 'assetID': self.asset['assetID']})
      self._purge_common_user()


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
  def test_asset_edit_page(self):
    site_url = config.get('ckan.site_url')
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

    # get first new user object
    user = session.query(model.User).get(self.common_user['id'])

    # create user asset
    context = dict(
      model=model,
      user=user.name,
      auth_user_obj=user
    )
    asset_dict = {
      'url':'http://some.image/com/a.jpg',
      'metadata': json.dumps({})
    }
    asset = dfmp_plugin.user_add_asset(context, asset_dict)

    self._user_login(login=self.common_user_dict['name'], password=self.common_user_dict['password'])

    print(asset)
    site_url = config.get('ckan.site_url')
    print dir(self.driver)
    # assert False, 'FUck'
    self.driver.get(site_url + '/asset')

    dfmp_plugin.user_remove_asset(context, {'id': asset['parent_id'], 'assetID': asset['assetID']})

    # create second user
    # self.common_user_dict = dict(
    #     password='aA129kk',
    #     name='test_user_created_for_selenium_probably_unique_name1',
    #     email='test.user@selenium.dfmp.test',
    #   )
    # self._create_common_user()

    # get second new user object
    # user1 = session.query(model.User).get(self.common_user['id'])


