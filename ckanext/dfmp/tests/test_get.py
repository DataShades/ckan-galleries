import ckan.model as model
session = model.Session

import itertools, nose
import nose.tools as nt


import ckan.plugins.toolkit as toolkit

import ckanext.dfmp.actions.action as common_actions
import ckanext.dfmp.actions.get as get_actions
from ckanext.dfmp.tests.test_assets import _get_admin_context, _make_combinations

class TestGetActions:
  def setUp(self):
    pass
  def tearDown(self):
    pass

  def test_get_single_asset_wrong_arguments(self):
    params = {
      'resource_id': 'frgdhfkjll675yt',
      'asset_id': 'afdrghjjklhgf'
    }
    for combination in _make_combinations( params, len(params) ):
      print combination
      nt.assert_raises(toolkit.ValidationError, get_actions.dfmp_get_asset, None, combination)

  def test_get_single_asset_resource_or_asset_not_exists(self):
    params = {
      'resource_id': 'ihopethisidnotexistsbuteverythinginthisworldisposiblesothistestmayfail',
      'asset_id': 'ihopethisidnotexistsbuteverythinginthisworldisposiblesothistestmayfail'
    }
    nt.assert_raises(toolkit.ObjectNotFound, get_actions.dfmp_get_asset, None, params)

  