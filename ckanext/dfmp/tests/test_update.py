import ckan.model as model
session = model.Session

import itertools
import nose.tools as nt

import ckan.plugins.toolkit as toolkit

import ckanext.dfmp.actions.action as action
import ckanext.dfmp.actions.update as update_action
import ckanext.dfmp.asset as asset


class TestAssetUpdate:

  def setUp(self):
    self.fake_arg =  dict(
      asset_id='someidwhichdefinitelycantbefoundbutsometimesshithappens',
      resource_id='someidwhichdefinitelycantbefoundbutsometimesshithappens',
    )
    self.admin_context = self._get_admin_context()
    self.anon_context = {}

  def tearDown(self):
    pass

  def _get_admin_context(self):
    admin = session.query(model.User).filter_by(sysadmin=True).first()
    context = dict(
      model=model,
      user=admin.name,
      auth_user_obj=admin
    )
    print context
    return context

  def test_asset_update_without_args(self):
    nt.assert_raises(TypeError, update_action.dfmp_update_asset)

  def test_asset_update_with_zero_or_one_args(self):
    for L in range(0, len(self.fake_arg)):
      for subset in itertools.combinations(self.fake_arg, L):
        data_dict = dict(map(lambda x: (x, self.fake_arg[x]), subset))
        nt.assert_raises(toolkit.ValidationError, update_action.dfmp_update_asset, self.admin_context, data_dict)

  def test_asset_update_with_fake_args_api_check(self):
    nt.assert_raises(toolkit.ObjectNotFound, update_action.dfmp_update_asset, self.admin_context, self.fake_arg)

  def test_asset_update_with_fake_args_by_anon_api_check(self):
    nt.assert_raises(toolkit.NotAuthorized, update_action.dfmp_update_asset, self.anon_context, self.fake_arg)

  def test_asset_update_with_fake_args_class_check(self):
    nt.assert_raises(toolkit.ObjectNotFound, asset.Asset.update, context=self.admin_context, data=self.fake_arg)

  def test_asset_update_with_fake_args_by_anon_class_check(self):
    nt.assert_raises(toolkit.NotAuthorized, asset.Asset.update, context=self.anon_context, data=self.fake_arg)

