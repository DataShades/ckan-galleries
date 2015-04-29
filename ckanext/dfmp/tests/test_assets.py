import ckan.model as model
session = model.Session

import itertools, nose
import nose.tools as nt


import ckan.plugins.toolkit as toolkit

import ckanext.dfmp.actions.action as common_actions
import ckanext.dfmp.actions.get as get_actions

def _get_admin_context():
  admin = session.query(model.User).filter_by(sysadmin=True).first()
  context = dict(
    model=model,
    user=admin.name,
    auth_user_obj=admin
  )
  print context
  return context

def _make_combinations(stuff, max_len):
  combinations = []
  for L in range(0, max_len):
    for subset in itertools.combinations(stuff, L):
      combinations.append( dict(map(lambda x: (x, stuff[x]), subset)) )
  return combinations

class TestUserCreation:

  def setUp(self):
    pass
  def tearDown(self):
    pass

  def test_user_create_with_dataset_without_args(self):
    nt.assert_raises(TypeError, common_actions.user_create_with_dataset)

  def test_user_create_with_dataset_with_wrong_ammount_of_args(self):

    stuff = dict(
      password='3211',
      name='asdfsdf',
      email='asdf@sdf.com'
    )
    for combination in _make_combinations(stuff, len(stuff) ):
      print combination
      nt.assert_raises(toolkit.ValidationError, common_actions.user_create_with_dataset, None, combination)

  def test_user_create_with_dataset_with_correct_ammount_of_args(self):
    data_dict = dict(
      password='3211',
      name='test_user_create_with_dataset_with_correct_ammount_of_args',
      email='asdf@sdf.com'
    )
    try:
      session.query(model.User).filter_by(name=data_dict['name']).one().purge()
      session.commit()
    except:
      pass

    new_user = common_actions.user_create_with_dataset(_get_admin_context(), data_dict)
    print new_user

    nt.assert_is_not_none(new_user)
    nt.assert_in('apikey', new_user)
    nt.assert_greater(new_user['apikey'], '')
    session.query(model.User).filter_by(name=new_user['name']).one().purge()
    session.commit()

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

  