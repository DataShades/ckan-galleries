import ckan.model as model
session = model.Session

import itertools
import nose.tools as nt

import ckan.plugins.toolkit as toolkit

import ckanext.dfmp.actions.action as action


class TestUserCreation:

  def setUp(self):
    pass
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

  def test_user_create_with_dataset_without_args(self):
    nt.assert_raises(TypeError, action.user_create_with_dataset)

  def test_user_create_with_dataset_with_wrong_ammount_of_args(self):

    stuff = dict(
      password='3211',
      name='asdfsdf',
      email='asdf@sdf.com'
    )
    for L in range(0, len(stuff)):
      for subset in itertools.combinations(stuff, L):
        data_dict = dict(map(lambda x: (x, stuff[x]), subset))
        print data_dict
        nt.assert_raises(toolkit.ValidationError, action.user_create_with_dataset, None, data_dict)

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

    new_user = action.user_create_with_dataset(self._get_admin_context(), data_dict)
    print new_user

    nt.assert_is_not_none(new_user)
    nt.assert_in('apikey', new_user)
    nt.assert_greater(new_user['apikey'], '')
    session.query(model.User).filter_by(name=new_user['name']).one().purge()
    session.commit()
