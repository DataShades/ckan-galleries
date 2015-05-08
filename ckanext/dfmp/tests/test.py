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
  return context

class AbstractDFMP(object):

  def _make_combinations(self, stuff, max_len):
    combinations = []
    for L in range(0, max_len):
      for subset in itertools.combinations(stuff, L):
        combinations.append( dict(map(lambda x: (x, stuff[x]), subset)) )
    return combinations

  def _create_common_user(self):
    try:
      session.query(model.User).filter_by(name=self.common_user_dict['name']).one().purge()
      session.commit()
    except:
      pass
    self.common_user = common_actions.user_create_with_dataset(_get_admin_context(), self.common_user_dict)

  def _purge_common_user(self, usr_name=None):
    usr_name = self.common_user_dict['name'] if not usr_name else usr_name
    session.query(model.User).filter_by(name=usr_name).one().purge()
    session.commit()

  def _purge_dataset(self, resource_id='', package_id=''):
    if resource_id:
      package_id = session.query(model.Resource).filter_by(id=resource_id).one().get_package_id()
    dataset = session.query(model.Package).get(package_id).purge()
    session.commit()

  def __init__(self):
    self.fake_arg =  dict(
      asset_id='someidwhichdefinitelycantbefoundbutsometimesshithappens',
      resource_id='someidwhichdefinitelycantbefoundbutsometimesshithappens',
    )
    self.admin_context = _get_admin_context()
    self.anon_context = {}

  def setUp(self):
    pass
    
  def tearDown(self):
    pass

