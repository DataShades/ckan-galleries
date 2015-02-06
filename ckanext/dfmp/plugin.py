import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.logic import side_effect_free
from datetime import datetime

class DFMPPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IActions)

    def get_actions(self):
        return {
          'user_add_asset': user_add_asset,
          'user_get_assets': user_get_assets,

        }




def user_add_asset(context, data_dict):
  """Add new asset"""
  dataset_name = 'dfmp_assets_'+context['auth_user_obj'].name
  orgs = toolkit.get_action('organization_list_for_user')(context, {'permission':'read'}) 

  owner_id    = orgs[0]['id']     if orgs else context['auth_user_obj'].id
  owner_name  = orgs[0]['title']  if orgs else context['auth_user_obj'].name

  try:
    toolkit.get_action('package_create')(context, { 'name' : dataset_name })
  except toolkit.ValidationError:
    pass

  resource = toolkit.get_action('resource_create')(context, { 
                                                    'package_id' : dataset_name, 
                                                    'url':data_dict['url'],
                                                    'name':data_dict['name'],
                                                    'size':data_dict['size'],
                                                    'mimetype':data_dict['type']
                                                  })

  datastore = toolkit.get_action('datastore_create')(context,{
                                            'force':True,
                                            'resource_id': resource['id'],
                                            'fields':[
                                              {'id':'date', 'type':'text'},
                                              {'id':'creator_id', 'type':'text'},
                                              {'id':'creator_name', 'type':'text'},
                                              {'id':'owner_id', 'type':'text'},
                                              {'id':'owner_name', 'type':'text'},
                                              {'id':'license_id', 'type':'text'},
                                              {'id':'type', 'type':'text'},
                                            ],
                                            'records': [
                                              {
                                                'creator_id':context['auth_user_obj'].id,
                                                'creator_name': context['auth_user_obj'].name,
                                                'date':datetime.now().isoformat(),
                                                'owner_id':owner_id,
                                                'owner_name':owner_name,
                                                'license_id':data_dict['license'],
                                                'type':data_dict['type'],
                                                'thumb':data_dict['thumbnailUrl'],
                                              } 
                                            ] 
          
                                       })

  return resource['id']




@side_effect_free
def user_get_assets(context, data_dict):
  """Get all assets of user"""
  try:
    dataset = toolkit.get_action('package_show')(context,{'id' : 'dfmp_assets_'+context['auth_user_obj'].name })
    for resource in dataset['resources']:
      resource.update( datastore = toolkit.get_action('datastore_search')(context,{'resource_id': resource['id']}).get('records') )
    return dataset
  except toolkit.ObjectNotFound, e:
    return {}







  