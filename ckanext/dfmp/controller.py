import ckan.plugins.toolkit as toolkit
import ckan.lib.base as base
import ckan.lib.helpers as h
import datetime, os, re, json
from dateutil import parser
import ckan.model as model
from pylons import config
from ckan.common import c, g, _, OrderedDict, request
from urllib import urlencode
from sqlalchemy import or_
import ckanext.dfmp.actions.get as dfmp_get_action
import ckanext.dfmp.actions as dfmp_parent_action
import json

session = model.Session
from ckanext.dfmp.dfmp_solr import DFMPSolr, DFMPSearchQuery, _asset_search
import logging
log = logging.getLogger(__name__)
import ckanext.dfmp.scripts as scripts
from ckanext.dfmp.actions.action import _asset_to_solr
from ckanext.dfmp.bonus import _unique_list, _name_normalize
ASSETS_PER_PAGE = 20
log_path = '/var/log/dfmp/'

twitter_api_keys = [
  dict(
    CK  = u'vKAo073zpwfmuiTkyR83qyZEe',
    CS  = u'cciB0ZCwQnBASvRp9HPN1vbBdZSCEzyu118igFhQFxOwDVFmVD',
    AT  = u'23904345-OmiSA5CLpceClmy46IRJ98ddEKoFJAPura2j53ryN',
    ATS = u'QYJwGyYODIFB5BJM8F5IXNUDn9coJnKzY6scJOErKRcAE'
  ),
  dict(
    CK  = u'VmNHkKuFcza5ouvkNiimpoU8E',
    CS  = u'E9CcaBikENNmbNC2LaG9aWhpiNuvpBBhUElPtZNGwulpvzIVu1',
    AT  = u'23904345-4PBhPAYyUn4XvniAFCDOv5HaVEIJt2ik2j7KhEWdx',
    ATS = u'a7Qtt296u2FnSia9fGGpbejJ3Jg420OC0LBPbCmYIIKVs',
  ),
  dict(
    CK  = u'A0aIjONlJLGHQxN9KR15OnQQp',
    CS  = u'khhb58i3Qi2BTD0QhxsfNPurOfZZ7YBQbtMheSoNWldWNyR2oe',
    AT  = u'23904345-2MpF4FY06gvwGV1rNuJQ5oEdpvVMlMpWmWoEFXzMi',
    ATS = u'8YExrwTKpPVDb3pEGTAGokDyuCzKvKUTLprzcxHlVQ5rG',
  ),
]

def search_url(params):
  url = h.url_for('search_assets')
  return url_with_params(url, params)

def url_with_params(url, params):
  params = _encode_params(params)
  return url + u'?' + urlencode(params)

def _encode_params(params):
  return [(k, v.encode('utf-8') if isinstance(v, basestring) else str(v))
    for k, v in params]

def _get_user_editable_datasets(context, user_id):
  all_organizations = toolkit.get_action('organization_list_for_user')(context, {'permission':'update_dataset'})

  org_ids = [x['id'] for x in all_organizations]

  datasets = session.query(model.Package.id)\
          .filter(or_(model.Package.creator_user_id == user_id, model.Package.owner_org.in_(org_ids)))\
          .all()
  return [x[0] for x in datasets]

class DFMPController(base.BaseController):

  def _init_context(self):
    self.context = {
      'model': model,
      'user': c.user or c.author,
      'auth_user_obj': c.userobj
    }

  # asset edit form
  def record_edit(self, resource, asset_id):
    # inits context
    self._init_context()

    # gets the list of datasets user can edit
    editable_datasets = _get_user_editable_datasets(self._init_context(), c.userobj.id) if c.userobj and c.userobj.get('id') else []

    # we need to make sure that requested asset exists
    try:
      # we use API action to get asset details
      if hasattr(dfmp_get_action, 'dfmp_get_asset'):
        asset = toolkit.get_action('dfmp_get_asset')(self.context, {
          'resource_id': resource,
          'asset_id': asset_id,
        })
      else:
        # DEPRICATED
        asset = toolkit.get_action('resource_items')(self.context, {
          'id': resource,
          'item': asset_id,
        })['records'][0]
    except toolkit.ValidationError, e:
      # returns "Resourse not found" page if no asset found
      return base.abort(404)
    log.warn(asset)
    #gets destination URL
    destination = request.params.get('destination') or c.environ.get('HTTP_REFERER') or ''

    # we ned to apply changes if from is submitted
    if request.method == 'POST' and request.params.get('save') == 'asset_update':
      while True:
        package_id = session.query(model.Resource).filter_by(id=resource).first().get_package_id()
        # only admins can modify assets
        if package_id not in editable_datasets and not (c.userobj and c.userobj.sysadmin):
          h.flash_error('You cannot modify this record.')
          break
        # asset changes dict
        asset_update = {
          'name': request.params.get('name'),
          'lastModified': request.params.get('last_modified')
        }
        # log.warn(dir(dfmp_update_action))
        # we need to update asset
        if hasattr(dfmp_parent_action, 'update') and hasattr(dfmp_parent_action.update, 'dfmp_update_asset'):
          asset_update['resource_id'] = request.params.get('resource_id')
          asset_update['asset_id'] = request.params.get('asset_id')
          asset = toolkit.get_action('dfmp_update_asset')(self.context, asset_update)
        else:
          # DEPRICATED
          asset_update['id'] = request.params.get('resource_id')
          asset_update['assetID'] = request.params.get('asset_id')
          asset = toolkit.get_action('user_update_asset')(self.context, asset_update)
        # notification about successful update
        h.flash_success('Asset has been updated.')
        if destination:
          base.redirect(destination)
        break

    # creates asset dict for Template
    asset = {
      'name': asset['name'],
      'resource_id': resource,
      'asset_id': asset['assetID'],
      'last_modified': asset['lastModified'],
      'url': asset['url'],
      'spatial': json.dumps(asset['spatial'], sort_keys=False, indent=2, separators=(',', ': ')),
      'metadata': json.dumps(asset['metadata'], sort_keys=False, indent=2, separators=(',', ': '))
    }
    # renders Edit form
    return base.render('assets/edit.html', {'asset': asset, 'destination': destination})


  def api_doc(self):
    return base.render('home/api_doc.html')

  def flickr_update(self):
    log.warn('FLICKR UPDATE')
    # inits context
    self._init_context()

    toolkit.get_action('dfmp_flickr_update')(self.context, {})
    
    # redirect to DFMP homepage
    base.redirect(c.environ.get('HTTP_REFERER', config.get('ckan.site_url','/')))

  def get_flickr(self):
    return base.render('package/dataset_from_flickr.html')

  def search_assets(self):
    editable_datasets = _get_user_editable_datasets(self._init_context(), c.userobj.id) if c.userobj else []

    q = c.q = request.params.get('q', u'')
    c.query_error = False
    sort_by = request.params.get('sort', None)
    c.sort_by_selected = sort_by
    try:
      page = int(request.params.get('page', 1))
    except ValueError, e:
      abort(400, ('"page" parameter must be an integer'))
    params_nopage = [
      (k, v) for k, v in request.params.items()
      if k != 'page'
    ]
    params_nosort = [(k, v) for k, v in params_nopage if k != 'sort']
    def _sort_by(fields):
      """
      Sort by the given list of fields.
      Each entry in the list is a 2-tuple: (fieldname, sort_order)
      eg - [('metadata_modified', 'desc'), ('name', 'asc')]
      If fields is empty, then the default ordering is used.
      """
      params = params_nosort[:]

      if fields:
        sort_string = ', '.join('%s %s' % f for f in fields)
        params.append(('sort', sort_string))
      return search_url(params)
    c.sort_by = _sort_by
    if sort_by is None:
      sort_by = 'metadata_modified desc'
      c.sort_by_fields = []
    else:
      c.sort_by_fields = [field.split()[0]
                            for field in sort_by.split(',')]


    def remove_field(key, value=None, replace=None):
      return h.remove_url_param(key, value=value, replace=replace,
                            controller='ckanext.dfmp.controller:DFMPController', action='search_assets')
    c.remove_field = remove_field

    def pager_url(q=None, page=None):
      params = list(params_nopage)
      params.append(('page', page))
      return search_url(params)
    c.search_url_params = urlencode(_encode_params(params_nopage))

    facet_fields = [
      'organization',
      # 'groups',
      'tags',
      # 'res_format',
      'license_id']

    fq = ''
    c.fields = []
    c.fields_grouped = {}
    search_extras = {}
    for (param, value) in request.params.items():
      if param not in ['q', 'page', 'sort'] \
          and len(value) and not param.startswith('_'):
        if not param.startswith('ext_'):
          c.fields.append((param, value))
          fq += ' %s:"%s"' % (param, value)
          if param not in c.fields_grouped:
            c.fields_grouped[param] = [value]
          else:
            c.fields_grouped[param].append(value)
        else:
          search_extras[param] = value

    for param in params_nosort:
      if param[0] in facet_fields:
        fq += u' +{0}:"{1}"'.format(*param)

    result = _asset_search(**{
      'q':q,
      'fq':fq,
      'facet_fields':facet_fields,
      'limit':ASSETS_PER_PAGE,
      'offset':(page-1)*ASSETS_PER_PAGE,
      'sort':sort_by,
    })

    default_facet_titles = {
      'organization': _('Organizations'),
      'groups': _('Groups'),
      'tags': _('Tags'),
      'res_format': _('Formats'),
      'license_id': _('Licenses'),
    }

    facets = OrderedDict()
    for facet in facet_fields:
      if facet in default_facet_titles:
        facets[facet] = default_facet_titles[facet]
      else:
        facets[facet] = facet
    c.facet_titles = facets

    restructured_facets = {}
    for key, value in result['facets'].items():
        restructured_facets[key] = {
                'title': key,
                'items': []
                }
        for key_, value_ in value.items():
            new_facet_dict = {}
            new_facet_dict['name'] = key_
            if key in ('groups', 'organization'):
                group = model.Group.get(key_)
                if group:
                    new_facet_dict['display_name'] = group.display_name
                else:
                    new_facet_dict['display_name'] = key_
            elif key == 'license_id':
                license = model.Package.get_license_register().get(key_)
                if license:
                    new_facet_dict['display_name'] = license.title
                else:
                    new_facet_dict['display_name'] = key_
            else:
                new_facet_dict['display_name'] = key_
            new_facet_dict['count'] = value_
            restructured_facets[key]['items'].append(new_facet_dict)
    c.search_facets = restructured_facets

    c.search_facets_limits = {}
    for facet in c.search_facets.keys():
      try:
        limit = int(request.params.get('_%s_limit' % facet,
                                       g.facets_default_number))
      except ValueError:
        abort(400, _('Parameter "{parameter_name}" is not '
           'an integer').format(
             parameter_name='_%s_limit' % facet
           ))
      c.search_facets_limits[facet] = limit

    assets = [ json.loads(item['data_dict']) for item in result['results'] ]

    for asset in assets:
      if asset['package_id'] in editable_datasets:
        asset['user_editable'] = True

    c.page = h.Page(
        collection=assets,#query['results'],
        page=page,#page,
        url=pager_url,#pager_url,
        item_count=result['count'],
        items_per_page=ASSETS_PER_PAGE,#limit
    )
    c.page.items = assets

    extra_vars = {
      'assets':assets,
      'action_url':h.url_for('ajax_actions'),
    }
    return base.render('package/search_assets.html', extra_vars = extra_vars)

  def flags(self):
    if not c.userobj or not c.userobj.sysadmin:
      base.abort(404)
    sort = request.params.get('sort', 'metadata_modified asc')
    flagged = DFMPSearchQuery.run({
      'q':'',
      'rows':100,
      'start':0,
      'sort': sort,
      'fl':'data_dict, metadata_modified',
      'fq':'+extras_flag:[* TO *]',
    })['results']
    assets = []
    if flagged:
      for item in flagged:
        asset = json.loads(item['data_dict'])
        asset['metadata_modified'] = item['metadata_modified']
        assets.append(asset)
    return base.render('admin/flags.html', extra_vars={'assets':assets, 'sort': sort})

  def terminate_listener(self, id, resource_id):
    self._listener_route('terminate', id, resource_id)

  def start_listener(self, id, resource_id):
    self._listener_route('start', id, resource_id)

  def solr_commit(self):
    DFMPSolr.commit()
    base.redirect(c.environ.get('HTTP_REFERER', config.get('ckan.site_url','/')))

  def solr_clean_index(self):
    result = DFMPSearchQuery.run({
      'q':'',
      'facet.field':'id',
      'rows':0,
    })['facets']['id'].keys()
    offset = 0
    limit = 2
    while True:
      resources = [
        item[0] for item
        in session.query(model.Resource.id)\
          .filter(model.Resource.state == 'active')\
          .limit(limit)\
          .offset(offset)\
          .all()
      ]
      result = filter(lambda x: x not in resources, result)

      offset += limit
      if not resources:
        break

    remover = DFMPSolr
    for item in result:
      remover.delete_asset({'whole_resource':item})

    self.solr_commit()

  def ckanadmin_org_relationship(self, org):
    # inits context
    self._init_context()

    if not c.userobj or not c.userobj.sysadmin:
      base.abort(404)

    params = dict(request.params)
    if 'route' in params and 'child' in params:
      if params['route'] == 'add':
        member_create = toolkit.get_action('member_create')
        member_create(self.context,{
          'id': org,
          'object': params['child'],
          'object_type': 'group',
          'capacity': 'child_organization'
          })
        member_create(self.context,{
          'id': params['child'],
          'object': org,
          'object_type': 'group',
          'capacity': 'parent_organization'
          })
      elif params['route'] == 'remove':
        member_delete = toolkit.get_action('member_delete')
        member_delete(self.context,{
          'id': org,
          'object': params['child'],
          'object_type': 'group',
          })
        member_delete(self.context,{
          'id': params['child'],
          'object': org,
          'object_type': 'group',
          })

    org_obj = session.query(model.Group).filter_by(id=org).first()
    children = [ item.table_id for item in filter(lambda x: x.capacity=='child_organization' and x.state == 'active', org_obj.member_all)]

    all_organizations = toolkit.get_action('organization_list')(self.context, {'all_fields':True})
    for o in all_organizations:
      if o['id'] == org:
        organization = o
    c.group_dict = organization
    return base.render('organization/relationship.html', extra_vars={
      'all_organizations':all_organizations,
      'children':children
    })
  

  def twitter_listeners(self):
    if not c.userobj or not c.userobj.sysadmin:
      base.abort(404)

    tasks = []
    for task, resource in session.query(model.TaskStatus, model.Resource)\
      .filter(
        model.TaskStatus.entity_id == model.Resource.id,
        model.TaskStatus.task_type == 'twitter_streaming',
        model.Resource.state == 'active').all():

      pid = task.error or ''
      if pid:
        task.pid = pid
        if not os.system('ps %s' % pid):
            task.is_active = True
      task.name = resource.name
      task.pkg = resource.get_package_id()
      tasks.append(task)
   
    extra_vars = {
      'listeners':tasks,
      'now':datetime.datetime.now()
    }
    return base.render('admin/twitter_listeners.html', extra_vars=extra_vars)

  def ajax_actions(self):
    # inits context
    self._init_context()

    if not c.userobj or not c.userobj.sysadmin:
      base.abort(404)

    action = request.params.get('action')
    res_id = request.params.get('res_id')
    assets = request.params.get('assets').split(' ')
    parent = toolkit.get_action('resource_show')(self.context, {'id': res_id})
    forbidden = json.loads(parent.get('forbidden_id', '[]'))
    solr = DFMPSolr

    if action == 'delete':
      toolkit.get_action('user_remove_asset')(self.context, {
        'items':[{
          'id': res_id,
          'assetID':assetID
        } for assetID in assets],
      })
      

      forbidden.extend(assets)

    elif action == 'hide':
      for visible_asset in assets:
        asset = toolkit.get_action('datastore_search')(self.context, {
          'id': res_id,
          'filters':{
            'assetID':visible_asset
          }
        })['records'][0]

        if type( asset['metadata'] ) == tuple:
          asset['metadata'] = json.loads(asset['metadata'][0])
        if type( asset['spatial'] ) == tuple:
          asset['spatial'] = json.loads(asset['spatial'][0])

        asset['metadata']['state'] = 'hidden'
        asset['id'] = res_id

        _asset_to_solr(asset, defer_commit=True)

        toolkit.get_action('datastore_delete')(self.context,{
          'resource_id': res_id,
          'force': True,
          'filters':{
            'assetID':visible_asset,
          }
        })

      forbidden.extend(assets)

    elif action == 'solr-delete':
      for assetID in assets:
        solr.remove_dict({
          'id' : res_id,
          'assetID' : assetID
        }, defer_commit=True)

      forbidden.extend(assets)

    elif action == 'unhide':
      hidden_assets = DFMPSearchQuery.run({
        'q':'assetID:({assets})'.format(assets=' OR '.join(assets)),
        'fl':'data_dict',
        'fq':'+state:hidden',
      })['results']
      for asset in hidden_assets:
        log.warn(asset)
        asset = json.loads(asset['data_dict'])
        asset['metadata']['state'] = 'active'

        datastore_item = toolkit.get_action('datastore_upsert')(self.context, {
          'resource_id':res_id,
          'force': True,
          'records':[{
            'assetID':asset['assetID'],
            'lastModified':asset['lastModified'],
            'name':asset['name'],
            'url':asset['url'],
            'spatial':asset['spatial'],
            'metadata':asset['metadata'],
            }
          ],
          'method':'upsert'
        })


        _asset_to_solr(asset, defer_commit=True)
      forbidden = filter(lambda x, assets=assets: not x in assets, forbidden)
      
    else:
      return 'Unrecognized action'

    forbidden = _unique_list(forbidden)
    parent['forbidden_id'] = json.dumps(forbidden)
    if not request.params.get('without_forbidding'):
      toolkit.get_action('resource_update')(self.context, parent)
    solr.commit()
    return 'Done'

  def manage_assets(self, id, resource_id):
    # inits context
    self._init_context()

    if not c.userobj or not c.userobj.sysadmin:
      base.abort(404)
    try:
      toolkit.c.pkg_dict = toolkit.get_action('package_show')(None, {'id': id})
      toolkit.c.resource = toolkit.get_action('resource_show')(None, {'id': resource_id})
    except toolkit.ObjectNotFound:
      base.abort(404, _('Resource not found'))
    except toolkit.NotAuthorized:
      base.abort(401, _('Unauthorized to edit this resource'))

    page = int(request.params.get('page',1))
    assets = []
    try:
      result = toolkit.get_action('datastore_search')(self.context,{
        'id':resource_id,
        'limit':ASSETS_PER_PAGE,
        'offset':(page-1)*ASSETS_PER_PAGE,
        'sort':'_id asc'
      })
      assets.extend(result['records'])
    except toolkit.ObjectNotFound:
      return base.render('package/manage_assets.html')
    hidden_assets = []
    hidden = DFMPSearchQuery.run({
      'q':'id:{res_id}'.format(res_id=resource_id),
      'rows':100,
      'start':0,
      'fq':'+state:hidden',
    })['results']
    if hidden:
      for item in hidden:
        hidden_assets.append(json.loads(item['data_dict']))

    extra_vars = {
      'assets':assets,
      'hidden_assets':hidden_assets,
      'action_url':h.url_for('ajax_actions'),
    }



    def pager_url(q=None, page=None):
      params = [
        ('page', page),
      ]
      url = h.url_for('manage_assets', id=id, resource_id=resource_id)
      return url_with_params(url, params)
    c.page = h.Page(
        collection=assets,
        page=page,
        url=pager_url,#pager_url,
        item_count=result.get('total',0),
        items_per_page=ASSETS_PER_PAGE,
    )

    return base.render('package/manage_assets.html', extra_vars=extra_vars)

  def _listener_route(self, action, id, resource_id):
    if not c.userobj or not c.userobj.sysadmin:
      base.abort(404)

    if action == 'terminate':
      task = session.query(model.TaskStatus)\
        .filter(
          model.TaskStatus.task_type=='twitter_streaming',
          model.TaskStatus.entity_id==resource_id)\
        .first()
      if not task:
        h.flash_error("Can't find listener")
      if task:
        pid = task.error or '' 
        if not pid:
          h.flash_error("Can't get PID of process")
        else:
          h.flash_success('Success')
          toolkit.get_action('task_status_update')(None, {
            'entity_id': resource_id,
            'task_type': 'twitter_streaming',
            'key': 'celery_task_id',
            'state': 'Terminated',
            'value': 'Ready for start',
            'error': pid,
            'last_updated': datetime.datetime.now().isoformat(),
            'entity_type': 'resource'
          })
          if os.system('kill -9 %s' % pid):
            toolkit.get_action('celery_revoke')(self.context, {'id': pid, 'resource': resource_id})
    base.redirect(h.url_for('getting_tweets', id=id, resource_id=resource_id))
    

  def getting_tweets(self, id, resource_id):
    # inits context
    self._init_context()

    if not c.userobj or not c.userobj.sysadmin:
      base.abort(404)

    log_access = os.access(log_path, os.W_OK)
    if not log_access:
      h.flash_error('Listener will be working without log file. Create or modify access to {log_path} if you want to see process log'.format(log_path=log_path))

    now = datetime.datetime.now() - datetime.timedelta(1)
    pid = None
    extra_vars = {
      'pkg_id':id,
      'res_id': resource_id,
      'getting_status': {},
      'streaming_status': {},
      'pull_data':{
          'pull_from': now.strftime('%Y-%m-%d'),
          'pull_from_time': now.strftime('%H:%M:%S'),
      },
      'pull_error_summary':{},
      'stream_error_summary':{},
      'key_list':[x for x in  range(len(twitter_api_keys))],
    }
    
    try:
      getting_status = toolkit.get_action('task_status_show')(None, {
        'task_type': 'getting_tweets',
        'entity_id': resource_id,
        'key':'celery_task_id'
      })
      extra_vars.update(getting_status=getting_status)
    except toolkit.ObjectNotFound:
      pass

    try:
      streaming_status = toolkit.get_action('task_status_show')(None, {
        'task_type': 'twitter_streaming',
        'entity_id': resource_id,
        'key':'celery_task_id'
        })
      extra_vars.update(streaming_status=streaming_status)
      if 'error' in streaming_status:
        try:
          pid = streaming_status['error']
          pid = int(pid)
          if not os.system('ps %s' % pid):
            extra_vars.update(may_kill = True)
        except:
          pass
    except toolkit.ObjectNotFound:
      pass

    if toolkit.request.method == 'POST':
      pst = toolkit.request.POST
      stable = True

      if 'kill_listener' in pst:
        self._listener_route('terminate', id, resource_id)

      if 'pull_from' in pst or 'pull_word' in pst:
        word = pst.get('pull_word')
        if not word:
          stable = False
          extra_vars['pull_error_summary'].update( { 'Hashtag': 'Must be defined' } )
        date = pst.get('pull_from', '') + ' ' + pst.get('pull_from_time', '')
        try:
          date = parser.parse(date).isoformat(' ')
        except ValueError:
          stable = False
          extra_vars['pull_error_summary'].update( { 'Date': 'Wrong date' } )

        if stable:
          getting = toolkit.get_action('celery_getting_tweets')(self.context,{
            'resource': resource_id,
            'word': pst['pull_word'],
            'deepness': date,
          })
          base.redirect(h.url_for('getting_tweets', id=id, resource_id=resource_id))

      elif 'stream_word' in pst:       
        word = pst['stream_word']
        key_list = int(pst['key_list'])
        if not word:
          stable = False
          extra_vars['stream_error_summary'].update( { 'Hashtag': 'Must be defined' } )

        if stable:
          args = ' --host {host} --ckan-api {apikey} --resource {resource} --ck {ck} --cs {cs} --at {at} --ats {ats} --search \'{word}\''.\
              format(
                host=config.get('ckan.site_url', ''),
                apikey=c.userobj.apikey,
                resource=resource_id,
                word=word,
                ck=twitter_api_keys[key_list]['CK'],
                cs=twitter_api_keys[key_list]['CS'],
                at=twitter_api_keys[key_list]['AT'],
                ats=twitter_api_keys[key_list]['ATS']
              )
          valid_word = _name_normalize(word)
          log_file = ( '>' + log_path + 'dfmp_{word}.log'.format(word=valid_word) ) if log_access else ''
          log.warn(log_file)
          command = 'nohup ' + os.path.dirname(scripts.__file__) + os.path.sep + 'twitter.py ' + args  +  log_file + ' & 1'
          log.warn(command)
          status = os.system(command)

          base.redirect(h.url_for('getting_tweets', id=id, resource_id=resource_id))

    try:
      c.pkg_dict = toolkit.get_action('package_show')(None, {'id': id})
      c.resource = toolkit.get_action('resource_show')(None, {'id': resource_id})
    except toolkit.ObjectNotFound:
      base.abort(404, _('Resource not found'))
    except toolkit.NotAuthorized:
      base.abort(401, _('Unauthorized to edit this resource'))



    return base.render('package/twitter_actions.html', extra_vars=extra_vars)

def _get_pid(val):
  res = re.search('\d+$',val)
  if res:
    return res.group()