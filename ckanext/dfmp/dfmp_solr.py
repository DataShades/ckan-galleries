import socket, string, logging, json, re, solr, datetime
from dateutil.parser import parse
from ckanext.dfmp.bonus import _get_index_id

from pylons import config
from paste.deploy.converters import asbool
from ckan.common import c
from ckan.lib.search.query import SearchQuery, VALID_SOLR_PARAMETERS, SearchQueryError, SearchError

from ckan.lib.search.common import SearchIndexError, make_connection
from ckan.lib.search.index import SearchIndex
import ckan.model as model

from ckanext.dfmp.bonus import _unjson, _unjson_base, _get_package_id_by_res

log = logging.getLogger(__name__)

QUERY_FIELDS = "name^4 title^4 tags^2 groups^2 text"

TYPE_FIELD = "entity_type"
ASSET_TYPE = "asset"
KEY_CHARS = string.digits + string.letters + "_-"
SOLR_FIELDS = [TYPE_FIELD, "res_url", "text", "urls", "indexed_ts", "site_id"]
RESERVED_FIELDS = SOLR_FIELDS + ["tags", "groups", "res_description",
                                 "res_format", "res_url"]
# Regular expression used to strip invalid XML characters
_illegal_xml_chars_re = re.compile(u'[\x00-\x08\x0b\x0c\x0e-\x1F\uD800-\uDFFF\uFFFE\uFFFF]')


def _asset_search(q = '*:*', fl = 'data_dict', fq = '', facet_fields = '', sort='score desc, metadata_created desc', limit=20, offset=1):
  return DFMPSearchQuery()({
    'q':q,
    'fl':fl,
    'fq':fq,
    'facet.field':facet_fields,
    'sort':sort,
    'rows':limit,
    'start':offset
  })

def escape_xml_illegal_chars(val, replacement=''):
  '''
    Replaces any character not supported by XML with
    a replacement string (default is an empty string)
    Thanks to http://goo.gl/ZziIz
  '''
  return _illegal_xml_chars_re.sub(replacement, val)


class DFMPSolr(SearchIndex):
  def remove_dict(self, ast_dict, defer_commit):self.delete_asset(ast_dict, defer_commit)

  def update_dict(self, ast_dict, defer_commit=False):self.index_asset(ast_dict, defer_commit)

  def index_asset(self, ast_dict, defer_commit=False):
    if ast_dict is None:
      return
    ast_dict[TYPE_FIELD] = ASSET_TYPE
    ast_dict['capacity'] = 'public'
    if not ast_dict.get('package_id'):
      ast_dict['package_id'] = _get_package_id_by_res(ast_dict['id'])

    bogus_date = datetime.datetime(1, 1, 1)
    try:
      ast_dict['metadata_created'] = parse(ast_dict['lastModified'][:19],  default=bogus_date).isoformat() + 'Z'
    except ValueError:
      ast_dict['metadata_created'] = None

    ast_dict['metadata_modified'] = datetime.datetime.now().isoformat()[:19] + 'Z'

    if type(ast_dict['metadata']) in (unicode, str):
      try:
        ast_dict['metadata'] = json.loads(_unjson_base(ast_dict['metadata']))
      except ValueError:
        ast_dict['metadata'] = json.loads(_unjson(ast_dict['metadata']))

    for field in ('organization', 'text', 'notes'):
      if not ast_dict['metadata'].get(field):
        ast_dict[field] = None
    if 'text' in ast_dict['metadata'] and not ast_dict['notes']:
      ast_dict['notes'] = ast_dict['metadata']['text']
    elif 'description' in ast_dict['metadata'] and not ast_dict['notes']:
      ast_dict['notes'] = ast_dict['metadata']['description']

    if not 'state' in ast_dict['metadata']:
      ast_dict['metadata']['state'] = 'active'
    for field in (('type', 'mimetype'),('mimetype', 'type')):
      if field[0] in ast_dict['metadata'] and field[1] not in ast_dict['metadata']:
        ast_dict['metadata'][field[1]] = ast_dict['metadata'][field[0]]

    tags = ast_dict['metadata'].get('tags')
    if type(tags) in (str, unicode): tags = [name.strip() for name in tags.split(',') if name]
    if type(tags) not in (list, tuple, set): tags = []
    ast_dict['tags'] = tags

    ast_dict['data_dict'] = json.dumps(ast_dict)

    index_fields = RESERVED_FIELDS + ast_dict.keys()

    # include the extras in the main namespace
    extras = ast_dict['metadata']
    for extra in extras:
      key, value = extra, extras[extra]
      if isinstance(value, (tuple, list)):
          value = " ".join(map(unicode, value))
      key = ''.join([c for c in key if c in KEY_CHARS])
      ast_dict['extras_' + key] = value
      if key not in index_fields:
        ast_dict[key] = value
    ast_dict.pop('metadata', None)

    context = {'model': model}

    # clean the dict fixing keys
    new_dict = {}
    for key, value in ast_dict.items():
      key = key.encode('ascii', 'ignore')
      new_dict[key] = value
    ast_dict = new_dict

    for k in ('title', 'notes', 'title_string', 'name'):
      if k in ast_dict and ast_dict[k]:
        ast_dict[k] = escape_xml_illegal_chars(ast_dict[k])

    # modify dates (SOLR is quite picky with dates, and only accepts ISO dates
    # with UTC time (i.e trailing Z)
    # See http://lucene.apache.org/solr/api/org/apache/solr/schema/DateField.html
    new_dict = {}
    for key, value in ast_dict.items():
      key = key.encode('ascii', 'ignore')
      if key.endswith('_date'):
        try:
          date = parse(value, default=bogus_date)
          if date != bogus_date:
            value = date.isoformat() + 'Z'
          else:
            # The date field was empty, so dateutil filled it with
            # the default bogus date
            value = None
        except ValueError:
          continue
      new_dict[key] = value
    ast_dict = new_dict

    # mark this CKAN instance as data source:
    ast_dict['site_id'] = config.get('ckan.site_id')

    # Strip a selection of the fields.
    # These fields are possible candidates for sorting search results on,
    # so we strip leading spaces because solr will sort " " before "a" or "A".
    for field_name in ['title', 'name']:
      try:
        value = ast_dict.get(field_name)
        if value:
          ast_dict[field_name] = value.lstrip()
      except KeyError:
        pass

    # add a unique index_id to avoid conflicts
    ast_dict['index_id'] = _get_index_id(ast_dict['id'], ast_dict['assetID'])


    # send to solr:
    try:
      conn = make_connection()
      commit = not defer_commit
      if not asbool(config.get('ckan.search.solr_commit', 'true')):
        commit = False
      conn.add_many([ast_dict], _commit=commit)
    except socket.error, e:
      err = 'Could not connect to Solr using {0}: {1}'.format(conn.url, str(e))
      log.error(err)
      raise SearchIndexError(err)
    except Exception, e:
      msg = 'Solr returned an error: {0} {1} - {2}'.format(
        e.httpcode, e.reason, e.body[:1000] # limit huge responses
      )
      raise SearchIndexError(msg)
    finally:
      conn.close()
    commit_debug_msg = 'Not commited yet' if defer_commit else 'Commited'
    log.debug('Updated index for %s [%s]' % (ast_dict.get('name'), commit_debug_msg))

  def commit(self):
    try:
      conn = make_connection()
      conn.commit(wait_searcher=False)
    except Exception, e:
      log.exception(e)
      raise SearchIndexError(e)
    finally:
      conn.close()


  def delete_asset(self, ast_dict, defer_commit=False):
    conn = make_connection()
    if ast_dict.get('remove_all_assets'):
      index = ''
    elif ast_dict.get('whole_resource'):
      index = ' +id:{id} '.format(id=ast_dict['whole_resource'])
    else:
      index = ' +index_id:\"{index}\"'.format(
        index=_get_index_id(ast_dict['id'], ast_dict['assetID'])
      )
    query = "+{type}:{asset} {index} +site_id:\"{site}\"".format(
      type=TYPE_FIELD,
      asset=ASSET_TYPE,
      index=index,
      site=config.get('ckan.site_id'))
    try:
      conn.delete_query(query)

      if not defer_commit:
        conn.commit()
    except Exception, e:
      log.exception(e)
      raise SearchIndexError(e)
    finally:
      conn.close()

class DFMPSearchQuery(SearchQuery):
  """Search for resources."""
    
  def run(self, query):
    '''
    Performs a asset search using the given query.

    @param query - dictionary with keys like: q, fq, sort, rows, facet
    @return - dictionary with keys results and count

    May raise SearchQueryError or SearchError.
    '''
    # check that query keys are valid
    if not set(query.keys()) <= VALID_SOLR_PARAMETERS:
      invalid_params = [s for s in set(query.keys()) - VALID_SOLR_PARAMETERS]
      raise SearchQueryError("Invalid search parameters: %s" % invalid_params)

    # default query is to return all documents
    q = query.get('q')
    if not q or q == '""' or q == "''":
      query['q'] = "*:*"

    # number of results
    rows_to_return = min(1000, int(query.get('rows', 20)))
    if rows_to_return > 0:
      # #1683 Work around problem of last result being out of order
      #       in SOLR 1.4
      rows_to_query = rows_to_return + 1
    else:
      rows_to_query = rows_to_return
    query['rows'] = rows_to_query

    # show only results from this CKAN instance
    
    fq = query.get('fq', '')
    if not '+site_id:' in fq:
      fq += ' +site_id:"%s"' % config.get('ckan.site_id')
    if not '+type:' in q and not '+mimetype:' in q and not '+type:' in fq and not '+mimetype:' in fq:
      fq += ' -type:image/x* -mimetype:image/x* '

    # filter for asset entity_type
    if not '+entity_type:' in fq:
      fq += " +entity_type:asset"
    if not '+state:' in q and not '+state:' in fq:
      fq += " -state:hidden -state:deleted"

    user = c.userobj
    if user and (user.sysadmin or user.email.endswith('@act.gov.au')): pass
    else:
      user_groups = []
      if user:
        for group in user.get_groups():
          user_groups.append(group.id)

          #get all child orgs
          user_groups.extend([
            item.table_id for item
            in filter(
              lambda x: x.capacity=='child_organization' and x.state == 'active',
              group.member_all
            )
          ])

          #get all brothers
          parents = model.Session.query(model.Group)\
            .filter(model.Group.id.in_([
              item.table_id for item
              in filter(
                lambda x: x.capacity=='parent_organization' and x.state == 'active',
                group.member_all
              )
            ])).all()
          for parent in parents:
            user_groups.extend([
              item.table_id for item
              in filter(
                lambda x: x.capacity=='child_organization' and x.state == 'active',
                parent.member_all
              )
            ])
      private_query = model.Session.query(model.Package.id, model.Package.owner_org).\
        filter(model.Package.private==True)
      if user_groups:
        private_query = private_query.filter(~model.Package.owner_org.in_(user_groups))

      private = private_query.all()
      for id in private:
        fq += " -package_id:{id}".format(id=id[0])
    query['fq'] = [fq]

    fq_list = query.get('fq_list', [])
    query['fq'].extend(fq_list)

    # faceting
    query['facet'] = query.get('facet', 'true')
    query['facet.limit'] = query.get('facet.limit', config.get('search.facets.limit', '50'))
    query['facet.mincount'] = query.get('facet.mincount', 1)

    # return the asset ID and search scores
    query['fl'] = query.get('fl', 'data_dict')

    # return results as json encoded string
    query['wt'] = query.get('wt', 'json')

    # If the query has a colon in it then consider it a fielded search and do use dismax.
    defType = query.get('defType', 'dismax')
    if ':' not in query['q'] or defType == 'edismax':
      query['defType'] = defType
      query['tie'] = query.get('tie', '0.1')
      # this minimum match is explained
      # http://wiki.apache.org/solr/DisMaxQParserPlugin#mm_.28Minimum_.27Should.27_Match.29
      query['mm'] = query.get('mm', '2<-1 5<80%')
      query['qf'] = query.get('qf', QUERY_FIELDS)


    conn = make_connection()
    # log.debug('Asset query: %r' % query)
    try:
      solr_response = conn.raw_query(**query)
    except Exception, e:
      raise SearchError('SOLR returned an error running query: %r Error: %r' %
                        (query, e.reason))
    try:
      data = json.loads(solr_response)
      response = data['response']
      self.count = response.get('numFound', 0)
      self.results = response.get('docs', [])

      # #1683 Filter out the last row that is sometimes out of order
      self.results = self.results[:rows_to_return]

      # get any extras and add to 'extras' dict
      for result in self.results:
        extra_keys = filter(lambda x: x.startswith('extras_'), result.keys())
        extras = {}
        for extra_key in extra_keys:
          value = result.pop(extra_key)
          extras[extra_key[len('extras_'):]] = value
        if extra_keys:
          result['extras'] = extras

      # if just fetching the id or name, return a list instead of a dict
      if query.get('fl') in ['id', 'name']:
        self.results = [r.get(query.get('fl')) for r in self.results]

      # get facets and convert facets list to a dict
      self.facets = data.get('facet_counts', {}).get('facet_fields', {})
      for field, values in self.facets.iteritems():
        self.facets[field] = dict(zip(values[0::2], values[1::2]))
    except Exception, e:
      log.exception(e)
      raise SearchError(e)
    finally:
      conn.close()
    return {'results': self.results, 'count': self.count, 'facets':self.facets}
  __call__ = run