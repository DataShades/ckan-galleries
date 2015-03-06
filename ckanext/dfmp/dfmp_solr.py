import socket, string, logging, json, hashlib, re, solr, datetime
from dateutil.parser import parse

from pylons import config
from paste.deploy.converters import asbool

from ckan.lib.search.common import SearchIndexError, make_connection
from ckan.lib.search.index import SearchIndex
import ckan.model as model

# import ckan.logic as logic
# import ckan.lib.plugins as lib_plugins
# import ckan.lib.navl.dictization_functions


log = logging.getLogger(__name__)


TYPE_FIELD = "entity_type"
ASSET_TYPE = "asset"
KEY_CHARS = string.digits + string.letters + "_-"
SOLR_FIELDS = [TYPE_FIELD, "res_url", "text", "urls", "indexed_ts", "site_id"]
RESERVED_FIELDS = SOLR_FIELDS + ["tags", "groups", "res_description",
                                 "res_format", "res_url"]
# Regular expression used to strip invalid XML characters
_illegal_xml_chars_re = re.compile(u'[\x00-\x08\x0b\x0c\x0e-\x1F\uD800-\uDFFF\uFFFE\uFFFF]')

def escape_xml_illegal_chars(val, replacement=''):
    '''
        Replaces any character not supported by XML with
        a replacement string (default is an empty string)
        Thanks to http://goo.gl/ZziIz
    '''
    return _illegal_xml_chars_re.sub(replacement, val)


class DFMPSolr(SearchIndex):
    def remove_dict(self, ast_dict):self.delete_asset(ast_dict)

    def update_dict(self, ast_dict, defer_commit=False):self.index_asset(ast_dict, defer_commit)

    def index_asset(self, ast_dict, defer_commit=False):
        if ast_dict is None:
            return
        ast_dict['data_dict'] = json.dumps(ast_dict)

        index_fields = RESERVED_FIELDS + ast_dict.keys()

        # include the extras in the main namespace
        extras = ast_dict.get('metadata', {})
        for extra in extras:
            key, value = extra, extras[extra]
            if isinstance(value, (tuple, list)):
                value = " ".join(map(unicode, value))
            key = ''.join([c for c in key if c in KEY_CHARS])
            ast_dict['extras_' + key] = value
            if key not in index_fields:
                ast_dict[key] = value
        ast_dict.pop('metadata', None)

        # add tags, removing vocab tags from 'tags' list and adding them as
        # vocab_<tag name> so that they can be used in facets
        non_vocab_tag_names = []
        tags = ast_dict.pop('tags', [])
        context = {'model': model}

        for tag in tags:
            non_vocab_tag_names.append(tag)

        ast_dict['tags'] = non_vocab_tag_names

        # we use the capacity to make things private in the search index
        ast_dict['capacity'] = 'public'

        # if there is an owner_org we want to add this to groups for index
        # purposes
        if not ast_dict.get('organization'):
           ast_dict['organization'] = None

        ast_dict[TYPE_FIELD] = ASSET_TYPE

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
        bogus_date = datetime.datetime(1, 1, 1)
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
        # log.warn(ast_dict)

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
        
        ast_dict['index_id'] = hashlib.md5('%s%s' % (ast_dict['id']+ast_dict['assetID'],config.get('ckan.site_id'))).hexdigest()


        # send to solr:
        try:
            conn = make_connection()
            commit = not defer_commit
            if not asbool(config.get('ckan.search.solr_commit', 'true')):
                commit = False
            conn.add_many([ast_dict], _commit=commit)
        except solr.core.SolrException, e:
            msg = 'Solr returned an error: {0} {1} - {2}'.format(
                e.httpcode, e.reason, e.body[:1000] # limit huge responses
            )
            raise SearchIndexError(msg)
        except socket.error, e:
            err = 'Could not connect to Solr using {0}: {1}'.format(conn.url, str(e))
            log.error(err)
            raise SearchIndexError(err)
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


    def delete_asset(self, ast_dict):
        conn = make_connection()
        query = "+{type}:{asset} +index_id:\"{index}\" +site_id:\"{site}\"".format(
            type=TYPE_FIELD,
            asset=ASSET_TYPE,
            index=hashlib.md5('%s%s' % (ast_dict['id']+ast_dict['assetID'],config.get('ckan.site_id'))).hexdigest(),
            site=config.get('ckan.site_id'))
        try:
            conn.delete_query(query)
            if asbool(config.get('ckan.search.solr_commit', 'true')):
                conn.commit()
        except Exception, e:
            log.exception(e)
            raise SearchIndexError(e)
        finally:
            conn.close()

    def search_asset(self):
        # query = search.PackageSearchQuery()
        # q = {
        #     'q': q,
        #     'fl': 'data_dict',
        #     'wt': 'json',
        #     'fq': 'site_id:"%s"' % config.get('ckan.site_id'),
        #     'rows': BATCH_SIZE
        # }

        # for result in query.run(q)['results']:
        #     data_dict = json.loads(result['data_dict'])
        pass
