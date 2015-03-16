import ckan.plugins.toolkit as toolkit


def _validate(data, *fields):
  for field in fields:
    if not field in data:

      raise toolkit.ValidationError('Parameter {%s} must be defined' % field)

def _unjson_base(string):
  return string\
    .replace('"("{','{')\
    .replace('("{','{')\
    .replace('}","")"','}')\
    .replace('}","")','}')\
    .replace('\\\\""','\\""')

def _unjson(string):
  return _unjson_base(string).replace('""','"')