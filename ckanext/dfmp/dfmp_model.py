import uuid
import ckan.model as model
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String


def make_uuid():
    return unicode(uuid.uuid4())



Base = declarative_base()

class DFMPAssets(Base):
  __tablename__ = 'dfmp_assets'

  id = Column(Integer, primary_key=True)
  item = Column(String)
  url = Column(String)
  parent = Column(String)
  name = Column(String)
  asset_metadata = Column(String)
  lastModified = Column(String)

  def __repr__(self):
    return "<DFMP Item(item='%s', url='%s', parent='%s',  name='%s')>" % (self.item, self.url, self.parent, self.name)

def init_tables():
  Base.metadata.drop_all(model.meta.engine, tables=[DFMPAssets.__table__], checkfirst=True) 
  Base.metadata.create_all(model.meta.engine, tables=[DFMPAssets.__table__], checkfirst=True) 