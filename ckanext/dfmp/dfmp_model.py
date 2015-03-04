import uuid
import ckan.model as model
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String


def make_uuid():
    return unicode(uuid.uuid4())



Base = declarative_base()

class DFMPAssets(Base):
  __tablename__ = 'dfmp_assets'

  # id = Column(Integer, primary_key=True, index=True)
  # item = Column(String, index=True)
  # url = Column(String)
  # parent = Column(String)
  # asset_metadata = Column(String)
  # lastModified = Column(String)
  id = Column(Integer, primary_key=True)
  parent = Column(String)
  asset_id = Column(String)
  name = Column(String, index=True)

  def __repr__(self):
    return "<DFMP Item(id='%s')>" % (self.id)

def init_tables():
  Base.metadata.drop_all(model.meta.engine, tables=[DFMPAssets.__table__], checkfirst=True) 
  Base.metadata.create_all(model.meta.engine, tables=[DFMPAssets.__table__], checkfirst=True) 