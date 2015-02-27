import logging
from ckan.lib.cli import CkanCommand

class InitDB(CkanCommand):
    """Initialise the DFMP's database tables"""
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 0
    min_args = 0

    def command(self):
        self._load_config()

        import ckan.model as model
        model.Session.remove()
        model.Session.configure(bind=model.meta.engine)
        log = logging.getLogger(__name__)

        import ckanext.dfmp.dfmp_model as dfmp_model
        dfmp_model.init_tables()
        log.info("DB tables are setup")
