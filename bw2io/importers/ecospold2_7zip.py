from ..extractors import Ecospold2Data7zipExtractor
from .ecospold2 import SingleOutputEcospold2Importer

class SingleOutputEcospold27zipImporter(SingleOutputEcospold2Importer):
    
    def __init__(self, dirpath, db_name, extractor=None,
                 use_mp=True, signal=None):
        super(SingleOutputEcospold27zipImporter, self).__init__(dirpath, db_name, extractor=Ecospold2Data7zipExtractor,
                                                               use_mp=True, signal=None)