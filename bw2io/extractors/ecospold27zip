from bw2io.extractors import Ecospold2DataExtractor
from bw2data.utils import recursive_str_to_unicode
import multiprocessing
import os
import pyprind
import sys

import py7zr
import tempfile

class Ecospold2Data7zipExtractor(Ecospold2DataExtractor):

    """
    Subclass of Ecospold2DataExtractor to extract a .7z file directly downloaded from the ecoinvent website
    """
    
    @classmethod
    def extract(cls, zfilepath, db_name, use_mp=True):
        assert os.path.exists(zfilepath)
        assert os.path.splitext(zfilepath)[1] == '.7z'
        
        with tempfile.TemporaryDirectory() as td:

            with py7zr.SevenZipFile(zfilepath, 'r') as z:
                z.extractall(path=td)
                
            dirpath = os.path.join(td, 'datasets')
                        
            if os.path.isdir(dirpath):
                filelist = [filename for filename in os.listdir(dirpath)
                            if os.path.isfile(os.path.join(dirpath, filename))
                            and filename.split(".")[-1].lower() == "spold"
                            ]
            elif os.path.isfile(dirpath):
                filelist = [dirpath]
            else:
                raise OSError("Can't understand path {}".format(dirpath))

            if sys.version_info < (3, 0):
                use_mp = False

            if use_mp:
                with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
                    print("Extracting XML data from {} datasets".format(len(filelist)))
                    results = [
                        pool.apply_async(
                            Ecospold2DataExtractor.extract_activity,
                            args=(dirpath, x, db_name)
                        ) for x in filelist
                    ]
                    data = [p.get() for p in results]
            else:
                pbar = pyprind.ProgBar(len(filelist), title="Extracting ecospold2 files:", monitor=True)

                data = []
                for index, filename in enumerate(filelist):
                    data.append(cls.extract_activity(dirpath, filename, db_name))
                    pbar.update(item_id = filename[:15])

                print(pbar)

        if sys.version_info < (3, 0):
            print("Converting to unicode")
            return recursive_str_to_unicode(data)
        else:
            return data
