# abstracted and active tables
# keys, tables and tableaux
import datajoint as dj
from datajoint.utils import to_camel_case
from datajoint.hash import key_hash
from importlib import import_module
schema = dj.Schema('abstracted')




@schema
class Keys(dj.Lookup):
    definition = """
    key_hash : char(32)
    ---
    key : blob
    """
    @property
    def key(self):
        return self.fetch('key').tolist()
    
    def include(self, key):
        self.insert1({"key": key, "key_hash": key_hash(key)}, skip_duplicates = True)
        return {'key_hash': key_hash(key)}



class ModularTables:    
    @property
    def obj(self):
        table_name, module_name = (Table.Modular & self).fetch1('table_name', 'module_name')
        module = import_module(module_name)
        return getattr(module, to_camel_case(table_name))


@schema
class Table(dj.Lookup, ModularTables):                          # TODO: module fetching from git
    definition = """                                            
        table_name : varchar(255)
        """
    contents = [['Sleep'],
                ['SleepMemory'],
              ["SomaExtraction"], 
              ["Decomposition"], 
              ["DecompositionCellType"], 
              ["AutoProofreadNeuron"]]
    
    class Modular(dj.Part, dj.Lookup):                                     
        definition = """                                        # tables in existing python modules
        module_name : varchar(64)
        -> Table
        """
        contents = [['sleep', "Sleep"],
                    ['sleep', "SleepMemory"],
                    ['h01process', "SomaExtraction"], 
              ['h01process', "Decomposition"], 
              ['h01process', "DecompositionCellType"], 
              ['h01process', "AutoProofreadNeuron"]]
    def include(self, table, module):
        self.insert1(to_camel_case(table.__name__))
        self.Modular.insert1(to_camel_case(module.__name__))
