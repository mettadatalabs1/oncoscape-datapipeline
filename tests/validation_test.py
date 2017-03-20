from db.mongo_connector import MongoConnector
from db import db_config
from workflow.executors import validation_tasks
from validators.validation_configurator import ValidationConfigurator
from settings import configs

def test_data_type_validation():
    """
    Test function for data type Validations
    """
    vc = ValidationConfigurator("mut01")
    assert vc.validate("0") == (True, True)
    vc = ValidationConfigurator("mut01")
    assert vc.validate("2") == (True, False)
    vc = ValidationConfigurator("cnv")
    assert vc.validate("2.21") == (True, None)
    vc = ValidationConfigurator("cnv")
    # this is an issue we need to fix. since "2" can be cast into a  float
    # this is returning true. Should be false
    assert vc.validate("2") == (False, None)

def test_protocol_creation():
    """
    Test function to check if mongo protocol string is being generated right
    """
    connection_params = configs.DB_CONFIG
    connection_string = db_config.create_mongo_protocol("oncoscape",
        connection_params)
    assert connection_string == \
    ("mongodb://oncoscapeRead:i1f4d9botHD4xnZ@oncoscape-dev-db1."
     "sttrcancer.io:27017,oncoscape-dev-db2.sttrcancer.io:27017,oncoscape-dev-d"
     "b3.sttrcancer.io:27017/tcga?authSource=admin")

def test_hugo_validation():
    """
    Test function for Hugo validations
    """
    cstring = ("mongodb://oncoscapeRead:i1f4d9botHD4xnZ@oncoscape-dev-db1."
     "sttrcancer.io:27017,oncoscape-dev-db2.sttrcancer.io:27017,oncoscape-dev-d"
     "b3.sttrcancer.io:27017/tcga?authSource=admin")
    mongo_connector = MongoConnector(cstring, "tcga")
    hugo_validator = validation_tasks.HugoValidator
    hugo_validator.populate_hugo_genes_map(mongo_connector,
                                            "lookup_oncoscape_genes")
    assert hugo_validator.validate_hugo("tcga","A4GNT") == (None, 'A4GNT')
    assert hugo_validator.validate_hugo("tcga","NM_016161") == ('NM_016161',
                                                                'A4GNT')
    assert hugo_validator.validate_hugo("tcga","INVALID") == (None, None)
