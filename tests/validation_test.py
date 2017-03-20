from db.mongo_connector import MongoConnector
from db import db_config
from workflow.executors import validation_tasks
from validators.validation_configurator import ValidationConfigurator
from pipeline.models import InputFile
from test_files import job_config
from settings import configs

def test_data_type_validation():
    """
    Test function for data type Validations
    """
    vc = ValidationConfigurator("mut01")
    assert vc.validate("0") == True
    vc = ValidationConfigurator("mut01")
    assert vc.validate("2") == False
    vc = ValidationConfigurator("cnv")
    assert vc.validate("2.21") == True
    vc = ValidationConfigurator("cnv")
    # this is an issue we need to fix. since "2" can be cast into a  float
    # this is returning true. Should be false
    assert vc.validate("2") == False

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

def test_file_load_and_validation():
    """
    Test function for loading, type and Hugo validating an input file from
    job config
    """
    ftv = InputFile(job_config.job_config)
    print ("="*20)
    print ("Running test for config parsing and input file object creation")
    file_type_check = ftv.datatype == "cnv"
    validator_check = ftv.datatype_validator.valid_values == None
    assert file_type_check and validator_check
    validation_tasks.validate_file(ftv)
    assert len(ftv.valid_samples) ==  1080
    assert ftv.valid_samples[1]["sample"] ==  "TCGA-3C-AALI-01"
    assert len(ftv.valid_samples[1]["values"]) == 7 and\
        "ANKRD65" not in ftv.valid_samples[1]["genes"]


    # TCGA-3C-AALI-01
    # ANKRD65
    # B3GALT6_INVALID
