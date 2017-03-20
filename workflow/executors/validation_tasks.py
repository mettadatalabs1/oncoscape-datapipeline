from validators.validation_configurator import ValidationConfigurator
from pipeline.models import InputFile


def validate_file(input_file_obj):
    if not input_file_obj.directory and not input_file_obj.s3_path:
        return None
    if not input_file_obj.file:
        return None
    input_file = (input_file_obj.directory
                        if input_file_obj.directory else input_file_obj.s3_path)
    input_file += "/" + input_file_obj.file
    with open(input_file, "r") as file_to_validate:
        header = file_to_validate.readline().strip("\n")
        samples = [{sample: {"values":[],"genes":[],},}
                    for sample in header.split("\t")[1:]]
        print (samples[-1])
        for line in file_to_validate:
            pass


class HugoValidator(object):
    # hugo_genes_map (Dictionary): a dictionary that has the hugo genes and
    # respective aliases. Each entry is db:{gene: Set(aliases),}.
    # This is created the first time the class is loaded and is static.
    # We use set because alias look up will be O(1) and the overall complexity
    # for each row is O(n), yielding a total complexity of O(n^2)
    # for an input file. The assumption is that different projects might have
    # different gene maps and we want to create the map per project once.
    hugo_genes_map = {}

    @classmethod
    def populate_hugo_genes_map(cls, mongo_connector,collection):
        """
            Populates the hugo_genes_map for a given database.
            Args:
            mongo_connector (db.mongo_connector.MongoConnector): The mongo
            connection holding the db name and the connection to the db
            collection: the name of the collection to query
        """
        db = mongo_connector.db.name
        if db not in HugoValidator.hugo_genes_map:
            gene_maps_from_db = mongo_connector.find(query=None,
                                                    collection=collection)
            gene_maps_local = {}
            for gene_map in gene_maps_from_db:
                gene_maps_local[gene_map["hugo"]] =\
                    frozenset(gene_map["symbols"])
            HugoValidator.hugo_genes_map[db] = gene_maps_local
        print (len(HugoValidator.hugo_genes_map[db]))

    @classmethod
    def validate_hugo(cls, db, gene_symbol):
        """
        Validates if a given gene symbol is a gene name, an alias, or is an
        invalid entry.
        Args:
        db (string): The database in which we want to check
        gene_symbol (string): The gene symbol to checking
        Returns:
        (string, string): A 2 tuple with gene_symbol that was sent and the
        parent if it is an alias. If a match, the tuple is (None, gene_symbol).
        If invalid, the tuple is (None, None)
        """
        gene_valid_status = (None, None)
        db_genes_map = HugoValidator.hugo_genes_map[db]
        if gene_symbol in db_genes_map:
            gene_valid_status = (None, gene_symbol)
        else:
            for gene in db_genes_map:
                if gene_symbol in db_genes_map[gene]:
                    gene_valid_status = (gene_symbol, gene)
                    break
        return gene_valid_status


job_config = {
    "dataset": "acc",
    "source": "ucsc",
    "type": "cnv",
    "directory": "/work/code/mettalabs/hutch/sttr/oncoscape-datapipeline/test_files",
    "process": "broadcurated",
    "file": "Gistic2_CopyNumber_Gistic2_all_data_by_genes.xxsmall",
    "row_identifier": "genes",
    "col_identifier": "cohort",
    "job_id": "job001",
    "job_status": {"state": "IN-PROGRESS",
                "details": ["INGESTED"]
               },
    "job_creation_time": None,
    "job_log_reference": "/var/logs/jobs/jobID/"
}
