import os
import subprocess
from pymongo import MongoClient
import pandas as pd


def read_vcf_and_write_header(file: str, static_folder: str) -> str:
    num_header = 0
    header_lines = []

    with open(file) as f:
        for line in f:
            if line.startswith("##"):
                num_header += 1
                continue
            elif line.startswith("#CHROM"):
                header_lines.append(line.strip())
                header_lines.append(',')
                break  # The header ends here

    # Write the header to a file in the static folder
    header_file = os.path.join('static', "vcf_header.txt")

    new_lines = [mystring for i in header_lines for mystring in i.split('\t')]
    with open(header_file, 'w') as header_f:
        header_f.write("\n".join(new_lines))

    return header_file


def remove_documents_with_field_equal_value(mongo_url: str, db_name: str, collection_name: str, fields_file: str):
    client = MongoClient(mongo_url)
    db = client[db_name]
    collection = db[collection_name]

    # Read field names from the fields file
    with open(fields_file, 'r') as file:
        fields = file.read().splitlines()

    # Find the first 100 documents
    documents = collection.find().limit(100)
    total_deleted = 0

    for doc in documents:
        for field in fields:
            if field in doc and doc[field] == field:
                collection.delete_one({"_id": doc["_id"]})
                total_deleted += 1
                break  # Break the inner loop as we already deleted the document

        # Check if the document has only one field besides _id
        if len(doc.keys()) == 2:  # Only _id and one other field
            collection.delete_one({"_id": doc["_id"]})
            total_deleted += 1

    print(
        f"Deleted a total of {total_deleted} unwanted documents in '{collection_name}'.")

# Function to import VCF file to MongoDB using mongoimport


def import_vcf_to_mongodb(vcf_file: str, mongo_url: str, db_name: str, collection_name: str, fields_file: str):
    cmd = [
        "mongoimport",
        "--uri", mongo_url,
        "--db", db_name,
        "--collection", collection_name,
        "--type", "tsv",
        "--file", vcf_file,
        "--fieldFile", fields_file,
        "--ignoreBlanks"
    ]

    subprocess.run(cmd, check=True)
    print(
        f"Data imported to MongoDB collection '{collection_name}' in database '{db_name}'.")

# Function to remove documents where a specific field equals a given value


def remove_documents_by_field(mongo_url: str, db_name: str, collection_name: str, field: str, value: str):
    client = MongoClient(mongo_url)
    db = client[db_name]
    result = db[collection_name].delete_many({field: value})
    print(f"Deleted {result.deleted_count} documents where {field} equals {value} from collection '{collection_name}'.")

# Function to extract metadata and upload metadata to MongoDB


def extract_and_upload_metadata(file: str, mongo_url: str, db_name: str):

    study_db_id = 'genomic_data_' + os.path.splitext(os.path.basename(file))[0]
    collection_name = study_db_id
    fields_file = read_vcf_and_write_header(file, 'static')
    # Path to the fields file

    # Import VCF file directly to MongoDB
    import_vcf_to_mongodb(file, mongo_url, db_name,
                          collection_name, fields_file)
    remove_documents_with_field_equal_value(
        mongo_url, db_name, collection_name, fields_file=fields_file)
    os.remove(fields_file)

    # Extract metadata
    call_set_count = 0
    variant_count = 0
    headers = []
    with open(file) as f:
        for line in f:
            if line.startswith("##"):
                continue
            elif line.startswith("#CHROM"):
                headers = line.strip().split('\t')
                call_set_count = len(headers) - headers.index("FORMAT") - 1
            else:
                variant_count += 1

    client = MongoClient(mongo_url)
    db = client[db_name]

    metadata = {
        "data_format": "VCF",
        "file_format": "text/tsv",
        "file_url": f"http://localhost:8000/static/{os.path.basename(file)}",
        "call_set_count": call_set_count,
        "reference_set_db_id": study_db_id,
        "study_db_id": study_db_id,
        "variant_count": variant_count,
        "variant_set_db_id": f"{study_db_id}-Run1",
        "variant_set_name": "Run1",
        "metadata_fields": [
            {"dataType": "integer", "fieldAbbreviation": "DP",
                "fieldName": "Read Depth"},
            {"dataType": "float", "fieldAbbreviation": "GL",
                "fieldName": "Genotype Probabilities as p(AA),p(AB),p(BB)"},
            {"dataType": "integer", "fieldAbbreviation": "PL",
                "fieldName": "Normalized, Phred-scaled likelihoods for genotypes as defined in the VCF specification"}
        ]
    }

    db.vcf_metadata.insert_one(metadata)
    print(f"Metadata uploaded to 'vcf_metadata' collection in the MongoDB database.")


# Example usage
if __name__ == "__main__":
    mongo_url = "mongodb://localhost:27017"
    db_name = "biologicalsamples"
    file_path = "../Ta_PRJEB31218_IWGSC-RefSeq-v1.0_filtered-SNPs.chr1B.vcf"

    # Extract and upload metadata
    extract_and_upload_metadata(file_path, mongo_url, db_name)
