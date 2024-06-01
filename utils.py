import pandas as pd
from sqlalchemy import text, create_engine
from sqlalchemy.orm import Session
import sqlalchemy
import json
import os
import re


def read_query(file_path):
    with open(file_path, 'r') as file:
        return file.read()


def format_sql_query(sql_query: str, placeholders: list, key: str) -> str:
    joined_placeholders = ', '.join([f"'{item}'" for item in placeholders])
    return sql_query % {key: joined_placeholders}


def read_vcf(file: str) -> pd.DataFrame:
    num_header = 0
    with open(file) as f:
        for line in f:
            if line.startswith("##"):
                num_header += 1
    # Read the VCF, assuming the column names are on the last line of the headers
    vcf = pd.read_csv(file, sep="\t", skiprows=num_header, dtype=str)
    vcf = vcf.rename(columns={"#CHROM": "CHROM"})
    return vcf

# Function to upload DataFrame to PostgreSQL


def upload_to_postgres(df: pd.DataFrame, table_name: str, engine):
    # Ensure the table name is valid by removing/escaping any characters that aren't allowed
    table_name = re.sub(r'\W+', '_', table_name)
    print(table_name)
    # Upload data to PostgreSQL table
    df.to_sql(table_name, engine, if_exists='replace',
              index=False, method='multi', chunksize=1000)
    print(f"Data uploaded to table '{table_name}' in the PostgreSQL database.")

# Function to check if study_db_id already exists


def check_study_db_id_exists(study_db_id: str, table_name: str, engine) -> bool:
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT 1 FROM {table_name} WHERE study_db_id = :study_db_id LIMIT 1"), {
                                    "study_db_id": study_db_id}).fetchone()
        return result is not None

# Function to extract metadata from VCF file and insert into PostgreSQL


def extract_and_upload_metadata(file: str, database_url: str):
    # Read VCF file
    vcf = read_vcf(file)

    # Calculate call_set_count and variant_count
    call_set_count = len(vcf.columns) - vcf.columns.get_loc("FORMAT") - 1
    variant_count = len(vcf)

    # Extract base identifier from filename
    study_db_id = os.path.splitext(os.path.basename(file))[0]

    study_db_id = 'genomic_data_'+study_db_id

    # Create SQLAlchemy engine
    engine = create_engine(database_url)

    # Check if the study_db_id already exists
    if check_study_db_id_exists(study_db_id, "vcf_metadata", engine):
        raise Exception(f"File with study_db_id {study_db_id} already exists")

    # Example metadata extraction (replace with actual extraction logic)
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

    # Convert metadata to DataFrame
    metadata_df = pd.DataFrame([metadata])
    metadata_df["metadata_fields"] = metadata_df["metadata_fields"].apply(
        json.dumps)

    # Adding study_db_id column to the VCF DataFrame
    vcf['study_db_id'] = study_db_id

    # Upload metadata to PostgreSQL table
    metadata_df.to_sql('vcf_metadata', engine, if_exists='append', index=False)

    # Upload genomic data to PostgreSQL with a dynamic table name
    upload_to_postgres(vcf, f'{study_db_id}', engine)

    print(
        f"Metadata uploaded to 'vcf_metadata' table and data uploaded to 'genomic_data_{study_db_id}' table in the PostgreSQL database.")
