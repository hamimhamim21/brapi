import pandas as pd
from sqlalchemy import text, create_engine
from sqlalchemy.orm import Session
import sqlalchemy
import json
import os
import re
import tempfile
import numpy as np


def valid_table_name(name):
    return re.sub(r'\W+', '_', name)


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


def upload_to_postgres(df: pd.DataFrame, table_name: str, engine, chunksize=100000):
    table_name = valid_table_name(table_name)
    print(f"Table name after validation: {table_name}")

    custom_query = read_query(
        './database_queries/create_table_genomic_data.sql')
    custom_query = custom_query.replace("%(table_name)s", table_name)

    total_chunks = (len(df) // chunksize) + 1

    with engine.connect() as connection:
        with connection.begin():
            connection.execute(text(custom_query))

            for chunk_number, chunk in enumerate(np.array_split(df, total_chunks)):
                chunk = chunk.rename(columns={"#CHROM": "CHROM"})

                with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_csv_file:
                    chunk.to_csv(temp_csv_file.name, index=False)
                    temp_csv_file_name = temp_csv_file.name

                try:
                    conn = connection.connection
                    cursor = conn.cursor()
                    with open(temp_csv_file_name, 'r') as f:
                        cursor.copy_expert(
                            f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)
                    conn.commit()
                    cursor.close()
                finally:
                    os.remove(temp_csv_file_name)

                print(
                    f"Chunk {chunk_number + 1}/{total_chunks} uploaded to table '{table_name}'.")

    print(
        f"All data uploaded to table '{table_name}' in the PostgreSQL database.")


def check_study_db_id_exists(study_db_id: str, table_name: str, engine) -> bool:
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT 1 FROM {table_name} WHERE study_db_id = :study_db_id LIMIT 1"), {
                                    "study_db_id": study_db_id}).fetchone()
        return result is not None

# Function to extract metadata from VCF file and insert into PostgreSQL


def extract_and_upload_metadata(file: str, database_url: str):
    study_db_id = 'genomic_data_' + os.path.splitext(os.path.basename(file))[0]

    vcf = read_vcf(file)
    call_set_count = len(vcf.columns) - vcf.columns.get_loc("FORMAT") - 1
    variant_count = len(vcf)

    engine = create_engine(database_url)

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

    metadata_df = pd.DataFrame([metadata])
    metadata_df["metadata_fields"] = metadata_df["metadata_fields"].apply(
        json.dumps)

    vcf['study_db_id'] = study_db_id
    metadata_df.to_sql('vcf_metadata', engine, if_exists='append', index=False)
    upload_to_postgres(vcf, f'{study_db_id}', engine)

    print(
        f"Metadata uploaded to 'vcf_metadata' table and data uploaded to 'genomic_data_{study_db_id}' table in the PostgreSQL database.")

    # Upload genomic data to PostgreSQL with a dynamic table name
    upload_to_postgres(vcf, f'{study_db_id}', engine)

    print(
        f"Metadata uploaded to 'vcf_metadata' table and data uploaded to 'genomic_data_{study_db_id}' table in the PostgreSQL database.")
