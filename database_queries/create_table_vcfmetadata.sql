DROP TABLE IF EXISTS vcf_metadata;

CREATE TABLE IF NOT EXISTS vcf_metadata
(
    id SERIAL PRIMARY KEY,
    data_format TEXT,
    file_format TEXT,
    file_url TEXT,
    call_set_count INTEGER,
    reference_set_db_id TEXT,
    study_db_id TEXT,
    variant_count INTEGER,
    variant_set_db_id TEXT,
    variant_set_name TEXT,
    metadata_fields JSONB
);

ALTER TABLE IF EXISTS public.vcf_metadata
    OWNER to postgres;
