SELECT study_db_id || '-' || "CHROM" AS referenceDbId, 
       study_db_id AS referenceSetDbId, 
       "CHROM" AS referenceName 
FROM "%(table_name)s"
GROUP BY referenceDbId, study_db_id, referenceName
