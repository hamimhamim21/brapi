SELECT "CHROM", "POS", "REF", "ALT", "QUAL", "FILTER", "INFO"
FROM genomic_data
WHERE "CHROM" = :chrom AND "POS" = :pos AND "REF" = :ref AND "ALT" = :alt;
