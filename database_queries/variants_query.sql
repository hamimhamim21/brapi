SELECT "CHROM", "POS", "REF", "ALT", "QUAL", "FILTER", "INFO"
FROM "%(table_name)s"
WHERE "CHROM" = '%(chrom)s' AND "REF" = '%(ref)s';
