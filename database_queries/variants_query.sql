SELECT "CHROM", "POS", "REF", "ALT", "QUAL", "FILTER", "INFO"
FROM "%(table_name)s"
WHERE "CHROM" = '%(chrom)s' AND "POS" = '%(pos)s'  AND "REF" = '%(ref)s'  AND "ALT" = '%(alt)s' ;
