SELECT column_name,table_name
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = '%(table_name)s'
