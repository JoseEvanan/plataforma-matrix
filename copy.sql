COPY listing
FROM 's3://mybucket/data/listings/parquet/'
IAM_ROLE 'arn:aws:iam::0123456789012:role/MyRedshiftRole'
FORMAT AS PARQUET;