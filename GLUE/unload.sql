  unload ('select * from prod__acselx.transacre')
        to 's3://ue1stgdesaas3mat005/BACKUP/'
        iam_role 'arn:aws:iam::477542548955:role/RedshiftRole'
        FORMAT AS PARQUET
        ALLOWOVERWRITE
        maxfilesize 512 mb;
