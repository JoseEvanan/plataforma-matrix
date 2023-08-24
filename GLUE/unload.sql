  unload ('select * from prod__acselx.transacre')
        to 's3://ue1stgdesaas3mat005/BACKUP/'
        iam_role 'arn:aws:iam::477542548955:role/RedshiftRole'
        FORMAT AS PARQUET
        ALLOWOVERWRITE
        maxfilesize 512 mb;


arn:aws:iam::198328445529:role/service-role/AmazonRedshift-CommandsAccessRole-20221129T004338