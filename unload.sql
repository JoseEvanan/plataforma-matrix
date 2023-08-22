  unload ('select * from test.m_puntoventa')
        to 's3://ue1stgdesaas3mat005/BACKUP/20230724/m_puntoventa/'
        iam_role 'arn:aws:iam::198328445529:role/service-role/AmazonRedshift-CommandsAccessRole-20221129T004338'
        FORMAT AS PARQUET
        ALLOWOVERWRITE
        maxfilesize 512 mb;
       