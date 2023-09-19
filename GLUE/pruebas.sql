select 'T_ACUMULACIONSUPERMERCADOS' name_, count(1) from test.t_acumulacionsupermercados 
union all
select 'M_ACUMULACIONSUPERMERCADOSDICCIONARIO' name_, count(1) from test.m_acumulacionsupermercadosdiccionario 

select 'T_ACUMULACIONSUPERMERCADOS' name_, count(1) from test.t_acumulacionsupermercados 
"count","name_"
425474,M_ACUMULACIONSUPERMERCADOSDICCIONARIO
522310713,T_ACUMULACIONSUPERMERCADOS

- archivo de transacciones detalle de las ventas = 522,310,647 reg
- archivo diccionario de productos = 425,474

select * from test.t_acumulacionsupermercados order by 1 asc limit 100

select 522310713 - 522310647
select 'T_ACUMULACIONSUPERMERCADOS' name_, count(1) from trusted.t_acumulacionsupermercados 
union all
select 'M_ACUMULACIONSUPERMERCADOSDICCIONARIO' name_, count(1) from trusted.m_acumulacionsupermercadosdiccionario


with tmp as (
select to_char(fec_transaccion , 'YYYY-MM') aniomes --, *
from test.t_acumulacionsupermercados 
)
select aniomes, count(1)
from tmp
group by aniomes 


grinded


select count(1) from trusted.m_acumulacionsupermercadosdiccionario
union all 
select count(1) from test.m_acumulacionsupermercadosdiccionario

----

with tmp as (
select to_char(fec_transaccion , 'YYYY-MM') aniomes --, *
from test.t_acumulacionsupermercados -- limit 10
) select aniomes, count(1)
from tmp
group by aniomes 


with tmp as (
select to_char(fec_transaccion , 'YYYY-MM') aniomes --, *
from test.t_acumulacionsupermercados -- limit 10
) select aniomes, count(1)
from tmp
group by aniomes 


---
with tmp as (
select to_char(fec_transaccion , 'YYYY-MM') aniomes --, *
from trusted.t_acumulacionsupermercados -- limit 10
) select aniomes, count(1)
from tmp
group by aniomes 

drop table "trusted".t_acumulacionsupermercados 


insert into trusted.t_acumulacionsupermercados select * from test.t_acumulacionsupermercados;

insert into trusted.m_acumulacionsupermercadosdiccionario select * from test.m_acumulacionsupermercadosdiccionario;

truncate table trusted.t_acumulacionsupermercados

INSERT INTO trusted.t_acumulacionsupermercados (des_acumulacioncadena,des_acumulaciontienda,cod_tarjeta,cod_personath,fec_transaccion,feh_transaccion,num_caja,num_secuenciacaja,imp_solespagados,des_signo,num_puntosacumulados,des_signo1,cod_promocion,cod_canal,des_acumulacionsupermercados,cod_cajera,tip_formapago,des_acumulacionsupermercados2,num_ruc,des_secuenciadetalle,cod_articulo,can_acumulacionsupermercados,des_signo2,imp_preciounitario,des_signo3,imp_descuento,des_signo4,cod_formapago,des_acumulacionsupermercados3,num_puntoscanjeados)
SELECT des_acumulacioncadena,des_acumulaciontienda,cod_tarjeta,cod_personath,fec_transaccion,feh_transaccion,num_caja,num_secuenciacaja,imp_solespagados,des_signo,num_puntosacumulados,des_signo1,cod_promocion,cod_canal,des_acumulacionsupermercados,cod_cajera,tip_formapago,des_acumulacionsupermercados2,num_ruc,des_secuenciadetalle,cod_articulo,can_acumulacionsupermercados,des_signo2,imp_preciounitario,des_signo3,imp_descuento,des_signo4,cod_formapago,des_acumulacionsupermercados3,num_puntoscanjeados
FROM test.t_acumulacionsupermercados
where to_char(fec_transaccion , 'YYYY-MM') = '2021-01' --- in ('2021-01','2021-02','2021-03')

truncate table trusted.m_acumulacionsupermercadosdiccionario

INSERT INTO trusted.m_acumulacionsupermercadosdiccionario (cod_articulo,cod_articulooriginal,des_supermercadosdiccionario,cod_departamento,des_acumulacionsupermercadodiccionario,des_departamento,des_categoria,cod_subcategoria,des_subcategoria,des_marca,des_acumulacionsupermercadodiccionario2,cod_proveedor,des_proveedor)
SELECT cod_articulo,cod_articulooriginal,des_supermercadosdiccionario,cod_departamento,des_acumulacionsupermercadodiccionario,des_departamento,des_categoria,cod_subcategoria,des_subcategoria,des_marca,des_acumulacionsupermercadodiccionario2,cod_proveedor,des_proveedor
FROM test.m_acumulacionsupermercadosdiccionario


-- test.t_acumulacionsupermercados definition

-- Drop table

-- DROP TABLE test.t_acumulacionsupermercados;

--DROP TABLE test.t_acumulacionsupermercados;
CREATE TABLE IF NOT EXISTS trusted.t_acumulacionsupermercados
(
	des_acumulacioncadena VARCHAR(65535)   ENCODE lzo
	,des_acumulaciontienda VARCHAR(65535)   ENCODE lzo
	,cod_tarjeta VARCHAR(65535)   ENCODE lzo
	,cod_personath VARCHAR(65535)   ENCODE lzo
	,fec_transaccion TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,feh_transaccion TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,num_caja INTEGER   ENCODE az64
	,num_secuenciacaja INTEGER   ENCODE az64
	,imp_solespagados REAL   ENCODE RAW
	,des_signo VARCHAR(65535)   ENCODE lzo
	,num_puntosacumulados INTEGER   ENCODE az64
	,des_signo1 VARCHAR(65535)   ENCODE lzo
	,cod_promocion VARCHAR(65535)   ENCODE lzo
	,cod_canal VARCHAR(65535)   ENCODE lzo
	,des_acumulacionsupermercados VARCHAR(65535)   ENCODE lzo
	,cod_cajera VARCHAR(65535)   ENCODE lzo
	,tip_formapago VARCHAR(65535)   ENCODE lzo
	,des_acumulacionsupermercados2 VARCHAR(65535)   ENCODE lzo
	,num_ruc INTEGER   ENCODE az64
	,des_secuenciadetalle VARCHAR(65535)   ENCODE lzo
	,cod_articulo VARCHAR(65535)   ENCODE lzo
	,can_acumulacionsupermercados REAL   ENCODE RAW
	,des_signo2 VARCHAR(65535)   ENCODE lzo
	,imp_preciounitario REAL   ENCODE RAW
	,des_signo3 VARCHAR(65535)   ENCODE lzo
	,imp_descuento REAL   ENCODE RAW
	,des_signo4 VARCHAR(65535)   ENCODE lzo
	,cod_formapago VARCHAR(65535)   ENCODE lzo
	,des_acumulacionsupermercados3 VARCHAR(65535)   ENCODE lzo
	,num_puntoscanjeados INTEGER   ENCODE az64
)
DISTSTYLE EVEN
;
ALTER TABLE test.t_acumulacionsupermercados owner to awsuser;



-- test.m_acumulacionsupermercadosdiccionario definition

-- Drop table

-- DROP TABLE test.m_acumulacionsupermercadosdiccionario;

--DROP TABLE test.m_acumulacionsupermercadosdiccionario;
CREATE TABLE IF NOT EXISTS trusted.m_acumulacionsupermercadosdiccionario
(
	cod_articulo VARCHAR(65535)   ENCODE lzo
	,cod_articulooriginal VARCHAR(65535)   ENCODE lzo
	,des_supermercadosdiccionario VARCHAR(65535)   ENCODE lzo
	,cod_departamento VARCHAR(65535)   ENCODE lzo
	,des_acumulacionsupermercadodiccionario VARCHAR(65535)   ENCODE lzo
	,des_departamento VARCHAR(65535)   ENCODE lzo
	,des_categoria VARCHAR(65535)   ENCODE lzo
	,cod_subcategoria VARCHAR(65535)   ENCODE lzo
	,des_subcategoria VARCHAR(65535)   ENCODE lzo
	,des_marca VARCHAR(65535)   ENCODE lzo
	,des_acumulacionsupermercadodiccionario2 VARCHAR(65535)   ENCODE lzo
	,cod_proveedor VARCHAR(65535)   ENCODE lzo
	,des_proveedor VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE test.m_acumulacionsupermercadosdiccionario owner to awsuser;


  unload ('select * from test.t_acumulacionsupermercados')
        to 's3://ue1stgdesaas3mat005/BACKUP/test.t_acumulacionsupermercados/'
        iam_role 'arn:aws:iam::198328445529:role/service-role/AmazonRedshift-CommandsAccessRole-20221129T004338'
        FORMAT AS PARQUET
        ALLOWOVERWRITE
        maxfilesize 512 mb;
        
       
COPY trusted.t_acumulacionsupermercados
FROM 's3://ue1stgdesaas3mat005/BACKUP/test.t_acumulacionsupermercados/'
IAM_ROLE 'arn:aws:iam::198328445529:role/service-role/AmazonRedshift-CommandsAccessRole-20221129T004338'
FORMAT AS PARQUET;


DELETE FROM trusted.t_acumulacionsupermercados 
where des_acumulacioncadena = '' and fec_transaccion is null;


select *
from trusted.t_acumulacionsupermercados
where des_acumulacioncadena = '' and fec_transaccion is null 
des_acumulacioncadena

CREATE TABLE trusted.t_acumulacionsupermercados2 AS SELECT * FROM test.t_acumulacionsupermercados;
       