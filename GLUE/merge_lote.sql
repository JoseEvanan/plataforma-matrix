  select count(1)
from test.m_cliente;
-- Before: 3,890,903
-- After:  3,183,624
 DELETE FROM test.m_cliente 
WHERE cod_personath IN (
    SELECT distinct cod_personath
    FROM test.m_cliente__cdc );
-- After:  3,900,206
  INSERT INTO test.m_cliente (  
	cod_personath,
tip_persona,
des_tipopersona,
tip_documentoidentidad,
des_documentoidentidad,
flg_documentoidentidad,
flg_ruc,
des_nombre,
des_apepaterno,
des_apematerno,
fec_nacimiento,
can_edad,
tip_sexo,
tip_estadocivil,
flg_tenenciahijos,
flg_telefonofijo,
flg_telefonocelular,
flg_consentimientollamada,
flg_correo,
flg_consentimientoemail,
flg_direccion,
flg_direccionerror,
flg_direcciongps,
tip_nivelsocioeconomico,
fec_afiliacion,
des_afiliacioncadena,
fec_creacion,
fec_ultimamodificacion,
flg_autorizacompartirdatosocio,
flg_clientefallecido,
flg_autocanje,
filename_matrix,
fec_proc_matrix,
load_user_matrix)
  SELECT 
  cod_personath,
tip_persona,
des_tipopersona,
tip_documentoidentidad,
des_documentoidentidad,
flg_documentoidentidad,
flg_ruc,
des_nombre,
des_apepaterno,
des_apematerno,
fec_nacimiento,
can_edad,
tip_sexo,
tip_estadocivil,
flg_tenenciahijos,
flg_telefonofijo,
flg_telefonocelular,
flg_consentimientollamada,
flg_correo,
flg_consentimientoemail,
flg_direccion,
flg_direccionerror,
flg_direcciongps,
tip_nivelsocioeconomico,
fec_afiliacion,
des_afiliacioncadena,
fec_creacion,
fec_ultimamodificacion,
flg_autorizacompartirdatosocio,
flg_clientefallecido,
flg_autocanje,
filename_matrix,
fec_proc_matrix,
load_user_matrix
  FROM (
    SELECT d.*, ROW_NUMBER() OVER(PARTITION BY d.cod_personath ORDER BY d.fec_proc_matrix DESC) AS row_num
	FROM test.m_cliente__cdc d
    )
  WHERE row_num = 1;