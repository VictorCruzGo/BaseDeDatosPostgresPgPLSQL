--v15.05.202.11:41
--todas las etapas
-----------------------------------
/*CREATE OR REPLACE FUNCTION sre_recaudaciones.sre_fac_migrar_tablas_certificacion(p_etapa integer)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$*/

do
$$
declare
	v_etapa_certificacion integer:=0;
	--v_sistema_id bigint:=0;
	v_usuario integer:=1000;
   	
   	v_sre_fac_casos_prueba varchar(200);
	v_sre_fac_log_prueba_nuevo varchar(200);
	v_sre_fac_log_antiguo varchar(200);

	v_sql_actualizar_caso_prueba_null varchar:='';
	v_sql_obtener_fechas_etapa varchar:='';
	v_sql_obtener_datos_tabla_antiguo varchar:='';
	v_sql_calcular_pruebas_esperadas varchar:='';
	v_sql_comprobar_totales varchar:='';
	v_sql_insertar_datos_migrados varchar:='';
	v_cantidad_registros_migrados integer:=0;
begin	
--Etapa - Obtención CUFD (ok)
	--v_etapa_certificacion:=3016;
--Etapa X – Envio Masivo (ok)
	--v_etapa_certificacion:=3053;
--Etapa II - Validaciones Generales XML/XSD 
	v_etapa_certificacion:=2852;
--Etapa IV - Sincronización de Fecha y Hora (ok)
	--v_etapa_certificacion:=2854;
--Etapa VI - Envío de Paquetes (ok)
	--v_etapa_certificacion:=2856;
--Etapa VIII - Gestión de sucursales (ok)
	--v_etapa_certificacion:=3069;
--Etapa IX – Anulaciones (ok)
	--v_etapa_certificacion:=3141;

		
	-- Obtener nombres de tablas por etapa
	case
		when v_etapa_certificacion=2850 then
			--2850	Etapa - Generación de CUF
			v_sre_fac_casos_prueba:='sre_fac_casos_prueba_0_cuf';
			v_sre_fac_log_prueba_nuevo:='sre_fac_log_prueba_0_cuf';
			v_sre_fac_log_antiguo:='sre_fac_log_0_cuf';
		when v_etapa_certificacion=2851 then			
			--2851	Etapa I - Consumo de Servicios
			v_sre_fac_casos_prueba:='sre_fac_casos_prueba_1_consumo_servicio';
			v_sre_fac_log_prueba_nuevo:='sre_fac_log_prueba_1_consumo_servicio';
			v_sre_fac_log_antiguo:='sre_fac_log_1_consumo_servicio';
		when v_etapa_certificacion=2852 then			
			--2852	Etapa II - Validaciones Generales XML/XSD
			v_sre_fac_casos_prueba:='sre_fac_casos_prueba_2_validacion_generales_xml_xsd';
			v_sre_fac_log_prueba_nuevo:='sre_fac_log_prueba_2_xml_xsd';
			v_sre_fac_log_antiguo:='sre_fac_log_2_validacion_generales_xml_xsd';
		when v_etapa_certificacion=2853 then			
			--2853	Etapa III - Sincronización de Catálogos
			v_sre_fac_casos_prueba:='sre_fac_casos_prueba_3_sincronizacion_catalogos';
			v_sre_fac_log_prueba_nuevo:='sre_fac_log_prueba_3_sincronizacion';
			v_sre_fac_log_antiguo:='sre_fac_log_3_sincronizacion_catalogo';
		when v_etapa_certificacion=2854 then			
			--2854	Etapa IV - Sincronización de Fecha y Hora
			v_sre_fac_casos_prueba:='sre_fac_casos_prueba_4_sincronizacionf_fecha_hora';
			v_sre_fac_log_prueba_nuevo:='sre_fac_log_prueba_4_sinc_fecha_hora';
			v_sre_fac_log_antiguo:='sre_fac_log_4_sincronizacionf_fecha_hora';
		when v_etapa_certificacion=2855 then			
			--2855	Etapa V - Eventos Significativos
			v_sre_fac_casos_prueba:='sre_fac_casos_prueba_5_eventos_significativos';
			v_sre_fac_log_prueba_nuevo:='sre_fac_log_prueba_5_eventos';
			v_sre_fac_log_antiguo:='sre_fac_log_5_eventos_significativos';
		when v_etapa_certificacion=2856 then			
			--2856	Etapa VI - Envío de Paquetes
			v_sre_fac_casos_prueba:='sre_fac_casos_prueba_6_envio_paquetes';
			v_sre_fac_log_prueba_nuevo:='sre_fac_log_prueba_6_paquetes';
			v_sre_fac_log_antiguo:='sre_fac_log_6_envio_paquetes';
		when v_etapa_certificacion=2857 then			
			--2857	Etapa VII - Firma Digital y Diagrama de Despliegue
			v_sre_fac_casos_prueba:='sre_fac_casos_prueba_7_firma_digital';
			v_sre_fac_log_prueba_nuevo:='sre_fac_log_prueba_7_firma';
			v_sre_fac_log_antiguo:='sre_fac_log_7_firma_digital';
		when v_etapa_certificacion=3069 then			
			--3069	Etapa VIII - Gestión de sucursales
			v_sre_fac_casos_prueba:='sre_fac_casos_prueba_8_gestion_sucursales';
			v_sre_fac_log_prueba_nuevo:='sre_fac_log_prueba_8_sucursales';
			v_sre_fac_log_antiguo:='sre_fac_log_8_gestion_sucursales';
		when v_etapa_certificacion=3052 then			
			--3052	Etapa XI – Sistema Proveedor
			v_sre_fac_casos_prueba:='sre_fac_casos_prueba_9_proveedor';
			v_sre_fac_log_prueba_nuevo:='sre_fac_log_prueba_9_proveedor';
			v_sre_fac_log_antiguo:='sre_fac_log_9_gestion_proveedor';
		when v_etapa_certificacion=3053 then			
			--3053	Etapa X – Envio Masivo
			v_sre_fac_casos_prueba:='sre_fac_casos_prueba_10_envio_masivos';
			v_sre_fac_log_prueba_nuevo:='sre_fac_log_prueba_10_masivos';
			v_sre_fac_log_antiguo:='sre_fac_log_10_envio_masivos';
		when v_etapa_certificacion=3141 then			
			--3141	Etapa IX – Anulaciones
			v_sre_fac_casos_prueba:='sre_fac_casos_prueba_11_anulaciones';
			v_sre_fac_log_prueba_nuevo:='sre_fac_log_prueba_11_anulaciones';
			v_sre_fac_log_antiguo:='sre_fac_log_11_anulaciones';
		when v_etapa_certificacion=3016 then			
			--3016	Etapa - Obtencion CUFD
			v_sre_fac_casos_prueba:='sre_fac_casos_prueba_obtencion_cufd';
			v_sre_fac_log_prueba_nuevo:='sre_fac_log_prueba_cufd';
			v_sre_fac_log_antiguo:='sre_fac_log_obtencion_cufd';			
	end case;

	------------------------Crear tabla temportal para guardar LOGS------------------------
	/*En esta tabla se guardaran los registros LOGS de la tabla antigua.*/
	 
	drop table if exists TMP_sre_fac_log_prueba;
	CREATE temp TABLE TMP_sre_fac_log_prueba (
	sistema_id int8,
	documento_sector_id int4,
	caso_prueba_id int8,
	hash varchar(250), 
	hash_descripcion varchar(250),
	pruebas_esperadas int4,
	pruebas_correctas int4,
	porcentaje_avance numeric(5,2),
	caso_prueba_concluido bool,
	fecha_registro timestamp,
	fecha_ultima_modificacion timestamp,
	usuario_registro_id int8 ,
	usuario_ultima_modificacion_id int8,
	estado_id varchar(2)
	);	
		
	------------------------Crear tabla temportal para guardar etapas------------------------
	/* En esta tabla se las etapas con las fechas inicio y fin.*/
	DROP table if exists TMP_etapas_fechas;
	CREATE temp TABLE TMP_etapas_fechas(
		sistema_id bigint,
		fecha_inicio_prueba timestamp,
		fecha_fin_prueba timestamp,
		etapa_completada bool,
		etapa integer
	);
	
	------------------------Obteniendo fecha inicio y fin de prueba------------------------
	/*Obteniendo los datos de necesarios de las etapas*/
	insert into  TMP_etapas_fechas(
		sistema_id,
		fecha_inicio_prueba,
		fecha_fin_prueba,
		etapa_completada,
		etapa
	)
	select 
	distinct
	SISTEMA.sistema_id,
	SISTEMA.fecha_inicio_prueba, 
	coalesce(SISTEMA.fecha_fin_prueba, now()) as fecha_fin_prueba, 
	SISTEMA.etapa_completada, 
	ETAPAS.etapa_certificacion_sistemas_id
	from sre_recaudaciones.sre_fac_pruebas_certificacion_sistemas  SISTEMA
	inner join sre_recaudaciones.sre_fac_pruebas_etapa_certificacion ETAPAS
	on SISTEMA.prueba_etapa_certificacion_id=ETAPAS.prueba_etapa_certificacion_id
    where  
    SISTEMA.estado_id='AC' and
    ETAPAS.estado_id='AC' and
    ETAPAS.etapa_certificacion_sistemas_id=v_etapa_certificacion;	
     
  		      
	------------------------Obtener registros Logs de la tabla antigua------------------------
	/*Obtenieno registros Logs. La consulta se arma segun el documento sector. 
	 * Por ejemplo las etapas 2852, 2856 y 3053 los casos de pruebas se diferencian por documento_sector.*/	
	v_sql_obtener_datos_tabla_antiguo :=
	'insert into TMP_sre_fac_log_prueba 
	(
	sistema_id,
	documento_sector_id,
	caso_prueba_id, 
	hash, 
	hash_descripcion,	
	pruebas_correctas
	)

	select 
	LOGS.sistema_id,
	'
	|| 
	(case
		--when v_etapa_certificacion = 2852 or v_etapa_certificacion =  2856 or v_etapa_certificacion = 3053 or v_etapa_certificacion = 3141 or v_etapa_certificacion = 2855  or v_etapa_certificacion = 2857 then 'LOGS.documento_sector_id,'
		when v_etapa_certificacion = 2852  or v_etapa_certificacion = 3141 or v_etapa_certificacion = 2855  or v_etapa_certificacion = 2857 then 'LOGS.documento_sector_id,'
		else '0 as documento_sector_id,'
	end)
	||
	'	
	LOGS.caso_prueba_id, 
	LOGS.hash,
	LOGS.hash_descripcion,
	count(LOGS.hash) as pruebas_correctas	

	from  sre_recaudaciones.'||v_sre_fac_log_antiguo||' LOGS
	where  
	LOGS.estado_match=1 and
	LOGS.estado_id=''AC'' and		
	LOGS.fecha_prueba between (select fecha_inicio_prueba from TMP_etapas_fechas where sistema_id = LOGS.sistema_id and etapa='||v_etapa_certificacion||') and 
	(select fecha_fin_prueba from TMP_etapas_fechas where sistema_id = LOGS.sistema_id and etapa='||v_etapa_certificacion||')
	
	group by 
	LOGS.sistema_id,	
	'
	|| 
	(case
		--when v_etapa_certificacion = 2852 or v_etapa_certificacion =  2856 or v_etapa_certificacion = 3053  or v_etapa_certificacion = 3141  or v_etapa_certificacion = 2855 or v_etapa_certificacion = 2857 then 'LOGS.documento_sector_id,'
		when v_etapa_certificacion = 2852   or v_etapa_certificacion = 3141  or v_etapa_certificacion = 2855 or v_etapa_certificacion = 2857 then 'LOGS.documento_sector_id,'
		--else 'documento_sector_id,'
		else ' '
	end)
	||
	'
	LOGS.caso_prueba_id, 
	LOGS.hash, 
	LOGS.hash_descripcion;';
			
	raise notice 'v_sql_obtener_datos_tabla_antiguo;=%',v_sql_obtener_datos_tabla_antiguo;
	execute v_sql_obtener_datos_tabla_antiguo;


	------------------------Actualizacion del campo caso_prueba_id------------------------
	/*Se observo que en algunas etapas, en la tabla Logs el campo caso_prueba_id con coincidia con el campo caso_prueba_id de la tabla casos de prueba. 
	 * Por este motivo se realizo una limpieza para obtener el campo caso_prueba_id a partir del hash*/	
		

	raise notice 'Actualizacion caso_prueba_id etapas restantes';
	v_sql_actualizar_caso_prueba_null :=
	'update TMP_sre_fac_log_prueba
	set
	caso_prueba_id=(select CASOS_PRUEBAS.caso_prueba_id 
					from sre_recaudaciones.'||v_sre_fac_casos_prueba||' CASOS_PRUEBAS 
					where 
					CASOS_PRUEBAS.estado_id=''AC'' and
					'
					|| 
					(case
						--when v_etapa_certificacion = 2852 or v_etapa_certificacion =  2856 or v_etapa_certificacion = 3053  or v_etapa_certificacion = 3141  or v_etapa_certificacion = 2855 or v_etapa_certificacion = 2857 
						when v_etapa_certificacion = 2852  or v_etapa_certificacion = 3141  or v_etapa_certificacion = 2855 or v_etapa_certificacion = 2857
							then 'CASOS_PRUEBAS.documento_sector_id=TMP_sre_fac_log_prueba.documento_sector_id and'
						else ' '
					end)
					||
					' 					
					CASOS_PRUEBAS.hash=TMP_sre_fac_log_prueba.hash 
					limit 1);';

	
	raise notice 'v_sql_actualizar_caso_prueba_null;=%',v_sql_actualizar_caso_prueba_null;
	execute v_sql_actualizar_caso_prueba_null;


	--Actualizacion del campo caso_prueba_id is null de la etapa 3053-Envios masivos
	if v_etapa_certificacion = 3053 then
	raise notice 'Actualizacion caso_prueba_id etapas restantes';
		v_sql_actualizar_caso_prueba_null :=
		'update TMP_sre_fac_log_prueba
		set
		caso_prueba_id=(select CASOS_PRUEBAS.caso_prueba_id 
						from sre_recaudaciones.'||v_sre_fac_casos_prueba||' CASOS_PRUEBAS 
						where 
						CASOS_PRUEBAS.estado_id=''AC'' and
						CASOS_PRUEBAS.hash=TMP_sre_fac_log_prueba.hash 
						limit 1)
		where caso_prueba_id is null;';
	
		raise notice 'v_sql_actualizar_caso_prueba_null;=%',v_sql_actualizar_caso_prueba_null;
		execute v_sql_actualizar_caso_prueba_null;	
	end if;

	------------------------Buscar caso_prueba_id equivalente, etapa CUFD------------------------
	/*Para la etapa CUFD, se calculo el campo caso_prueba_id a partir del hast_descripcion.*/	
	
	if v_etapa_certificacion=2850 then
		update TMP_sre_fac_log_prueba
			set
				hash_descripcion=case
									when POSITION('|' IN hash_descripcion)>2 then concat('0|',substring(hash_descripcion, POSITION('|' IN hash_descripcion)+1, LENGTH(hash_descripcion)))  
									else hash_descripcion
								end
		where caso_prueba_id is null;	
	
		update TMP_sre_fac_log_prueba
			set
				caso_prueba_id=(select CASO_PRUEBA.caso_prueba_id from sre_recaudaciones.sre_fac_casos_prueba_0_cuf CASO_PRUEBA where CASO_PRUEBA.estado_id='AC' and  CASO_PRUEBA.hash_descripcion=TMP_sre_fac_log_prueba.hash_descripcion limit 1)
		where caso_prueba_id is null;	
	end if;

	------------------------Obtener pruebas esperadas------------------------
	/*Para cada caso de pruebas obtiene la cantidad de pruebas esperadas*/
	case
		when v_etapa_certificacion = 2850 then 
			v_sql_calcular_pruebas_esperadas :=
			'update TMP_sre_fac_log_prueba	
			set
			pruebas_esperadas=	(select  CASOS_PRUEBAS.total_pruebas 
								 from sre_recaudaciones.'||v_sre_fac_casos_prueba||' CASOS_PRUEBAS 
								 where 
								 CASOS_PRUEBAS.hash=TMP_sre_fac_log_prueba.hash AND
								 CASOS_PRUEBAS.estado_id=''AC''
								 limit 1);';
		
			raise notice 'v_sql_calcular_pruebas_esperadas;=%',v_sql_calcular_pruebas_esperadas;
			execute v_sql_calcular_pruebas_esperadas;

								
		else 
			v_sql_calcular_pruebas_esperadas :=
			'update TMP_sre_fac_log_prueba	
			set
			pruebas_esperadas=	(select  CASOS_PRUEBAS.total_pruebas 
								 from sre_recaudaciones.'||v_sre_fac_casos_prueba||' CASOS_PRUEBAS 
								 where 
								 CASOS_PRUEBAS.caso_prueba_id=TMP_sre_fac_log_prueba.caso_prueba_id AND
								'
								|| 
								(case
									--when v_etapa_certificacion = 2852 or v_etapa_certificacion =  2856 or v_etapa_certificacion = 3053  or v_etapa_certificacion = 3141  or v_etapa_certificacion = 2855 or v_etapa_certificacion = 2857 then 'CASOS_PRUEBAS.documento_sector_id=TMP_sre_fac_log_prueba.documento_sector_id and'
									when v_etapa_certificacion = 2852 or v_etapa_certificacion = 3141  or v_etapa_certificacion = 2855 or v_etapa_certificacion = 2857 then 'CASOS_PRUEBAS.documento_sector_id=TMP_sre_fac_log_prueba.documento_sector_id and'
									else ' '
								end)
								||
								' 													 
								 CASOS_PRUEBAS.estado_id=''AC''
								 limit 1);';
								
			raise notice 'v_sql_calcular_pruebas_esperadas;=%',v_sql_calcular_pruebas_esperadas;
			execute v_sql_calcular_pruebas_esperadas;
		
			--Actualizacion del campo caso_prueba_id y pruebas_esperadas is null de la etapa 3053-Envios masivos
			v_sql_calcular_pruebas_esperadas :=
			'update TMP_sre_fac_log_prueba	
			set
			pruebas_esperadas=	(select  CASOS_PRUEBAS.total_pruebas 
								 from sre_recaudaciones.'||v_sre_fac_casos_prueba||' CASOS_PRUEBAS 
								 where 
								 CASOS_PRUEBAS.caso_prueba_id=TMP_sre_fac_log_prueba.caso_prueba_id AND
								 CASOS_PRUEBAS.estado_id=''AC''
								 limit 1)
			where pruebas_esperadas is null;';
								
			raise notice 'v_sql_calcular_pruebas_esperadas;=%',v_sql_calcular_pruebas_esperadas;
			execute v_sql_calcular_pruebas_esperadas;
	end case;
		


	--Actualizar pruebas_esperadas a 1. 
	/*En la etapa CUF, existen hash de la tabla log que no estan en la tabla casos de prueba.*/
	--2850	Etapa - Generación de CUF
	if v_etapa_certificacion = 2850 then 
		--caso_prueba_id is not null -> obligatorias=2; declaradas=1  --VIC
		raise notice 'CUF 1';
		update TMP_sre_fac_log_prueba	
		set
		pruebas_esperadas =	(select  CASOS_PRUEBAS.total_pruebas
							 from sre_recaudaciones.sre_fac_casos_prueba_0_cuf CASOS_PRUEBAS 
							 where 
							 CASOS_PRUEBAS.caso_prueba_id=TMP_sre_fac_log_prueba.caso_prueba_id AND
							 CASOS_PRUEBAS.estado_id='AC'
							 limit 1);
		raise notice 'CUF 1.1.';
		--caso_prueba_id is null -> 1
		update TMP_sre_fac_log_prueba
		set
			pruebas_esperadas=1
		where caso_prueba_id is null;
	
		raise notice 'CUF 1.2.';
	end if;

	------------------------Obtener casos de pruebas incorrectos------------------------
	/*Para las etapas CUF y CONSUMO DE SERVICIO se obtiene en una tabla temporal la cantidad de casos de prueba incorrectos. para posteriormente restar a las correctas*/		
	if v_etapa_certificacion=2850 or v_etapa_certificacion=2851 then
	
		drop table if exists TMP_sre_fac_log_prueba_erroneas;
		CREATE temp TABLE TMP_sre_fac_log_prueba_erroneas (
		sistema_id int8,																	 							
		caso_prueba_id int8,
		hash varchar(250),
		hash_descripcion varchar(250),
		pruebas_incorrectas int4
		);	
		
		--2850	Etapa - Generación de CUF	
		if v_etapa_certificacion=2850 then
			raise notice 'Etapa - Generación de CUF';
			insert into TMP_sre_fac_log_prueba_erroneas (
				sistema_id,																	 							
				caso_prueba_id,
				hash, 
				hash_descripcion,
				pruebas_incorrectas
			)
			
			select 
			LOGS.sistema_id,
			LOGS.caso_prueba_id,
			LOGS.hash,
			LOGS.hash_descripcion,
			count(LOGS.hash) as pruebas_incorrectas
			from  sre_recaudaciones.sre_fac_log_0_cuf LOGS inner join sre_recaudaciones.sre_fac_casos_prueba_0_cuf CASOS_PRUEBA
			on LOGS.hash=CASOS_PRUEBA.hash inner join TMP_etapas_fechas FECHAS on
			LOGS.sistema_id = FECHAS.sistema_id
			where 
			LOGS.estado_match=0 and
			LOGS.estado_id='AC'	and 
			CASOS_PRUEBA.estado_id='AC' and  			
			LOGS.fecha_prueba between FECHAS.fecha_inicio_prueba and FECHAS.fecha_fin_prueba
			group by 
			LOGS.sistema_id,
			LOGS.caso_prueba_id,
			LOGS.hash,
			LOGS.hash_descripcion;
		
			update TMP_sre_fac_log_prueba	
			set
			pruebas_correctas = pruebas_correctas -	coalesce((
							select TMP.pruebas_incorrectas
							from TMP_sre_fac_log_prueba_erroneas TMP
							where  	
							TMP.sistema_id = TMP_sre_fac_log_prueba.sistema_id and
							TMP.hash = TMP_sre_fac_log_prueba.hash
							limit 1
							),0);
		end if;
	
		--2851	Etapa I - Consumo de Servicios
		raise notice 'Etapa I - Consumo de servicios';
		if v_etapa_certificacion=2851 then
			insert into TMP_sre_fac_log_prueba_erroneas (
				sistema_id,																	 							
				caso_prueba_id,
				hash, 		
				pruebas_incorrectas
			)
			select 
			LOGS.sistema_id,
			LOGS.caso_prueba_id,
			LOGS.hash,
			count(LOGS.caso_prueba_id) as pruebas_incorrectas
			from  sre_recaudaciones.sre_fac_log_1_consumo_servicio LOGS inner join sre_recaudaciones.sre_fac_casos_prueba_1_consumo_servicio CASOS_PRUEBA
			on LOGS.caso_prueba_id=CASOS_PRUEBA.caso_prueba_id inner join TMP_etapas_fechas FECHAS on
			LOGS.sistema_id = FECHAS.sistema_id
			where 
			LOGS.estado_match=1 and
			LOGS.estado_id='AC'	and 			
			CASOS_PRUEBA.estado_id='AC' and  
			CASOS_PRUEBA.caso_prueba like '%ERRONEO' and 
			LOGS.fecha_prueba between FECHAS.fecha_inicio_prueba and FECHAS.fecha_fin_prueba
			group by 
			LOGS.sistema_id,
			LOGS.caso_prueba_id,
			LOGS.hash;
		
			update TMP_sre_fac_log_prueba	
			set
			pruebas_correctas = pruebas_correctas -	coalesce((
							select TMP.pruebas_incorrectas
							from TMP_sre_fac_log_prueba_erroneas TMP
							where  	
							TMP.sistema_id = TMP_sre_fac_log_prueba.sistema_id and
							TMP.caso_prueba_id = TMP_sre_fac_log_prueba.caso_prueba_id
							limit 1
							),0);
						
		end if;					
	end if;		
						
	------------------------Calcular procentaje avance------------------------
	--	porcentaje_avance,
	update TMP_sre_fac_log_prueba	
	set
	--porcentaje_avance = case when (pruebas_correctas*100::numeric/pruebas_esperadas)>=100 then 100 else  (pruebas_correctas*100::numeric/pruebas_esperadas) end;
	porcentaje_avance = case when (pruebas_correctas*95::numeric/pruebas_esperadas)>=95 then 100 else  (pruebas_correctas*100::numeric/pruebas_esperadas) end;

	--20200519
	update TMP_sre_fac_log_prueba	
	set
	--porcentaje_avance = case when pruebas_correctas>=pruebas_esperadas then 100 else  0 end;
	porcentaje_avance = case when porcentaje_avance>=95 then 100 else  0 end;


	
	--Calcular caso de prueba concluido
	update TMP_sre_fac_log_prueba	
	set
	--caso_prueba_concluido = case when porcentaje_avance>=100 then true else false end;
	caso_prueba_concluido = case when porcentaje_avance>=95 then true else false end;

	------------------------Actaualizar campos comunes------------------------
	/*Actualiza los campos comunes como ser: fecha_registro, usuario_registro, etc*/
	update TMP_sre_fac_log_prueba	
	set
	fecha_registro=(select FECHAS.fecha_inicio_prueba from TMP_etapas_fechas FECHAS where FECHAS.sistema_id = TMP_sre_fac_log_prueba.sistema_id limit 1),
	fecha_ultima_modificacion=fecha_registro,
	usuario_registro_id=v_usuario,
	usuario_ultima_modificacion_id=v_usuario,
	estado_id='VI';

	update TMP_sre_fac_log_prueba	
	set
	fecha_ultima_modificacion=fecha_registro;

	
	------------------------Insertar Datos migrados------------------------
	/*Inserta en la tabla fisica LOGS los registros precalculados de la tabla temporal TMP_sre_fac_log_prueba*/


	v_sql_insertar_datos_migrados :=
	'update sre_recaudaciones.'||v_sre_fac_log_prueba_nuevo||'		
	set estado_id=''AN'';

	insert into sre_recaudaciones.'||v_sre_fac_log_prueba_nuevo||' 
	(sistema_id,																	 							
	caso_prueba_id,
	pruebas_esperadas,
	pruebas_correctas,
	porcentaje_avance,
	caso_prueba_concluido,
	fecha_registro,
	fecha_ultima_modificacion,
	usuario_registro_id,
	usuario_ultima_modificacion_id,
	estado_id)		
	select 
	sistema_id,
	'
	|| 
	(case
		when v_etapa_certificacion = 2850 then 'coalesce(caso_prueba_id,0),'
		else 'caso_prueba_id,'
	end)
	||
	'
	pruebas_esperadas,
	pruebas_correctas,
	porcentaje_avance,
	caso_prueba_concluido,
	fecha_registro ,
	fecha_ultima_modificacion ,
	usuario_registro_id  ,
	usuario_ultima_modificacion_id ,
	estado_id 	
	from TMP_sre_fac_log_prueba;';

	raise notice 'v_sql_insertar_datos_migrados;=%',v_sql_insertar_datos_migrados;
	execute v_sql_insertar_datos_migrados;


	------------------------Comprobar cantidad de registros migrados------------------------
	/*Compreba el tatal de registro insertados en la tabla fisica LOGS*/

	v_sql_comprobar_totales:='(select count(*) from sre_recaudaciones.'||v_sre_fac_log_prueba_nuevo||' where estado_id=''VI'');';
	raise notice 'v_sql_comprobar_totales 1;=%',v_sql_comprobar_totales;	
	execute v_sql_comprobar_totales  into v_cantidad_registros_migrados;

	raise notice 'v_cantidad_registros_migrados=%',v_cantidad_registros_migrados;

	if ((select count(*) from TMP_sre_fac_log_prueba)=v_cantidad_registros_migrados) then
		v_sql_comprobar_totales:=
		'update sre_recaudaciones.'||v_sre_fac_log_prueba_nuevo||'
		set estado_id=''AC''
		where estado_id=''VI'';';
	
		raise notice 'v_sql_comprobar_totales 2;=%',v_sql_comprobar_totales;	
		execute v_sql_comprobar_totales;

		raise notice 'Exportacion de registros correctos. %',(select count(*) from TMP_sre_fac_log_prueba);

		--return 'Exportacion de registros correctos. '||(select count(*) from TMP_sre_fac_log_prueba);
	else
		raise notice 'No fueron exportados todos los regitros, solictar rollback.';
	
		--return 'No fueron exportados todos los regitros, solictar rollback.';
	end if;	
	

end
$$
language plpgsql;

/*$function$
;*/


/*
 
--3141-Etapa IX – Anulaciones
--48 registros
select *
from TMP_sre_fac_log_prueba
where sistema_id=163

--documentos sectores (En esta etapa se considera el documento sector)
select distinct documento_sector_id
from TMP_sre_fac_log_prueba

--163	2019-09-25 09:39:36	2020-03-11 14:11:30	true	3141
select * 
from TMP_etapas_fechas
where sistema_id=163

--48 registros, documento sector =1
select * from sre_recaudaciones.sre_fac_casos_prueba_11_anulaciones where estado_id ='AC' and documento_sector_id =1;

--48 registros
select * from sre_recaudaciones.sre_fac_log_prueba_11_anulaciones where sistema_id = 163 and porcentaje_avance =100 and estado_id ='AC';

select distinct caso_prueba_id 
from sre_recaudaciones.sre_fac_log_11_anulaciones 
where sistema_id =163 and estado_id ='AC'
order by caso_prueba_id ;










--3016-Etapa - Obtención CUFD
--****** 1011 para este sistema se ha comprobado que existen casos de prueba en la tabla logs con etado match=1 con el campo caso_prueba_id igual null. No deberia existir problemas puesto que el porcentaje total de la etapa aumentara
v_sre_fac_casos_prueba:='sre_fac_casos_prueba_obtencion_cufd';
			v_sre_fac_log_prueba_nuevo:='sre_fac_log_prueba_cufd';
			v_sre_fac_log_antiguo:='sre_fac_log_obtencion_cufd';
--10 registros
select *
from TMP_sre_fac_log_prueba
where sistema_id=1011

--sector 0
select distinct documento_sector_id
from TMP_sre_fac_log_prueba

--1011	2020-02-28 14:18:34 - 2020-05-19 11:12:50	false	3016
select * 
from TMP_etapas_fechas
where sistema_id=1011

--21 registros. 2 obligatorios y 19 declarados
select * 
from sre_recaudaciones.sre_fac_casos_prueba_obtencion_cufd where estado_id ='AC'

--10 registros
select * 
from sre_recaudaciones.sre_fac_log_prueba_cufd 
where sistema_id = 1011 and
estado_id ='AC' and
porcentaje_avance =100;

--10 registros
--select caso_prueba_id, count(*)
select hash, count(*)
from sre_recaudaciones.sre_fac_log_obtencion_cufd 
where sistema_id =1011 and 
estado_id ='AC' and
estado_match=1 and
fecha_prueba between '2020-02-28 14:18:34' and '2020-05-19 11:12:50'
group by hash

--8 registros, 7 con valores en al campo caso_prueba_id, 1 con valor null en el mismo campo.
select caso_prueba_id, count(*)
from sre_recaudaciones.sre_fac_log_obtencion_cufd 
where sistema_id =1011 and 
estado_id ='AC' and 
estado_match=1 and
fecha_prueba between '2020-02-28 14:18:34' and '2020-05-19 11:12:50'
group by caso_prueba_id

--Listando los casos de prueba identificados, pero con valor null en el campo_caso_prueba_id
select *
from sre_recaudaciones.sre_fac_log_obtencion_cufd 
where sistema_id =1011 and 
estado_id ='AC' and
estado_match=1 and
caso_prueba_id is null and
fecha_prueba between '2020-02-28 14:18:34' and '2020-05-19 11:12:50';

select *
from sre_recaudaciones.sre_fac_log_obtencion_cufd 
where sistema_id =1011 and 
estado_id ='AC' and
estado_match=1 and
hash in (
'168a8008d18bd9b42452d0e96422b2d6',
'6ebab40b95ecffb957968b1f0835593b',
'47a77039e8664c9ea21aeb841e8d745d')


select * 
from sre_recaudaciones.sre_fac_casos_prueba_obtencion_cufd
where 
estado_id='AC' and
opcional=1
hash in 
('168a8008d18bd9b42452d0e96422b2d6',
'6ebab40b95ecffb957968b1f0835593b',
'47a77039e8664c9ea21aeb841e8d745d')


--****** 749 la etapa fue cerrado en 95%.
--21 registros
select *
from TMP_sre_fac_log_prueba
where sistema_id=749

--sector 0
select distinct documento_sector_id
from TMP_sre_fac_log_prueba

749	2019-09-11 11:58:24	2020-05-19 11:12:50		3016
select * 
from TMP_etapas_fechas
where sistema_id=749

--21 registros. 2 obligatorios y 19 declarados
select * 
from sre_recaudaciones.sre_fac_casos_prueba_obtencion_cufd where estado_id ='AC'

--20 registros
select * 
from sre_recaudaciones.sre_fac_log_prueba_cufd 
where sistema_id = 749 and
estado_id ='AC' and
porcentaje_avance =100;

--21 registros
--select caso_prueba_id, count(*)
select hash, count(*)
from sre_recaudaciones.sre_fac_log_obtencion_cufd 
where sistema_id =749 and 
estado_id ='AC' and
estado_match=1 and
fecha_prueba between '2019-09-11 11:58:24' and	'2020-05-19 11:12:50'
group by hash

--21 registros
select caso_prueba_id, count(*)
from sre_recaudaciones.sre_fac_log_obtencion_cufd 
where sistema_id =749 and 
estado_id ='AC' and 
estado_match=1 and
fecha_prueba between '2019-09-11 11:58:24' and	'2020-05-19 11:12:50'
group by caso_prueba_id



--****** 928 
--2 registros
select *
from TMP_sre_fac_log_prueba
where sistema_id=928

--sector 0
select distinct documento_sector_id
from TMP_sre_fac_log_prueba

--928	2019-09-24 16:45:39	2020-05-19 11:12:50	false	3016
select * 
from TMP_etapas_fechas
where sistema_id=928

--21 registros. 2 obligatorios y 19 declarados
select * 
from sre_recaudaciones.sre_fac_casos_prueba_obtencion_cufd where estado_id ='AC'

--0 registros
select * 
from sre_recaudaciones.sre_fac_log_prueba_cufd 
where sistema_id = 928 and
estado_id ='AC' and
porcentaje_avance =100;

--21 registros
--select caso_prueba_id, count(*)
select hash, count(*)
from sre_recaudaciones.sre_fac_log_obtencion_cufd 
where sistema_id =928 and 
estado_id ='AC' and
estado_match=1 and
fecha_prueba between '2019-09-24 16:45:39' and	'2020-05-19 11:12:50'
group by hash

--21 registros
select caso_prueba_id, count(*)
from sre_recaudaciones.sre_fac_log_obtencion_cufd 
where sistema_id =928 and 
estado_id ='AC' and 
estado_match=1 and
fecha_prueba between '2019-09-24 16:45:39' and	'2020-05-19 11:12:50'
group by caso_prueba_id

select *
from TMP_sre_fac_log_prueba
where sistema_id=928


--****** 761. Los totales y porcentaje en la tabla son correctos. El valor de la tabla principal no cuadra con los registros del log.  No deberia existir problemas puesto que el porcentaje total de la etapa aumentara.
--20 registros
select *
from TMP_sre_fac_log_prueba
where sistema_id=761

--sector 0
select distinct documento_sector_id
from TMP_sre_fac_log_prueba

--761	2019-09-25 23:42:47	2020-05-19 13:39:29	false	3016
select * 
from TMP_etapas_fechas
where sistema_id=761

--21 registros. 2 obligatorios y 19 declarados
select * 
from sre_recaudaciones.sre_fac_casos_prueba_obtencion_cufd where estado_id ='AC'

--20 registros
select * 
from sre_recaudaciones.sre_fac_log_prueba_cufd 
where sistema_id = 761 and
estado_id ='AC' and
porcentaje_avance =100;

--20 registros
--select caso_prueba_id, count(*)
select hash, count(*)
from sre_recaudaciones.sre_fac_log_obtencion_cufd 
where sistema_id =761 and 
estado_id ='AC' and
estado_match=1 and
fecha_prueba between '2019-09-25 23:42:47' and	'2020-05-19 13:39:29'
group by hash


--20 registros
select caso_prueba_id, count(*)
from sre_recaudaciones.sre_fac_log_obtencion_cufd 
where sistema_id =761 and 
estado_id ='AC' and 
estado_match=1 and
fecha_prueba between '2019-09-25 23:42:47' and	'2020-05-19 13:39:29'
group by caso_prueba_id

select *
from TMP_sre_fac_log_prueba
where sistema_id=761

--21 Registros
select * 
from sre_recaudaciones.sre_fac_casos_prueba_obtencion_cufd 
where estado_id='AC' and 
hash='2548d1364eafc901e67b27006fd75a75';

--20 Registros
select caso_prueba_id, count(*)
from sre_recaudaciones.sre_fac_log_prueba_cufd 
where sistema_id=761 AND estado_id='AC'
group by caso_prueba_id
order by 1

select caso_prueba_id, count(*)
from sre_recaudaciones.sre_fac_log_obtencion_cufd 
where sistema_id=761 AND estado_id='AC' and estado_match=1 and
fecha_prueba between '2019-09-25 23:42:47' and	'2020-05-19 13:39:29'
group by caso_prueba_id
order by 1

select hash,caso_prueba_id, count(*)
from sre_recaudaciones.sre_fac_log_obtencion_cufd 
where sistema_id=761 AND estado_id='AC' and estado_match=1 and
fecha_prueba between '2019-09-25 23:42:47' and	'2020-05-19 13:39:29'
group by hash,caso_prueba_id
order by 2










--3053 = Etapa X – Envio Masivo (ok)----------------------------------------------------------------------------------
--166****
--13 registros
select documento_sector_id, caso_prueba_id, hash, pruebas_esperadas, pruebas_correctas, porcentaje_avance
from TMP_sre_fac_log_prueba
where sistema_id=166 and porcentaje_avance=100 and hash='f6b12339fbff5e86c6304c49f1296b3d'

select * 
from TMP_sre_fac_log_prueba
where sistema_id=166 and porcentaje_avance=100 and hash='f6b12339fbff5e86c6304c49f1296b3d'

select *
from sre_recaudaciones.sre_fac_log_prueba_10_masivos
where sistema_id=166 and porcentaje_avance=100 and estado_id='AC'

select *	
from  sre_recaudaciones.sre_fac_obtener_casos_prueba_etapa_10(166,3053)
where hash in (
select hash
from TMP_sre_fac_log_prueba
where sistema_id=166 and porcentaje_avance=100
)

select *	
from  sre_recaudaciones.sre_fac_obtener_casos_prueba_etapa_10(166,3053)
where hash = 'f6b12339fbff5e86c6304c49f1296b3d'

--documentos sectores (En esta etapa se considera el documento sector)
select distinct documento_sector_id
from TMP_sre_fac_log_prueba
where sistema_id=166

--166	2019-09-10 13:40:43	2020-05-19 14:58:31		3053
select * 
from TMP_etapas_fechas
where sistema_id=166

--47 registros
select * 
from sre_recaudaciones.sre_fac_casos_prueba_10_envio_masivos 
where estado_id ='AC';

--4 registros exitosos
select * 
from sre_recaudaciones.sre_fac_log_prueba_10_masivos 
where sistema_id = 166 and 
--porcentaje_avance =100 and 
estado_id ='AC';

--9
select documento_sector_id, caso_prueba_id, pruebas_correctas
from TMP_sre_fac_log_prueba
where sistema_id=166  and
porcentaje_avance=0
order by 1,2


select documento_sector_id, caso_prueba_id, count(*)
from sre_recaudaciones.sre_fac_log_10_envio_masivos 
where sistema_id =166 and 
estado_id ='AC' and
estado_match=1
group by documento_sector_id, caso_prueba_id
order by 1,2









--2854 = Etapa IV - Sincronización de Fecha y Hora----------------------------------------------------------------------------------
--Los siguientes sistemas no cuadran puesto que la tabla principal de las etapas no fue actualizado. Sin embargo haciendo la verficiacion de los casos de pruebas existen casos de prueba exitosos
--575***
--927**

select *
from TMP_sre_fac_log_prueba
where sistema_id=575

select *
from sre_recaudaciones.sre_fac_log_prueba_4_sinc_fecha_hora
where 
estado_id='AC' and
sistema_id=575

select *
from sre_recaudaciones.sre_fac_log_4_sincronizacionf_fecha_hora
where 
estado_id='AC' and
sistema_id=575 and
estado_match=1 and
fecha_prueba between '2019-09-16 21:48:06' and '2020-05-19 18:06:31'

575	2019-09-16 21:48:06 - 2020-05-19 18:06:31	false	2854
select * 
from TMP_etapas_fechas
where sistema_id=575

select *
from sre_recaudaciones.sre_fac_obtener_casos_prueba_etapa_4(575,2854)
where porcentaje_avance=100





--2856 = Etapa VI - Envío de Paquetes----------------------------------------------------------------------------------
--1071  --el resutlado se comprobo en la tabla de logs, hay que actualizar la tabla principal de etapas
			v_sre_fac_casos_prueba:='sre_fac_casos_prueba_6_envio_paquetes';
			v_sre_fac_log_prueba_nuevo:='sre_fac_log_prueba_6_paquetes';
			v_sre_fac_log_antiguo:='sre_fac_log_6_envio_paquetes';

--2020-04-21 22:00:36	2020-05-19 18:55:52
select * 						
from TMP_etapas_fechas			
where sistema_id=1071

--casos de pruebas correctas
select *
from TMP_sre_fac_log_prueba
where sistema_id=1071 and 
porcentaje_avance=100

--casos de prueba
--51
select count(*)
from sre_recaudaciones.sre_fac_casos_prueba_6_envio_paquetes

--un solo sector
select distinct documento_sector_id
from sre_recaudaciones.sre_fac_casos_prueba_6_envio_paquetes

select caso_prueba_id, count(*)
from sre_recaudaciones.sre_fac_log_6_envio_paquetes
where sistema_id=1071 and
estado_match=1 and 
estado_id='AC' and
fecha_prueba between '2020-04-21 22:00:36' and '2020-05-19 18:55:52'
group by caso_prueba_id







--3069-Etapa VIII - Gestión de sucursales---------------------------------------
-En la tabla principal los totales no estan correctamente calculados. por ejemplo el sitema 680 no tiene casos de pruebas existos que lleguen al 100%, por lo tanto
--el porcentaje de la etapa deberia ser 0. En la tabla principal esta con un valor diferente a cero.
--680	2020-03-26 17:34:51	2020-05-19 19:21:58	false	3069

select * 						
from TMP_etapas_fechas			
where sistema_id=680

--casos de pruebas correctas
select *
from TMP_sre_fac_log_prueba
where sistema_id=680 and 
porcentaje_avance=100

--14 casos de prueba
select * from sre_recaudaciones.sre_fac_casos_prueba_8_gestion_sucursales where estado_id='AC';
select * from sre_recaudaciones.sre_fac_log_prueba_8_sucursales where estado_id='AC' and sistema_id=680;
select * from sre_recaudaciones.sre_fac_log_8_gestion_sucursales where estado_id='AC' and sistema_id=680 and estado_match=1;
select caso_prueba_id, count(*) from sre_recaudaciones.sre_fac_log_8_gestion_sucursales where estado_id='AC' and sistema_id=680 and estado_match=1 group by caso_prueba_id;





*/









/*
select * from TMP_sre_fac_log_prueba
select count(*) from TMP_etapas_fechas  


 --Pruebas funcionales ETAPA sincronizacion de catalgos
select * 
from TMP_sre_fac_log_prueba  
where hash = 'b78ba0bbe0b67d9fc1765b89fe064057' and
sistema_id=210

 
 
  
--Pruebas funcionales ETAPA 11
select * from TMP_sre_fac_log_prueba
select * from TMP_etapas_fechas  
 
 
  
--Pruebas funcionales ETAPA 10
select * from TMP_sre_fac_log_prueba
select * from TMP_etapas_fechas  


  
 
--Pruebas funcionales ETAPA 2
select * from TMP_sre_fac_log_prueba where porcentaje_avance<100 
select * from TMP_etapas_fechas  




--Pruebas funcionales CUF
select * from TMP_sre_fac_log_prueba where caso_prueba_id is null
select * from TMP_sre_fac_log_prueba where sistema_id=1071
select * from TMP_etapas_fechas where sistema_id=1071 --'2020-04-21 20:44:34	2020-04-29 20:17:36'
select * from TMP_sre_fac_log_prueba_erroneas



select CUF.hash, CUF.hash_descripcion, CP.hash_descripcion as hash_equivalente
from sre_recaudaciones.sre_fac_log_0_cuf CUF inner join TMP_sre_fac_log_prueba CP
on CUF.hash=CP.hash
where CP.caso_prueba_id is null


select * 
from sre_recaudaciones.sre_fac_log_0_cuf sflc
where hash_descripcion in (
'0|33|1|1|2|18|0',
'0|100|1|1|2|18|0',
'0|101|1|1|1|1|0')



770		089fb639c4716c4350e4050727437ab6	0|33|1|1|2|18|0
770		08e1284160e94c6f38a4121cd902f171	0|100|1|1|2|18|0
770		09dee11e644db4de35b60cabd6042e96	0|101|1|1|1|1|0
770		0a830f6091e65f96aecf3cb4911959dd	0|74|1|2|1|29|0
770		0a895a6c05a869d80e14c6f664a1be9e	0|41|1|2|2|18|0
770		0a9f65a43a74fa2adb54987fe38d9502	0|57|1|1|1|1|0




select * 
from sre_recaudaciones.sre_fac_log_0_cuf
where fecha_prueba>='2020-04-01 01:01:01'


select * 
from sre_recaudaciones.sre_fac_casos_prueba_0_cuf
where hash_descripcion='0|0|1|1|1|1|0'
--where fecha_registro >='2020-04-01 01:01:01'

select *
from sre_recaudaciones.sre_fac_log_0_cuf
where hash_descripcion like '%0|0|1|1|1|1|0%'

0|0|1|1|1|1|0
0|0|1|2|1|1|0

select *
from sre_recaudaciones.sre_fac_log_prueba_0_cuf 
where estado_id='AC' 
and sistema_id=1071
order by sistema_id

select sistema_id, count(*)
from sre_recaudaciones.sre_fac_log_0_cuf 
where estado_id='AC' and
estado_match=1
group by sistema_id
order by 1 desc

select *  
from sre_recaudaciones.sre_fac_log_0_cuf

--sistema 1071
select * from TMP_sre_fac_log_prueba where sistema_id=1071
select * from TMP_etapas_fechas where sistema_id=1071 --'2020-04-21 20:44:34	2020-04-29 20:17:36'
select * from TMP_sre_fac_log_prueba_erroneas where sistema_id=1071


select hash, COUNT(*)
from sre_recaudaciones.sre_fac_log_0_cuf
where sistema_id=1071 and 
estado_match=1 and
estado_id='AC'
group by hash

select distinct fecha_prueba
from sre_recaudaciones.sre_fac_log_0_cuf
where sistema_id=1071 and 
estado_match=1 and
estado_id='AC'

 
select * from sre_recaudaciones.sre_fac_log_prueba_0_cuf where sistema_id=1071 and estado_id='AC';

select * from sre_recaudaciones.sre_fac_casos_prueba_0_cuf where caso_prueba_id in (2,30)


*/

--where sistema_id=966;

--Pruebas funcionales CONSUMO DE SERVICIO
/*
select * from TMP_sre_fac_log_prueba
select * from TMP_etapas_fechas
select * from TMP_sre_fac_log_prueba_erroneas

select * from sre_recaudaciones.sre_fac_log_prueba_1_consumo_servicio where estado_id='AC' 
select * from sre_recaudaciones.sre_fac_log_1_consumo_servicio where caso_prueba_id is null and estado_id='AC' and hash in ('918ef32c704890c7ee5247b83d9541f6','918ef32c704890c7ee5247b83d9541f6')
select * from sre_recaudaciones.sre_fac_casos_prueba_1_consumo_servicio where hash in ('918ef32c704890c7ee5247b83d9541f6','918ef32c704890c7ee5247b83d9541f6') and estado_id='AC'
*/	

--
----232
--select count(*) 
--from sre_recaudaciones.sre_fac_casos_prueba_1_consumo_servicio
--where estado_id='AC'
--
----116
--select count(*) 
--from sre_recaudaciones.sre_fac_casos_prueba_1_consumo_servicio
--where estado_id='AC' and caso_prueba like '%EXITOSO'
--
----116
--select count(*) 
--from sre_recaudaciones.sre_fac_casos_prueba_1_consumo_servicio
--where estado_id='AC' and  caso_prueba not like '%ERRONEO'
--
--select * 
--from sre_recaudaciones.sre_fac_casos_prueba_1_consumo_servicio
--where estado_id='AC' and  (caso_prueba not like '%EXITOSO' and caso_prueba not like '%ERRONEO') 
--
--
--select *
--from sre_recaudaciones.sre_fac_casos_prueba_1_consumo_servicio;
--
--select *
--from sre_recaudaciones.sre_fac_log_prueba_1_consumo_servicio;
--
--select *
--from sre_recaudaciones.sre_fac_log_1_consumo_servicio;
--
