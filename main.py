from create_tables import create_trx_table_pyme
from create_tables import create_master_campaign
from create_tables import create_clientes_final
from create_tables import ejecucion_resultados1
from create_tables import ejecucion_resultados2
from create_tables import ejecucion_resultados3
from create_tables import ejecucion_resultado_aliados_marcas_categorias_canal
from create_tables import ejecucion_resultado_canal
from create_tables import ejecucion_resultado_segmentos_pco
from create_tables import ejecucion_resultado_otros_segmentos
from create_tables import ejecucion_resultado_genero_edad_ciudad
from create_tables import ejecucion_resultado_gustos
from create_tables import ejecucion_resultado_recencia
import pandas as pd
from create_excel import pegar_df_en_excel
from create_excel import pegar_df_en_excel_sin_encabezado
from create_excel import pegar_string_en_excel
import openpyxl
from datetime import datetime, timedelta
import datetime as dt
import os
import openpyxl
from components.s3_access import S3Access

s3Access = S3Access()

#-------1.Modifica el archivo txt query_audiencia.txt
#         la query debe contener al menos los siguientes campos
#         customer_id_. Sin embargo se recomienda 
#.        que ademas tenga muestra y saldo_puntos (tal cual).

##------2.Creacion de variables obligatorias
date_from='2023-04-13'
date_to='2023-05-15'
nombre_campana='MADRES PRUEBA FINAL'#No escriban campaña que les queda doble
tipo_efectividad="MACRO" #Opciones: MACRO,EMAIL,SMS,PUSH
ids_campana_email=[1618389, 1637329, 1645789,1618749,1637989,1644649]#Poner ids sin comillas dentro de la lista [] y separado por comas ej: [1644649,1645789], si no hay nada se pone [0]
ids_campana_sms=[1668709,1669029]#Poner ids sin comillas dentro de la lista [] y separado por comas ej: [1644649,1645789], si no hay nada se pone [0]
ids_campana_push=[1684089]#Poner ids sin comillas dentro de la lista [] y separado por comas ej: [1644649,1645789], si no hay nada se pone [0]


##------3. Creacion de variables opcionales para la transaccional
#        --> #Aca se puede poner el condicional que se desee, el nombre de la variable debe existir en la transaccional
#             la idea es dejar siempre todas las posibles variables que puedan ir en la clausula where
#             y si simplemente no la necesitamos dejamos la lita dentro de la lista vacia, 
#             ejemplo si ponemos-->    'tipo_documento':['and',[]]    <---entonces tipo_documento no se va tener en cuenta en el where
clause_where = {
    'nit': ['and', []],
    'trn_type': ['and', []], #Opciones: 'ER', 'BR'
    'tipo_cliente': ['and', []], #Opciones: 'Empresas', 'Personas'
    'tipo_documento': ['and', []], 
    'trn_prt_id not':['and',[]],
    'marca': ['and', []],
    'categoria': ['and', []],
    'canal':['and',[]] #Opciones: 'Tienda Online', 'Tienda Física','Bonos','Viajes','Transferencia Puntos','Pymes','Boton'
    }

#NOTA1: Lo que pongas dentro de la lista debe ser tal cual lo escribirias en el where, si lleva comillas ponle comillas
#si no lleva no se las pongas. Cualquier duda acude a la izquierda a query_trx_mercadeo.txt para que veas como esta construida la transaccional























############################################################################################################
##------ Ruta para guardar todos los archivos
query_trx="MERCADEO" #No cambiarlo
now = dt.datetime.now()
# Definimos la ruta completa de la carpeta donde se guardarán los archivos
folder_path = os.path.join(os.getcwd(), "fecha_reporte", now.strftime('%Y'), now.strftime('%B'))
# Creamos la estructura de carpetas necesaria si no existe
os.makedirs(folder_path, exist_ok=True)
date_today = now.strftime('%Y-%m-%d')


############################################################################################################

        #Poner coma antes de unir todos los ids por coma
ids_campana_email_str = ","+",".join(str(ids) for ids in ids_campana_email)
ids_campana_sms_str = ","+",".join(str(ids) for ids in ids_campana_sms)
ids_campana_push_str = ","+",".join(str(ids) for ids in ids_campana_push)


        # Convertir las fechas a objetos datetime
date_from_obj = datetime.strptime(date_from, '%Y-%m-%d')
date_to_obj = datetime.strptime(date_to, '%Y-%m-%d')
        # Restar un año a cada fecha
date_from_ano_anterior = (date_from_obj - timedelta(days=365)).strftime('%Y-%m-%d')
date_to_ano_anterior = (date_to_obj - timedelta(days=365)).strftime('%Y-%m-%d')
        #restar 9 meses para recencia
date_from_recencia = (date_from_obj - timedelta(days=181)).strftime('%Y-%m-%d')
date_to_recencia = (date_from_obj - timedelta(days=1)).strftime('%Y-%m-%d')
        #Cambiar formato fecha para mostrar en excel
date_from_corto=date_from_obj.strftime('%B/%d/%Y')
date_to_corto=date_to_obj.strftime('%B/%d/%Y')

date_from_ano_anterior_corto=(date_from_obj - timedelta(days=365)).strftime('%B/%d/%Y')
date_to_ano_anterior_corto=(date_to_obj - timedelta(days=365)).strftime('%B/%d/%Y')


date_from_recencia_corto=(date_from_obj - timedelta(days=181)).strftime('%B/%d/%Y')
date_to_recencia_corto=(date_from_obj - timedelta(days=1)).strftime('%B/%d/%Y')






        #Nombrar las tablas y strings 
trx_table_name=f'auto_trx_campaña_{nombre_campana}_{date_from}_{date_to}'
maestra_table_name=f'auto_maestra_campaña_{tipo_efectividad}_{nombre_campana}_{date_from}_{date_to}'
fecha_campaña=f'{date_from_corto}  -  {date_to_corto}'
fecha_campaña_año_anterior=f'{date_from_ano_anterior_corto}  -  {date_to_ano_anterior_corto}'
fecha_campaña_recencia=f'{date_from_recencia_corto}  -  {date_to_recencia_corto}'
trx_table_name_ano_anterior=f'auto_trx_ano_anterior_campaña_{nombre_campana}_{date_from}_{date_to}'
clientes_final_table=f'auto_temporal_clientes_final_{date_today}'
trx_table_name_recencia=f'auto_trx_recencia_campaña_{nombre_campana}_{date_from_recencia}_{date_to_recencia}'



##------Crear transaccional
create_trx_table_pyme(query_trx,nombre_campana,date_from,date_to,trx_table_name,"NO",clause_where=clause_where)

##------Crear maestra 
create_master_campaign(nombre_campana,tipo_efectividad,maestra_table_name,ids_campana_email_str,ids_campana_push_str,ids_campana_sms_str,trx_table_name)

#-------Crear transaccional año anterior
create_trx_table_pyme(query_trx,nombre_campana,date_from_ano_anterior,date_to_ano_anterior, trx_table_name_ano_anterior,"SI",tipo_efectividad, maestra_table_name,clause_where=clause_where)

#-------Crear clientes final
create_clientes_final(date_today,nombre_campana)

#-------Crear transaccional recencia
create_trx_table_pyme(query_trx,nombre_campana,date_from_recencia,date_to_recencia, trx_table_name_recencia,"SI",tipo_efectividad, maestra_table_name,clause_where=clause_where)



##------Crear dataframes con los datos a pegar
#Hoja efectividad
df_resultados1=ejecucion_resultados1(nombre_campana,tipo_efectividad,maestra_table_name)
df_resultados2=ejecucion_resultados2(nombre_campana,tipo_efectividad,trx_table_name)
df_resultados3=ejecucion_resultados3(nombre_campana,tipo_efectividad,trx_table_name)
valor = df_resultados3.iloc[0, 0]


#Hoja aliados
df_aliados_acum=ejecucion_resultado_aliados_marcas_categorias_canal(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,"ALIADO")
df_aliados_reden=ejecucion_resultado_aliados_marcas_categorias_canal(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,"ALIADO")
df_aliados_acum_aa=ejecucion_resultado_aliados_marcas_categorias_canal(nombre_campana,'ER',tipo_efectividad,trx_table_name_ano_anterior,maestra_table_name,"ALIADO")
df_aliados_reden_aa=ejecucion_resultado_aliados_marcas_categorias_canal(nombre_campana,'BR',tipo_efectividad,trx_table_name_ano_anterior,maestra_table_name,"ALIADO")

#Hoja marcas
df_marcas_acum=ejecucion_resultado_aliados_marcas_categorias_canal(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,"MARCA")
df_marcas_reden=ejecucion_resultado_aliados_marcas_categorias_canal(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,"MARCA")
df_marcas_acum_aa=ejecucion_resultado_aliados_marcas_categorias_canal(nombre_campana,'ER',tipo_efectividad,trx_table_name_ano_anterior,maestra_table_name,"MARCA")
df_marcas_reden_aa=ejecucion_resultado_aliados_marcas_categorias_canal(nombre_campana,'BR',tipo_efectividad,trx_table_name_ano_anterior,maestra_table_name,"MARCA")

#Hoja canal
df_canal_acum=ejecucion_resultado_aliados_marcas_categorias_canal(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,"CANAL")
df_canal_reden=ejecucion_resultado_aliados_marcas_categorias_canal(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,"CANAL")
#Hoja canal (ALIADOS X CANAL)
df_online_acum=ejecucion_resultado_canal(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,'Tienda Online')
df_online_reden=ejecucion_resultado_canal(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,'Tienda Online')
df_fisica_acum=ejecucion_resultado_canal(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,'Tienda Física')
df_fisica_reden=ejecucion_resultado_canal(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,'Tienda Física')
df_viajes_acum=ejecucion_resultado_canal(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,'Viajes')
df_viajes_reden=ejecucion_resultado_canal(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,'Viajes')
df_bonos_acum=ejecucion_resultado_canal(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,'Bonos')
df_bonos_reden=ejecucion_resultado_canal(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,'Bonos')
df_pymes_acum=ejecucion_resultado_canal(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,'Pymes')
df_pymes_reden=ejecucion_resultado_canal(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,'Pymes')
df_boton_acum=ejecucion_resultado_canal(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,'Boton')
df_boton_reden=ejecucion_resultado_canal(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,'Boton')
df_transferencia_reden=ejecucion_resultado_canal(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,'Transferencia Puntos')

#Hoja segmentos
df_seg_pco_acum=ejecucion_resultado_segmentos_pco(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name)
df_seg_pco_red=ejecucion_resultado_segmentos_pco(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name)
df_seg_mill_acum=ejecucion_resultado_otros_segmentos(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,"MILLEROS")
df_seg_mill_reden=ejecucion_resultado_otros_segmentos(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,"MILLEROS")
df_seg_viaj_acum=ejecucion_resultado_otros_segmentos(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,"VIAJEROS")
df_seg_viaj_reden=ejecucion_resultado_otros_segmentos(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,"VIAJEROS")
df_seg_exito_dec_acum=ejecucion_resultado_otros_segmentos(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,"EXITO_DECIL")
df_seg_exito_dec_reden=ejecucion_resultado_otros_segmentos(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,"EXITO_DECIL")
df_seg_exito_seg_acum=ejecucion_resultado_otros_segmentos(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,"EXITO_SEG")
df_seg_exito_seg_reden=ejecucion_resultado_otros_segmentos(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,"EXITO_SEG")
df_seg_banco_acum=ejecucion_resultado_otros_segmentos(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,"BANCO")
df_seg_banco_reden=ejecucion_resultado_otros_segmentos(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,"BANCO")
df_seg_carulla_acum=ejecucion_resultado_otros_segmentos(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,"CARULLA")
df_seg_carulla_reden=ejecucion_resultado_otros_segmentos(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,"CARULLA")


# Hoja segmentos demograficos
df_seg_gnro_acum=ejecucion_resultado_genero_edad_ciudad(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,"GENERO",clientes_final_table)
df_seg_gnro_reden=ejecucion_resultado_genero_edad_ciudad(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,"GENERO",clientes_final_table)
df_seg_edad_acum=ejecucion_resultado_genero_edad_ciudad(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,"EDAD",clientes_final_table)
df_seg_edad_reden=ejecucion_resultado_genero_edad_ciudad(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,"EDAD",clientes_final_table)
df_seg_ciudad_acum=ejecucion_resultado_genero_edad_ciudad(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,"CIUDAD",clientes_final_table)
df_seg_ciudad_reden=ejecucion_resultado_genero_edad_ciudad(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,"CIUDAD",clientes_final_table)

#Hoja gustos
df_gustos_comunicados=ejecucion_resultado_gustos(nombre_campana,tipo_efectividad,maestra_table_name,clientes_final_table,trx_table_name,'NO')
df_gustos_comunicados_que_transaccionaron=ejecucion_resultado_gustos(nombre_campana,tipo_efectividad,maestra_table_name,clientes_final_table,trx_table_name,'SI')

#Hoja categorias
df_categ_acum=ejecucion_resultado_aliados_marcas_categorias_canal(nombre_campana,'ER',tipo_efectividad,trx_table_name,maestra_table_name,"CATEGORIA")
df_categ_reden=ejecucion_resultado_aliados_marcas_categorias_canal(nombre_campana,'BR',tipo_efectividad,trx_table_name,maestra_table_name,"CATEGORIA")
df_categ_acum_aa=ejecucion_resultado_aliados_marcas_categorias_canal(nombre_campana,'ER',tipo_efectividad,trx_table_name_ano_anterior,maestra_table_name,"CATEGORIA")
df_categ_reden_aa=ejecucion_resultado_aliados_marcas_categorias_canal(nombre_campana,'BR',tipo_efectividad,trx_table_name_ano_anterior,maestra_table_name,"CATEGORIA")

#Hoja resencia
df_recencia=ejecucion_resultado_recencia(nombre_campana,"('ER','BR')",tipo_efectividad,trx_table_name_recencia,maestra_table_name,date_from_recencia)
df_recencia_acum=ejecucion_resultado_recencia(nombre_campana,"('ER')",tipo_efectividad,trx_table_name_recencia,maestra_table_name,date_from_recencia)
df_recencia_reden=ejecucion_resultado_recencia(nombre_campana,"('BR')",tipo_efectividad,trx_table_name_recencia,maestra_table_name,date_from_recencia)



    
##------ Cargue de datos a excel
book = openpyxl.load_workbook("Formato_Inicial.xlsx")
if tipo_efectividad=='EMAIL':
    hoja = book["bd_email"]
    hoja1= book["email"]
    hoja_a_eliminar=book["bd_resto"]
    hoja_a_eliminar1=book["resto"]
else:
    hoja= book["bd_resto"]
    hoja1=book["resto"]
    hoja_a_eliminar=book["bd_email"]
    hoja_a_eliminar1=book["email"]
hoja_aliados=book["aliados"]
hoja_marcas=book["marcas"]
hoja_canal=book["canal"]
hoja_segmentos=book["segmentos"]
hoja_segmentos_demograficos=book["segmentos demograficos"]
hoja_gustos=book["gustos"]
hoja_categorias=book["categorias"]
hoja_recencia=book["recencia"]
    
book.remove(hoja_a_eliminar)
book.remove(hoja_a_eliminar1)

hoja1.title = "Efectividad"
hoja.sheet_state = 'hidden'


##Pegar efectividad en excel
if (len(df_resultados1) > 0):
    pegar_df_en_excel(df_resultados1, hoja, "B11")
    pegar_df_en_excel(df_resultados2, hoja, "B3")
    pegar_string_en_excel(valor,hoja,"C6")
    
#Pegar fecha y nombre de campaña en excel 
pegar_string_en_excel(fecha_campaña,hoja_aliados,"B8")
pegar_string_en_excel(fecha_campaña_año_anterior,hoja_aliados,"X8")
pegar_string_en_excel(fecha_campaña_recencia,hoja_recencia,"B8")
pegar_string_en_excel(f'CAMPAÑA {nombre_campana.upper()}',hoja_aliados,"A1") 

#Pegar  aliados en excel
if (len(df_aliados_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_aliados_acum,hoja_aliados,"B11")
if (len(df_aliados_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_aliados_reden,hoja_aliados,"L11")
if (len(df_aliados_acum_aa) > 0):
    pegar_df_en_excel_sin_encabezado(df_aliados_acum_aa,hoja_aliados,"X11")
if (len(df_aliados_reden_aa) > 0):
    pegar_df_en_excel_sin_encabezado(df_aliados_reden_aa,hoja_aliados,"AH11")
    
    
#Pegar  marcas en excel
if (len(df_marcas_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_marcas_acum,hoja_marcas,"B11")
if (len(df_marcas_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_marcas_reden,hoja_marcas,"L11")
if (len(df_marcas_acum_aa) > 0):
    pegar_df_en_excel_sin_encabezado(df_marcas_acum_aa,hoja_marcas,"X11")
if (len(df_marcas_reden_aa) > 0):
    pegar_df_en_excel_sin_encabezado(df_marcas_reden_aa,hoja_marcas,"AH11")

#Pegar  canal en excel
if (len(df_canal_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_canal_acum,hoja_canal,"B11")
if (len(df_canal_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_canal_reden,hoja_canal,"L11")
#Pegar aliados x canal
if (len(df_online_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_online_acum,hoja_canal,"X11")
if (len(df_online_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_online_reden,hoja_canal,"AH11")
if (len(df_fisica_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_fisica_acum,hoja_canal,"AT11")
if (len(df_fisica_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_fisica_reden,hoja_canal,"BD11")
if (len(df_viajes_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_viajes_acum,hoja_canal,"BP11")
if (len(df_viajes_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_viajes_reden,hoja_canal,"BZ11")
if (len(df_bonos_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_bonos_acum,hoja_canal,"CL11")
if (len(df_bonos_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_bonos_reden,hoja_canal,"CV11")
if (len(df_pymes_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_pymes_acum,hoja_canal,"DH11")
if (len(df_pymes_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_pymes_reden,hoja_canal,"DR11")
if (len(df_boton_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_boton_acum,hoja_canal,"ED11")
if (len(df_boton_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_boton_reden,hoja_canal,"EN11")
if (len(df_transferencia_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_transferencia_reden,hoja_canal,"EZ11")


    
#pegar segmentos en excel
if (len(df_seg_pco_acum) > 0): 
    pegar_df_en_excel_sin_encabezado(df_seg_pco_acum,hoja_segmentos,"B11")
if (len(df_seg_pco_red) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_pco_red,hoja_segmentos,"L11")
if (len(df_seg_mill_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_mill_acum,hoja_segmentos,"B22")
if (len(df_seg_mill_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_mill_reden,hoja_segmentos,"L22")
if (len(df_seg_viaj_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_viaj_acum,hoja_segmentos,"B29")
if (len(df_seg_viaj_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_viaj_reden,hoja_segmentos,"L29")
if (len(df_seg_exito_dec_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_exito_dec_acum,hoja_segmentos,"B36")
if (len(df_seg_exito_dec_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_exito_dec_reden,hoja_segmentos,"L36")
if (len(df_seg_exito_dec_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_exito_seg_acum,hoja_segmentos,"B52")
if (len(df_seg_exito_dec_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_exito_seg_reden,hoja_segmentos,"L52")
if (len(df_seg_banco_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_banco_acum,hoja_segmentos,"B64")
if (len(df_seg_banco_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_banco_reden,hoja_segmentos,"L64")    
if (len(df_seg_carulla_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_carulla_acum,hoja_segmentos,"B77")
if (len(df_seg_carulla_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_carulla_reden,hoja_segmentos,"L77")

    
#pegar segmentos demograficos (de la tabla de clientes final)  en excel
if (len(df_seg_gnro_acum) > 0): 
    pegar_df_en_excel_sin_encabezado(df_seg_gnro_acum,hoja_segmentos_demograficos,"B11")
if (len(df_seg_gnro_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_gnro_reden,hoja_segmentos_demograficos,"L11")
if (len(df_seg_edad_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_edad_acum,hoja_segmentos_demograficos,"B22")
if (len(df_seg_edad_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_edad_reden,hoja_segmentos_demograficos,"L22")
if (len(df_seg_ciudad_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_ciudad_acum,hoja_segmentos_demograficos,"B35")
if (len(df_seg_ciudad_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_seg_ciudad_reden,hoja_segmentos_demograficos,"L35")
    
#pegar en hoja recencia
if (len(df_recencia) > 0):
    pegar_df_en_excel_sin_encabezado(df_recencia,hoja_recencia,"B11")
if (len(df_recencia_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_recencia_acum,hoja_recencia,"F11") 
if (len(df_recencia_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_recencia_reden,hoja_recencia,"J11")   
    
    
#pegar en hoja gustos
if (len(df_gustos_comunicados) > 0):
    pegar_df_en_excel_sin_encabezado(df_gustos_comunicados,hoja_gustos,"E11")
if (len(df_gustos_comunicados_que_transaccionaron) > 0):
    pegar_df_en_excel_sin_encabezado(df_gustos_comunicados_que_transaccionaron,hoja_gustos,"H11")
    
#pegar en hoja categorias
if (len(df_categ_acum) > 0):
    pegar_df_en_excel_sin_encabezado(df_categ_acum,hoja_categorias,"B11")
if (len(df_categ_reden) > 0):
    pegar_df_en_excel_sin_encabezado(df_categ_reden,hoja_categorias,"L11")
if (len(df_categ_acum_aa) > 0):
    pegar_df_en_excel_sin_encabezado(df_categ_acum_aa,hoja_categorias,"X11")
if (len(df_categ_reden_aa) > 0):
    pegar_df_en_excel_sin_encabezado(df_categ_reden_aa,hoja_categorias,"AH11")
    

# Guardar el archivo de Excel
book.save(os.path.join(folder_path,nombre_campana,f"Campaña {nombre_campana} {tipo_efectividad} from {date_from} - to {date_to}.xlsx"))

    


