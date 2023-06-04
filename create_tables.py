from components.params import Params
from components.athena_access import AthenaAccess
from components.s3_access import S3Access
from functions import update_lines
from functions import update_trx_lines
from functions import update_lines_aliado_categoria_marca
from functions import update_lines_genero_edad_ciudad
from functions import update_lines_otros_segmentos
from functions import create_where_clause
from functions import save_query_in_txt
from functions import adaptar_columnas_audiencia
from functions import definir_query_trx
from functions import completar_df_resultado1
from functions import completar_df_resultado2
import pandas as pd
import datetime as dt


athena = AthenaAccess()
s3Access = S3Access()


#############################################################################################

def create_trx_table_pyme(query,nombre_campana,date_from,date_to,trx_table_name,con_query_comunicados,tipo_efectividad=None,maestra_table_name=None,clause_where=None):
    query=definir_query_trx(query)
    with open(query) as f:
        if con_query_comunicados=="SI":
            lines = f.readlines()
            #En caso de poner 'SI' descomenta lo que esta comentado en el query_trx_pyme, por lo tanto descomenta el join con los clientes comunicados
            query=update_trx_lines(lines)
            
            #Trae el query de clientes comunicados y lo inserta en el query_trx_pyme
            query_comunicados=read_query_comunicados(nombre_campana,tipo_efectividad,maestra_table_name)
            query = query.replace('{{query_comunicados}}', query_comunicados)
            
            
        elif con_query_comunicados=="NO":
            query = f.read()
    #En caso de poner 'No' simplemente reemplaza los siguientes datos en el query
    query = query.replace('{{date_from}}', date_from)
    query = query.replace('{{date_to}}', date_to)
    query = query.replace('{{trx_table_name}}', trx_table_name)
    
    #Si se establece un condicional where lo añade a la ultima linea
    if clause_where:
        where_clause = create_where_clause(clause_where)
        query += where_clause
    

    save_query_in_txt(query,f"query_trx_{date_from}_{date_to}",nombre_campana)
        

        
    athena.athena_to_s3('DATALAKE',query,500)
    

    
        
#############################################################################################    
def create_master_campaign(nombre_campana,tipo_efectividad, maestra_table_name, ids_campana_email, ids_campana_push, ids_campana_sms, trx_table_name):
    query_audiencia=adaptar_columnas_audiencia()
    with open("query_maestra.txt", "r") as f:
        lines = f.readlines()
    

    query = update_lines(tipo_efectividad, lines)
    query = query.replace('{{maestra_table_name}}', maestra_table_name)
    query = query.replace('{{ids_campana_email}}', ids_campana_email)
    query = query.replace('{{ids_campana_sms}}', ids_campana_sms)
    query = query.replace('{{ids_campana_push}}', ids_campana_push)
    query = query.replace('{{query_audiencia}}', query_audiencia)
    query = query.replace('{{trx_table_name}}', trx_table_name)

    save_query_in_txt(query,"query_maestra_campaña",nombre_campana)

    athena.athena_to_s3('DATALAKE',query,500)

 #############################################################################################   
def ejecucion_resultados1(nombre_campana,tipo_efectividad, maestra_table_name):
    df = None
    with open("query_resultados1.txt", "r") as f:
        lines = f.readlines()

    lines = update_lines(tipo_efectividad, lines)
    query = ''.join(lines)
    query = query.replace('{{maestra_table_name}}', maestra_table_name)

    result = athena.athena_to_s3('DATALAKE', query, 500)

    if result:
        obj = s3Access.readS3File(result)
        df = pd.read_csv(obj['Body'])
    df=completar_df_resultado1(df,tipo_efectividad)
    save_query_in_txt(query,"query_resultados1",nombre_campana)
    return df
    
    
#############################################################################################    
def ejecucion_resultados2(nombre_campana,tipo_efectividad,trx_table_name):
    df=None
    with open("query_resultados2.txt", "r") as f:
        query = f.read()
    query = query.replace('{{trx_table_name}}', trx_table_name)
    result=athena.athena_to_s3('DATALAKE',query,500)
    if result != False:
        obj = s3Access.readS3File(result)
        df = pd.read_csv(obj['Body'])
    save_query_in_txt(query,"query_resultados2",nombre_campana)
    completar_df_resultado2(df)
    return df
    
#############################################################################################    
    
def ejecucion_resultados3(nombre_campana,tipo_efectividad,trx_table_name):
    df=None
    with open("query_resultados3.txt", "r") as f:
        query = f.read()
    query = query.replace('{{trx_table_name}}', trx_table_name)
        
    result=athena.athena_to_s3('DATALAKE',query,500)
    if result != False:
        obj = s3Access.readS3File(result)
        df = pd.read_csv(obj['Body'])
    save_query_in_txt(query,"query_resultados3",nombre_campana)
    return df

 #############################################################################################    
      
    
def read_query_comunicados(nombre_campana,tipo_efectividad=None,maestra_table_name=None):
    if tipo_efectividad != None and maestra_table_name!=None:
        with open("query_comunicados.txt", "r") as f:
            lines = f.readlines()
            
        # Update lines based on tipo_efectividad
        query = update_lines(tipo_efectividad, lines)
        query= query.replace('{{maestra_table_name}}', maestra_table_name)
        save_query_in_txt(query,"query_comunicados",nombre_campana)
        return query   
        

    
    
  ############################################################################################# 
  
def ejecucion_resultado_aliados_marcas_categorias_canal(nombre_campana,tipo_trx,tipo_efectividad,trx_table_name,maestra_table_name,eleccion):
    df=None
    with open("query_trx_aliados_marcas_categorias_canal.txt", "r") as f:
        lines = f.readlines()
    query=update_lines_aliado_categoria_marca(eleccion,lines)
    query= query.replace('{{trx_table_name}}', trx_table_name)
    query= query.replace('{{tipo_trx}}', tipo_trx)
    query_comunicados=read_query_comunicados(nombre_campana,tipo_efectividad,maestra_table_name)
    query= query.replace('{{query_comunicados}}', query_comunicados)
    
    result=athena.athena_to_s3('DATALAKE',query,500)
    if result != False:
        obj = s3Access.readS3File(result)
        df = pd.read_csv(obj['Body'])
    save_query_in_txt(query,f"query_trx_{eleccion}",nombre_campana)
    return df
    
    
    
  ############################################################################################# 
  
def ejecucion_resultado_canal(nombre_campana,tipo_trx,tipo_efectividad,trx_table_name,maestra_table_name,canal):
    df=None
    with open("query_trx_aliados_x_canal.txt", "r") as f:
        query = f.read()
    query= query.replace('{{trx_table_name}}', trx_table_name)
    query= query.replace('{{tipo_trx}}', tipo_trx)
    query= query.replace('{{canal}}', canal)
    query_comunicados=read_query_comunicados(nombre_campana,tipo_efectividad,maestra_table_name)
    query= query.replace('{{query_comunicados}}', query_comunicados)
    
    result=athena.athena_to_s3('DATALAKE',query,500)
    if result != False:
        obj = s3Access.readS3File(result)
        df = pd.read_csv(obj['Body'])
    save_query_in_txt(query,f"query_trx_{canal}",nombre_campana)
    return df
        
  ############################################################################################# 
  
def ejecucion_resultado_segmentos_pco(nombre_campana,tipo_trx,tipo_efectividad,trx_table_name,maestra_table_name):
    df=None
    with open("query_trx_segmentos_pco.txt", "r") as f:
        query = f.read()
    query= query.replace('{{trx_table_name}}', trx_table_name)
    query= query.replace('{{tipo_trx}}', tipo_trx)
    query_comunicados=read_query_comunicados(nombre_campana,tipo_efectividad,maestra_table_name)
    query= query.replace('{{query_comunicados}}', query_comunicados)

    result=athena.athena_to_s3('DATALAKE',query,500)
    if result != False:
        obj = s3Access.readS3File(result)
        df = pd.read_csv(obj['Body'])
    save_query_in_txt(query,"query_segmentos",nombre_campana)
    return df
    
#############################################################################################    
def create_clientes_final(date_today,nombre_campana):
    with open("query_clientes_final.txt", "r") as f:
        query = f.read()
    query = query.replace('{{date_today}}', date_today)
    save_query_in_txt(query,"query_clientes_final",nombre_campana)
    athena.athena_to_s3('DATALAKE',query,500)
    
    
 
def ejecucion_resultado_genero_edad_ciudad(nombre_campana,tipo_trx,tipo_efectividad,trx_table_name,maestra_table_name,eleccion,clientes_final_table):
    df=None
    with open("query_genero_edad_ciudad.txt", "r") as f:
        lines = f.readlines()
    query=update_lines_genero_edad_ciudad(eleccion,lines)
    query= query.replace('{{trx_table_name}}', trx_table_name)
    query= query.replace('{{tipo_trx}}', tipo_trx)
    query_comunicados=read_query_comunicados(nombre_campana,tipo_efectividad,maestra_table_name)
    query= query.replace('{{query_comunicados}}', query_comunicados)    
    query= query.replace('{{clientes_final_table}}', clientes_final_table)

    result=athena.athena_to_s3('DATALAKE',query,500)
    if result != False:
        obj = s3Access.readS3File(result)
        df = pd.read_csv(obj['Body'])
    save_query_in_txt(query,f"query_segmentos_{eleccion}",nombre_campana)
    return df    
    
    
def ejecucion_resultado_gustos(nombre_campana,tipo_efectividad,maestra_table_name,clientes_final_table,trx_table_name,ver_trx):
    df=None
    with open("query_gustos.txt", "r") as f:
        if ver_trx=="SI":
            lines = f.readlines()
            query=''.join(lines)
            query = query.replace("--", "")
            query= query.replace('{{trx_table_name}}', trx_table_name)
        else:
            query = f.read() 
    query_comunicados=read_query_comunicados(nombre_campana,tipo_efectividad,maestra_table_name)
    query= query.replace('{{query_comunicados}}', query_comunicados)
    query= query.replace('{{clientes_final_table}}', clientes_final_table)
    
    result=athena.athena_to_s3('DATALAKE',query,500)
    if result != False:
        obj = s3Access.readS3File(result)
        df = pd.read_csv(obj['Body'])
    save_query_in_txt(query,"query_gustos",nombre_campana)
    return df 

  ############################################################################################# 
  
def ejecucion_resultado_otros_segmentos(nombre_campana,tipo_trx,tipo_efectividad,trx_table_name,maestra_table_name,eleccion):
    df=None
    with open("query_trx_otros_segmentos.txt", "r") as f:
        lines = f.readlines()
    query=update_lines_otros_segmentos(eleccion,lines)
    query= query.replace('{{trx_table_name}}', trx_table_name)
    query= query.replace('{{tipo_trx}}', tipo_trx)
    query_comunicados=read_query_comunicados(nombre_campana,tipo_efectividad,maestra_table_name)
    query= query.replace('{{query_comunicados}}', query_comunicados)

    
    result=athena.athena_to_s3('DATALAKE',query,500)
    if result != False:
        obj = s3Access.readS3File(result)
        df = pd.read_csv(obj['Body'])
    save_query_in_txt(query,f"query_trx_{eleccion}",nombre_campana)
    return df
    
def ejecucion_resultado_recencia(nombre_campana,tipo_trx,tipo_efectividad,trx_table_name_recencia,maestra_table_name,date_from_recencia):
    df=None
    with open("query_trx_recencia.txt", "r") as f:
        query = f.read()
    query= query.replace('{{trx_table_name_recencia}}', trx_table_name_recencia)
    query= query.replace('{{tipo_trx}}', tipo_trx)
    query_comunicados=read_query_comunicados(nombre_campana,tipo_efectividad,maestra_table_name)
    query= query.replace('{{query_comunicados}}', query_comunicados)
    query=query.replace('{{date_from_recencia}}', date_from_recencia)
    result=athena.athena_to_s3('DATALAKE',query,500)
    if result != False:
        obj = s3Access.readS3File(result)
        df = pd.read_csv(obj['Body'])
    save_query_in_txt(query,f"query_trx_recencia",nombre_campana)
    return df
        


    

    


    








