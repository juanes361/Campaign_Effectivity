from datetime import datetime, timedelta
import datetime as dt
import os
from components.params import Params
from components.athena_access import AthenaAccess
from components.s3_access import S3Access
import pandas as pd

def update_lines(tipo_efectividad, lines):
    for i in range(len(lines)):
        if tipo_efectividad == 'MACRO' and "MACRO_LINE" in lines[i]:
            lines[i] = lines[i].replace("--", "")
        elif tipo_efectividad == 'EMAIL' and "EMAIL_LINE" in lines[i]:
            lines[i] = lines[i].replace("--", "")
        elif tipo_efectividad == 'SMS' and "SMS_LINE" in lines[i]:
            lines[i] = lines[i].replace("--", "")
        elif tipo_efectividad == 'PUSH' and "PUSH_LINE" in lines[i]:
            lines[i] = lines[i].replace("--", "")
    return ''.join(lines)
    
def update_lines_genero_edad_ciudad(eleccion, lines):
    for i in range(len(lines)):
        if eleccion == 'CIUDAD' and "CIUDAD_LINE" in lines[i]:
            lines[i] = lines[i].replace("--", "")
        elif eleccion == 'GENERO' and "GENERO_LINE" in lines[i]:
            lines[i] = lines[i].replace("--", "")
        elif eleccion == 'EDAD' and "RANGO_EDAD_LINE" in lines[i]:
            lines[i] = lines[i].replace("--", "")
    return ''.join(lines)
    
def update_lines_aliado_categoria_marca(eleccion, lines):
    for i in range(len(lines)):
        if eleccion == 'CATEGORIA' and "CATEGORIA_LINE" in lines[i]:
            lines[i] = lines[i].replace("--", "")
        elif eleccion == 'ALIADO' and "ALIADO_LINE" in lines[i]:
            lines[i] = lines[i].replace("--", "")
        elif eleccion == 'MARCA' and "MARCA_LINE" in lines[i]:
            lines[i] = lines[i].replace("--", "")
        elif eleccion == 'CANAL' and "CANAL_LINE" in lines[i]:
            lines[i] = lines[i].replace("--", "")
    return ''.join(lines)
    

    
def update_lines_otros_segmentos(eleccion, lines):
    for i in range(len(lines)):
        if eleccion == 'MILLEROS' and "MILLEROS_LINE" in lines[i]:
            lines[i] = lines[i].replace("--", "")
        elif eleccion == 'VIAJEROS' and "VIAJEROS_LINE" in lines[i]:
            lines[i] = lines[i].replace("--", "")
        elif eleccion == 'CARULLA' and "CARULLA_LINE" in lines[i]:
            lines[i] = lines[i].replace("--", "")
        elif eleccion == 'EXITO_DECIL' and "EXITO_DECIL_LINE" in lines[i]:
            lines[i] = lines[i].replace("--", "")
        elif eleccion == 'EXITO_SEG' and "EXITO_SEG_LINE" in lines[i]:
            lines[i] = lines[i].replace("--", "")
        elif eleccion == 'BANCO' and "BANCO_LINE" in lines[i]:
            lines[i] = lines[i].replace("--", "")
    return ''.join(lines)
    

def update_trx_lines(lines):
    for i in range(len(lines)):
        lines[i] = lines[i].replace("--", "")
    return ''.join(lines)
    
    
    
def create_where_clause(where_dict):
    where_clause = ""
    for key, value in where_dict.items():
        if value and isinstance(value, list) and len(value[1]) > 0:
            if len(value[1]) == 1:
                # Agrega comillas al valor si es una cadena
                if isinstance(value[1][0], str):
                    value_str = f"'{value[1][0]}'"
                else:
                    value_str = str(value[1][0])
                where_clause += f"{value[0]} {key} IN ({value_str}) "
            else:
                where_clause += f"{value[0]} {key} IN {tuple(value[1])} "
        elif value and isinstance(value, str):
            where_clause += f"{value} {key} "
    if where_clause:
        where_clause = where_clause.strip()
    return where_clause



    

def save_query_in_txt(query,file_name,nombre_campana):

    now = dt.datetime.now()
    folder_path = os.path.join(os.getcwd(), "fecha_reporte", now.strftime('%Y'), now.strftime('%B'),nombre_campana)
    os.makedirs(folder_path, exist_ok=True)
    with open(os.path.join(folder_path,f'{file_name}.txt'), 'w') as file:
        # Write to the file
        file.write("--Revisar el siguiente codigo:/n     "+query)




def adaptar_columnas_audiencia():
    athena = AthenaAccess()
    s3Access = S3Access()
    df=None

    columna_a_agregar=[]
    with open('query_audiencia.txt', "r") as f:
        query_audiencia = f.read()
    result=athena.athena_to_s3('pco_datalake_prod_datalake_odsclm_db',query_audiencia,500)
    if result != False:
        obj = s3Access.readS3File(result)
        df = pd.read_csv(obj['Body'])
   
    columnas=df.columns.tolist()
    columnas_verificacion=['customer_id_','email','celular','saldo_puntos','muestra']
    columna_a_agregar = [c for c in columnas_verificacion if c not in columnas]
    
    query_str = ""  # Inicializar la variable con una cadena vacía
    
    if 'saldo_puntos' in columna_a_agregar and 'muestra' in columna_a_agregar:
        query_str = ",0 as saldo_puntos,'Muestra' as muestra"
    elif 'saldo_puntos' in columna_a_agregar:
        query_str = ",0 as saldo_puntos"
    elif 'muestra' in columna_a_agregar:
        query_str = ",'Muestra' as muestra"
    
    with open('query_transformacion_audiencia.txt', "r") as f:
        query = f.read()
    
    query= query.replace('{{query_audiencia}}', query_audiencia)
    query=query.replace('{{query_str}}', query_str)
    return query
    

def definir_query_trx(query):
    query_txt=None
    if query=="PYME":
        query_txt="query_trx_pyme.txt"
    if query=="JOURNEY":
        query_txt="query_trx_journeys.txt"
    if query=="MEDIA":
        query_txt="query_trx_media.txt"
    if query=="MERCADEO":
        query_txt="query_trx_mercadeo.txt"
    if query=="BOTON":
        query_txt="query_trx_boton.txt"
    return query_txt
    
    

def completar_df_resultado1(df,tipo_efectividad):
    
    if tipo_efectividad=='EMAIL':
        muestra_estado = [('CONTROL', '2. No enviado'),
                          ('MUESTRA', '2. No enviado'),
                          ('MUESTRA', '3. No entregado'),
                          ('MUESTRA', '4. Entregado y no abierto'),
                          ('MUESTRA', '5. Abierto sin click'),
                          ('MUESTRA', '6. Abierto con click')]
                      
    else:
        muestra_estado = [('CONTROL', '2. No enviado'),
                          ('MUESTRA', '2. No enviado'),
                          ('MUESTRA', '3. No entregado'),
                          ('MUESTRA', '4. Enviado y sin click'),
                          ('MUESTRA', '5. Enviado y con click')]
    
    for me in muestra_estado:
        if not ((df['muestra'] == me[0]) & (df['Estado'] == me[1])).any():
            new_row = {'muestra': me[0], 'Estado': me[1], 'clientes': 0, 'cliente_acumula': 0,'puntos_acumula': 0,
       'valor_acumula': 0, 'trx_acumula': 0, 'cliente_reden': 0, 'puntos_reden': 0,
       'valor_reden': 0, 'trx_reden': 0, 'cliente_trx': 0, 'SALDO_PUNTOS': 0}
            df = df.append(new_row, ignore_index=True)
    
    return df.sort_values(by=['muestra', 'Estado'])
    
def completar_df_resultado2(df):
    muestra_trn_type = ['ER','BR']
    
    for tt in muestra_trn_type:
        if not (df['trn_type'] == tt).any():
            new_rows = {'trn_type': tt, 'clientes_trx': 0, 'puntos': 0, 'valor': 0,'trx': 0,
                          'valor_acumula': 0}
            df = df.append(new_rows, ignore_index=True)
    return df.sort_values(by='trn_type')

    
    
