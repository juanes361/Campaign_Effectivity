import openpyxl
import pandas as pd
import numpy as np
from openpyxl.utils import get_column_letter
from openpyxl.styles.alignment import Alignment
from openpyxl.worksheet.cell_range import CellRange
from openpyxl.worksheet.dimensions import ColumnDimension



def limpiar_hoja(hoja):
    """
    Esta función limpia todos los datos de la hoja de Excel

    hoja: objeto de la hoja de Excel a limpiar
    """

    # Recorrer todas las celdas de la hoja y asignarles un valor de None
    for fila in hoja:
        for celda in fila:
            celda.value = None



def pegar_df_en_excel(df, hoja, celda_inicio):
    """
    Esta función pega un DataFrame con encabezado en una hoja de Excel
    a partir de una celda específica.

    df: DataFrame a pegar
    hoja: objeto de la hoja de Excel a pegar el DataFrame
    celda_inicio: celda en la que se iniciará la pegada (ejemplo: "A1")
    """

    # Convertir el DataFrame a una matriz numpy
    matriz = df.values

    # Obtener el encabezado del DataFrame
    encabezado = list(df.columns)

    # Calcular la cantidad de columnas y filas del DataFrame
    num_columnas = len(df.columns)
    num_filas = len(df)

    # Inicializar la celda en la que se pegará el DataFrame
    celda = hoja[celda_inicio]

    # Pegar el encabezado en la hoja de Excel
    for j in range(num_columnas):
        celda_encabezado = encabezado[j]
        celda_actual = celda.offset(row=0, column=j)
        celda_actual.value = celda_encabezado

    # Pegar los datos en la hoja de Excel
    for i in range(num_filas):
        for j in range(num_columnas):
            celda_valor = matriz[i][j]
            celda_actual = celda.offset(row=i+1, column=j)
            celda_actual.value = celda_valor


def pegar_string_en_excel(texto, hoja, celda):
    """
    Esta función pega un string en una celda específica de una hoja de Excel.

    texto: string a pegar
    hoja: objeto de la hoja de Excel a pegar el string
    celda: celda en la que se pegará el string (ejemplo: "A1")
    """

    # Obtener la celda en la que se pegará el string
    celda_obj = hoja[celda]

    # Pegar el string en la celda
    celda_obj.value = texto
    
    

def pegar_df_en_excel_sin_encabezado(df, hoja, celda_inicio):
    """
    Esta función pega un DataFrame sin encabezado en una hoja de Excel
    a partir de una celda específica.

    df: DataFrame a pegar
    hoja: objeto de la hoja de Excel a pegar el DataFrame
    celda_inicio: celda en la que se iniciará la pegada (ejemplo: "A1")
    """

    # Convertir el DataFrame a una matriz numpy
    matriz = df.values

    # Calcular la cantidad de columnas y filas del DataFrame
    num_columnas = len(df.columns)
    num_filas = len(df)

    # Inicializar la celda en la que se pegará el DataFrame
    celda = hoja[celda_inicio]
    

    # Pegar los datos en la hoja de Excel
    for i in range(num_filas):
        for j in range(num_columnas):
            celda_valor = matriz[i][j]
            celda_actual = celda.offset(row=i, column=j)
            celda_actual.value = celda_valor

    






