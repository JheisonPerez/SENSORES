import logging
import azure.functions as func
import pyodbc #PERMITE CONECTAR CON BASES DE DATOS sql
import pandas as pd #permite trabajar con vectores filas y col
import json #PERMITE INTERCAMBIO DE DATOS DENTRO DE LA MISMA RED
import uuid #PERMITE CREAR VALORES ALFANUM DE 12 DIGITOS
import os #PERMITE NAVEGAR POR CARPETAS

# Definicion de la funcion de traseo de error.
def traceDB(cnxnAzure,uuid,message):
    query = "INSERT INTO [dbo].[logs] ([ID],[Fecha],[Descripcion]) VALUES ('{}',GETDATE(),'{}')".format(uuid,message) #INSERTAR DENTOR DE BD LOGS EN LOS CAMPOS DESCRITOS
    cnxnAzure.execute(query)
    cnxnAzure.commit() #IMPRIMIRLO
    return(True)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Seteo de variables globales.') #SOLO SIRVE PARA IMPRIMIR MENSAJES EN PANTALLA A VER OCMO VA EL CODE
    ID = str(uuid.uuid1())
    logging.info(ID)
    driverAzure = os.environ["DriverAzure"]
    serverAzure = os.environ["ServerBdAzure"]
    databaseAzure = os.environ["DataBaseAzure"]
    usernameAzure = os.environ["UserNameBdAzure"]
    passwordAzure = os.environ["PassWordBdAzure"]
    SQL_datos = os.environ["SQL_datos"]
    logging.info(SQL_datos)  #SE CREA UNA VARIABLE SQLDATOS

    logging.info('Establece conexión con la base de datos Conectados.')
    logging.warning('Establece conexión con la base de datos Conectados.')
    logging.error('Establece conexión con la base de datos Conectados.')
    conStringAzure = "DRIVER={{{}}};SERVER={};DATABASE={};UID={};PWD={}".format(driverAzure,serverAzure,databaseAzure,usernameAzure,passwordAzure) #INGRESA LOS DATOS EN ORDEN
    logging.info(conStringAzure)
    cnxnAzure = pyodbc.connect(conStringAzure) #CONECTMOS A LA INFORMACION DEL .FORMAT
    logging.info('Conexión establecida con la base de datos Azure.')
    traceDB(cnxnAzure,ID,'Inicio servicio web.PRUEBA LOCAL, ENTREGA 10% VIDEO') #CONECTAR A LA BASE DE DATOS

    logging.info('Obtiene parámetros del JSON.')
    traceDB(cnxnAzure,ID,'Parámetros del servicio recibidos.')
    req_body = req.get_json()
    variable1 = req_body.get('variable1')
    logging.info(variable1)

    query = (SQL_datos)
    df_datos = pd.read_sql_query(query,cnxnAzure) #SELECCIONE TODA LA INFO DE LA TABLA DATOS A TRAVES DE LA CONEXIO cnxn
    diccionario = df_datos.to_dict('dict')
    json_response = json.dumps(diccionario,indent=2) #se convierte a diccionario de python
    traceDB(cnxnAzure,ID,'Se envia respuesta del servicio :D .')
    if variable1 < 10:
        return func.HttpResponse(json_response)
    else:
        return func.HttpResponse("local Puede que se ingresara in valor mal en el postman pero la funcion se ejecuto meleramente",status_code=200)