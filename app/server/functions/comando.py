import json
from server.database import collection  ,conexion_externa
from bson import regex
from datetime import datetime,timedelta
#import mysql.connector
import requests





def bd_gene(imei):
    fet =datetime.now()
    #part = fet.strftime('%d_%m_%Y')
    part = fet.strftime('_%m_%Y')
    colect ="D_"+imei+part
    return colect

def bd_gene_tk(imei):
    fet =datetime.now()
    #part = fet.strftime('%d_%m_%Y')
    part = fet.strftime('_%m_%Y')
    colect ="TK_"+imei+part
    return colect

def bd_gene_2(imei):
    fet =datetime.now()
    #part = fet.strftime('%d_%m_%Y')
    part = fet.strftime('_%m_%Y')
    colect ="TUNEL_"+imei+part
    return colect




async def GuardarComandostexas(ztrack_data: dict) -> dict:
    #dat = ztrack_data['fecha']
    #print(ztrack_data)
    traer_id = []
    ids_collection = collection("ids")
    async for notificacion in ids_collection.find({"id":1},{"_id":0}):
        print(notificacion)
        traer_id.append(notificacion)
    print(traer_id)
    id_comando =  traer_id[0]['comando_id'] +1 if len(traer_id)!=0 else 1
    ztrack_data['id']=id_comando

    data_collection = collection(bd_gene_tk("control"))
    fet =datetime.now()
    ztrack_data['fecha_creacion'] = fet
    #primero consultar si ya existe un comando pendiente 
    encontrado = await data_collection.find_one({"imei":ztrack_data['imei'],"estado":1},{"_id":0})
    if encontrado :
        return 0
    notificacion = await data_collection.insert_one(ztrack_data)
    updated_ids = await ids_collection.update_one(
        {"id": 1}, {"$set": {"comando_id":id_comando}}
    )
    new_notificacion = await data_collection.find_one({"_id": notificacion.inserted_id},{"_id":0})
    return new_notificacion

async def GuardarComandos_libretexas(ztrack_data: dict) -> dict:
    #dat = ztrack_data['fecha']
    #print(ztrack_data)
    data_collection = collection(bd_gene_tk("control"))
    fet =datetime.now()
    ztrack_data['fecha_creacion'] = fet
    #primero consultar si ya existe un comando pendiente 
    conteo =[]

    traer_id = []
    ids_collection = collection("ids")
    async for notificacion in ids_collection.find({"id":1},{"_id":0}):
        print(notificacion)
        traer_id.append(notificacion)
    print(traer_id)
    id_comando =  traer_id[0]['comando_id'] +1 if len(traer_id)!=0 else 1
    ztrack_data['id']=id_comando

    async for notificacionok in  data_collection.find({"estado":1},{"_id":0}):
        conteo.append(notificacionok)
  
    if len(conteo)>=2 :
        return 0

    notificacion = await data_collection.insert_one(ztrack_data)
    updated_ids = await ids_collection.update_one(
        {"id": 1}, {"$set": {"comando_id":id_comando}}
    )
    new_notificacion = await data_collection.find_one({"_id": notificacion.inserted_id},{"_id":0})
    return new_notificacion

async def GuardarComandos_super_libretexas(ztrack_data: dict) -> dict:
    #dat = ztrack_data['fecha']
    #print(ztrack_data)
    data_collection = collection(bd_gene_tk("control"))
    fet =datetime.now()
    ztrack_data['fecha_creacion'] = fet

    traer_id = []
    ids_collection = collection("ids")
    async for notificacion in ids_collection.find({"id":1},{"_id":0}):
        print(notificacion)
        traer_id.append(notificacion)
    print(traer_id)
    id_comando =  traer_id[0]['comando_id'] +1 if len(traer_id)!=0 else 1
    ztrack_data['id']=id_comando

    notificacion = await data_collection.insert_one(ztrack_data)
    updated_ids = await ids_collection.update_one(
        {"id": 1}, {"$set": {"comando_id":id_comando}}
    )
    new_notificacion = await data_collection.find_one({"_id": notificacion.inserted_id},{"_id":0})
    return new_notificacion


def con_h(x,y):
   y = float(y)
   if 0 < y < 100:
      x=1
   return x   


async def Control_tunel_joya():
    #dat = ztrack_data['fecha']
    #print(ztrack_data)
    ztrack_data ={
        "imei": "868428047321157",
        "estado": 1,
        "fecha_ejecucion": None,
        "comando": "DEFROST:120",
        #"comando": "MANUAL_RIPE(15.60,90,120,6.00)",
        "evento": "Control automatico de tunel de joya ",
        "user_c": "supervisado_demonio",
        "status": 2
    }
    data_collection = collection(bd_gene_2("control"))
    fet =datetime.now()
    ztrack_data['fecha_creacion'] = fet

    notificacion = await data_collection.insert_one(ztrack_data)

    return notificacion

async def Control_texas():
    #dat = ztrack_data['fecha']
    #print(ztrack_data)
    ztrack_data ={
        "imei": "863576045638595",
        "estado": 1,
        "fecha_ejecucion": None,
        #"comando": "CMD:MANUAL_RIPE(17.70,88,1,1.00)",
        "comando": "CMD:MANUAL_RIPE(18.00,90,200,4.00)",
        "dispositivo":"Demonio_automatico",
        
        "evento": "Control automatico texas  ",
        "user": "supervisado_demonio",
        "status": 2
    }
    data_collection = collection(bd_gene_tk("control"))
    fet =datetime.now()
    ztrack_data['fecha_creacion'] = fet

    notificacion = await data_collection.insert_one(ztrack_data)

    return notificacion

async def Control_texas_off():
    #dat = ztrack_data['fecha']
    #print(ztrack_data)
    ztrack_data ={
        "imei": "863576045638595",
        "estado": 1,
        "fecha_ejecucion": None,
        "comando": "CMD:MANUAL_RIPE(20.00,90,1,3.00)",
        "dispositivo":"Demonio_automatico",
        
        "evento": "Control automatico texas  ",
        "user": "supervisado_demonio",
        "status": 2
    }
    data_collection = collection(bd_gene_tk("control"))
    fet =datetime.now()
    ztrack_data['fecha_creacion'] = fet

    notificacion = await data_collection.insert_one(ztrack_data)

    return notificacion

async def GuardarComandos_super_libre_supervisado():
    #dat = ztrack_data['fecha']
    #print(ztrack_data)
    ztrack_data ={
        "imei": "866782048942516",
        "estado": 1,
        "fecha_ejecucion": None,
        "comando": "MANUAL_RIPE(14.40,90,150,5.00)",
        #"comando": "MANUAL_RIPE(15.60,90,120,6.00)",
        "dispositivo": "FAIL",
        "evento": "demonio en accion cada hora  ",
        "user": "supervisado_demonio",
        "receta": "sin receta",
        "tipo": 0,
        "status": 2,
        "dato": None
    }
    data_collection = collection(bd_gene("control"))
    fet =datetime.now()
    ztrack_data['fecha_creacion'] = fet

    traer_id = []
    ids_collection = collection("ids")
    async for notificacion in ids_collection.find({"id":1},{"_id":0}):
        print(notificacion)
        traer_id.append(notificacion)
    print(traer_id)
    id_comando =  traer_id[0]['comando_id'] +1 if len(traer_id)!=0 else 1
    ztrack_data['id']=id_comando

    notificacion = await data_collection.insert_one(ztrack_data)
    updated_ids = await ids_collection.update_one(
        {"id": 1}, {"$set": {"comando_id":id_comando}}
    )
    new_notificacion = await data_collection.find_one({"_id": notificacion.inserted_id},{"_id":0})
    return new_notificacion



async def GuardarComandos(ztrack_data: dict) -> dict:
    #dat = ztrack_data['fecha']
    #print(ztrack_data)
    traer_id = []
    ids_collection = collection("ids")
    async for notificacion in ids_collection.find({"id":1},{"_id":0}):
        print(notificacion)
        traer_id.append(notificacion)
    print(traer_id)
    id_comando =  traer_id[0]['comando_id'] +1 if len(traer_id)!=0 else 1
    ztrack_data['id']=id_comando

    data_collection = collection(bd_gene("control"))
    fet =datetime.now()
    ztrack_data['fecha_creacion'] = fet
    #primero consultar si ya existe un comando pendiente 
    encontrado = await data_collection.find_one({"imei":ztrack_data['imei'],"estado":1},{"_id":0})
    if encontrado :
        return 0
    notificacion = await data_collection.insert_one(ztrack_data)
    updated_ids = await ids_collection.update_one(
        {"id": 1}, {"$set": {"comando_id":id_comando}}
    )
    new_notificacion = await data_collection.find_one({"_id": notificacion.inserted_id},{"_id":0})
    return new_notificacion

async def GuardarComandos_libre(ztrack_data: dict) -> dict:
    #dat = ztrack_data['fecha']
    #print(ztrack_data)
    data_collection = collection(bd_gene("control"))
    fet =datetime.now()
    ztrack_data['fecha_creacion'] = fet
    #primero consultar si ya existe un comando pendiente 
    conteo =[]

    traer_id = []
    ids_collection = collection("ids")
    async for notificacion in ids_collection.find({"id":1},{"_id":0}):
        print(notificacion)
        traer_id.append(notificacion)
    print(traer_id)
    id_comando =  traer_id[0]['comando_id'] +1 if len(traer_id)!=0 else 1
    ztrack_data['id']=id_comando

    async for notificacionok in  data_collection.find({"estado":1},{"_id":0}):
        conteo.append(notificacionok)
  
    if len(conteo)>=2 :
        return 0

    notificacion = await data_collection.insert_one(ztrack_data)
    updated_ids = await ids_collection.update_one(
        {"id": 1}, {"$set": {"comando_id":id_comando}}
    )
    new_notificacion = await data_collection.find_one({"_id": notificacion.inserted_id},{"_id":0})
    return new_notificacion

async def GuardarComandos_super_libre(ztrack_data: dict) -> dict:
    #dat = ztrack_data['fecha']
    #print(ztrack_data)
    data_collection = collection(bd_gene("control"))
    fet =datetime.now()
    ztrack_data['fecha_creacion'] = fet

    traer_id = []
    ids_collection = collection("ids")
    async for notificacion in ids_collection.find({"id":1},{"_id":0}):
        print(notificacion)
        traer_id.append(notificacion)
    print(traer_id)
    id_comando =  traer_id[0]['comando_id'] +1 if len(traer_id)!=0 else 1
    ztrack_data['id']=id_comando

    notificacion = await data_collection.insert_one(ztrack_data)
    updated_ids = await ids_collection.update_one(
        {"id": 1}, {"$set": {"comando_id":id_comando}}
    )
    new_notificacion = await data_collection.find_one({"_id": notificacion.inserted_id},{"_id":0})
    return new_notificacion

async def RetrieveComandos(imei: str):
    notificacions = []
    print("me voy a casa")
    print(imei)
    print("me voy a casa")
    data_collection = collection(bd_gene("control"))
    async for notificacion in data_collection.find({"imei":imei},{"_id":0}):
        notificacions.append(notificacion)
    return notificacions


async def procesar_on_guardia_civil():
    fet =datetime.now()
    valor_general = {
        "imei": "865691035501170",
        "estado": 1,
        "comando": "Trama_Writeout(29,1,1)",
        "dispositivo": "FAIL",
        "evento": "turn on the reefer machine",
        "user": "pabecsa",
        "receta": "sin receta pabecsa",
        "tipo": 1,
        "status": 1,
        "dato": 1,
        "id": 1300
    }
    valor_general['fecha_creacion'] = fet
    valor_general['fecha_ejecucion'] = fet

    data_collection = collection(bd_gene("control"))
    #primero consultar si ya existe un comando pendiente 
    encontrado = await data_collection.find_one({"imei":valor_general['imei'],"estado":1},{"_id":0})
    if encontrado :
        return 0
    notificacion = await data_collection.insert_one(valor_general)

    return notificacion


async def procesar_off_guardia_civil():
    fet =datetime.now()
    valor_general = {
        "imei": "865691035501170",
        "estado": 1,
        "comando": "Trama_Writeout(29,0,1)",
        "dispositivo": "FAIL",
        "evento": "turn off the reefer machine",
        "user": "pabecsa",
        "receta": "sin receta pabecsa",
        "tipo": 1,
        "status": 1,
        "dato": 1,
        "id": 1300
    }
    valor_general['fecha_creacion'] = fet
    valor_general['fecha_ejecucion'] = fet

    data_collection = collection(bd_gene("control"))
    #primero consultar si ya existe un comando pendiente 
    encontrado = await data_collection.find_one({"imei":valor_general['imei'],"estado":1},{"_id":0})
    if encontrado :
        return 0
    notificacion = await data_collection.insert_one(valor_general)
    return notificacion



async def procesar_10_dex():
    fet =datetime.now()
    valor_general = {
        "imei": "868428046606400",
        "estado": 1,
        "comando": "1X04,UNIT111,HEAD:1&(0,-10.0,100)&,12341X02",
        "dispositivo": "FAIL",
        "evento": "Temperature level change to 14.0 F°",
        "user": "dexterity",
        "receta": "sin receta dexterity",
        "tipo": 7,
        "status": 1,
        "dato": -10,
        "id": 1300
    }
    valor_general['fecha_creacion'] = fet
    valor_general['fecha_ejecucion'] = fet

    data_collection = collection(bd_gene_tk("control"))
    #primero consultar si ya existe un comando pendiente 
    encontrado = await data_collection.find_one({"imei":valor_general['imei'],"estado":1},{"_id":0})
    if encontrado :
        return 0
    notificacion = await data_collection.insert_one(valor_general)

    return notificacion


async def procesar_25_dex():
    fet =datetime.now()
    valor_general = {
        "imei": "868428046606400",
        "estado": 1,
        "comando": "1X04,UNIT111,HEAD:1&(0,25.0,100)&,12341X02",
        "dispositivo": "FAIL",
        "evento": "Temperature level change to 77.0 F°",
        "user": "dexterity",
        "receta": "sin receta dexterity",
        "tipo": 7,
        "status": 1,
        "dato": 25,
        "id": 1300
    }
    valor_general['fecha_creacion'] = fet
    valor_general['fecha_ejecucion'] = fet

    data_collection = collection(bd_gene_tk("control"))
    #primero consultar si ya existe un comando pendiente 
    encontrado = await data_collection.find_one({"imei":valor_general['imei'],"estado":1},{"_id":0})
    if encontrado :
        return 0
    notificacion = await data_collection.insert_one(valor_general)
    return notificacion


async def procesar_on_pabecsa():
    fet =datetime.now()
    valor_general = {
        "imei": "863576043636583",
        "estado": 1,
        "comando": "Trama_Writeout(29,1,1)",
        "dispositivo": "FAIL",
        "evento": "turn on the reefer machine",
        "user": "pabecsa",
        "receta": "sin receta pabecsa",
        "tipo": 1,
        "status": 1,
        "dato": 1,
        "id": 1300
    }
    valor_general['fecha_creacion'] = fet
    valor_general['fecha_ejecucion'] = fet

    data_collection = collection(bd_gene("control"))
    #primero consultar si ya existe un comando pendiente 
    encontrado = await data_collection.find_one({"imei":valor_general['imei'],"estado":1},{"_id":0})
    if encontrado :
        return 0
    notificacion = await data_collection.insert_one(valor_general)

    return notificacion


async def procesar_off_pabecsa():
    fet =datetime.now()
    valor_general = {
        "imei": "863576043636583",
        "estado": 1,
        "comando": "Trama_Writeout(29,0,1)",
        "dispositivo": "FAIL",
        "evento": "turn off the reefer machine",
        "user": "pabecsa",
        "receta": "sin receta pabecsa",
        "tipo": 1,
        "status": 1,
        "dato": 1,
        "id": 1300
    }
    valor_general['fecha_creacion'] = fet
    valor_general['fecha_ejecucion'] = fet

    data_collection = collection(bd_gene("control"))
    #primero consultar si ya existe un comando pendiente 
    encontrado = await data_collection.find_one({"imei":valor_general['imei'],"estado":1},{"_id":0})
    if encontrado :
        return 0
    notificacion = await data_collection.insert_one(valor_general)
    return notificacion


async def RetrieveComandos_test(imei: str):
    notificacions = []
    data_collection = collection(bd_gene("control"))
    async for notificacion in data_collection.find({"imei":imei,"estado":3,},{"_id":0}).sort({"fecha_creacion":-1}):
        notificacions.append(notificacion)
    return notificacions

async def RetrieveComandos_oficial(imei: str):
    notificacions = []
    data_collection = collection(bd_gene("control"))
    #{user:"jhonvena",$or:[{status:0},{status:2}]}
    async for notificacion in data_collection.find({"imei":imei ,"user":"jhonvena","$or":[{"status":1},{"status":2}]},{"_id":0}).sort({"fecha_creacion":-1}):
        notificacions.append(notificacion)
    return notificacions

async def RetrieveComandos_oficial_dexterity(imei: str):
    notificacions = []
    data_collection = collection(bd_gene_tk("control"))
    #{user:"jhonvena",$or:[{status:0},{status:2}]}
    async for notificacion in data_collection.find({"imei":imei ,"user":"dexterity","$or":[{"status":1},{"status":2}]},{"_id":0}).sort({"fecha_creacion":-1}): 
        notificacions.append(notificacion)
    return notificacions


def validar_tipo(dato,tipo,json_v):
    res=None
    if(tipo==1):
        if(json_v['power_state']==dato):
            res="ok"
    elif(tipo==2):
        if(json_v['power_state']==dato):
            res="ok"
    elif(tipo==3):
        if(json_v['set_point_co2']==dato):
            res="ok"
    elif(tipo==4):
        if(json_v['sp_ethyleno']==dato):
            res="ok"
    elif(tipo==5):
        if(json_v['inyeccion_hora']==dato):
            res="ok"
    elif(tipo==6):
        if(json_v['humidity_set_point']==dato):
            res="ok"
    elif(tipo==7):
        if(json_v['set_point']==dato):
            res="ok"
    elif(tipo==8):
        if(json_v['avl']==dato):
            res="ok"
    elif(tipo==9):
        if(json_v['avl']==dato):
            res="ok"
    elif(tipo==10):
        if(json_v['controlling_mode']==dato):
            res="ok"
    #elif(tipo==11):
        #if(json_v['power_sate']==dato):
            #res="ok"
    elif(tipo==12):
        if(json_v['fresh_air_mode']==dato):
            res="ok"
    else:
        res=None
    return res
    
    
async def comando_jhon_vena(imei: str):
    notificacions = []
    print("me voy a casa")
    print(imei)
    print("me voy a casa")
    data_collection = collection(bd_gene("control"))
    fecha_actual = datetime.now()
    #fecha_modificada = fecha_actual - timedelta(hours=1)
    fecha_modificada = fecha_actual - timedelta(hours=1)
    cont =0
    print(fecha_actual)
    print(fecha_modificada)
    #fechaI=datetime.fromisoformat(fecha_actual)
    #fechaF=datetime.fromisoformat(fecha_modificada)

    consulta_mysql =[]
    #pedir ultimos datos con esas carcteristicas
    cnx = mysql.connector.connect(
        host= "localhost",
        user= "ztrack2023",
        passwd= "lpmp2018",
        database="zgroupztrack"
    )
    curB = cnx.cursor()
    consulta_J = (
        "SELECT * FROM contenedores WHERE telemetria_id = %s"
    )
    curB.execute(consulta_J, (14872,))
    for data in curB :
        consulta_mysql.append(data)
        #print(data[0])
    obj_vali ={
        "menbrete":consulta_mysql[0][4],
        "power_state":consulta_mysql[0][53],
        "set_point_co2":consulta_mysql[0][61],
        "sp_ethyleno":consulta_mysql[0][79],
        "inyeccion_hora":consulta_mysql[0][77],
        "humidity_set_point":consulta_mysql[0][56],
        "set_point":consulta_mysql[0][10],
        "avl":consulta_mysql[0][27],
        "controlling_mode":consulta_mysql[0][54],
        "fresh_air_mode":consulta_mysql[0][57],
    }
    curB.close()
    cnx.close()




    async for notificacion in data_collection.find({"$and": [{"fecha_creacion": {"$lte": fecha_modificada}},{"estado":1},{"user":"jhonvena"}]},{"_id":0}).sort({"fecha_creacion":-1}):
        cont =cont+1
        #validar_tipo(dato,tipo,json_v):
        valor =validar_tipo(notificacion['dato'],notificacion['tipo'],obj_vali)
        notificacion['validacion']=valor
        notificacions.append(notificacion)
    async for notificacion in data_collection.find({"$and": [{"fecha_creacion": {"$gte": fecha_modificada}},{"user":"jhonvena"}]},{"_id":0}).sort({"fecha_creacion":-1}):
        cont =cont+1
        valor =validar_tipo(notificacion['dato'],notificacion['tipo'],obj_vali)
        notificacion['validacion']=valor
        notificacions.append(notificacion)
    print(cont)

    res ={
        "contador":cont,
        "objeto_datos":obj_vali,
        "fecha_menor" :fecha_modificada,
        "consulta_mysql" :consulta_mysql,

        "lista":notificacions
    }
    #async for notificacion in data_collection.find({"imei":imei},{"_id":0}):
        #notificacions.append(notificacion)
    #return notificacions
    return res

def convertir_a_float(dato):
    if isinstance(dato, float):
        return dato
    try:
        float_value = float(dato)
        return round(float_value,1)
    except ValueError:
        return None


def pasar_temp(numero):
    if -40 <= numero <= 130:
        return numero
    else:
        return None

async def ProcesarData():
    print("yamos jodidos")
    dispositivos=[]
    dispositivos_collection = collection(bd_gene("dispositivos"))
    #async for notificacion in dispositivos_collection.find({"estado":1,"imei":"866782048942516"},{"_id":0}):
    async for notificacion in dispositivos_collection.find({"estado":1},{"_id":0}):
        #aqui procesamos 
        datos_dispositivo =bd_gene(notificacion['imei'])
        print(datos_dispositivo)
        unidad_collection = collection(datos_dispositivo)
        async for trama in unidad_collection.find({"estado":1},{"_id":0}):     
            #dato_id = await dispositivos_collection.find_one({"estado":1,"imei":"866782048942516"},{"_id":0})
            dato_id = await dispositivos_collection.find_one({"estado":1},{"_id":0})
            id_con = int(dato_id['id_cont']) +1 if dato_id['id_cont'] else 300000000
            #print("ya no tan jodido")
            #print(trama)
            #print("ya no tan jodido")
            trama_ok =str(trama['c'])
            #print("**********************")
            #print(trama['c'])
            #print("**********************")
            transformado = trama_ok.split(',')
            longitud_trama = 1 if len(transformado)>=65 else 0

            print("**********************")
            print(transformado)
            print("**********************")

            print(notificacion['imei'])
            print(longitud_trama)
            print("**********************")
            if longitud_trama==1:
                #procesar datos 
                if notificacion['imei']=="866782048942516"  :
                    vali =transformado
                    #idProgre=1
                    idProgre=id_con
                    tele_dispositivo =14872
                    comparador1 = vali[65] if len(transformado)>65 else 0
                    comparador2 = vali[66] if len(transformado)>66 else 0

                    #14952 ->863576041438461 -> ZGRU6709537
                    if (notificacion['imei']=="OTRO"):
                        tele_dispositivo =10000
                        valorP =  0
                        lat = -12.09858
                        lon = -77.01155                    

                    else:
                        valorP = 5 if int(comparador1)==1 else 0
                        lat = 35.7396
                        lon = -119.238

                    objetoV = {
                            "id": idProgre, 
                            "set_point": pasar_temp(convertir_a_float(vali[1])), 
                            "temp_supply_1": pasar_temp(convertir_a_float(vali[2])),
                            "temp_supply_2": pasar_temp(convertir_a_float(vali[3])),
                            "return_air": pasar_temp(convertir_a_float(vali[4])), 
                            "evaporation_coil": pasar_temp(convertir_a_float(vali[5])),
                            "condensation_coil": pasar_temp(convertir_a_float(vali[6])),
                            "compress_coil_1": pasar_temp(convertir_a_float(vali[7])),
                            "compress_coil_2": pasar_temp(convertir_a_float(vali[8])), 
                            "ambient_air": pasar_temp(convertir_a_float(vali[9])), 
                            "cargo_1_temp": pasar_temp(convertir_a_float(vali[10])), 
                            "cargo_2_temp": pasar_temp(convertir_a_float(vali[11])), 
                            "cargo_3_temp": pasar_temp(convertir_a_float(vali[12])), 
                            "cargo_4_temp": pasar_temp(convertir_a_float(vali[13])), 
                            "relative_humidity": convertir_a_float(vali[14]), 
                            "avl": convertir_a_float(vali[15]), 
                            "suction_pressure": convertir_a_float(vali[16]), 
                            "discharge_pressure": convertir_a_float(vali[17]), 
                            "line_voltage": convertir_a_float(vali[18]), 
                            "line_frequency": convertir_a_float(vali[19]), 
                            "consumption_ph_1": convertir_a_float(vali[20]), 
                            "consumption_ph_2": convertir_a_float(vali[21]), 
                            "consumption_ph_3": convertir_a_float(vali[22]), 
                            "co2_reading": convertir_a_float(vali[23]), 
                            "o2_reading": convertir_a_float(vali[24]), 
                            "evaporator_speed": convertir_a_float(vali[25]), 
                            "condenser_speed": convertir_a_float(vali[26]),
                            "power_kwh": convertir_a_float(vali[27]),
                            "power_trip_reading": convertir_a_float(vali[28]),
                            "suction_temp": convertir_a_float(vali[29]),
                            "discharge_temp": convertir_a_float(vali[30]),
                            "supply_air_temp": convertir_a_float(vali[31]),
                            "return_air_temp": convertir_a_float(vali[32]),
                            "dl_battery_temp": convertir_a_float(vali[33]),
                            "dl_battery_charge": convertir_a_float(vali[34]),
                            "power_consumption": convertir_a_float(vali[35]),
                            "power_consumption_avg": convertir_a_float(vali[36]),
                            "alarm_present": convertir_a_float(vali[37]),
                            "capacity_load": convertir_a_float(vali[38]),
                            "power_state":convertir_a_float(con_h( vali[39],vali[14])), 
                            "controlling_mode": vali[40],
                            "humidity_control": convertir_a_float(vali[41]),
                            "humidity_set_point": convertir_a_float(vali[42]),
                            "fresh_air_ex_mode": convertir_a_float(vali[43]),
                            "fresh_air_ex_rate": convertir_a_float(vali[44]),
                            "fresh_air_ex_delay": convertir_a_float(vali[45]),
                            "set_point_o2": convertir_a_float(vali[46]),
                            "set_point_co2": convertir_a_float(vali[47]),
                            "defrost_term_temp": convertir_a_float(vali[48]),
                            "defrost_interval": convertir_a_float(vali[49]),
                            "water_cooled_conde": convertir_a_float(vali[50]),
                            "usda_trip": convertir_a_float(vali[51]),
                            "evaporator_exp_valve": convertir_a_float(vali[52]),
                            "suction_mod_valve": convertir_a_float(vali[53]),
                            "hot_gas_valve": convertir_a_float(vali[54]),
                            "economizer_valve": convertir_a_float(vali[55]),
                            "ethylene": convertir_a_float(vali[56]),
                            "stateProcess": valorP ,
                            "stateInyection": vali[64],
                            #$document['stateProcess']==5.00 vali[57]
                            "timerOfProcess": convertir_a_float(0),
                            "battery_voltage": convertir_a_float(0),
                            "power_trip_duration":convertir_a_float(0),
                            "modelo": "THERMOKING",
                            "latitud": lat,
                            "longitud":  lon,
                            "created_at": trama['fecha'],
                            "telemetria_id": tele_dispositivo,
                            "inyeccion_etileno": 0,
                            "defrost_prueba": 0,
                            "ripener_prueba": 0,
                            "sp_ethyleno": convertir_a_float(vali[61]),
                            "inyeccion_hora": convertir_a_float(vali[58]),
                            "inyeccion_pwm": convertir_a_float(vali[63]),
                            "extra_1": 0,
                            "extra_2": 0,
                            "extra_3": 0,
                            "extra_4": 0,
                            "extra_5": 0,
                            "imei":trama['i'],
                            "tiempo_paso":comparador2,
                            "device":vali[0]

                    }
                    print(objetoV)
                    #conectar a la base de datos 
                    #SE NECESITA ENVIAR ESTE OBJETO A LA BASE DE DATOS EXTERNA A TRAVES DE UNA API
                    urll ="http://161.132.206.104:9050/Madurador/TestIntegracion/"
                    headers = {
                    'Content-Type': 'application/json'
                    }
                    objetoV['created_at'] = objetoV['created_at'].isoformat()
                    #objetoV['fecha'] = objetoV['created_at'].isoformat()
                    objetoV['fecha'] = objetoV['created_at']

                    response = requests.post(urll, headers=headers, json=objetoV)
                    print(response.status_code)
                    print(response.text)
                    #AHORA ACTUALIZAR LA TABLA MYSQL 
                    json_ok_ok ={                    
                        "ultima_fecha": objetoV['created_at'].strftime("%Y-%m-%d %H:%M:%S"),
                        "fecha": objetoV['created_at'].strftime("%Y-%m-%d %H:%M:%S"),
                        "set_point": objetoV['set_point'],
                        "return_air": objetoV['return_air'],
                        "temp_supply_1": objetoV['temp_supply_1'],
                        "ambient_air": objetoV['ambient_air'],
                        "relative_humidity": objetoV['relative_humidity'],
                        "avl": objetoV['avl'],
                        "inyeccion_pwm": objetoV['inyeccion_pwm'],
                        "inyeccion_hora": objetoV['inyeccion_hora'],
                        "ethylene": objetoV['ethylene'],
                        "set_point_co2": objetoV['set_point_co2'],
                        "co2_reading": objetoV['co2_reading'],
                        "humidity_set_point": objetoV['humidity_set_point'],
                        "sp_ethyleno": objetoV['sp_ethyleno'],
                        "compress_coil_1": objetoV['compress_coil_1'],
                        "power_state": objetoV['power_state'],
                        "evaporation_coil": objetoV['evaporation_coil'],
                        "controlling_mode": objetoV['controlling_mode'],
                        "stateProcess": objetoV['stateProcess'],
                        "cargo_1_temp": objetoV['cargo_1_temp'],
                        "cargo_2_temp": objetoV['cargo_2_temp'],
                        "cargo_3_temp": objetoV['cargo_3_temp'],
                        "cargo_4_temp": objetoV['cargo_4_temp'],
                        "fresh_air_ex_mode": objetoV['fresh_air_ex_mode'],
                        "telemetria_id": objetoV['telemetria_id']
                    }
                    url2 ="http://161.132.206.104:9010/contenedores/actualizar_data"
                    headers_3 = {
                    'Content-Type': 'application/json'
                    }
                    print("hhhhhhhhhhh")
                    print(json_ok_ok)
                    print("hhhhhhhhhhh")

                    #json_str = json.dumps(json_ok_ok, default=str)
                    response1 = requests.post(url2, headers=headers_3, json=json_ok_ok)
                    print(response1.status_code)
                    print(response1.text)

        dispositivos.append(notificacion)
    return dispositivos

                        
