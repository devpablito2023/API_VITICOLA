#import motor.motor_asyncio
from bson.objectid import ObjectId
from decouple import config
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
load_dotenv()
print("luis estuvo aqui")
GENIAL = os.getenv("MONGO_DETAILS_OK") 
print("*************")
print(GENIAL)
print("*************")
mongo_db_name=os.getenv('BD_DETAILS_OK')
print("*************")
print(mongo_db_name)
print("*************")
client = AsyncIOMotorClient(GENIAL)
database_mongo = client[mongo_db_name] 

def collection(data):
    data_collection = database_mongo.get_collection(data)
    return data_collection

#conexion_externa
def conexion_externa(data):
    data_collection = database_mongo.get_collection(data)
    return data_collection



def collection(data):
    data_collection = database_mongo.get_collection(data)
    return data_collection

log_general_collection =  collection("log_general")
ids_collection = collection("ids_proyectos")
eventos_telemetria_collection = collection("eventos_telemetria")
contador_general_collection = collection("contador_general")

#aspectos para usuarios y token 
#token_collection = database_token.get_collection("token_ztrack")
evento_telemetria_collection = database_mongo.get_collection("evento_telemetria")
#usuarios_collection = database_token.get_collection("usuario_ztrack")
#h_usuarios_collection = database_token.get_collection("h_usuario_ztrack")

from datetime import datetime,timedelta

#async def validar_token_y_extender(token: str, user_id: int):
    #token_data = await token_collection.find_one({"token_ztrack": token, "estado_token": 1, "usuario_id": user_id}, {"_id": 0})
    #if not token_data:
        #return None
    #if token_data['fecha_fin'] <= datetime.now():
        #await token_collection.update_one({"token_ztrack": token}, {"$set": {"estado_token": 0, "fecha_invalidar": datetime.now()}})
        #return None
    # Extiende el token
    #nueva_fecha = token_data['fecha_fin'] + timedelta(minutes=30) if user_id == 1 else datetime.now() + timedelta(minutes=30)
    #await token_collection.update_one({"token_ztrack": token}, {"$set": {"fecha_fin": nueva_fecha}})
    #return token_data

async def guardar_evento_telemetria(responsable="SIN RESPONSABLE",mensaje="SIN MENSAJE", solicitud=0) :
    ids_proyectos = await ids_collection.find_one({"id_evento_telemetria": {"$exists": True}})
    evento_telemetria_data={}
    evento_telemetria_data['responsable'] = responsable
    evento_telemetria_data['mensaje'] = mensaje
    evento_telemetria_data['solicitud_id'] = solicitud
    evento_telemetria_data['fecha_evento'] = datetime.now() 
    evento_telemetria_data['estado_evento'] = 1
    evento_telemetria_data['id_evento_telemetria'] = ids_proyectos['id_evento_telemetria']+1 if ids_proyectos else 1
    print("validar _id de evento - parte 2")
    print(evento_telemetria_data)
    guardar_evento_telemetria = await evento_telemetria_collection.insert_one(evento_telemetria_data)
    s_ids ={"id_evento_telemetria":evento_telemetria_data['id_evento_telemetria'],"fecha":datetime.now()}
    procesar_ids = await ids_collection.update_one({"_id":ids_proyectos['_id'] },{"$set":s_ids}) if ids_proyectos else await ids_collection.insert_one(s_ids)
    proyecto_ok = "ok"
    return proyecto_ok

async def validar_usuario( user_id: int):
    #comercial_ok=None
    #buscar_comercial = await usuarios_collection.find_one({"estado_usuario": 1, "id_usuario": user_id}, {"_id": 0,"nombres_usuario":1,"apellidos_usuario":1})
    if user_id==0 :
        comercial_ok="zgroup"
    else :
        comercial_ok="dexterity"
    return comercial_ok

cont_general = {
    "id_contador" : 2,
    "modulo" :"solicitud_telemetria",
    "general":{
        "creado":3 ,
        "eliminado":2,
        "reestablecido":1
    },
    "20_05_2025":{
        "creado":1 ,
    },
    "21_05_2025":{
        "creado":2 ,
        "eliminado":2,
        "reestablecido":1
    }
}
TIPO_OPERACION = {
    1: "creado",
    2: "eliminado",
    3: "reestablecido",
    4: "editado"
}
async def contador_general(modulo:str="SIN_MODULO",tipo:int=1):
    hoy = datetime.now().strftime("%d_%m_%Y")
    operacion = TIPO_OPERACION.get(tipo)
    if not operacion:
        return None
    campos_inc = {f"general.{operacion}": 1,f"{hoy}.{operacion}": 1}
    campos_set = {}
    coincidencia = await contador_general_collection.find_one({"modulo":str(modulo)},{"_id": 0,"created_at":1})
    if coincidencia :
        campos_set["updated_at"] = datetime.now()
        result = await contador_general_collection.update_one(
            {"modulo": modulo},
            {"$inc": campos_inc,
            "$set": campos_set
            },
        upsert=True  # crea el documento si no existe
    )
    else :
        campos_set.setdefault("general", {})[operacion] = 1
        campos_set.setdefault(hoy, {})[operacion] = 1
        campos_set["modulo"] = modulo
        campos_set["created_at"] = datetime.now()
        result = await contador_general_collection.insert_one(campos_set)
    return "ok"

async def estaditica_general(modulo:str="SIN_MODULO"):
    coincidencia = await contador_general_collection.find_one({"modulo":str(modulo)},{"_id": 0})
    if coincidencia :
        return coincidencia
    else :
        return []