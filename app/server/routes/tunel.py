from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from server.functions.tunel import (
    Guardar_Datos,
    Procesar_Trama,
    #retrieve_datos,
    buscar_imei,
    insertar_comando,
    datos_totales,
    grafica_total,
    datos_totales_ok,
    grafica_total_ok,
    buscar_live,
    procesar_data_termoking,
  
)
#Aqui importamos el modelo necesario para la clase 
from server.models.tunel import (
    ErrorResponseModel,
    ResponseModel,
    TunelSchema,
    BusquedaSchema,
    ComandoSchema,

)
router = APIRouter()


@router.get("/PreTermoking/", response_description="Datos agregados a la base de datos.")
async def pre_termoking():
    new_notificacion = await procesar_data_termoking()
    return new_notificacion

@router.post("/live/", response_description="Datos agregados a la base de datos.")
async def buscar_live_ok(datos: BusquedaSchema = Body(...)):
    datos = jsonable_encoder(datos)   
    new_notificacion = await buscar_live(datos)


@router.post("/comando/", response_description="Datos agregados a la base de datos.")
async def add_comando(datos: ComandoSchema = Body(...)):
    datos = jsonable_encoder(datos)   
    new_notificacion = await insertar_comando(datos)
    return new_notificacion

@router.post("/General/", response_description="Datos agregados a la base de datos.")
async def add_data(datos: TunelSchema = Body(...)):
    #new_notificacion = await Guardar_Datos(datos)
    new_notificacion = await Guardar_Datos(datos.model_dump())

    
    return new_notificacion

@router.post("/imei/", response_description="Datos agregados a la base de datos.")
async def buscar_imei_ok(datos: BusquedaSchema = Body(...)):
    datos = jsonable_encoder(datos)   
    new_notificacion = await buscar_imei(datos)
    return new_notificacion

@router.post("/ListarTabla/", response_description="Datos agregados a la base de datos.")
async def buscar_tabla_ok(datos: BusquedaSchema = Body(...)):
    datos = jsonable_encoder(datos)   
    new_notificacion = await datos_totales(datos)
    return new_notificacion

@router.post("/ListarTablaOK/", response_description="Datos agregados a la base de datos.")
async def buscar_tabla_okok(datos: BusquedaSchema = Body(...)):
    datos = jsonable_encoder(datos)   
    new_notificacion = await datos_totales_ok(datos)
    return new_notificacion

@router.post("/ListarGrafica/", response_description="Datos agregados a la base de datos.")
async def buscar_grafica_ok(datos: BusquedaSchema = Body(...)):
    datos = jsonable_encoder(datos)   
    new_notificacion = await grafica_total(datos)
    return new_notificacion

@router.post("/ListarGraficaOK/", response_description="Datos agregados a la base de datos.")
async def buscar_grafica_okok(datos: BusquedaSchema = Body(...)):
    datos = jsonable_encoder(datos)   
    new_notificacion = await grafica_total_ok(datos)
    return new_notificacion

@router.get("/procesar_termo_king", response_description="Datos agregados a la base de datos.")
async def procesar_termo():
    new_notificacion = await Procesar_Trama()
    return new_notificacion


