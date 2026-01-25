from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

#aqui pedimos las funciones que incluyen nuestro CRUD
from server.functions.control_temperatura import (
    guardar_control_temperatura,
    listar_control_temperatura,
    ver_control_temperatura,
    eliminar_control_temperatura,
    reestablecer_control_temperatura,
    estadistica_control_temperatura,   
    seguimiento_supervisado,
    #Control_tunel_joya,
)
#Aqui importamos el modelo necesario para la clase 
from server.models.control_temperatura import (
    ErrorResponseModel,
    ResponseModel,
    control_temperatura_Schema,
    ConsultarSchema,
    BusquedaSchema,
)
router = APIRouter()

@router.get("/Control_tunel_joya/", response_description="Datos recuperados")
async def Control_tunel_joya_OK():
    notificacions = await Control_tunel_joya()
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta")

@router.get("/seguimiento_supervisado/", response_description="Datos recuperados")
async def seguimiento_supervisado_ok():
    notificacions = await seguimiento_supervisado()
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta")

@router.post("/", response_description="Datos agregados a la base de datos.")
async def guardar_control_temperatura_ok(datos: control_temperatura_Schema = Body(...)):
    datos = jsonable_encoder(datos)   
    new_notificacion = await guardar_control_temperatura(datos)

    return ResponseModel(new_notificacion, "ok")

@router.post("/listar", response_description="Datos Listados de los usuarios.")
async def listar_control_temperatura_ok(datos: ConsultarSchema = Body(...)):
    datos = jsonable_encoder(datos) 
    new_notificacion = await listar_control_temperatura(datos)
    if  new_notificacion:
        return ResponseModel(new_notificacion, "ok")
    else :
        return ErrorResponseModel("VERIFICA TUS DATOS", 404, "NO SE HA ENCONTRADO")

@router.post("/ver", response_description="Datos Listados de los usuarios.")
async def ver_control_temperatura_ok(datos: ConsultarSchema = Body(...)):
    datos = jsonable_encoder(datos) 
    new_notificacion = await ver_control_temperatura(datos)
    if  new_notificacion:
        return ResponseModel(new_notificacion, "ok")
    else :
        return ErrorResponseModel("verifica tus datos", 404, "NO SE HA ENCONTRADO")

@router.post("/eliminar", response_description="Datos Listados de los usuarios.")
async def eliminar_control_temperatura_ok(datos: ConsultarSchema = Body(...)):
    datos = jsonable_encoder(datos) 
    new_notificacion = await eliminar_control_temperatura(datos)
    if  new_notificacion:
        return ResponseModel(new_notificacion, "ok")
    else :
        return ErrorResponseModel("verifica tus datos", 404, "NO SE HA ENCONTRADO")

@router.post("/reestablecer", response_description="Datos Listados de los usuarios.")
async def reestablecer_control_temperatura_ok(datos: ConsultarSchema = Body(...)):
    datos = jsonable_encoder(datos) 
    new_notificacion = await reestablecer_control_temperatura(datos)
    if  new_notificacion:
        return ResponseModel(new_notificacion, "ok")
    else :
        return ErrorResponseModel("verifica tus datos", 404, "NO SE HA ENCONTRADO")
    
@router.post("/estadistica", response_description="Datos Listados de los usuarios.")
async def estadistica_control_temperatura_ok(datos: ConsultarSchema = Body(...)):
    datos = jsonable_encoder(datos) 
    new_notificacion = await estadistica_control_temperatura(datos)
    if  new_notificacion:
        return ResponseModel(new_notificacion, "ok")
    else :
        return ErrorResponseModel("verifica tus datos", 404, "NO SE HA ENCONTRADO")
    