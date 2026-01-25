from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

#aqui pedimos las funciones que incluyen nuestro CRUD
from server.functions.comando import (
    GuardarComandos,
    RetrieveComandos,
    ProcesarData,
    GuardarComandos_libre,
    GuardarComandos_super_libre,
    comando_jhon_vena,
    RetrieveComandos_test,
    RetrieveComandos_oficial,
    GuardarComandos_super_libre_supervisado,
    procesar_on_pabecsa,
    procesar_off_pabecsa,
    procesar_off_guardia_civil,
    procesar_on_guardia_civil,
    RetrieveComandos_oficial_dexterity,
    procesar_10_dex,
    procesar_25_dex,
    Control_tunel_joya,
    GuardarComandos_super_libretexas,
    GuardarComandos_libretexas,
    GuardarComandostexas,
    Control_texas,
    Control_texas_off,
)
#Aqui importamos el modelo necesario para la clase 
from server.models.comando import (
    ErrorResponseModel,
    ResponseModel,
    ComandoSchema,
    
)
router = APIRouter()

@router.get("/Control_texas_off/", response_description="Datos recuperados")
async def Control_texas_off_ok():
    notificacions = await Control_texas_off()
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta")


@router.get("/Control_texas/", response_description="Datos recuperados")
async def Control_texas_ok():
    notificacions = await Control_texas()
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta")


@router.get("/Control_tunel_joya/", response_description="Datos recuperados")
async def Control_tunel_joya_OK():
    notificacions = await Control_tunel_joya()
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta")

@router.post("/", response_description="Datos agregados a la base de datos.")
async def add_comando(datos: ComandoSchema = Body(...)):
    datos = jsonable_encoder(datos)   
    new_notificacion = await GuardarComandos(datos)
    print("*******")
    print(new_notificacion)
    print("*******")

    return new_notificacion
@router.post("/libre/", response_description="Datos agregados a la base de datos.")
async def add_comando_2(datos: ComandoSchema = Body(...)):
    datos = jsonable_encoder(datos)   
    new_notificacion = await GuardarComandos_libre(datos)
    print("*******")
    print(new_notificacion)
    print("*******")

    return new_notificacion

@router.post("/super_libre/", response_description="Datos agregados a la base de datos.")
async def add_comando_2(datos: ComandoSchema = Body(...)):
    datos = jsonable_encoder(datos)   
    new_notificacion = await GuardarComandos_super_libre(datos)
    print("*******")
    print(new_notificacion)
    print("*******")

    return new_notificacion



@router.post("/texas", response_description="Datos agregados a la base de datos.")
async def add_comandotexas(datos: ComandoSchema = Body(...)):
    datos = jsonable_encoder(datos)   
    new_notificacion = await GuardarComandostexas(datos)
    print("*******")
    print(new_notificacion)
    print("*******")

    return new_notificacion
@router.post("/libretexas/", response_description="Datos agregados a la base de datos.")
async def add_comando_2texas(datos: ComandoSchema = Body(...)):
    datos = jsonable_encoder(datos)   
    new_notificacion = await GuardarComandos_libretexas(datos)
    print("*******")
    print(new_notificacion)
    print("*******")

    return new_notificacion

@router.post("/super_libretexas/", response_description="Datos agregados a la base de datos.")
async def add_comando_2texas(datos: ComandoSchema = Body(...)):
    datos = jsonable_encoder(datos)   
    new_notificacion = await GuardarComandos_super_libretexas(datos)
    print("*******")
    print(new_notificacion)
    print("*******")

    return new_notificacion

@router.get("/buscar/testapi/{imei}", response_description="Datos recuperados")
async def get_comandos_testapi(imei:str):
    notificacions = await RetrieveComandos_test(imei)
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta")

@router.get("/buscar/oficial/{imei}", response_description="Datos recuperados")
async def get_comandos_oficial(imei:str):
    notificacions = await RetrieveComandos_oficial(imei)
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta")

@router.get("/buscar/oficial/dexterity/{imei}", response_description="Datos recuperados")
async def get_comandos_oficial(imei:str):
    notificacions = await RetrieveComandos_oficial_dexterity(imei)
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta")
@router.get("/buscar/{imei}", response_description="Datos recuperados")
async def get_comandos(imei:str):
    notificacions = await RetrieveComandos(imei)
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta")

#secuencua para homologara datos a la plataforma ztrack
@router.get("/Homologar/", response_description="Datos procesados")
async def procesar_comandos():
    notificacions = await ProcesarData()
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta xx")

#secuencua para homologara datos a la plataforma ztrack
@router.get("/Supervisar_Demonio/", response_description="Datos procesados")
async def supervisar():
    notificacions = await GuardarComandos_super_libre_supervisado()
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta xx")

#secuencua para homologara datos a la plataforma ztrack
@router.get("/JhonVena/{imei}", response_description="Datos procesados")
async def procesar_comandos_jhon_vena(imei:str):
    notificacions = await comando_jhon_vena(imei)
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta xx")


#secuencua para homologara datos a la plataforma ztrack
@router.get("/Pabesca_on/", response_description="Datos procesados")
async def procesar_on_comandos():
    notificacions = await procesar_on_pabecsa()
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta xx")

#secuencua para homologara datos a la plataforma ztrack
@router.get("/Pabesca_off/", response_description="Datos procesados")
async def procesar_off_comandos():
    notificacions = await procesar_off_pabecsa()
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta xx")

#secuencua para homologara datos a la plataforma ztrack
@router.get("/dex/cambio_10/", response_description="Datos procesados")
async def procesar_10_dex_ok():
    notificacions = await procesar_10_dex()
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta xx")

#secuencua para homologara datos a la plataforma ztrack
@router.get("/dex/cambio_25/", response_description="Datos procesados")
async def procesar_25_dex_ok():
    notificacions = await procesar_25_dex()
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta xx")


#secuencua para homologara datos a la plataforma ztrack
@router.get("/Guardia_on/", response_description="Datos procesados")
async def procesar_on_comandos_1():
    notificacions = await procesar_on_guardia_civil()
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta xx")

#secuencua para homologara datos a la plataforma ztrack
@router.get("/Guardia_off/", response_description="Datos procesados")
async def procesar_off_comandos_2():
    notificacions = await procesar_off_guardia_civil()
    if notificacions:
        return ResponseModel(notificacions, "Datos  recuperados exitosamente.")
    return ResponseModel(notificacions, "Lista vacía devuelta xx")





