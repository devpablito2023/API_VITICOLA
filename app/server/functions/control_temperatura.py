from server.database import estaditica_general,contador_general,validar_usuario,guardar_evento_telemetria,collection  ,log_general_collection,ids_collection
from datetime import datetime,timedelta
from server.functions.generales import  procesar_historico , procesar_log , filtrar_no_none , convertir_fecha_inicio , convertir_fecha_fin ,comparar_json,formato_texto_evento
#Estandar para funciones de agregar , editar , buscar , listar 
control_temperatura_collection = collection("control_temperatura")
h_control_temperatura_collection = collection("h_control_temperatura")

async def procesar_historico_log(mensaje,responsable,data,unico):
    proceso = procesar_historico(mensaje,responsable,data,unico)
    log = procesar_log(mensaje, responsable, unico)
    await h_control_temperatura_collection.insert_one(proceso)
    await log_general_collection.insert_one(log)

def validar_control(tipo,total,lista ,fin):
    if tipo not in [0, 1]:
        return "FAIL_TIPO"
    if len(lista)!=total :
        return "FAIL_TOTAL"
    #analizar lista
    if tipo==0 :
        if fin is None:
            return "FAIL_FIN"
        hora_fin = validar_fecha_control(fin)
        if hora_fin is None:
            return "FAIL_FIN_2"  
        # Validar cada hora de inicio de etapa
        lista_etapas = lista
        for i in range(len(lista_etapas) - 1):
            hora_inicio_etapa_actual = validar_fecha_control(lista_etapas[i]["hora_inicio_etapa"])
            hora_inicio_etapa_siguiente = validar_fecha_control(lista_etapas[i + 1]["hora_inicio_etapa"])  
            if hora_inicio_etapa_actual is None or hora_inicio_etapa_siguiente is None:
                return "FAIL_FECHA"    
            if hora_inicio_etapa_actual >= hora_inicio_etapa_siguiente:
                return f"FAIL_FECHA_ORDEN" 
        # Verificar que la hora de fin es mayor que todas las horas de inicio de etapa
        for etapa in lista_etapas:
            hora_inicio_etapa = validar_fecha_control(etapa["hora_inicio_etapa"])
            if hora_inicio_etapa and hora_inicio_etapa >= hora_fin:
                return "FAIL_FECHA_FIN"
    else :
        lista_etapas = lista
        for i in range(len(lista_etapas) - 1):
            hora_inicio_etapa_actual = validar_fecha_ciclo(lista_etapas[i]["hora_inicio_etapa"])
            hora_inicio_etapa_siguiente = validar_fecha_ciclo(lista_etapas[i + 1]["hora_inicio_etapa"])  
            if hora_inicio_etapa_actual is None or hora_inicio_etapa_siguiente is None:
                return "FAIL_FECHA"    
            if hora_inicio_etapa_actual >= hora_inicio_etapa_siguiente:
                return "FAIL_FECHA_ORDEN" 
    return None
def validar_fecha_control(fecha_str):
    try:
        # Intentamos convertir el string en un objeto datetime con el formato dado
        return datetime.strptime(fecha_str, "%d-%m-%Y_%H-%M")
    except ValueError:
        return None  # Si no puede ser convertido, retorna None

def validar_fecha_ciclo(fecha_str):
    try:
        # Intentamos convertir el string en un objeto datetime con el formato dado
        return datetime.strptime(fecha_str, "%H-%M")
    except ValueError:
        return None  # Si no puede ser convertido, retorna None

def validar_control_temperatura(datos):
    # Validar la fecha de fin
    hora_fin = validar_fecha_control(datos["hora_fin_control_temperatura"])
    if hora_fin is None:
        return "Hora fin control temperatura tiene un formato incorrecto"  
    # Validar cada hora de inicio de etapa
    lista_etapas = datos["lista_control_temperatura"]
    for i in range(len(lista_etapas) - 1):
        hora_inicio_etapa_actual = validar_fecha_control(lista_etapas[i]["hora_inicio_etapa"])
        hora_inicio_etapa_siguiente = validar_fecha_control(lista_etapas[i + 1]["hora_inicio_etapa"])  
        if hora_inicio_etapa_actual is None or hora_inicio_etapa_siguiente is None:
            return "FAIL_FECHA"    
        if hora_inicio_etapa_actual >= hora_inicio_etapa_siguiente:
            return f"FAIL_FECHA_ORDEN" 
    # Verificar que la hora de fin es mayor que todas las horas de inicio de etapa
    for etapa in lista_etapas:
        hora_inicio_etapa = validar_fecha_control(etapa["hora_inicio_etapa"])
        if hora_inicio_etapa and hora_inicio_etapa >= hora_fin:
            return "FAIL_FECHA_FIN"
    print("validacion corecta")

async def ocupado_control_temperatura(data): 
    especifico = await control_temperatura_collection.find_one({"imei_control_temperatura ":data,"estado_control_temperatura":1},{"_id":0 ,"id_control_temperatura ":1})
    return especifico   

async def cambiar_estado_control_temperatura(control_temperatura_data: dict, nuevo_estado: int, mensaje_log: str, estado_actual: int) -> str:

    if not control_temperatura_data.get('especifico'):
        return "SIN_ESPECIFICO"
    objeto = {"estado_control_temperatura": nuevo_estado,"user_m": control_temperatura_data['id_usuario'],"updated_at": datetime.now()}
    filtro = {"id_control_temperatura": control_temperatura_data['especifico'],"estado_control_temperatura": estado_actual}
    especifico = await control_temperatura_collection.find_one(filtro,{"_id": 0, "id_control_temperatura":1,"imei_control_temperatura": 1})
    if not especifico :
        return "FAIL"
    if estado_actual ==0 :
        if await ocupado_control_temperatura(especifico['imei_control_temperatura']):
            return "OCUPADO"

    validar_user = await validar_usuario(control_temperatura_data['id_usuario'])
    objeto['user_m_nombre']=validar_user
    await control_temperatura_collection.update_one(filtro, {"$set": objeto})
    await procesar_historico_log(mensaje_log, control_temperatura_data['id_usuario'], objeto, control_temperatura_data['especifico'])
    await contador_general("control_temperatura",(nuevo_estado+2))
    return "OK"

async def guardar_control_temperatura(control_temperatura_data: dict) -> dict:
    #token_data = await validar_token_y_extender(control_temperatura_data['token_ztrack'], control_temperatura_data['user_c'])
    #if not token_data:
        #return "TOKEN_INVALIDO"
    proyecto_ok ="FAIL"
    id_value = control_temperatura_data['id_control_temperatura'] if 'id_control_temperatura' in control_temperatura_data else 0
    #control_temperatura_data = {k: v for k, v in control_temperatura_data.items() if k not in ['token_ztrack']}
    validar_user = await validar_usuario(control_temperatura_data['user_c'])
    coincidencia_dato = await control_temperatura_collection.find_one({"proceso_control_temperatura":control_temperatura_data['proceso_control_temperatura'] ,"estado_control_temperatura":1},{"_id":0})
    if id_value==0 :
        if coincidencia_dato :
            proyecto_ok =  "DUPLICADO"
        else :
            existe_Activo = await control_temperatura_collection.find_one({"condicion_control_temperatura":1 ,"estado_control_temperatura":1},{"_id":0})
            if existe_Activo :
                return "FAIL_ACTIVO :"+str(existe_Activo['id_control_temperatura'])

            ids_proyectos = await ids_collection.find_one({"id_control_temperatura": {"$exists": True}})
            control_temperatura_data['created_at'] = datetime.now() 
            control_temperatura_data['user_c_nombre'] = validar_user  
            control_temperatura_data['id_control_temperatura'] = ids_proyectos['id_control_temperatura']+1 if ids_proyectos else 1
            #procesar datos para validar el control tipo_control ,total_control , lista_control ,hora_fin_control
            if control_temperatura_data['lista_control_temperatura'] ==[] :
                return "FAIL_LISTA"
            tema =  validar_control(control_temperatura_data['tipo_control_temperatura'] ,control_temperatura_data['total_control_temperatura'] ,control_temperatura_data['lista_control_temperatura'] ,control_temperatura_data['hora_fin_control_temperatura'] )
            if tema :
                return tema
            guardar_control_temperatura = await control_temperatura_collection.insert_one(control_temperatura_data)
            s_ids ={"id_control_temperatura":control_temperatura_data['id_control_temperatura'],"fecha":datetime.now()}
            procesar_ids = await ids_collection.update_one({"_id":ids_proyectos['_id'] },{"$set":s_ids}) if ids_proyectos else await ids_collection.insert_one(s_ids)
            proyecto_ok = await control_temperatura_collection.find_one({"_id": guardar_control_temperatura.inserted_id},{"_id":0,"id_control_temperatura":1,"imei_control_temperatura":1})
            await procesar_historico_log("control_temperatura se ha  GUARDADO",control_temperatura_data['user_c'],control_temperatura_data,control_temperatura_data['id_control_temperatura'])
            await contador_general("control_temperatura",1)
    else :  
        coincidencia_id = await control_temperatura_collection.find_one({"id_control_temperatura":control_temperatura_data['id_control_temperatura'] ,"estado_control_temperatura":1},{"_id":0})
        if coincidencia_id :
            if coincidencia_dato :
                if coincidencia_id['id_control_temperatura']!=coincidencia_dato['id_control_temperatura']:
                    return "DUPLICADO"
            #se actualiza la informacion 
            filter_proyecto = {k: v for k, v in control_temperatura_data.items() if k not in ['id_control_temperatura', 'estado_control_temperatura','user_m','user_c','updated_at','created_at']}
            filter_proyecto2 =filtrar_no_none(filter_proyecto)
            #datos encontrados al momento de querer modificar 
            json_comparado =comparar_json(coincidencia_id,filter_proyecto2)
            if(json_comparado):
                json_comparado['updated_at']=datetime.now()
                json_comparado['user_m']=control_temperatura_data['user_c']
                json_comparado['user_m_nombre']=validar_user
                actualizar_proyecto = await control_temperatura_collection.update_one({"id_control_temperatura": control_temperatura_data['id_control_temperatura'],"estado_control_temperatura":1},{"$set":json_comparado}) 
                await procesar_historico_log("control_temperatura se ha EDITADO",control_temperatura_data['user_c'],json_comparado,control_temperatura_data['imei_control_temperatura'])
                await contador_general("control_temperatura",4)
                proyecto_ok = {"id_control_temperatura":control_temperatura_data['id_control_temperatura'],"imei_control_temperatura ":control_temperatura_data['imei_control_temperatura']}
            else :
                proyecto_ok="SINCAMBIOS"
        else :
            proyecto_ok="FAIL"
    return proyecto_ok

def bd_gene(imei):
    fet =datetime.now()
    part = fet.strftime('_%m_%Y')
    colect ="TK_"+imei+part
    print(colect)
    return colect

def convertir_a_datetime(fecha_str):
    return datetime.strptime(fecha_str, "%d-%m-%Y_%H-%M")


async def enviar_comando_proceso(imei,temperatura):
    data_collection = collection(bd_gene("control"))

    #validar si ya existe un comando ,no enviar nada imprimir " ya existe comando pendiente"

    #sino existe validar ultimo dato si entre la ejecucion y la fecha actual ha pasado mas de 10 minutos , volver a intentar 
    fet =datetime.now()
    valor_general = {
        "imei": imei,
        "estado": 1,
        "comando": "1X04,UNIT111,HEAD:1&(0,"+str(temperatura)+",100)&,12341X02",
        "dispositivo": "FAIL",
        "evento": "automatic control, temperature : "+str(temperatura)+" C°",
        "user": "automatic",
        "receta": "sin receta automatic",
        "tipo": 7,
        "status": 1,
        "dato": temperatura,
        "id": 1300
    }
    valor_general['fecha_creacion'] = fet
    valor_general['fecha_ejecucion'] = fet

    #primero consultar si ya existe un comando pendiente 
    encontrado = await data_collection.find_one({"imei":valor_general['imei'],"estado":1},{"_id":0})
    if encontrado :
        print("ya existe comando pendiente")
    else :      
        #sino existe validar ultimo dato si entre la ejecucion y la fecha actual ha pasado mas de 10 minutos , volver a intentar 
        notificacions = []
        async for notificacion in data_collection.find({"imei":valor_general['imei']},{"_id":0 }).sort({"fecha_ejecucion":-1}).limit(1):  
            notificacions.append(notificacion)
        if notificacions[0]['fecha_ejecucion'] :
            diferencia =fet -notificacions[0]['fecha_ejecucion'] 
            if diferencia.total_seconds() > 10 * 60:
                notificacion = await data_collection.insert_one(valor_general)
                print("cambiando temperatura :")
                print(temperatura)
                return notificacion
            else :
                print("aun es pronto , espermos a mas datos ")


async def seguimiento_supervisado(): 
    # Consultar el control activo
    especifico = await control_temperatura_collection.find_one({
        "imei_control_temperatura": "868428046606400",
        "estado_control_temperatura": 1,
        "condicion_control_temperatura": 1
    }, {"_id": 0 })   

    if especifico:
        print("--------------Control especifico -------------")
        print(especifico)
        print("---------------Datos Actuales----------------")
        
        # Consultar último estado de los datos en vivo (TK_dis)
        data_ultimo = collection("868428046606400_OFICIAL_2025")
        notificacions = []
        async for notificacion in data_ultimo.find({}, {"_id": 0, "temp_supply_1": 1, "fecha": 1, "return_air": 1, "set_point": 1, "humidity_set_point": 1, "relative_humidity": 1, "power_state": 1}).sort({"fecha": -1}).limit(1):  
            notificacions.append(notificacion)
        
        print(notificacions[0])
        #hora_actual = notificacions[0]["fecha"]
        hora_actual_2 = notificacions[0]["fecha"]
        print("milisegundo")
        hora_actual = hora_actual_2.replace(microsecond=0)
        hora_inicio_primera_etapa = convertir_a_datetime(especifico["lista_control_temperatura"][0]["hora_inicio_etapa"])
        print("*******")
        print(hora_actual)
        print("********")
        print(hora_inicio_primera_etapa)
        print("********")

        # Evaluar si la hora actual está dentro de los rangos de cada etapa
        if hora_inicio_primera_etapa > hora_actual:
            print("Proceso de control aún no iniciado")
        else:
            # Recorrer todas las etapas
            for i in range(len(especifico["lista_control_temperatura"])):
                etapa = especifico["lista_control_temperatura"][i]
                print("aqui la etapa ----------")
                print(etapa)
                hora_inicio_etapa = convertir_a_datetime(etapa["hora_inicio_etapa"])

                # Definir la hora final de la etapa (para la última etapa, la hora final es la hora de fin del control)
                if i + 1 < len(especifico["lista_control_temperatura"]):
                    hora_fin_etapa = convertir_a_datetime(especifico["lista_control_temperatura"][i + 1]["hora_inicio_etapa"])
                else:
                    hora_fin_etapa = convertir_a_datetime(especifico["hora_fin_control_temperatura"])
                print("#################")
                print("hora inicio etapa :")
                print(hora_inicio_etapa)
                print("hora fin etapa :")
                print(hora_fin_etapa)
                print("HORA ACTUAL :")
                print(hora_actual)
                # Comprobar si la hora actual está dentro del rango de la etapa
                if hora_inicio_etapa <= hora_actual < hora_fin_etapa:
                    print("estoy en este rango")
                    temperatura_etapa = etapa["temperatura_etapa"]
                    # Verificar si el set_point del equipo coincide con la temperatura de la etapa
                    print("#################")
                    print("temperatura a controlar :")
                    print(temperatura_etapa)
                    print("#################")
                    if notificacions[0]["set_point"] != temperatura_etapa:
                        await enviar_comando_proceso(especifico["imei_control_temperatura"], temperatura_etapa)
                    else:
                        print("En control")
                    break  # Salir del ciclo si encontramos el rango actual

            # Si la hora actual está fuera de los rangos de las etapas, imprimir "Control fuera de rango"
            if hora_actual >= hora_fin_etapa:
                #actualizar condicion a completado
                #actualizar_proyecto = await control_temperatura_collection.update_one({"id_control_temperatura": control_temperatura_data['id_control_temperatura'],"estado_control_temperatura":1},{"$set":json_comparado}) 

                completado = await control_temperatura_collection.update_one({
                    "imei_control_temperatura": "868428046606400",
                    "estado_control_temperatura": 1,
                    "condicion_control_temperatura": 1
                }, {"$set":{"condicion_control_temperatura":2}})  
                print("Control fuera de rango")

    else:
        print("sin data")


async def seguimiento_supervisado_1(): 
    #consultar el control activo
    especifico = await control_temperatura_collection.find_one({"imei_control_temperatura": "868428046606400","estado_control_temperatura":1,"condicion_control_temperatura":1},{"_id":0 })   
    if especifico :
        print("--------------Control especifico -------------")
        print(especifico)
        print("---------------Datos Actuales----------------")
        #consultar ultimo estado de los datos en vivo TK_dis
        #data_dispositivo = collection(bd_gene("dispositivos"))
        #fecha_dispositivo = await data_dispositivo.find_one({"imei": "868428046606400","estado":1},{"_id":0 ,"ultimo_dato":1})   
        #print("ya estamos en marcha")
        #print(fecha_dispositivo["ultimo_dato"])
        #validamos el ultimo dato
        data_ultimo = collection("868428046606400_OFICIAL_2025")
        #ultima_data = await data_ultimo.find_one({"fecha":fecha_dispositivo["ultimo_dato"]},{"_id":0 })
        #ultima_data = await data_ultimo.find_one({},{"_id":0 }).sort({"fecha:-1"})
        notificacions = []
        async for notificacion in data_ultimo.find({},{"_id":0 ,"temp_supply_1":1,"fecha":1,"return_air":1,"set_point":1,"humidity_set_point":1,"relative_humidity":1,"power_state":1}).sort({"fecha":-1}).limit(1):  
            notificacions.append(notificacion)
        print(notificacions[0])
        hora_actual_2 = notificacions[0]["fecha"]
        print("milisegundo")
        hora_actual = hora_actual_2.replace(microsecond=0)
        hora_inicio_primera_etapa = convertir_a_datetime(especifico["lista_control_temperatura"][0]["hora_inicio_etapa"])
        print("*******")
        print(hora_actual)
        print("********")
        print(hora_inicio_primera_etapa)
        print("********")

        # Evaluar si la hora actual está dentro de los rangos de cada etapa
        if hora_inicio_primera_etapa > hora_actual:
            print("Proceso de control aún no iniciado")
        else :
            for i in range(len(especifico["lista_control_temperatura"])):
                etapa = especifico["lista_control_temperatura"][i]
                print("aqui la etapa ----------")
                print(etapa)
                hora_inicio_etapa = convertir_a_datetime(etapa["hora_inicio_etapa"])

                # Definir la hora final de la etapa (para la última etapa, la hora final es la hora de fin del control)
                if i + 1 < len(especifico["lista_control_temperatura"]):
                    hora_fin_etapa = convertir_a_datetime(especifico["lista_control_temperatura"][i + 1]["hora_inicio_etapa"])
                else:
                    hora_fin_etapa = convertir_a_datetime(especifico["hora_fin_control_temperatura"])
                # Comprobar si la hora actual está dentro del rango de la etapa
                if hora_inicio_etapa <= hora_actual < hora_fin_etapa:
                    temperatura_etapa = etapa["temperatura_etapa"]
                    # Verificar si el set_point del equipo coincide con la temperatura de la etapa
                    print("#################")
                    print("temperatura a controlar :")
                    print(temperatura_etapa)
                    print("#################")
                    print("hora incio etapa :")
                    print(hora_inicio_etapa)
                    print("hora fin etapa :")
                    print(hora_fin_etapa)
                    print("#################")
                    if notificacions[0]["set_point"] != temperatura_etapa:
                        #print(f"Debe cambiar a {temperatura_etapa}")

                        await enviar_comando_proceso(especifico["imei_control_temperatura"],temperatura_etapa)
                        #ordenar el cambio de temperatura 
                    else:
                        print("En control")
                    break  # Salir del ciclo si encontramos el rango actual

            # Si la hora actual es fuera del rango del proceso, imprimir "Control fuera de rango"
            if hora_actual >= hora_fin_etapa:
                print("Control fuera de rango")


    else :
        print("sin data")

    #comparar y ejecutar logica 

async def ver_control_temperatura(control_temperatura_data: dict) -> dict:
    if not control_temperatura_data.get('especifico'):
        return "SIN_ESPECIFICO"
    #realizar secuencia para ver informacion especifica 
    especifico = await control_temperatura_collection.find_one({"id_control_temperatura":control_temperatura_data['especifico'],"estado_control_temperatura":1},{"_id":0 })             
    await procesar_historico_log("Se solicito info de control_temperatura  ",control_temperatura_data['id_usuario'],{},control_temperatura_data['especifico'])
    return especifico

async def listar_control_temperatura(control_temperatura_data: dict) -> dict:
    notificacions = []
    fecha_inicio = convertir_fecha_inicio(control_temperatura_data['fecha_inicio']) if control_temperatura_data['fecha_inicio'] else datetime.now() - timedelta(days=30)
    fecha_fin = convertir_fecha_fin(control_temperatura_data['fecha_fin']) if control_temperatura_data['fecha_fin'] else datetime.now() 
    query = {"created_at": {"$gte": fecha_inicio, "$lte": fecha_fin}}
    if control_temperatura_data['tipo_usuario'] == 2:
        query["estado_control_temperatura"] = 1
    elif control_temperatura_data['tipo_usuario'] not in (1, 2):
        query["estado_control_temperatura"] = 1
        query["user_c"] = control_temperatura_data['id_usuario']
    campos ={"_id":0,"id_control_temperatura":1,"proceso_control_temperatura":1,"imei_control_temperatura":1,"total_control_temperatura":1,"estado_control_temperatura":1,"condicion_control_temperatura":1,
             "tipo_control_temperatura":1}   
    async for notificacion in control_temperatura_collection.find(query,campos).sort({"created_at":-1}):  
        notificacions.append(notificacion)
    res = {"fecha_inicio" :fecha_inicio,"fecha_fin" :fecha_fin ,"resultado" :notificacions}
    await procesar_historico_log("Se solicito listado de control_temperatura ",control_temperatura_data['id_usuario'],{},"TODOS")
    return res 

async def eliminar_control_temperatura(control_temperatura_data: dict) -> dict:
    return await cambiar_estado_control_temperatura(control_temperatura_data,nuevo_estado=0,
        mensaje_log="Se eliminó control_temperatura", estado_actual=1)
    
async def reestablecer_control_temperatura(control_temperatura_data: dict) -> dict:
    return await cambiar_estado_control_temperatura(control_temperatura_data,nuevo_estado=1,
        mensaje_log="Se reestableció control_temperatura",estado_actual=0)

async def disponible_control_temperatura(control_temperatura_id: int,operacion :int=1) :
    busqueda = {"id_control_temperatura":control_temperatura_id,"condicion_control_temperatura":1,"estado_control_temperatura":1} if operacion==1 else {"id_control_temperatura":control_temperatura_id,"estado_control_temperatura":1}
    result = await control_temperatura_collection.find_one(busqueda,{"_id":0 ,"imei_control_temperatura":1}) 
    if result:
        return  result['imei_control_temperatura']
    else :
        return None

async def listar_disponible_control_temperatura(control_temperatura_data: dict) -> dict:
    dato = control_temperatura_data['especifico']
    query = {"condicion_control_temperatura":1,"estado_control_temperatura":1}
    notificacions=[]
    async for notificacion in control_temperatura_collection.find(query,{"_id":0,"id_control_temperatura":1,"imei_control_temperatura":1}).sort({"created_at":-1}):  
        notificacions.append(notificacion)
    if dato !=0 :
        query = {"id_control_temperatura":dato,"estado_control_temperatura":1}
        extra = await control_temperatura_collection.find_one(query,{"_id":0,"id_control_temperatura":1,"imei_control_temperatura":1,"condicion_control_temperatura":1})
        print("???????")
        print(extra)
        if extra : 
            if extra["condicion_control_temperatura"]==2 :
                notificacions.append({"id_control_temperatura":extra['id_control_temperatura'],"imei_control_temperatura":extra['imei_control_temperatura']})
    return notificacions

async def actualizar_condicion_control_temperatura(control_temperatura_id: int, usuario: int, condicion: int, mensaje_log: str) -> str:
    objeto = {"condicion_control_temperatura": condicion,"user_m": usuario,"updated_at": datetime.now()}
    objeto['user_m_nombre']=await validar_usuario(usuario)
    await control_temperatura_collection.update_one({"id_control_temperatura": control_temperatura_id},{"$set": objeto})
    await procesar_historico_log(mensaje_log, usuario, objeto, control_temperatura_id)
    return "ok"

async def quitar_relacion_control_temperatura(control_temperatura_id: int,usuario :int) :
    return await actualizar_condicion_control_temperatura(control_temperatura_id, usuario, condicion=1,
        mensaje_log="Se pone DISPONIBLE el control_temperatura")

async def agregar_relacion_control_temperatura(control_temperatura_id: int,usuario :int) :
    return await actualizar_condicion_control_temperatura(control_temperatura_id, usuario, condicion=2,
        mensaje_log="Se pone ASIGNADO el control_temperatura")

async def estadistica_control_temperatura(control_temperatura_id: dict) -> dict:
    data =  await estaditica_general('control_temperatura')
    return data


