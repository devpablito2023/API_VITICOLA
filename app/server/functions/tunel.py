import json
from server.database import collection 
from bson import regex
from datetime import datetime,timedelta
import requests
from datetime import datetime,timedelta

def invertir_texto_en_par(text: str) -> str:
    pairs = [text[i:i+2] for i in range(0, len(text), 2)]
    inverted_text = ''.join(pairs[::-1]) 
    return inverted_text
def convert_number(valve: int, divisor: int) -> float:
    val = 0.0
    if 0x7FEF <= valve <= 0x7FFF:
        val = float(valve)
    else:
        if valve > 0x7FFF:
            valve = 0xFFFF - valve
            valve += 1
            val = float(valve)
            val = -val
        else:
            val = float(valve)
        val =val/divisor
    return float(val)
def encontrar_parte_en_texto(text: str, parte: str):
    text_lower = text.lower()
    sequence_lower = parte.lower()
    index = text_lower.find(sequence_lower)
    if index != -1:
        part1 = text[:index + len(parte)]
        part2 = text[index + len(parte):]
        return part2
    else:
        return None
def texto_error(number: int) -> str:
    return f"E{number:02}"
def cortar_texto_1B04(texto, marcador):
    indice = texto.find(marcador)
    if indice != -1:
        texto_cortado = texto[:indice]
    else:
        texto_cortado = texto  # Si no se encuentra, se devuelve igual
    cantidad_letras = len(texto_cortado)
    return [texto_cortado, cantidad_letras]
def procesar_trama(cadena_general ,etiquetas,divisores,lista_caracter,valor_json={}):
    current_position_set = 0
    for idx ,num_chars in enumerate(lista_caracter):
        substring = cadena_general[current_position_set:current_position_set + num_chars]    
        if num_chars==2 and len(substring)==2 :        
            transformado =int(substring, 16)
            if substring =='FF' or substring =='FE' :
                res_trans = texto_error(98)
            else :
                if idx ==3 :
                    transformado = (transformado >> 1) & 1
                res_trans = transformado
        elif num_chars==4 and len(substring)==4 :
            inverso =invertir_texto_en_par(substring)
            try:
                index_error = A_sensor_codigo.index(inverso)
            except ValueError:
                index_error = -1  
            if index_error == -1 :
                transformado = convert_number(int(invertir_texto_en_par(substring), 16),divisores[idx])
                res_trans = transformado if transformado or transformado ==0 else texto_error(98)
            else :
                res_trans = texto_error(index_error)
        elif num_chars==8 and len(substring)==8 :
            transformado = convert_number(int(invertir_texto_en_par(substring), 16),divisores[idx])
            res_trans = round(transformado/divisores[idx],1)
        else :
            res_trans = texto_error(99)   
        valor_json[etiquetas[idx]]=res_trans
        current_position_set += num_chars
    return valor_json

A_sensor_eventos = ["Sensor is initializing" ,"Value not applicable (N/A)" ,"Sensor error","Sensor open","Sensor short","Sensor above",
                    "Sensor below","Sensor no comm","Sensor warm up","This is the max value that a sensor can have","Values not represent a reading"]
A_sensor_codigo = ["7FFF","7FFE","7FFD","7FFC","7FFB","7FFA","7FF9","7FF8","7FF7","7FEF","7FF0"]
A_sensor_readout_dataset = ["temp_supply_1","temp_supply_2","return_air","evaporation_coil","condensation_coil","compress_coil_1","compress_coil_2",
                          "ambient_air","cargo_1_temp","cargo_2_temp","cargo_3_temp","cargo_4_temp","relative_humidity","avl","suction_pressure",
                          "discharge_pressure","line_voltage","line_frequency","consumption_ph_1","consumption_ph_2","consumption_ph_3","co2_reading",
                          "o2_reading","evaporator_speed","condenser_speed","battery_voltage","power_kwh","power_trip_reading","power_trip_duration",
                          "suction_temp","discharge_temp","supply_air_temp","return_air_temp","dl_battery_temp","dl_battery_charge","power_consumption",
                          "power_consumption_avg","suction_pressure_2","suction_temp_2"]
A_sensor_caracter = [4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,8,8,8,4,4,4,4,4,4,4,4,4,4]
A_sensor_divisor = [10,10,10,10,10,10,10,10,10,10,10,10,1,1,100,100,1,1,10,10,10,10,10,1,1,10,10,10,1,10,10,10,10,100,100,100,100,100,10]
A_sensor_readout_set = ["alarm_present","set_point","capacity_load","power_state","controlling_mode","humidity_control","humidity_set_point",
                          "fresh_air_ex_mode","fresh_air_ex_rate","fresh_air_ex_delay","set_point_o2","set_point_co2","defrost_term_temp","defrost_interval","water_cooled_conde",
                          "usda_trip","evaporator_exp_valve","suction_mod_valve","hot_gas_valve","economizer_valve"]
A_sensor_caracter_set = [4,4,4,2,2,2,2,2,4,4,4,4,4,2,2,2,2,2,2,2]
A_sensor_divisor_set = [1,100,1,1,1,1,1,1,1,10,10,10,100,1,1,1,1,1,1,1]
A_sensor_readout_alarma = ["numero_alarma","alarma_01","alarma_02","alarma_03","alarma_04","alarma_05","alarma_06","alarma_07","alarma_08","alarma_09","alarma_10"]
A_sensor_caracter_alarma = [4,4,4,4,4,4,4,4,4,4,4]
A_sensor_divisor_alarma =  [1,1,1,1,1,1,1,1,1,1,1]
palabras = ["82A700","82A701","82A706"]
#conjunto de etiquetas 
conjunto_etiquetas = A_sensor_readout_dataset + A_sensor_readout_set + A_sensor_readout_alarma 
def procesar_texto_h(trama,data={}):
    #50.0,2.0,5.0,31.5,51.5
    #50.0,2.0,5.0,30.8,52.5,00001
    #0,  ,1  ,2  ,3   ,4   ,5
    lista=[]
    set_p=None
    suply=None
    humedad=None
    conju = None
    heter = None
    evap_baja = None
    evap_alta = None
    if trama : 
        #lista = [float(x) for x in trama.split(",")]
        lista = trama.split(',')
        set_p =  lista[0]
        suply =  lista[4]
        humedad =  lista[3]
        conju = lista[5]
        print("+++++++")
        print(conju)
        print("+++++++")
        if conju : 
            heter = 1 if '1' in conju[:3] else 0
            print(conju)
            evap_baja = conju[3]
            evap_alta = conju[4]

    print ("debe entraar aqui para val")
    json_val = {
        "temp_supply_1": float(suply),
        "temp_supply_2": "E99",
        "return_air": float(suply),
        "evaporation_coil": "E99",
        "condensation_coil": "E99",
        "compress_coil_1": "E99",
        "compress_coil_2": "E99",
        "ambient_air": "E99",
        "cargo_1_temp": float(heter),
        "cargo_2_temp": float(evap_baja),
        "cargo_3_temp": float(evap_alta),
        "cargo_4_temp": "E01",
        "relative_humidity": humedad,
        "avl": "E01",
        "suction_pressure": "E01",
        "discharge_pressure": "E01",
        "line_voltage": "E99",
        "line_frequency": "E07",
        "consumption_ph_1": "E99",
        "consumption_ph_2": "E99",
        "consumption_ph_3": "E99",
        "co2_reading": "E01",
        "o2_reading": "E01",
        "evaporator_speed": "E99",
        "condenser_speed": "E99",
        "battery_voltage": "E99",
        "power_kwh": "E99",
        "power_trip_reading": "E99",
        "power_trip_duration": "E99",
        "suction_temp": "E01",
        "discharge_temp": "E01",
        "supply_air_temp": "E99",
        "return_air_temp": "E99",
        "dl_battery_temp": "E99",
        "dl_battery_charge": "E99",
        "power_consumption": "E99",
        "power_consumption_avg": "E99",
        "suction_pressure_2": "E01",
        "suction_temp_2": "E00",
        "alarm_present": "E99",
        "set_point": float(set_p),
        "capacity_load": "E00",
        "power_state": 1,
        "controlling_mode": "E99",
        "humidity_control": "E99",
        "humidity_set_point": "E98",
        "fresh_air_ex_mode": "E99",
        "fresh_air_ex_rate": "E01",
        "fresh_air_ex_delay": "E01",
        "set_point_o2": "E01",
        "set_point_co2": "E01",
        "defrost_term_temp": "E99",
        "defrost_interval": "E99",
        "water_cooled_conde": "E99",
        "usda_trip": "E99",
        "evaporator_exp_valve": "E98",
        "suction_mod_valve": "E98",
        "hot_gas_valve": "E98",
        "economizer_valve": "E98"

    }
    return json_val
def procesar_texto(texto,palabras,data={}):
    data_json={}
    if texto :
        for index ,palabra in enumerate(palabras):
            posicion = texto.find(palabra)  # Buscar posición inicial
            if posicion != -1:
                cadena_ok = encontrar_parte_en_texto(texto,palabras[index])
                cadena_ok =cortar_texto_1B04(cadena_ok,"1B04FF") 
                print("////////")
                print(cadena_ok)
                print("////////")

                cadena_ok =cadena_ok[0]
                if index== 0 :
                    data_json = procesar_trama(cadena_ok ,A_sensor_readout_dataset,A_sensor_divisor,A_sensor_caracter,data)
                elif index== 1 :
                    data_json = procesar_trama(cadena_ok ,A_sensor_readout_set,A_sensor_divisor_set,A_sensor_caracter_set,data)
                elif index== 2 :
                    data_json = procesar_trama(cadena_ok ,A_sensor_readout_alarma,A_sensor_divisor_alarma,A_sensor_caracter_alarma,data)
                else :
                    data_json = procesar_trama("" ,[],[],[],data)
            else :
                data_json = procesar_trama("" ,[],[],[],data)
    return data_json
def proceso_general_termo_king(data):
    total ={}
    print(data)
    for key, value in data.items():
        if key.startswith("d") and key[1:].isdigit():
            print("jete")
            if value :
                resultado_json = procesar_texto(value,palabras,total)
                total =resultado_json
        if key=="val" :
            print("****pasamos detectamos val **** ")
            if value :
                resultado_json = procesar_texto_h(value,total)
                total =resultado_json
    #d07, iPowerDO_Compresor, iPowerDO_EvaporatorH, iPowerDO_EvaporatorL, iPowerDO_Condenser ,etileno
    #d07 "0,0,0,0" ,-1 (es -1 el etileno si no hay lectura ) "0,0,0,0,-1" 
    #d03 "UNIT555,ZGRU0000005"
    #d03 ID TUNEL , codigo del controlador 
    #aqui agregamos lo que falta 
    total['imei']=data['i']
    total['ip']=data['ip']
    #total['device']=data['d01']
    total['fecha']=data['fecha']
    #procesar_d07 = cadena_general_d07(data['d07'])
    #total['iPowerDO_Compresor'] =procesar_d07[0]
    #total['iPowerDO_EvaporatorH'] = procesar_d07[1]
    #total['iPowerDO_EvaporatorL'] =procesar_d07[2]
    #total['iPowerDO_Condenser'] =procesar_d07[3]
    #total['ietileno'] = procesar_d07[4]
    #procesar_d03 = cadena_general_d03(data['d03'])
    #total['UNIT']=procesar_d03[0]
    #total['device']=procesar_d03[1]
    #if procesar_d03[0] :
        #total['imei']=data['i']+"_"+procesar_d03[0]

    return total


def cadena_general_d07(cadena ,total_indices=5):
    if cadena is None :
        cadena ="0,0,0,0,-1"
    valores = list(map(float, cadena.split(",")))
    valores_completos = valores + [-1] * (total_indices - len(valores))
    return valores_completos
def cadena_general_d03(cadena ,total_indices=3):
    if cadena is None :
        cadena ="NA,NA"
    valores = cadena.split(",")
    valores_completos = valores + ["NA"] * (total_indices - len(valores))
    return valores_completos
def bd_gene(imei):
    fet =datetime.now()
    part = fet.strftime('_%m_%Y')
    colect ="TUNEL_"+imei+part
    return colect

def bd_gene_fecha(imei,fecha):
    colect ="TUNEL"+imei+"_"+fecha
    return colect

def bd_oficial(imei="",periodo=""):
    per_actual = datetime.now().year
    bd_ok ="TUNEL_OFICIAL_"+str(per_actual)
    #if imei!=""and periodo!="" :
    if imei!="" :
        bd_ok = str(imei)+"_"+bd_ok
    return bd_ok

def col_conforme(imei,unidad):
    conforme = str(imei)+"_"+str(unidad)+"_OK"
    conforme_collection =collection(bd_gene(conforme))
    return conforme_collection


def obtener_meses_entre_fechas(fecha_inicial_str, fecha_final_str):
    res=[]
    formato = "%d-%m-%Y_%H-%M-%S"
    ahora = datetime.now()
    if fecha_inicial_str == "0":
        fecha_inicial = ahora - timedelta(hours=12)
    else:
        try:
            fecha_inicial = datetime.strptime(fecha_inicial_str, formato)
        except ValueError:
            return res
    # Procesar fecha final
    if fecha_final_str == "0":
        fecha_final = ahora
    else:
        try:
            fecha_final = datetime.strptime(fecha_final_str, formato)
        except ValueError:
            return res
    if fecha_final <= fecha_inicial:
        return res
    # Generar lista de meses entre las fechas
    meses = []
    anio = fecha_inicial.year
    mes = fecha_inicial.month
    while anio < fecha_final.year or (anio == fecha_final.year and mes <= fecha_final.month):
        meses.append(f"{mes:02d}_{anio}")
        mes += 1
        if mes > 12:
            mes = 1
            anio += 1
    res=[fecha_inicial,fecha_final,meses]
    return res
def generar_filtros_por_mes(meses, fecha_inicial, fecha_final):
    filtros = []
    for i, mes_anio in enumerate(meses):
        mes, anio = map(int, mes_anio.split('_'))
        inicio_mes = datetime(anio, mes, 1)
        if mes == 12:
            fin_mes = datetime(anio + 1, 1, 1) - timedelta(seconds=1)
        else:
            fin_mes = datetime(anio, mes + 1, 1) - timedelta(seconds=1)
        if len(meses) == 1:
            filtros.append({"fecha": {"$gte": fecha_inicial,"$lte": fecha_final}})
        else:
            if i == 0:
                filtros.append({"fecha": {"$gte": fecha_inicial,"$lte": fin_mes}})
            elif i == len(meses) - 1:
                filtros.append({"fecha": {"$gte": inicio_mes,"$lte": fecha_final}})
            else:
                filtros.append({"fecha": {"$gte": inicio_mes,"$lte": fin_mes}})
    return filtros
 

#async def Guardar_Datos_3(ztrack_data: Union[TunelSchema, dict]) -> str:
async def Guardar_Datos_3(ztrack_data) -> str:

    # 1) Asegurar dict
    if isinstance(ztrack_data, TunelSchema):
        ztrack_data = ztrack_data.model_dump()
    elif not isinstance(ztrack_data, dict):
        raise TypeError(f"Tipo no soportado: {type(ztrack_data)}")

    # 2) Setear fecha
    ztrack_data["fecha"] = datetime.now()

    comando = "sin comandos pendientes"
    Hay_dispositivo = ""

    data_collection = collection(bd_gene(ztrack_data["i"]))
    dispositivos_collection = collection(bd_gene("dispositivos"))
    control_collection = collection(bd_gene("control"))

    # Guardar trama
    notificacion = await data_collection.insert_one(ztrack_data)

    # (opcional) si no lo usas, puedes quitarlo para ahorrar consulta
    # new_notificacion = await data_collection.find_one({"_id": notificacion.inserted_id}, {"_id": 0})

    # Verificar dispositivo
    dispositivo_encontrado = await dispositivos_collection.find_one(
        {"imei": ztrack_data["i"], "estado": 1},
        {"_id": 0}
    )

    if dispositivo_encontrado:
        Hay_dispositivo = dispositivo_encontrado.get("imei", "")

    # Upsert (evita insertar duplicados si hay carreras)
    if Hay_dispositivo:
        await dispositivos_collection.update_one(
            {"imei": ztrack_data["i"], "estado": 1},
            {"$set": {"ultimo_dato": datetime.now()}}
        )
    else:
        await dispositivos_collection.insert_one(
            {"imei": ztrack_data["i"], "estado": 1, "fecha": datetime.now(), "tipo": "TermoKing"}
        )

    # Control / comandos
    control_encontrado = await control_collection.find_one(
        {"imei": ztrack_data["i"], "estado": 1},
        {"_id": 0}
    )

    if control_encontrado and control_encontrado.get("comando"):
        comando = control_encontrado["comando"]

        # Evitar negativos
        veces_control = max(int(control_encontrado.get("estado", 1)) - 1, 0)

        await control_collection.update_one(
            {"imei": ztrack_data["i"], "estado": 1},
            {"$set": {"estado": veces_control, "estatus": 2, "fecha_ejecucion": datetime.now()}}
        )

    return comando

async def Guardar_Datos(ztrack_data: dict) -> dict:
    ztrack_data['fecha'] = datetime.now()  
    comando ="sin comandos pendientes"
    Hay_dispositivo=""
    data_collection = collection(bd_gene(ztrack_data['i']))
    #COLECCION PARA TODOS LOS DISPOSITIVOS
    dispositivos_collection = collection(bd_gene('dispositivos'))
    #COLECCION ESPECIFICA PARA EL CONTROL
    control_collection = collection(bd_gene("control"))
    #AQUI SE GUARDA LA TRAMA 
    notificacion = await data_collection.insert_one(ztrack_data)
    new_notificacion = await data_collection.find_one({"_id": notificacion.inserted_id},{"_id":0})
    #Verificar que exista el dispositivo en el registro
    dispositivo_encontrado = await dispositivos_collection.find_one({"imei": ztrack_data['i'],"estado":1},{"_id":0})
    if dispositivo_encontrado is not None:
        try:
            Hay_dispositivo= dispositivo_encontrado['imei'] 
            print("Elemento encontrado")
        except ValueError:
            print("NO SE ENCONTRO CONTROL")
    verificar_dispositivo = await dispositivos_collection.update_one({"imei": ztrack_data['i'],"estado":1},{"$set":{"ultimo_dato":datetime.now() }}) if Hay_dispositivo else await dispositivos_collection.insert_one({"imei":ztrack_data['i'],"estado":1,"fecha":datetime.now()  ,"tipo":"TermoKing"})
    control_encontrado =    await control_collection.find_one({"imei": ztrack_data['i'],"estado":1},{"_id":0})
    if control_encontrado :
        veces_control = control_encontrado['estado']-1 if control_encontrado['comando'] else 0
        comando = control_encontrado['comando']
        actualizar_comando = await control_collection.update_one({"imei": ztrack_data['i'],"estado":1},{"$set":{"estado": veces_control,"estatus":2,"fecha_ejecucion":datetime.now() }})
    return comando


async def Procesar_Trama() :
    #pedir todo los dispositivos afiliados 
    data_general_collection = collection(bd_gene("dispositivos"))
    notificacions=[]
    secuencia_notificacions = []

    async for notificacion in data_general_collection.find({"estado":1},{"_id":0}).sort({"created_at":-1}):
        notificacion_procesos = []

        #if(notificacion['imei'] =="860389053949943") :
            #procesar cerro prieto a aparte coleccion #cerro_prieto
            #procesar la telemetra en hexa 

            #proceso_general_termo_king
            #procesar la trama 

        notificacions.append(notificacion)
        proceso_collection = collection(bd_gene("proceso"))
        imei =  notificacion['imei'] if notificacion['imei'] else None
        print(imei)
        print("********")
        print(notificacion['imei'])
        if imei :
            print("oliii")
            base_collection = collection(bd_gene(notificacion['imei']))
            conforme =str(notificacion['imei'])+"_OK"
            conforme_collection = collection(bd_gene(conforme))

            coincidencia_dato = await proceso_collection.find_one({"imei":notificacion['imei'] ,"estado":1},{"_id":0})
            #se entinede que no hay datos buscamos todo
            if coincidencia_dato :
                fecha_ok =coincidencia_dato['hasta']
                async for notificacion_proceso in base_collection.find({"i":notificacion['imei'] ,"estado":1, "fecha": {"$gt": coincidencia_dato["hasta"]}},{"_id":0}).sort({"fecha":-1}):
                    fecha_ok=notificacion_proceso['fecha']
                    notificacion_procesos.append(notificacion_proceso)
                    validado_json = proceso_general_termo_king(notificacion_proceso)
    
                    ok_conforme = conforme_collection.insert_one(validado_json)

                    #actualizar a cada proceso a estado 2
                    estado_dos = base_collection.update_one({"fecha":notificacion_proceso['fecha']},{"$set":{"estado":2}})
                ok_proceso = proceso_collection.update_one({"imei":notificacion['imei']},{"$set":{"hasta":fecha_ok}})
            else :
                fecha_ok =None
                async for notificacion_proceso in base_collection.find({"i":notificacion['imei'] ,"estado":1},{"_id":0}).sort({"fecha":-1}):
                    fecha_ok=notificacion_proceso['fecha']
                    notificacion_procesos.append(notificacion_proceso)
                    validado_json = proceso_general_termo_king(notificacion_proceso)
                    ok_conforme = conforme_collection.insert_one(validado_json)
                    #actualizar a cada proceso a estado 2
                    estado_dos = base_collection.update_one({"fecha":notificacion_proceso['fecha']},{"$set":{"estado":2}})
                #procesar y actualizar proceso 
                proceso_json = {"imei":notificacion['imei'],"estado":1,"fecha_inicio":datetime.now(),"hasta":fecha_ok}
                ok_proceso = proceso_collection.insert_one(proceso_json)
        secuencia_notificacions.append(notificacion_procesos)
    res = {
        "general" : notificacions ,
        "secuencia" : secuencia_notificacions
    }
    return res

async def Procesar_Trama_ex_tunel() :
    #pedir todo los dispositivos afiliados 
    data_general_collection = collection(bd_gene("dispositivos"))
    notificacions=[]
    res ={}
    async for notificacion in data_general_collection.find({"estado":1},{"_id":0}).sort({"created_at":-1}):
        notificacions.append(notificacion)
        proceso_collection = collection(bd_gene("proceso"))
        imei =  notificacion['imei'] if notificacion['imei'] else None
        print(imei)
        print("********")
        print(notificacion['imei'])
        if imei :
            print("oliii")
            base_collection = collection(bd_gene(notificacion['imei']))
            #se entinede que no hay datos buscamos todo
            lista_tunel = ["UNIT111","UNIT222","UNIT333","UNIT444","UNIT555"]
            #    {"d03": {"$regex": "UNIT111"}} # buscar coincidencia el UNIT111 en el campo UNIT111
            fecha_ok = None
            cadena_fecha=[]
            for unidad in lista_tunel:
                filtro_base = {"i": notificacion['imei'],"estado": 1}
                filtro_base["d03"] = {"$regex": unidad}
                novo = str(notificacion['imei'])+"_"+unidad
                coincidencia_dato = await proceso_collection.find_one({"imei":novo,"estado":1},{"_id":0})
                if coincidencia_dato: # Agrega el filtro de fecha solo si hay coincidencia previa
                    filtro_base["fecha"] = {"$gt": coincidencia_dato["hasta"]}
                cursor = base_collection.find(filtro_base, {"_id": 0}).sort("fecha", -1)
                async for notificacion_proceso in cursor:
                    fecha_ok = notificacion_proceso['fecha']
                    validado_json = proceso_general_termo_king(notificacion_proceso)
                    conforme_collection = col_conforme(notificacion['imei'], unidad)
                    await conforme_collection.insert_one(validado_json)
                    await base_collection.update_one({"fecha": notificacion_proceso['fecha']},{"$set": {"estado": 2}})
                    cadena_fecha.append(fecha_ok)
                #esparcimos los dispositivos en la colleccion procesos
                if fecha_ok :
                    if coincidencia_dato:
                        await proceso_collection.update_one({"imei": novo},{"$set": {"hasta": fecha_ok}})
                    else:
                        proceso_json = {"imei": novo,"estado": 1,"fecha_inicio": datetime.now(),"hasta": fecha_ok}
                        await proceso_collection.insert_one(proceso_json)
        res = {
            "general" : imei ,
            "evento" : "Procesado correctamente",
            "fechas" :cadena_fecha
        }
    return res

async def test(): 
    if coincidencia_dato :
        for unidad in lista_tunel:
            async for notificacion_proceso in base_collection.find({"i":notificacion['imei'] ,"d03": {"$regex": unidad},"estado":1, "fecha": {"$gt": coincidencia_dato["hasta"]}},{"_id":0}).sort({"fecha":-1}):
                fecha_ok=notificacion_proceso['fecha']
                validado_json = proceso_general_termo_king(notificacion_proceso)
                conforme_collection = col_conforme(notificacion['imei'],unidad)
                ok_conforme = conforme_collection.insert_one(validado_json)
                estado_dos = base_collection.update_one({"fecha":notificacion_proceso['fecha']},{"$set":{"estado":2}})
        ok_proceso = proceso_collection.update_one({"imei":notificacion['imei']},{"$set":{"hasta":fecha_ok}})
    else :
        fecha_ok =None
        for unidad in lista_tunel:
            async for notificacion_proceso in base_collection.find({"i":notificacion['imei'] ,"d03": {"$regex": unidad},"estado":1},{"_id":0}).sort({"fecha":-1}):
                fecha_ok=notificacion_proceso['fecha']
                validado_json = proceso_general_termo_king(notificacion_proceso)
                conforme_collection = col_conforme(notificacion['imei'],unidad)
                ok_conforme = conforme_collection.insert_one(validado_json)
                estado_dos = base_collection.update_one({"fecha":notificacion_proceso['fecha']},{"$set":{"estado":2}})
        proceso_json = {"imei":notificacion['imei'],"estado":1,"fecha_inicio":datetime.now(),"hasta":fecha_ok}
        ok_proceso = proceso_collection.insert_one(proceso_json)
    res = {
        "general" : imei ,
        "evento" : "Procesado correctamente"
    }
    return "oli"
    
async def buscar_imei(ztrack_data: dict) -> dict:
    conforme =str(ztrack_data['imei'])+"OK"
    conforme_collection = collection(bd_gene(conforme))

    imei =ztrack_data['imei']
    notificacion_procesos=[]
    async for notificacion_proceso in conforme_collection.find({"imei":ztrack_data['imei'] },{"_id":0,"set_point":1,
        "temp_supply_1":1,"return_air":1,"evaporation_coil":1,"ambient_air":1,"cargo_1_temp":1,"cargo_2_temp":1,"cargo_3_temp":1,
        "cargo_4_temp":1,"relative_humidity":1,"avl":1,"line_voltage":1,"co2_reading":1,"power_state":1,"set_point_co2":1,
        "ip":1,"device":1,"fecha":1}).sort({"fecha":-1}).limit(10):
        fecha_ok=notificacion_proceso['fecha']
        notificacion_procesos.append(notificacion_proceso)
    return notificacion_procesos

async def buscar_live(ztrack_data: dict) -> dict:
    conforme =str(ztrack_data['imei'])+"OK"
    conforme_collection = collection(bd_gene(conforme))
    res ="FAIL"
    imei =ztrack_data['imei']
    notificacion_procesos=[]
    async for notificacion_proceso in conforme_collection.find({"imei":ztrack_data['imei'] },{"_id":0,"set_point":1,
        "temp_supply_1":1,"return_air":1,"evaporation_coil":1,"ambient_air":1,"capacity_load":1,"cargo_1_temp":1,"cargo_2_temp":1,"cargo_3_temp":1,
        "cargo_4_temp":1,"relative_humidity":1,"co2_reading":1,"condensation_coil":1,"compress_coil_1":1,"avl":1,"line_voltage":1,"line_frequency":1,
        "consumption_ph_1":1,"consumption_ph_2":1,"consumption_ph_3":1,"evaporator_speed":1,"condenser_speed":1,"power_kwh":1,"alarm_present":1,
        "power_state":1,"device":1,"fecha":1}).sort({"fecha":-1}).limit(1):
        fecha_ok=notificacion_proceso['fecha']
        notificacion_procesos.append(notificacion_proceso)
    if notificacion_procesos[0]:
        res=notificacion_procesos[0]
    return res

#insertar comandos al imei 
async def insertar_comando(ztrack_data: dict) -> dict:
    control_collection = collection(bd_gene("control"))
    #capturar la hora 
    ztrack_data['fecha_creacion'] = datetime.now()
    #AQUI SE GUARDA LA TRAMA 
    notificacion = await control_collection.insert_one(ztrack_data)
    if notificacion :
        res = ztrack_data['imei']
    else :
        res="ERROR"
    return res

async def datos_totales(ztrack_data: dict) -> dict:
    conforme =str(ztrack_data['imei'])+"OK"
    conforme_collection = collection(bd_gene(conforme))

    imei =ztrack_data['imei']
    notificacion_procesos=[]
    async for notificacion_proceso in conforme_collection.find({"imei":ztrack_data['imei'] },{"_id":0,"set_point":1,
        "temp_supply_1":1,"return_air":1,"evaporation_coil":1,"ambient_air":1,"capacity_load":1,"cargo_1_temp":1,"cargo_2_temp":1,"cargo_3_temp":1,
        "cargo_4_temp":1,"relative_humidity":1,"co2_reading":1,"condensation_coil":1,"compress_coil_1":1,"avl":1,"line_voltage":1,"line_frequency":1,
        "consumption_ph_1":1,"consumption_ph_2":1,"consumption_ph_3":1,"evaporator_speed":1,"condenser_speed":1,"power_kwh":1,"alarm_present":1,
        "power_state":1,"device":1,"fecha":1}).sort({"fecha":-1}).limit(200):
        fecha_ok=notificacion_proceso['fecha']
        notificacion_procesos.append(notificacion_proceso)
    return notificacion_procesos


async def datos_totales_ok(ztrack_data: dict) -> dict:
    meses = obtener_meses_entre_fechas(ztrack_data['fecha_inicio'],ztrack_data['fecha_fin'])
    if len(meses)!=3 :
        return []
    else :
        filtros = generar_filtros_por_mes(meses[2],meses[0],meses[1])
        #mes_ok =meses[2]
        #mes_ok_2 =mes_ok[::-1]
        #print("----****")
        #print(mes_ok)
        #print("----****")
        #print(mes_ok_2)
        #print("----****")
        meses[2]=meses[2][::-1]
        filtros =filtros[::-1]
        if len(filtros)==0 :
            return [1]
        else :
            #print("***********")
            #print(filtros)
            #print("***********")
            notificacion_procesos=[]
            for index,f in enumerate(filtros):
                conforme =str(ztrack_data['imei'])+"OK"
                #print("-----------------")
                #print(meses[2])
                #print(f)
                #print(index)
                #print(meses[2][0])
                #print("-----------------")

                conforme_collection = collection(bd_gene_fecha(conforme,meses[2][index]))
                async for notificacion_proceso in conforme_collection.find(f,{"_id":0,"set_point":1,
                    "temp_supply_1":1,"return_air":1,"evaporation_coil":1,"ambient_air":1,"capacity_load":1,"cargo_1_temp":1,"cargo_2_temp":1,"cargo_3_temp":1,
                    "cargo_4_temp":1,"relative_humidity":1,"co2_reading":1,"condensation_coil":1,"compress_coil_1":1,"avl":1,"line_voltage":1,"line_frequency":1,
                    "consumption_ph_1":1,"consumption_ph_2":1,"consumption_ph_3":1,"evaporator_speed":1,"condenser_speed":1,"power_kwh":1,"alarm_present":1,
                    "power_state":1,"device":1,"fecha":1}).sort({"fecha":-1}):
                    #fecha_ok=notificacion_proceso['fecha']
                    notificacion_procesos.append(notificacion_proceso)
                    #notificacion_procesos.insert(0, notificacion_proceso)  # inserta al inicio
                #notificacion_procesos=notificacion_procesos.reverse()
            return notificacion_procesos

async def grafica_total_ok(ztrack_data: dict) -> dict:
    meses = obtener_meses_entre_fechas(ztrack_data['fecha_inicio'],ztrack_data['fecha_fin'])
    if len(meses)!=3 :
        return []
    else :
        filtros = generar_filtros_por_mes(meses[2],meses[0],meses[1])
        meses[2]=meses[2][::-1]
        filtros =filtros[::-1]
        if len(filtros)==0 :
            return [1]
        else :
            print("***********")
            print(filtros)
            print("***********")
            notificacion_procesos=[]
            set_point=[]
            temp_supply_1=[]
            return_air=[]
            evaporation_coil=[]
            ambient_air=[]
            capacity_load=[]
            cargo_1_temp=[]
            cargo_2_temp=[]
            cargo_3_temp=[]
            cargo_4_temp=[]
            relative_humidity=[]
            co2_reading=[]
            power_state=[]
            fecha =[]
            for index,f in enumerate(filtros):
                conforme =str(ztrack_data['imei'])+"OK"
                print("-----------------")
                print(meses[2])
                print(f)
                print(index)
                print(meses[2][index])
                print("-----------------")
                conforme_collection = collection(bd_gene_fecha(conforme,meses[2][index]))
                async for notificacion_proceso in conforme_collection.find(f,{"_id":0,"set_point":1,
                    "temp_supply_1":1,"return_air":1,"evaporation_coil":1,"ambient_air":1,"capacity_load":1,"cargo_1_temp":1,"cargo_2_temp":1,"cargo_3_temp":1,
                    "cargo_4_temp":1,"relative_humidity":1,"co2_reading":1,"power_state":1,"fecha":1}).sort({"fecha":-1}):
                    if notificacion_proceso.get('set_point', 0) and notificacion_proceso.get('temp_supply_1', 0) :
                        set_point.insert(0,procesar_temp(notificacion_proceso['set_point']))
                        temp_supply_1.insert(0,procesar_temp(notificacion_proceso['temp_supply_1']))
                        return_air.insert(0,procesar_temp(notificacion_proceso['return_air']))
                        evaporation_coil.insert(0,procesar_temp(notificacion_proceso['evaporation_coil']))
                        ambient_air.insert(0,procesar_temp(notificacion_proceso['ambient_air']))
                        capacity_load.insert(0,procesar_por(notificacion_proceso['capacity_load']))
                        cargo_1_temp.insert(0,procesar_temp(notificacion_proceso['cargo_1_temp']))
                        cargo_2_temp.insert(0,procesar_temp(notificacion_proceso['cargo_2_temp']))
                        cargo_3_temp.insert(0,procesar_temp(notificacion_proceso['cargo_3_temp']))
                        cargo_4_temp.insert(0,procesar_temp(notificacion_proceso['cargo_4_temp']))
                        relative_humidity.insert(0,procesar_por(notificacion_proceso['relative_humidity']))
                        co2_reading.insert(0,procesar_co2(notificacion_proceso['co2_reading']))
                        power_state.insert(0,notificacion_proceso['power_state'])
                        fecha.insert(0, notificacion_proceso['fecha'])

            notificacion_procesos={
                "set_point":set_point ,
                "temp_supply_1": temp_supply_1,
                "return_air": return_air,
                "evaporation_coil":evaporation_coil ,
                "ambient_air": ambient_air,
                "capacity_load":capacity_load ,
                "cargo_1_temp": cargo_1_temp,
                "cargo_2_temp": cargo_2_temp,
                "cargo_3_temp": cargo_3_temp,
                "cargo_4_temp": cargo_4_temp,
                "relative_humidity": relative_humidity,
                "co2_reading": co2_reading,
                "power_state": power_state,
                "fecha":fecha

            }
        return notificacion_procesos

def procesar_por(dato):
    try :
        dato =float(dato)
        if dato>0 and dato<101 :
            res = dato
        else:
            res =None
    except :
        res =None
    return res

def procesar_co2(dato):
    try :
        dato =float(dato)
        if dato>0 and dato<20 :
            res = dato
        else:
            res =None
    except :
        res =None
    return res

def procesar_temp(dato):
    if isinstance(dato, str):
        try:
            dato = float(dato)
        except ValueError:
            return None
    else:
        try:
            dato = float(dato)
        except (ValueError, TypeError):
            return None
    if -50 < dato < 130:
        return dato
    else:
        return None
    
def procesar_dato(dato, min_temp, max_temp=1000000000):
    try:
        min_temp = float(min_temp)
        max_temp = float(max_temp)
        dato = float(dato)
    except (ValueError, TypeError):
        return None
    if min_temp <= dato <= max_temp:
        return dato
    else:
        return None

def validar_valor(json_data, clave,min_temp, max_temp=1000000000):

    valor = json_data.get(clave, None)
    if valor is None:
        return None
    try:
        min_temp = float(min_temp)
        max_temp = float(max_temp)
        valor = float(valor)
    except (ValueError, TypeError):
        return None
    if min_temp <= valor <= max_temp:
        return valor
    else:
        return None

def detectar_errores(etiquetas, datos):
    resultado = {}
    for etiqueta in etiquetas:
        valor = datos.get(etiqueta)
        if isinstance(valor, str) and "E" in valor:
            resultado[etiqueta] = valor
        elif valor is None:
            resultado[etiqueta] = "E100"
    return resultado
def estructura_termoking(json_validar):
    json_ok = {
        "temp_supply_1": validar_valor(json_validar,"temp_supply_1",-50,130),
        "temp_supply_2": validar_valor(json_validar,"temp_supply_2",-50,130),
        "return_air": validar_valor(json_validar,"return_air",-50,130),
        "evaporation_coil": validar_valor(json_validar,"evaporation_coil",-50,130),
        "condensation_coil": validar_valor(json_validar,"condensation_coil",-50,130),
        "compress_coil_1": validar_valor(json_validar,"compress_coil_1",-50,130),
        "compress_coil_2": validar_valor(json_validar,"compress_coil_2",-50,130),
        "ambient_air": validar_valor(json_validar,"ambient_air",-50,130),
        "cargo_1_temp": validar_valor(json_validar,"cargo_1_temp",-50,130),
        "cargo_2_temp": validar_valor(json_validar,"cargo_2_temp",-50,130),
        "cargo_3_temp": validar_valor(json_validar,"cargo_3_temp",-50,130),
        "cargo_4_temp": validar_valor(json_validar,"cargo_4_temp",-50,130),
        "relative_humidity": validar_valor(json_validar,"relative_humidity",0,100),
        "avl": validar_valor(json_validar,"avl",0,250),
        "suction_pressure": validar_valor(json_validar,"suction_pressure",-0),
        "discharge_pressure": validar_valor(json_validar,"discharge_pressure",0),
        "line_voltage": validar_valor(json_validar,"line_voltage",0,500),
        "line_frequency": validar_valor(json_validar,"line_frequency",0,100),
        "consumption_ph_1": validar_valor(json_validar,"consumption_ph_1",0,100),
        "consumption_ph_2": validar_valor(json_validar,"consumption_ph_2",0,100),
        "consumption_ph_3": validar_valor(json_validar,"consumption_ph_3",0,100),
        "co2_reading": validar_valor(json_validar,"co2_reading",0,22),
        "o2_reading": validar_valor(json_validar,"o2_reading",0,22),
        "evaporator_speed": validar_valor(json_validar,"evaporator_speed",-50,130),
        "condenser_speed": validar_valor(json_validar,"condenser_speed",-50,130),
        "battery_voltage": validar_valor(json_validar,"battery_voltage",-50,130),
        "power_kwh":validar_valor(json_validar,"power_kwh",0),
        "power_trip_reading": validar_valor(json_validar,"power_trip_reading",0),
        "power_trip_duration": validar_valor(json_validar,"power_trip_duration",0),
        "suction_temp": validar_valor(json_validar,"suction_temp",-50,130),
        "discharge_temp": validar_valor(json_validar,"discharge_temp",-50,130),
        "supply_air_temp": validar_valor(json_validar,"supply_air_temp",-50,130),
        "return_air_temp": validar_valor(json_validar,"return_air_temp",-50,130),
        "dl_battery_temp": validar_valor(json_validar,"dl_battery_temp",-50,130),
        "dl_battery_charge": validar_valor(json_validar,"dl_battery_charge",0),
        "power_consumption": validar_valor(json_validar,"power_consumption",0),
        "power_consumption_avg": validar_valor(json_validar,"power_consumption_avg",0),
        "suction_pressure_2": validar_valor(json_validar,"suction_pressure_2",0),
        "suction_temp_2": validar_valor(json_validar,"suction_temp_2",-50,130),
        "alarm_present": validar_valor(json_validar,"alarm_present",0,100),
        "set_point": validar_valor(json_validar,"set_point",-50,130),
        "capacity_load": validar_valor(json_validar,"capacity_load",0,100),
        "power_state": validar_valor(json_validar,"power_state",0,2),
        "controlling_mode": validar_valor(json_validar,"controlling_mode",0,10),
        "humidity_control": validar_valor(json_validar,"humidity_control",0,2),
        "humidity_set_point": validar_valor(json_validar,"humidity_set_point",0,100),
        "fresh_air_ex_mode": validar_valor(json_validar,"fresh_air_ex_mode",0,3),
        "fresh_air_ex_rate": validar_valor(json_validar,"fresh_air_ex_rate",0),
        "fresh_air_ex_delay": validar_valor(json_validar,"fresh_air_ex_delay",0),
        "set_point_o2": validar_valor(json_validar,"set_point_o2",0,22),
        "set_point_co2": validar_valor(json_validar,"set_point_co2",0,22),
        "defrost_term_temp": validar_valor(json_validar,"defrost_term_temp",-50,130),
        "defrost_interval": validar_valor(json_validar,"defrost_interval",0,24),
        "water_cooled_conde": validar_valor(json_validar,"water_cooled_conde",0,2),
        "usda_trip": validar_valor(json_validar,"usda_trip",0,2),
        "evaporator_exp_valve": validar_valor(json_validar,"evaporator_exp_valve",0),
        "suction_mod_valve": validar_valor(json_validar,"suction_mod_valve",0),
        "hot_gas_valve": validar_valor(json_validar,"hot_gas_valve",0),
        "economizer_valve": validar_valor(json_validar,"economizer_valve",0),
        #agregar alarmas :
        "numero_alarma": validar_valor(json_validar,"numero_alarma",0,11),
        "alarma_01": validar_valor(json_validar,"alarma_01",0,300),
        "alarma_02": validar_valor(json_validar,"alarma_02",0,300),
        "alarma_03": validar_valor(json_validar,"alarma_03",0,300),
        "alarma_04": validar_valor(json_validar,"alarma_04",0,300),
        "alarma_05": validar_valor(json_validar,"alarma_05",0,300),
        "alarma_06": validar_valor(json_validar,"alarma_06",0,300),
        "alarma_07": validar_valor(json_validar,"alarma_07",0,300),
        "alarma_08": validar_valor(json_validar,"alarma_08",0,300),
        "alarma_09": validar_valor(json_validar,"alarma_09",0,300),
        "alarma_10": validar_valor(json_validar,"alarma_10",0,300),
        "lecturas_erradas" : detectar_errores(conjunto_etiquetas,json_validar),
        "imei": json_validar['imei'],
        "ip": json_validar['ip'],
        #"device": json_validar['device'],
    
        #"iPowerDO_Compresor": json_validar['iPowerDO_Compresor'],
        #"iPowerDO_EvaporatorH": json_validar['iPowerDO_EvaporatorH'],
        #"iPowerDO_EvaporatorL": json_validar['iPowerDO_EvaporatorL'],
        #"iPowerDO_Condenser": json_validar['iPowerDO_Condenser'],
        #"ietileno": json_validar['ietileno'],
        #"UNIT": json_validar['UNIT'],

        "fecha": json_validar['fecha']

    }
    return json_ok

def json_serial(obj):
    """Función para serializar datetime"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Tipo no serializable")


def decision_datos_extunel(datos, rango_dias ,ultimo_guardado=None ):
    if rango_dias <= 6:
        intervalo = 3  # minutos
        tolerancia = 0.05  # 5%
    elif 6 < rango_dias <= 24:
        intervalo = 20  # minutos
        tolerancia = 0.15  # 15%
    else:
        intervalo = 60  # minutos
        tolerancia = 0.30  # 30%
    resultado = []
    urll ="http://161.132.206.104:9050/Madurador/TestIntegracion/"
    headers = {
    'Content-Type': 'application/json'
    }

    #print("------------")
    #print("---ESQUEMA---")
    #print(rango_dias)
    #print(intervalo)
    #print(tolerancia)
    #print("---FIN ESQUEMA ---")
    for d in datos:
        d=estructura_termoking(d)
        #print("*****dato a procesar ----------")
        #print(d)
        if ultimo_guardado is None  :
            if d['set_point']is None  and d['return_air']is None :
                ultimo_guardado = None
            else :
                ultimo_guardado={
                    "set_point" :d['set_point'],
                    "return_air" :d['return_air'],
                    "temp_supply_1" :d['temp_supply_1'],
                    "evaporation_coil" :d['evaporation_coil'],
                    "set_point" :d['set_point'],
                    "fecha":d["fecha"]        
                }
                #resultado.append(d['fecha'])

                # Mapeo de IMEIs a telemetria_id
                telemetria_map = {
                    #"TUNEL001_UNIT111": 15600,
                    #"TUNEL001_UNIT222": 15601,
                    #"TUNEL001_UNIT333": 15602,
                    #"TUNEL001_UNIT444": 15603,
                    #"TUNEL001_UNIT555": 15604
                    "868428042470744": 15766,
                    "867856034810421": 15767,
                    "868428041473624": 15769,
                    "866262036779949": 15770,
                    "866782049606565": 15772,
                    "867372055881051": 15774,
                    "868428047321157" : 15570, #tunel la joya 
                    "867856038562796" : 15784,
                    "865691037001658" : 15785,
                    "868428040551750" : 15786,
                    "868428041677810" : 15806, #la joya USDAS
                                                "18:FE:34:D8:B0:BC" : 4530, #1
                    "48:55:19:DF:D7:7B" : 4531, #2
                    "48:55:19:E0:29:A9" : 4532, #3
                    "48:55:19:E0:58:98" : 4529, #5
                    "84:F3:EB:83:43:84" : 4533, #4

                    "866782048661041": 15775         
                }
                imei = d.get('imei')
                if imei in telemetria_map:
                    d['created_at'] = d['fecha']
                    d['ethylene'] = d['ietileno']
                    d['telemetria_id'] = telemetria_map[imei]
                    # Campos estándar a dejar en None
                    for campo in ['sp_ethyleno', 'stateProcess', 'inyeccion_pwm', 'longitud', 'latitud']:
                        d[campo] = None
                    x1 = d.copy()
                    x1['created_at'] = x1['created_at'].isoformat()
                    x1['fecha'] = x1['fecha'].isoformat()
                    response = requests.post(urll, headers=headers, json=x1)
                    print(response.status_code)
                    print(response.text)

                resultado.append(d)
        else :
            if ultimo_guardado['set_point'] and ultimo_guardado['return_air']:
                #print("wwwwwwwwwwwwwwwwwwww")
                #print(d['set_point'])
                #print(d['return_air'])
                #print("wwwwwwwwwwwwwwwwwwww")

                if  d['set_point']is None  or  d['return_air']is None :
                    ultimo_guardado = ultimo_guardado
                else :
                    print("SDDDdddddddddddddd")
                    print(ultimo_guardado["fecha"])
                    tiempo_diferencia = (d["fecha"] - ultimo_guardado["fecha"]).total_seconds() / 60
                    if d['temp_supply_1']is None  or  d['evaporation_coil']is None  or ultimo_guardado['evaporation_coil']is None  or ultimo_guardado['temp_supply_1']is None :
                        variacion = max(
                            abs(d["set_point"] - ultimo_guardado["set_point"]) / abs(ultimo_guardado["set_point"] or 1),
                            abs(d["return_air"] - ultimo_guardado["return_air"]) / abs(ultimo_guardado["return_air"] or 1),
                        )
                    else :
                        variacion = max(
                            abs(d["set_point"] - ultimo_guardado["set_point"]) / abs(ultimo_guardado["set_point"] or 1),
                            abs(d["return_air"] - ultimo_guardado["return_air"]) / abs(ultimo_guardado["return_air"] or 1),
                            abs(d["temp_supply_1"] - ultimo_guardado["temp_supply_1"]) / abs(ultimo_guardado["temp_supply_1"] or 1),
                            abs(d["evaporation_coil"] - ultimo_guardado["evaporation_coil"]) / abs(ultimo_guardado["evaporation_coil"] or 1),
                        )

                    if tiempo_diferencia >= intervalo or variacion > tolerancia:
                        #resultado.append(d['fecha'])
                        # Mapeo de IMEIs a telemetria_id
                        telemetria_map = {
                            #"TUNEL001_UNIT111": 15600,
                            #"TUNEL001_UNIT222": 15601,
                            #"TUNEL001_UNIT333": 15602,
                            #"TUNEL001_UNIT444": 15603,
                            #"TUNEL001_UNIT555": 15604
                            "868428042470744": 15766,
                            "867856034810421": 15767,
                            "868428041473624": 15769,
                            "866262036779949": 15770,
                            "866782049606565": 15772,
                            "867372055881051": 15774,
                            "868428047321157" : 15570, #tunel la joya 
                            "867856038562796" : 15784,
                            "865691037001658" : 15785,
                            "868428040551750" : 15786,
                            "868428041677810" : 15806, #la joya USDAS

                            "18:FE:34:D8:B0:BC" : 4530, #1
                            "48:55:19:DF:D7:7B" : 4531, #2
                            "48:55:19:E0:29:A9" : 4532, #3
                            "48:55:19:E0:58:98" : 4529, #5
                            "84:F3:EB:83:43:84" : 4533, #4
                        
                            "866782048661041": 15775 
                        }
                        imei = d.get('imei')
                        if imei in telemetria_map:
                            d['created_at'] = d['fecha']
                            d['ethylene'] = d['ietileno']
                            d['telemetria_id'] = telemetria_map[imei]
                            # Campos estándar a dejar en None
                            for campo in ['sp_ethyleno', 'stateProcess', 'inyeccion_pwm', 'longitud', 'latitud']:
                                d[campo] = None
                            x1 = d.copy()
                            x1['created_at'] = x1['created_at'].isoformat()
                            x1['fecha'] = x1['fecha'].isoformat()
                            response = requests.post(urll, headers=headers, json=x1)
                            print(response.status_code)
                            print(response.text)

                        resultado.append(d)
                        d1 = {
                            "set_point":d["set_point"],
                            "return_air":d["return_air"],
                            "temp_supply_1":d["temp_supply_1"],
                            "evaporation_coil":d["evaporation_coil"],
                            "fecha":d["fecha"]
                        }
                        ultimo_guardado = d1
                        print(ultimo_guardado)

    #print("******proceso acumulacion de resultados  ************")
    #print(resultado)
    #print("******proceso ultimo guardado ************")
    #print(ultimo_guardado)
    return [resultado,ultimo_guardado]



def decision_datos(datos, rango_dias, ultimo_guardado=None):
    # Definir el intervalo y tolerancia según el rango de días
    if rango_dias <= 6:
        intervalo = 5  # minutos
        tolerancia = 0.05  # 5%
    elif 6 < rango_dias <= 24:
        intervalo = 20  # minutos
        tolerancia = 0.15  # 15%
    else:
        intervalo = 60  # minutosACCIONES /FUNCIONES 
        tolerancia = 0.30  # 30%

    resultado = []
    urll = "http://161.132.206.104:9050/Madurador/TestIntegracion/"
    url2 = "http://161.132.206.104:9010/contenedores/actualizar_data"
    headers = {'Content-Type': 'application/json'}
    headers_3 = {'Content-Type': 'application/json'}

    imei_to_telemetria = {
        "868428042470744": 15766, #ok
        "867856034810421": 15767, #ok
        "868428041473624": 15769,
        "866262036779949": 15770, #ok
        "866782049606565": 15772, #ok
        "867372055881051": 15774, #ok
        "868428047321157" : 15570, #tunel la joya #ok
        "867856038562796" : 15784,#ok
        "865691037001658" : 15785, #ok
        "868428040551750" : 15786,
        "868428041677810" : 15806, #la joya USDAS #ok
        "866262034327402": 15849,
        "865674030073288": 15850,
        "863576046886862": 15851,
        "863576049740900": 15852,
        "868428044595035" :15853,
        "863576043599872" :15854,
        "868428040662979" :15855,
        "867856037799423": 15856,
        "866262035841310": 15857,
        "866262034780196": 15858,
        "867856037754477": 15859,

        "18:FE:34:D8:B0:BC" : 4530, #1
        "48:55:19:DF:D7:7B" : 4531, #2
        "48:55:19:E0:29:A9" : 4532, #3
        "48:55:19:E0:58:98" : 4529, #5
        "84:F3:EB:83:43:84" : 4533, #4

        "866782048661041": 15775   
    }

    # Recorrer los datos
    for d in datos:
        d = estructura_termoking(d)

        # Verificar si el imei está en el diccionario y enviar los datos
        if d['imei'] in imei_to_telemetria:
            #data.setdefault("ethylene", None) 
            #data.setdefault("inyeccion_hora", None) 
            #data.setdefault("sp_ethyleno", None) 
            #data.setdefault("stateProcess", None)  
            # Asignar valores iniciales
            print("jojojojo")
            print(d)
            print("jojojojo")
            d['created_at'] = d['fecha']
            d['telemetria_id'] = imei_to_telemetria[d['imei']]
            if "sp_ethyleno" in d and d["sp_ethyleno"] is not None:
                d["sp_ethyleno"]=d["sp_ethyleno"]
            else:
                d["sp_ethyleno"]=None
            if "stateProcess" in d and d["stateProcess"] is not None:
                d["stateProcess"]=d["stateProcess"]
            else:
                d["stateProcess"]=None
            if "inyeccion_hora" in d and d["inyeccion_hora"] is not None:
                d["inyeccion_hora"]=d["inyeccion_hora"]
            else:
                d["inyeccion_hora"]=None
            if "ethylene" in d and d["ethylene"] is not None:
                d["ethylene"]=d["ethylene"]
            else:
                d["ethylene"]=None

            #d["sp_ethyleno"] = d.get("sp_ethyleno") if d.get("sp_ethyleno") is not None else None
            #d["stateProcess"] = d.get("stateProcess") if d.get("stateProcess") is not None else None
            #d["inyeccion_hora"] = d.get("inyeccion_hora") if d.get("inyeccion_hora") is not None else None
            #d["ethylene"] = d.get("ethylene") if d.get("ethylene") is not None else None

            d['inyeccion_pwm'] = None
            d['longitud'] = None
            d['latitud'] = None


            # Generar el JSON a enviar
            json_ok_ok = {
                "ultima_fecha": d['created_at'].strftime("%Y-%m-%d %H:%M:%S"),
                "fecha": d['created_at'].strftime("%Y-%m-%d %H:%M:%S"),
                "set_point": d['set_point'],
                "return_air": d['return_air'],
                "temp_supply_1": d['temp_supply_1'],
                "ambient_air": d['ambient_air'],
                "relative_humidity": d['relative_humidity'],
                "avl": d['avl'],
                "inyeccion_pwm": d['inyeccion_pwm'],
                "inyeccion_hora": d['inyeccion_hora'],
                "ethylene": d['ethylene'],
                "set_point_co2": d['set_point_co2'],
                "co2_reading": d['co2_reading'],
                "humidity_set_point": d['humidity_set_point'],
                "sp_ethyleno": d['sp_ethyleno'],
                "compress_coil_1": d['compress_coil_1'],
                "power_state": d['power_state'],
                "evaporation_coil": d['evaporation_coil'],
                "controlling_mode": d['controlling_mode'],
                "stateProcess": d['stateProcess'],
                "cargo_1_temp": d['cargo_1_temp'],
                "cargo_2_temp": d['cargo_2_temp'],
                "cargo_3_temp": d['cargo_3_temp'],
                "cargo_4_temp": d['cargo_4_temp'],
                "o2_reading": d['o2_reading'],
                "set_point_o2": d['set_point_o2'],

                "condensation_coil": d['condensation_coil'],
                "consumption_ph_1": d['consumption_ph_1'],
                "consumption_ph_2": d['consumption_ph_2'],
                "consumption_ph_3": d['consumption_ph_3'],
                "evaporator_speed": d['evaporator_speed'],
                "capacity_load": d['capacity_load'],
                "humidity_control": d['humidity_control'],
                "condenser_speed": d['condenser_speed'],
                "line_frequency": d['line_frequency'],
                "line_voltage": d['line_voltage'],

                "fresh_air_ex_mode": d['fresh_air_ex_mode'],
                "telemetria_id": d['telemetria_id']
            }

            if d['telemetria_id'] ==157130 :
                json_ok_ok['set_point'] =-0.5
                #generar_numero(0, -20, 15)
                json_ok_ok['return_air'] =generar_numero(0.4, -20, 20)
                json_ok_ok['temp_supply_1'] =generar_numero(-0.5, -30, 20)
                json_ok_ok['ambient_air'] =generar_numero(25, -100, 200)
                json_ok_ok['relative_humidity'] =generar_numero(87, -100, 200)
                json_ok_ok['power_state'] =1
                json_ok_ok['cargo_1_temp'] =generar_numero(0, -16, 16)
                json_ok_ok['cargo_2_temp'] =generar_numero(0, -16, 16)
                json_ok_ok['cargo_3_temp'] =generar_numero(0.2, -16, 16)

                d['set_point']=json_ok_ok['set_point'] 
                d['return_air']=json_ok_ok['return_air'] 
                d['temp_supply_1']=json_ok_ok['temp_supply_1'] 
                d['ambient_air']=json_ok_ok['ambient_air']
                d['relative_humidity']=json_ok_ok['relative_humidity']
                d['power_state']=json_ok_ok['power_state'] 
                d['cargo_1_temp']=json_ok_ok['cargo_1_temp']
                d['cargo_2_temp']=json_ok_ok['cargo_2_temp']
                d['cargo_3_temp']=json_ok_ok['cargo_3_temp'] 
            


                    








            # Enviar los datos a la URL2
            response1 = requests.post(url2, headers=headers_3, json=json_ok_ok)
            print(response1.status_code)
            print(response1.text)

            # Si hay que enviar también a otro URL, lo hacemos aquí
            response2 = enviar_datos(d, urll, headers)
            print(response2.status_code)

        # El último guardado se actualiza de acuerdo a los valores
        if ultimo_guardado is None:
            if d['set_point'] is None and d['return_air'] is None:
                ultimo_guardado = None
            else:
                ultimo_guardado = {
                    "set_point": d['set_point'],
                    "return_air": d['return_air'],
                    "temp_supply_1": d['temp_supply_1'],
                    "evaporation_coil": d['evaporation_coil'],
                    "fecha": d["fecha"]
                }

                if d['imei'] in imei_to_telemetria:
                    x1 = d.copy()
                    print("jejejejejeje")
                    print(x1)
                    print("jejejejejeje")

                    x1['created_at'] = x1['created_at'].isoformat()
                    x1['fecha'] = x1['fecha'].isoformat()
                    response = requests.post(urll, headers=headers, json=x1)
                    print(response.status_code)
                    print(response.text)

                resultado.append(d)

    return [resultado, ultimo_guardado]

def enviar_datos(d, url, headers):
    """Envía los datos del dispositivo a la URL especificada."""
    x1 = d.copy()
    x1['created_at'] = x1['created_at'].isoformat()
    x1['fecha'] = x1['fecha'].isoformat()
    response = requests.post(url, headers=headers, json=x1)
    print(f"Enviado {x1['imei']}: {response.status_code} - {response.text}")
    return response



async def procesar_data_termoking():
    #validamos los procesos realizados el mes actual 
    data_proceso_actual = collection(bd_gene("proceso"))
    xl = bd_oficial()
    print(xl)
    colleccion_oficial = collection(bd_oficial())

    datos_5 = None
    datos_20 = None
    datos_60 = None
    hasta =None
    async for notificacion in data_proceso_actual.find({"estado":1},{"_id":0}).sort({"fecha_inicio":-1}):
        #consultar a coleccion oficial si existe el imei en cuestion 
        data_oficial = await colleccion_oficial.find_one({"imei":notificacion['imei'] ,"estado":1},{"_id":0})
        mensaje = "imei oficial " if data_oficial else "no hay imei en oficial"
        print(">>>>>>>>>>>>>>>>>>>>>>>>")
        print(data_oficial)
        print(">>>>>>>>>>>>>>>>>>>>>>>>")
        if data_oficial :
            datos_5 =data_oficial.get('datos_5', None)
            datos_20 =data_oficial.get('datos_20', None)
            datos_60 =data_oficial.get('datos_60', None)
            hasta =data_oficial.get('hasta', None)
        consulta_oficial = { "fecha": {"$gt": data_oficial["hasta"]}} if hasta else {}
        data_procesar = []
        base_procesar =str(notificacion['imei'])+"_OK"
        procesar_collection = collection(bd_gene(base_procesar))
        cont =0
        valor_hasta =hasta
        #guardardamos en base de datos cada 5 minutos
        bd_5_minutos= collection(bd_oficial(notificacion['imei']))
        async for notificacion_ok in procesar_collection.find(consulta_oficial,{"_id":0}).sort({"fecha":1}).limit(50):
            data_procesar.append(notificacion_ok)
            hasta =notificacion_ok['fecha']
            cont+=1
        #validamos la solicitud de informacion :
        print("++++ cantidad de datos agregados")
        print(cont)
        print("*****")
        print(notificacion['imei'])
        validar_60 = decision_datos(data_procesar,3,datos_60)
        #print(validar_60[0])
        #print("+++++++++")
        #print(validar_60[1])
        #print("+++++++++++++++++")
        #print(hasta)
        #insertar varios a la base de datos 
        if validar_60[0] :
            many_5 = await bd_5_minutos.insert_many(validar_60[0])
            # TELEMTRIA ID 15575

        if data_oficial :
            #si hay datos se entiende que se actualiza 
            estructura_data_oficial = {
                "ultimo_proceso" : datetime.now(),
                "data_5": validar_60[1],
                "hasta":hasta
            }
            oficial_ok = await colleccion_oficial.update_one({"imei":notificacion['imei']},{"$set":estructura_data_oficial})
        else :
            #sino existe se crea de cero
            estructura_data_oficial = {
                "imei": notificacion['imei'],
                "proceso_inicio" : datetime.now(),
                "data_5": validar_60[1],
                "estado":1,
                "hasta":hasta
            }
            oficial_ok = await colleccion_oficial.insert_one(estructura_data_oficial)
        hasta =None
        
        #solicitar informacion a procesar 
    return 0


async def grafica_total(ztrack_data: dict) -> dict:
    conforme =str(ztrack_data['imei'])+"OK"
    conforme_collection = collection(bd_gene(conforme))
    imei =ztrack_data['imei']
    notificacion_procesos=[]
    set_point=[]
    temp_supply_1=[]
    return_air=[]
    evaporation_coil=[]
    ambient_air=[]
    capacity_load=[]
    cargo_1_temp=[]
    cargo_2_temp=[]
    cargo_3_temp=[]
    cargo_4_temp=[]
    relative_humidity=[]
    co2_reading=[]
    power_state=[]
    fecha =[]
    async for notificacion_proceso in conforme_collection.find({"imei":ztrack_data['imei'] },{"_id":0,"set_point":1,
        "temp_supply_1":1,"return_air":1,"evaporation_coil":1,"ambient_air":1,"capacity_load":1,"cargo_1_temp":1,"cargo_2_temp":1,"cargo_3_temp":1,
        "cargo_4_temp":1,"relative_humidity":1,"co2_reading":1,"power_state":1,"fecha":1}).sort({"fecha":-1}).limit(200):
        #fecha_ok=notificacion_proceso['fecha']
        #notificacion_procesos.append(notificacion_proceso)
        #print(notificacion_proceso)
        #print("----okoko")
        #if notificacion_proceso['set_point'] :
        if notificacion_proceso.get('set_point', 0) and notificacion_proceso.get('temp_supply_1', 0) :
            set_point.append(procesar_temp(notificacion_proceso['set_point']))
            temp_supply_1.append(procesar_temp(notificacion_proceso['temp_supply_1']))
            return_air.append(procesar_temp(notificacion_proceso['return_air']))
            evaporation_coil.append(procesar_temp(notificacion_proceso['evaporation_coil']))
            ambient_air.append(procesar_temp(notificacion_proceso['ambient_air']))
            capacity_load.append(procesar_por(notificacion_proceso['capacity_load']))
            cargo_1_temp.append(procesar_temp(notificacion_proceso['cargo_1_temp']))
            cargo_2_temp.append(procesar_temp(notificacion_proceso['cargo_2_temp']))
            cargo_3_temp.append(procesar_temp(notificacion_proceso['cargo_3_temp']))
            cargo_4_temp.append(procesar_temp(notificacion_proceso['cargo_4_temp']))
            relative_humidity.append(procesar_por(notificacion_proceso['relative_humidity']))
            co2_reading.append(procesar_co2(notificacion_proceso['co2_reading']))
            power_state.append(notificacion_proceso['power_state'])
            fecha.append(notificacion_proceso['fecha'])
    notificacion_procesos={
        "set_point":set_point ,
        "temp_supply_1": temp_supply_1,
        "return_air": return_air,
        "evaporation_coil":evaporation_coil ,
        "ambient_air": ambient_air,
        "capacity_load":capacity_load ,
        "cargo_1_temp": cargo_1_temp,
        "cargo_2_temp": cargo_2_temp,
        "cargo_3_temp": cargo_3_temp,
        "cargo_4_temp": cargo_4_temp,
        "relative_humidity": relative_humidity,
        "co2_reading": co2_reading,
        "power_state": power_state,
        "fecha":fecha 

    }
    return notificacion_procesos

#ver comandos inscritos por imei

#homolgar a ZTRACK GENERAL para que se vea en ztrack.app

