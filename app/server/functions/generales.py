from datetime import datetime,timedelta

def procesar_historico(mensaje,user_c,objeto,especifico=""):
    filter_proyecto = {k: v for k, v in objeto.items() if k not in [ 'user_c','user_m','updated_at','created_at']}
    filter_proyecto['mensaje'] = str(mensaje) +"_"+str(especifico)
    filter_proyecto['user_evento']=user_c
    filter_proyecto['fecha_evento']=datetime.now()
    return filter_proyecto
def procesar_log(evento,usuario,campo) :
    mensaje2 =str(evento)+" : "+str(campo)+" , hecho por : "+str(usuario)
    agrupado = {"evento":mensaje2,"fecha" :datetime.now()}
    return agrupado

def filtrar_no_none(data: dict) -> dict:
    """Filtra y devuelve solo los elementos del diccionario que no tienen valores None."""
    return {k: v for k, v in data.items() if v is not None}
def convertir_fecha_inicio(fecha_str):
    try:
        #30-05-2025_12-00-00
        return datetime.strptime(fecha_str, "%d-%m-%Y_%H-%M-%S")
    except ValueError:
        return datetime.now()- timedelta(days=30)
def convertir_fecha_fin(fecha_str):
    try:
        return datetime.strptime(fecha_str, "%d-%m-%Y_%H-%M-%S")
    except ValueError:
        return datetime.now() 

def comparar_json(j1, j2):
    j3 = {}
    for clave, valor in j2.items():
        if clave in j1 and j1[clave] != valor:
            j3[clave] = valor
    return j3

def formato_texto_evento(tex: str) -> str:
    return tex.replace("_", " ").upper()