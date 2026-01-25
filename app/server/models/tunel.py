from typing import Optional,List
from datetime import  datetime
from pydantic import BaseModel, Field

class ComandoSchema(BaseModel):
    imei :str = Field(...)
    comando: Optional[str] | None ="SIN COMANDO"
    estado: Optional[int] | None =1
    estatus: Optional[int] | None =1
    fecha_creacion : Optional[datetime] | None =None
    fecha_ejecucion : Optional[datetime] | None =None
    user_c : Optional[int] | None =0
    class Config:
        json_schema_extra = {
            "example": {
                "imei":"867858039011138",
                "comando":"comando",
                "estado": None,
                "estatus":None,
                "fecha_creacion":None,
                "fecha_ejecucion":None,
                "user_c":None
            }
        }

class TunelSchema(BaseModel):
    i :str = Field(...)
    ip: Optional[str] | None =None
    c: Optional[str] | None =None
    d00: Optional[str] | None =None
    d01: Optional[str] | None =None
    d02: Optional[str] | None =None
    d03: Optional[str] | None =None
    d04: Optional[str] | None =None
    d05: Optional[str] | None =None
    d06: Optional[str] | None =None
    d07: Optional[str] | None =None
    d08: Optional[str] | None =None
    d08: Optional[str] | None =None
    gps: Optional[str] | None =None
    val : Optional[str] | None =None
    rs: Optional[str] | None =None
    r: Optional[str] | None =None
    estado: Optional[int] | None =1
    class Config:
        json_schema_extra = {
            "example": {
                "i":"aqui av el IMEI",
                "ip":"aqui e dato del ip y la intensidad de la señal",
                "c":"aqui el codigo del equipo",
                "d00": "Aqui va los datos d de los sensores TermoKing",
                "d01": "Aqui va los datos d1 de los sensores TermoKing",
                "d02": "Aqui va los datos d2 de los sensores TermoKing",
                "d03": "Aqui va los datos d3 de los sensores TermoKing",
                "d04": "Aqui va los datos d4 de los sensores TermoKing",
                "d05": "Aqui va los datos d de los sensores TermoKing",
                "d06": "Aqui va los datos d1 de los sensores TermoKing",
                "d07": "Aqui va los datos d2 de los sensores TermoKing",
                "d08": "Aqui va los datos d3 de los sensores TermoKing",
                "gps": "Aqui va el GPS",
                "r": "Aqui va lel resultado de los comandos",
                "rs": "Aqui va las respuestas  de los comandos",
                "val": "Aqui va los valores de los sensores",

                "estado": None,
            }
        }
    
class BusquedaSchema(BaseModel):
    imei :str = Field(...)
    fecha_inicio: Optional[str] | None="0"
    fecha_fin: Optional[str] | None ="0"
    class Config:
        json_schema_extra = {
            "example": {
                "imei":"867858039011138",
                "fecha_inicio":None,
                "fecha_fin":None
            }
        }
#respuesta cuando todo esta bien
def ResponseModel(data, message):
    return {
        "data": data,
        "code": 200,
        "message": message,
    }

#respuesta cuando algo sale mal 
def ErrorResponseModel(error, code, message):
    return {"error": error, "code": code, "message": message}
