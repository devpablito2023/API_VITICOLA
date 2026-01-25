from typing import Optional,List
from datetime import  datetime
from pydantic import BaseModel, Field


class ValidacionSchema(BaseModel):
    #pre_validacion_id: Optional[int] = None
    nombre_etapa : str = Field(...)
    hora_inicio_etapa: str = Field(...) 
    temperatura_etapa: float = Field(...)
    humedad_etapa: Optional[float] | None =None

class control_temperatura_Schema(BaseModel) :
    id_control_temperatura : Optional[int] | None =0
    imei_control_temperatura :Optional[str] | None ="868428046606400" 
    proceso_control_temperatura : str = Field(...)
    tipo_control_temperatura :  int = Field(...)
    total_control_temperatura : float = Field(...)
    lista_control_temperatura: Optional[List[ValidacionSchema]] = []
    hora_fin_control_temperatura: Optional[str] | None =None 
    hora_proceso_control_temperatura: Optional[float] | None =None    
    condicion_control_temperatura : Optional[int] | None =1   
    estado_control_temperatura: Optional[int] | None =1
    updated_at: Optional[datetime] | None =None #generico
    created_at: Optional[datetime] | None =None #generico
    user_c_nombre:Optional[str] | None =None 
    user_m_nombre:Optional[str] | None =None 
    user_c:Optional[int] | None =0 #generico
    user_m:Optional[int] | None =None #generico

    class Config:
        json_schema_extra = {
            "example": {
                "id_control_temperatura":0,
                "proceso_control_temperatura" : "Proceso 25/06/2025",
                "imei_control_temperatura":"868428046606400",
                "tipo_control_temperatura":0,
                "total_control_temperatura":3,
                "hora_proceso_control_temperatura":48,
                "lista_control_temperatura" : [
                    {"nombre_etapa":"etapa 1" ,"hora_inicio_etapa":"25-06-2025_10-12","temperatura_etapa":20.2,"humedad_etapa":None,"duracion":4},
                    {"nombre_etapa":"etapa 2" ,"hora_inicio_etapa":"25-06-2025_13-12","temperatura_etapa":18.4,"humedad_etapa":None,"duracion":5},
                    {"nombre_etapa":"etapa 3" ,"hora_inicio_etapa":"25-06-2025_15-42","temperatura_etapa":16,"humedad_etapa":None,"duracion":3}
                ],
                "hora_fin_control_temperatura":"25-06-2025_17-42",
                "condicion_control_temperatura" :None,
                "estado_control_temperatura":None,
                "created_at":None,
                "user_c_nombre":None,
                "user_c":0
            }
        }

class BusquedaSchema(BaseModel):
    imei :Optional[str] | None ="868428046606400" 
    fecha_inicio: Optional[str] | None="0"
    fecha_fin: Optional[str] | None ="0"
    duracion :Optional[int] | None =None 
    class Config:
        json_schema_extra = {
            "example": {
                "imei":"867858039011138",
                "fecha_inicio":None,
                "fecha_fin":None
            }
        }


class ConsultarSchema(BaseModel):
    id_usuario: Optional[int] | None =0
    tipo_usuario: Optional[int] | None =0
    #token_ztrack :  Optional[str] | None ="balblabla"
    especifico: Optional[int] | None =0
    fecha_inicio: Optional[str] | None=None
    fecha_fin: Optional[str] | None =None
    class Config:
        json_schema_extra = {
            "example": {
                "id_usuario": 0,
                "tipo_usuario": 0,           
                #"token_ztrack":"0f2adb0aee3de894ac4e28bfce85a54f5",
                "especifico" :0,
                "fecha_inicio" :None,
                "fecha_fin" :None
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
