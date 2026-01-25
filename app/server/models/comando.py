from typing import Optional,List
from datetime import  datetime
from pydantic import BaseModel, Field

class ComandoSchema(BaseModel):
    imei :str = Field(...)
    estado : Optional[int] |None =1
    fecha_creacion : Optional[datetime] |None = datetime.now()
    fecha_ejecucion : Optional[datetime] |None =None
    comando : str = Field(...)
    dispositivo : Optional[str] |None = "FAIL"
    evento : Optional[str] |None = "SIN REGISTRO "
    user :Optional[str] |None ="default"
    receta : Optional[str] |None ="sin receta"
    tipo : Optional[int] |None =0
    status : Optional[int] |None =1
    dato : Optional[float] | None =None
    id : Optional[int] |None =0



    class Config:
        json_schema_extra = {
            "example": {
                "imei":"test01",
                "estado": 1,
                "fecha_creacion": "2024-08-17T14:43:11",
                "comando": "Trama_Readout(3)",
                "dispositivo": "ZGRU1234567"

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

