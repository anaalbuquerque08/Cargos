from fastapi import FastAPI
from fastapi.responses import JSONResponse
import json

app = FastAPI()
 
json_file_path_1 = "tres_recursos_mais_usados.json"
json_file_path_2 = "media_salarios_por_departamento.json"
json_file_path_3 = "custo_por_departamento.json"

@app.get("/json1")  
async def get_json1():
    try:
        with open(json_file_path_1, "r") as f:
            data = json.load(f)
        return JSONResponse(content=data)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/json2")   
async def get_json2():
    try:
        with open(json_file_path_2, "r") as f:
            data = json.load(f)
        return JSONResponse(content=data)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/json3")   
async def get_json3():
    try:
        with open(json_file_path_3, "r") as f:
            data = json.load(f)
        return JSONResponse(content=data)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)