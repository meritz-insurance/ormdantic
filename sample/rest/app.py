from typing import List, Dict, Any

from fastapi import FastAPI, Request
from orjson import loads

from ormdantic import (
    get_type_named_model_type, DatabaseConnectionPool, query_records, parse_obj_for_model, 
    delete_objects, find_object, upsert_objects
)
from ormdantic.database import Where

app = FastAPI()

pool = DatabaseConnectionPool({})

@app.post('/models/{model_name}')
async def query_models(model_name:str, request:Request) -> List[Dict[str, Any]]:
    where = build_where(await request.body())

    records = list(
        query_records(pool, get_type_named_model_type(model_name), where)
    )
    return records


@app.get('/model/{model_name}/{id}')
def load_models(model_name:str, id:str) -> str:
    obj = find_object(pool, get_type_named_model_type(model_name), (('id', '=', id),))

    if obj is None:
        raise RuntimeError('no such object')
    
    return obj.json()


@app.delete('/models/{model_name}')
async def delete_models(model_name:str, id:str, request:Request):
    where = build_where(await request.body())

    delete_objects(pool, get_type_named_model_type(model_name), where)


@app.put('/models/{model_name}')
async def save_models(model_name:str, id:str, request:Request):
    models = loads(await request.body())
    model_type = get_type_named_model_type(model_name)

    return upsert_objects(pool, 
                          [parse_obj_for_model(obj, model_type) for obj in models]
                          )

 
def build_where(body:bytes) -> Where:
    data = loads(body)

    return tuple(data)
