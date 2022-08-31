from typing import List, Dict, Any, Tuple

from fastapi import FastAPI, Request
from orjson import loads

from ormdantic import (
    get_type_named_model_type, DatabaseConnectionPool, query_records, parse_object_for_model, 
    delete_objects, find_object, upsert_objects, create_table
)
from ormdantic.database import Where
from example.sampleweb.domain import Company, Person, Flag

import uvicorn


app = FastAPI()

_config = {
    'user':'orm',
    'password':'iamroot',
    'database':'json_storage',
    'host':'localhost',
    'port':33069
}

pool = DatabaseConnectionPool(_config)

create_table(pool, Company, Person, Flag)


@app.post('/models/{model_name}')
async def query_models(model_name:str, fields:str, where:Tuple[Tuple[str, str, Any],...]) -> List[Dict[str, Any]]:
    records = list(
        query_records(pool, get_type_named_model_type(model_name), where)
    )
    return records


@app.get('/models/{model_name}/{id}')
def load_model(model_name:str, id:str, concat_shared_model:bool = True):
    obj = find_object(pool, get_type_named_model_type(model_name), (('id', '=', id),), 
                      concat_shared_models=concat_shared_model)

    if obj is None:
        raise RuntimeError('no such object')
    
    return obj


@app.delete('/models/{model_name}')
async def delete_models(model_name:str, id:str, where:Tuple[Tuple[str, str, Any],...]):
    if not where:
        raise RuntimeError('deleting all is disabled.')

    delete_objects(pool, get_type_named_model_type(model_name), where)


@app.put('/models/{model_name}')
async def save_models(model_name:str, models:List[Dict[str, Any]]):
    model_type = get_type_named_model_type(model_name)

    return upsert_objects(pool, 
                          [parse_object_for_model(obj, model_type) for obj in models]
                          )

if __name__ == "__main__":
    uvicorn.run("app:app", host="localhost", port=8080, log_level="debug")
