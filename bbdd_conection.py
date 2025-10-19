# mysql_query_runner.py
import os
from typing import Any, Dict, Iterator, Optional, Union

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine
from dotenv import load_dotenv

def get_engine(dotenv_path: str = ".env") -> Engine:
    load_dotenv(dotenv_path)
    url = URL.create(
        "mysql+pymysql",
        username=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT")),
        database=os.getenv("MYSQL_DATABASE"),
    )
    return create_engine(url, pool_pre_ping=True)

def run_query(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    chunksize: Optional[int] = None,
    as_dataframe: bool = True,
    engine: Optional[Engine] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame], list[dict]]:
    """
    Ejecuta una consulta SQL arbitraria sobre MySQL.
    - query: tu SQL (puede incluir JOINs, CTEs, etc.)
    - params: parámetros opcionales (usa :nombre en el SQL)
    - chunksize: si lo indicas, devuelve un iterador de DataFrames por trozos
    - as_dataframe: True -> DataFrame/iterador; False -> lista de dicts (para resultados pequeños)
    - engine: puedes pasar un Engine ya creado; si no, se crea con get_engine()

    Ejemplos de params:
        SELECT * FROM pedidos WHERE estado=:estado AND total>=:min_total
        params={"estado": "PAGADO", "min_total": 100}
    """
    eng = engine or get_engine()
    sql = text(query)

    if chunksize:
        # Iterador eficiente para resultados grandes
        with eng.connect() as conn:
            return pd.read_sql(sql, conn, params=params, chunksize=chunksize)

    with eng.connect() as conn:
        if as_dataframe:
            return pd.read_sql(sql, conn, params=params)
        else:
            # Lista de dicts (útil si no quieres pandas)
            result = conn.execute(sql, params or {})
            cols = result.keys()
            return [dict(zip(cols, row)) for row in result.fetchall()]


if __name__ == '__main__':

    query = """
    SELECT *
    FROM pasajero
    limit 100
    """
    df = run_query(query)
    print(df)