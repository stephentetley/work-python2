"""
Copyright 2025 Stephen Tetley

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import os
import duckdb


def execute_sql_file(script_path: str,
                     *,                      
                     con: duckdb.DuckDBPyConnection, 
                     parameters: object = None) -> duckdb.DuckDBPyConnection:
    if os.path.exists(script_path):
        with open(script_path) as file:
            statements = file.read()
            try: 
                con.execute(statements, parameters=parameters)
                con.commit()
                return con
            except Exception as exn: 
                print(f"SQL script failed:")
                print(statements)
                print(exn)
                raise(exn)
    else: 
        print(f"SQL file does not exist {script_path}")
        raise FileNotFoundError(f"SQL file does not exist {script_path}")
    
def execute_work_sql_script(rel_path: str, 
                            con: duckdb.DuckDBPyConnection, 
                            parameters: object = None) -> duckdb.DuckDBPyConnection:
    work_sql_root = os.environ.get('WORK_SQL_ROOT')
    sql_file_path = os.path.normpath(os.path.join(work_sql_root, rel_path))
    con = execute_sql_file(script_path=sql_file_path, con=con, parameters=parameters)
    return con

def create_landing_table_via_read(*, 
                                  landing_table_name: str,
                                  read_function: str,
                                  source_file_path: str, 
                                  select_body: str | None = None) -> str:
    select_body = select_body if select_body else "*"
    sql_text = f"""
        CREATE OR REPLACE TABLE {landing_table_name} AS
        SELECT {select_body} FROM {read_function}('{source_file_path}');
    """
    return sql_text


def create_landing_table_via_read_union(*,
                                        landing_table_name: str,
                                        read_function: str,
                                        source_file_paths: list[str], 
                                        select_body: str | None = None) -> str:
    select_body = select_body if select_body else "*"
    def make_select_line(path): 
        return f"(SELECT {select_body} FROM {read_function}('{path}'))"
    select_lines = [make_select_line(path) for path in source_file_paths]
    select_unions = "\nUNION\n".join(select_lines)
    sql_text = f"""
        CREATE OR REPLACE TABLE {landing_table_name} AS
        {select_unions};
    """
    return sql_text    