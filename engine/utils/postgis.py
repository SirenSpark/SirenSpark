from pyspark.sql import SparkSession


def get_table_columns(db_host, db_port, db_database, db_user, db_password, table_name):
    """
    Interroge Postgres et récupère les définitions des colonnes
    """

    # start Spark session
    spark = SparkSession.builder.appName("SirenSpark").getOrCreate()

    # Get the schema and table name from the table_name parameter
    if '.' in table_name:
        schema_name, table_name = table_name.split('.')
    else:
        schema_name = 'public'

    # Get the column names and types of the existing table
    query = f"SELECT column_name, character_maximum_length, data_type, udt_name, geometry_columns.srid, " \
            f"geometry_columns.type, geometry_columns.coord_dimension " \
            f"FROM information_schema.columns a " \
            f"LEFT JOIN public.geometry_columns ON a.table_schema=geometry_columns.f_table_schema " \
            f"AND a.table_name=geometry_columns.f_table_name " \
            f"AND a.column_name=geometry_columns.f_geometry_column " \
            f"WHERE table_schema='{schema_name}' " \
            f"AND table_name='{table_name}'"
    existing_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_database}") \
        .option("query", query) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    table_columns = []
    for row in existing_df.collect():
        table_columns.append(row.asDict())

    return table_columns


def get_column_types(table_columns):
    result = {}
    for item in table_columns:
        column_name = item['column_name']
        if column_name not in result:

            if item['data_type'] != 'USER-DEFINED':
                result[column_name] = {
                    'data_type': map_postgres_type_to_spark_type(item['data_type']),
                    'pg_data_type': item['data_type'],
                    'pg_character_maximum_length': item['character_maximum_length'],
                }
            elif item['udt_name'] == 'geometry':
                result[column_name] = {
                    'data_type': 'geometry',
                    'pg_data_type': 'geometry',
                    'type': item['type'],
                    'srid': item['srid'],
                    'coord_dimension': item['coord_dimension']
                }

    return result


def create_table_query(table_name, types):
    """
    Generates a SQL query for creating a PostGIS table based on the given types dictionary
    """
    if '.' in table_name:
        schema, table = table_name.split('.')
    else:
        schema = 'public'
        table = table_name
    columns = []
    for col_name, col_type in types.items():
        data_type = col_type.get("data_type", None)
        pg_data_type = col_type.get("pg_data_type", None)
        column_type = None
        if pg_data_type != None:
            if pg_data_type == "character":
                column_type = f"VARCHAR({col_type.get('character_maximum_length', '255')})"
            elif pg_data_type == "character varying":
                column_type = f"VARCHAR({col_type.get('character_maximum_length', '255')})"
            elif pg_data_type == "geometry":
                type_str = col_type.get('type', '')
                srid_str = col_type.get('srid', '')
                coord_dim_str = col_type.get('coord_dimension', '')
                column_type = "GEOMETRY"
                if type_str:
                    column_type += f"({type_str}"
                    if srid_str:
                        column_type += f", {srid_str}"
                        if coord_dim_str:
                            column_type += f", {coord_dim_str}"
                    column_type += ")"
            else :
                column_type = pg_data_type
        elif data_type != None:
            if data_type == "boolean":
                column_type = "boolean"
            elif data_type == "string":
                column_type = f"varchar({col_type.get('character_maximum_length', '255')})"
            elif data_type == "binary":
                column_type = "bytea"
            elif data_type == "date":
                column_type = "date"
            elif data_type == "timestamp":
                column_type = "timestamp with time zone"
            elif data_type == "double":
                column_type = "double precision"
            elif data_type == "integer":
                column_type = "integer"
            elif data_type == "decimal":
                column_type = "numeric"
            elif data_type == "float":
                column_type = "real"
            elif data_type == "short":
                column_type = "smallint"
            elif data_type == "long":
                column_type = "bigint"
            else:
                raise ValueError(f"Unsupported data type: {data_type}")
        else:
            raise ValueError(f"Data type undefined: {col_type}")
        if column_type != None:
            columns.append(f'"{col_name}" {column_type}')
    columns_str = ", ".join(columns)
    query = f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({columns_str});'
    return query


def cast_df_by_table_columns(df, table_columns):
    """
    Cast un DF en fonction de colonnes récupérées par get_table_columns
    """
    for col in df.columns:
        existing_col = next(
            (c for c in table_columns if c['column_name'] == col), None)
        if existing_col:
            data_type = existing_col['data_type']
            udt_name = existing_col['udt_name']
            if udt_name.startswith('geometry'):
                # Cast geometry columns to WKT format
                df = df.withColumn(col, df[col].cast('binary'))
            elif data_type in ['bigint', 'bigserial']:
                df = df.withColumn(col, df[col].cast('bigint'))
            elif data_type in ['bit', 'varbit']:
                df = df.withColumn(col, df[col].cast('binary'))
            elif data_type == 'boolean':
                df = df.withColumn(col, df[col].cast('boolean'))
            elif data_type == 'box':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'bytea':
                df = df.withColumn(col, df[col].cast('binary'))
            elif data_type in ['character', 'char']:
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type in ['character varying', 'varchar']:
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'cidr':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'circle':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'date':
                df = df.withColumn(col, df[col].cast('date'))
            elif data_type == 'double precision':
                df = df.withColumn(col, df[col].cast('double'))
            elif data_type == 'inet':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'integer':
                df = df.withColumn(col, df[col].cast('bigint'))
            elif data_type == 'interval':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'json':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'jsonb':
                df = df.withColumn(col, df[col].cast('binary'))
            elif data_type == 'line':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'lseg':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'macaddr':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'macaddr8':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'money':
                df = df.withColumn(col, df[col].cast('double'))
            elif data_type == 'numeric':
                df = df.withColumn(col, df[col].cast('decimal'))
            elif data_type == 'path':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'pg_lsn':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'pg_snapshot':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'point':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'polygon':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'real':
                df = df.withColumn(col, df[col].cast('float'))
            elif data_type == 'smallint':
                df = df.withColumn(col, df[col].cast('smallint'))
            elif data_type == 'smallserial':
                df = df.withColumn(col, df[col].cast('smallint'))
            elif data_type == 'serial':
                df = df.withColumn(col, df[col].cast('integer'))
            elif data_type == 'text':
                df = df.withColumn(col, df[col].cast('string'))
            elif 'timestamp' in data_type:
                if 'with time zone' in data_type:
                    df = df.withColumn(col, df[col].cast('timestamp'))
                else:
                    df = df.withColumn(col, df[col].cast('timestamp'))
            elif 'time' in data_type:
                if 'with time zone' in data_type:
                    df = df.withColumn(col, df[col].cast('timestamp'))
                else:
                    df = df.withColumn(col, df[col].cast('time'))
            elif data_type == 'tsquery':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'tsvector':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'txid_snapshot':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'uuid':
                df = df.withColumn(col, df[col].cast('string'))
            elif data_type == 'xml':
                df = df.withColumn(col, df[col].cast('string'))
            else:
                # Cast other columns to string
                df = df.withColumn(col, df[col].cast('string'))

    return df


def map_postgres_type_to_spark_type(postgres_type):
    type_map = {
        "boolean": "boolean",
        "box": "string",
        "bytea": "binary",
        "cidr": "string",
        "circle": "string",
        "date": "date",
        "double precision": "double",
        "inet": "string",
        "integer": "integer",
        "interval": "string",
        "json": "string",
        "jsonb": "string",
        "line": "string",
        "lseg": "string",
        "macaddr": "string",
        "macaddr8": "string",
        "money": "double",
        "numeric": "decimal",
        "path": "string",
        "pg_lsn": "string",
        "pg_snapshot": "string",
        "point": "string",
        "polygon": "string",
        "real": "float",
        "smallint": "short",
        "smallserial": "short",
        "serial": "integer",
        "text": "string",
        "time": "timestamp",
        "timestamp": "timestamp",
        "tsquery": "string",
        "tsvector": "string",
        "txid_snapshot": "string",
        "uuid": "string",
        "xml": "string",
        "bigint": "long",
        "bigserial": "long",
        "bit": "string",
        "varbit": "string",
        "character": "string",
        "char": "string",
        "character varying": "string",
        "varchar": "string",
        "time with time zone": "timestamp",
        "timestamp with time zone": "timestamp"
    }
    return type_map.get(postgres_type, "string")


def table_exists(db_host, db_port, db_database, db_user, db_password, table_name):
    """
    Check if a table exists
    """
    table_columns = get_table_columns(
        db_host, db_port, db_database, db_user, db_password, table_name)
    if len(table_columns) > 0:
        return True
    else:
        return False
