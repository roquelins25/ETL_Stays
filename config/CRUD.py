from psycopg2 import Error

def insert(connection, table: str, data: dict) -> bool:
    columns = ", ".join(data.keys())
    placeholders = ", ".join(["%s"] * len(data))
    sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

    try:
        cursor = connection.cursor()
        cursor.execute(sql, list(data.values()))
        connection.commit()
        cursor.close()
        return True
    except Error as e:
        connection.rollback()
        print(f"Erro ao inserir em '{table}': {e}")
        return False


def insert_many(connection, table: str, data: list[dict]) -> int:
    if not data:
        return 0

    columns = ", ".join(data[0].keys())
    placeholders = ", ".join(["%s"] * len(data[0]))
    sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

    try:
        cursor = connection.cursor()
        cursor.executemany(sql, [list(row.values()) for row in data])
        connection.commit()
        inserted = cursor.rowcount
        cursor.close()
        return inserted
    except Error as e:
        connection.rollback()
        print(f"Erro ao inserir múltiplos registros em '{table}': {e}")
        return 0


def update(connection, table: str, data: dict, where: dict) -> bool:
    set_clause = ", ".join([f"{col} = %s" for col in data.keys()])
    where_clause = " AND ".join([f"{col} = %s" for col in where.keys()])
    sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

    params = list(data.values()) + list(where.values())

    try:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        connection.commit()
        updated = cursor.rowcount
        cursor.close()
        return updated > 0
    except Error as e:
        connection.rollback()
        print(f"Erro ao atualizar '{table}': {e}")
        return False


def upsert(connection, table: str, data: dict, conflict_columns: list[str]) -> bool:
    columns = ", ".join(data.keys())
    placeholders = ", ".join(["%s"] * len(data))
    conflict = ", ".join(conflict_columns)

    update_cols = [col for col in data.keys() if col not in conflict_columns]
    set_excluded = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])

    sql = (
        f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict}) DO UPDATE SET {set_excluded}"
    )

    try:
        cursor = connection.cursor()
        cursor.execute(sql, list(data.values()))
        connection.commit()
        cursor.close()
        return True
    except Error as e:
        connection.rollback()
        print(f"Erro ao fazer upsert em '{table}': {e}")
        return False
