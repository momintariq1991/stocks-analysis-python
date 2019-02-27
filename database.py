import psycopg2

def open_connection():
    conn = None
    try:
        conn = psycopg2.connect("dbname='stocks' user='momintariq' host='localhost' password=''")
        print('database connection opened')
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
        return conn
    finally:
        return conn

def close_connection(connection):
    if(connection is not None):
        connection.close()
        print('database connection closed')

def get_all_tables(connection):
    cursor = connection.cursor()
    cursor.execute('select table_name from information_schema.tables where table_schema in (\'public\')')
    tables_tuple = cursor.fetchall()
    tables = []
    for i in tables_tuple:
        tables.append(i[0])
    return tables

def delete_all_tables(connection, tables):
    cursor = connection.cursor()
    for i in tables:
        cursor.execute('drop table ' + i)
        connection.commit()
        print('deleted table ' + i)

if __name__ == '__main__':
    connection = open_connection()
    tables = get_all_tables(connection)
    delete_all_tables(connection, tables)
    close_connection(connection)