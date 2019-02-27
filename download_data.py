import requests
import time
import threading
import queue
import database as db

intrinio_username = ''
intrinio_password = ''

def get_tickers(page_number):
    response = requests.get('https://api.intrinio.com/securities/search.csv?conditions=marketcap~gt~10000000000,volume~gt~1000000&actively_traded=true&primary_only=true', auth=(intrinio_username, intrinio_password))
    data = str(response.content).split('\\n')
    tickers = queue.Queue()
    for row in data[2:]:
        row_splitted = row.split(',')
        if(len(row_splitted) == 3):
            ticker = row.split(',')[2]
            if(ticker.isalpha()):
                tickers.put(ticker)
    return tickers

def get_historical_prices(tickers_queue, connections_queue):
    while True:
        ticker = tickers_queue.get(block=True)
        connection = connections_queue.get(block=True)
        response = requests.get('https://api.intrinio.com/prices.csv?identifier=' + ticker, auth=(intrinio_username, intrinio_password))
        data = str(response.content).split('\\n')
        del data[-1]
        data_tuple_array = []
        cursor = connection.cursor()
        drop_table_query = 'drop table if exists ' + ticker + '_historical_prices'
        create_table_query = 'create table ' + ticker + '_historical_prices' + ' (date text, open decimal, high decimal, low decimal, close decimal, volume integer)'
        insert_table_query = 'insert into ' + ticker + '_historical_prices' + ' values (%s, %s, %s, %s, %s, %s)'
        cursor.execute(drop_table_query)
        cursor.execute(create_table_query)
        flag = True
        rowCount = 1
        for row in data[2:]:
            row = str(row).split(',')
            if len(row) == 14:
                date = row[0]
                open = row[8]
                high = row[9]
                low = row[10]
                close = row[11]
                volume = row[12]
                if len(date) > 0 and len(open) > 0 and len(high) > 0 and len(low) > 0 and len(close) > 0 and len(
                        volume) > 0:
                    data_tuple_array.append(
                        (date, float(open), float(high), float(low), float(close), int(volume.split('.')[0])))
                    rowCount = rowCount + 1
                else:
                    flag = False
                    print(ticker + ' empty data')
                    break
            else:
                flag = False
                print(ticker + ' invalid row size, row number : ' + str(rowCount) + ' ' + str(row))
                break

        if flag == True:
            cursor.executemany(insert_table_query, data_tuple_array)
            connection.commit()
            print('persisted ' + str(len(data_tuple_array)) + ' historical prices for ' + ticker)

        connections_queue.put(connection)
        tickers_queue.task_done()

def create_threads(num_threads, tickers_queue, connections_queue):
    for i in range(num_threads):
        thread = threading.Thread(target=get_historical_prices, args=(tickers_queue, connections_queue,))
        thread.setDaemon(True)
        thread.start()

def instantiate_connection_pool(num_connections):
    connection_pool = queue.Queue()
    for i in range(num_connections):
        connection = db.open_connection()
        connection_pool.put(connection)
    return connection_pool

def terminate_connection_pool(connection_pool):
    while not connection_pool.empty():
        connection = connection_pool.get()
        db.close_connection(connection)

if __name__ == '__main__':
    num_threads = 5
    start_time = time.time()
    tickers_queue = get_tickers(1)
    queue_size = tickers_queue.qsize()
    connections_queue = instantiate_connection_pool(num_threads)
    create_threads(num_threads, tickers_queue, connections_queue)
    tickers_queue.join()
    terminate_connection_pool(connections_queue)
    elapsed_time = time.time() - start_time
    print('added historical prices for ' + str(queue_size) + ' tickers')
    print('took ' + str(elapsed_time / 60) + ' minutes to execute')