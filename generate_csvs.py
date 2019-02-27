import pandas as pd
import database as db
import queue
import time
import generate_csv as gcsv
import download_data as dd
import threading

pd.set_option('display.max_columns', 25)
pd.set_option('display.width', 400)
date = 'date'
open = 'open'
high = 'high'
low = 'low'
close = 'close'
volume = 'volume'

def generate_csvs(tickers_queue, connections_queue):
    while True:
        ticker = tickers_queue.get(block=True)
        connection = connections_queue.get(block=True)
        data = gcsv.load_data_from_db(connection, ticker)
        gcsv.build_dataframe(data, ticker)
        connections_queue.put(connection)
        tickers_queue.task_done()

def get_tickers_queue(tables):
    tickers = queue.Queue()
    for t in tables:
        ticker = str(t).split('_')[0]
        tickers.put(ticker)
    return tickers

def create_threads(num_threads, tickers_queue, connections_queue):
    for i in range(num_threads):
        thread = threading.Thread(target=generate_csvs, args=(tickers_queue, connections_queue,))
        thread.setDaemon(True)
        thread.start()

if __name__ == '__main__':
    start_time = time.time()
    num_threads = 3
    connection = db.open_connection()
    tables = db.get_all_tables(connection)
    db.close_connection(connection)
    tickers_queue = get_tickers_queue(tables)
    connections_queue = dd.instantiate_connection_pool(num_threads)
    create_threads(num_threads, tickers_queue, connections_queue)
    tickers_queue.join()
    dd.terminate_connection_pool(connections_queue)
    elapsed_time = time.time() - start_time
    print('took ' + str(elapsed_time / 60) + ' minutes to execute')