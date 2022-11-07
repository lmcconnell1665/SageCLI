import logging

logger = logging.getLogger('Sage Logger')

import grab_sage_entity
import pandas as pd
from datetime import datetime as dt

def months_to_scan(start_date: str, end_date: str):
    '''Creates a list of queries for months to save based on start and end dates as well as an audit log to store results'''

    start_date_range = pd.date_range(start_date,end_date,freq='MS').strftime("%m/%d/%Y").tolist()
    end_date_range = pd.date_range(start_date,end_date,freq='M').strftime("%m/%d/%Y").tolist()
    file_date_prefix = pd.date_range(start_date,end_date,freq='MS').strftime("%Y_%m").tolist()

    list_of_queries = list()

    for i in range(0,len(start_date_range)-1):
        qry = f'WHENMODIFIED >= {start_date_range[i]} AND WHENMODIFIED <= {end_date_range[i]}'
        list_of_queries.append(qry)

    audit_logs = pd.DataFrame(list_of_queries, columns=['Query'])
    audit_logs['StartDate'] = start_date_range[0:len(start_date_range)-1]
    audit_logs['EndDate'] = end_date_range[0:len(start_date_range)-1]
    audit_logs['FileDatePrefix'] = file_date_prefix[0:len(file_date_prefix)-1]
    audit_logs['TotalRows'] = None
    audit_logs['NumberRemaining'] = None
    audit_logs['Pages'] = None
    audit_logs['Status'] = 'Needs loading'

    return audit_logs

def main(entity: str, start_date: str = '2022-01-01', end_date: str = '2022-06-01'):
    '''Starts a long-running operation to save an entity's history'''

    long_running_start_time = dt.now()

    scanning_df = months_to_scan(start_date, end_date)

    for i in range(0, len(scanning_df)):
        scanning_df.Status[i] == 'Loading data'

        logger.info(f"Starting {entity} for {start_date} to {end_date} at {long_running_start_time}")
        results = grab_sage_entity.main(entity, scanning_df.Query[i], scanning_df.FileDatePrefix[i])

        scanning_df.TotalRows[i] = results[0]
        scanning_df.NumberRemaining[i] = results[1]
        scanning_df.Pages[i] = results[2]

        logger.info(f"Finished {entity} for {start_date} to {end_date}")
        scanning_df.Status[i] = 'Finished'

        dur = dt.now() - long_running_start_time
        scanning_df.to_csv(f'{entity}_{long_running_start_time}_audit.csv')
        logger.info(f"Scan for {entity} completed in {dur}. Audit log saved.")


if __name__ == '__main__':
    main()