# always run 
# export GOOGLE_APPLICATION_CREDENTIALS="/Users/Kunal/Downloads/Reporting-fb5b7e0aa2c7.json" 
# before executing the script


"""Command-line application to perform an asynchronous query in BigQuery.
"""

import argparse
import json
import time
import uuid
import pandas as pd
import googleapiclient.discovery
from pandas.io import gbq
import datetime
from datetime import timedelta
import ftplib
import os
from credentials1 import helper_config

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/Kunal/Downloads/pythonproject/Reporting-fb5b7e0aa2c7 (1).json"
now1 = datetime.datetime.utcnow()

for x in range(1):
    lag = 2
    now = now1 - timedelta(hours=lag)
    print(now)
    def proper (number):    
        if(number<10):
            return ("0"+str(number))
        else:
            return (str(number))
    datefinal = str(now.year)+proper(now.month)+proper(now.day)+proper(now.hour)
    filename = "DeviceId_"+datefinal+".csv"
    threshold = 4
    print(datefinal)
    #query_string = "SELECT device_id, SUM(demand_revenue) AS demand_revenue, SUM(auctions) AS auctions, SUM(impressions) AS impressions, SUM(wins)*100/SUM(auctions) AS fillRate, SUM(impressions)*100/SUM(wins) AS efficiency, SUM(impressions)*100/SUM(auctions) AS utilization, SUM(demand_revenue)*1000000/SUM(auctions) AS revPerMillionAuctions FROM (SELECT minute(auction.timestamp) AS time, auction.device.ifa as device_id, SUM(IF(tracker.measure = 'dsp_win'AND auction.bidResponses.responseStatus='WON', auction.bidResponses.winPriceDemand, 0))/1000 AS demand_revenue, SUM(IF(tracker.measure = 'dsp_win'AND auction.bidResponses.responseStatus='WON', auction.bidResponses.winPriceSupply, 0))/1000 AS supply_revenue, SUM(IF(tracker.measure = 'dsp_win'AND auction.bidResponses.responseStatus='WON', 1, 0)) AS impressions FROM (SELECT * FROM (SELECT * FROM TABLE_QUERY(chocolate_raw, "table_id CONTAINS 'auctions_served' AND table_id CONTAINS \'" + str(datefinal) +"\' \")) AS auction JOIN (SELECT * FROM TABLE_QUERY(chocolate_raw, "table_id CONTAINS 'trackers_essential_' AND table_id CONTAINS \'" + str(datefinal) +"\' \")) AS tracker ON auction.id = tracker.auctionId ) GROUP BY time, device_id ), (SELECT minute(timestamp) AS time, device.ifa as device_id, COUNT(1) AS auctions, SUM(IF(bidResponses.responseStatus = 'WON', 1, 0) ) AS wins FROM TABLE_QUERY(chocolate_raw, "table_id CONTAINS 'auctions' AND table_id CONTAINS \'" + str(datefinal) + "\' \") GROUP BY time, device_id ) GROUP BY time,device_id having device_id is not null and impressions > 4 ORDER BY impressions desc"
    query_string = "SELECT device_id,  SUM(auctions) AS auctions, SUM(impressions) AS impressions, SUM(wins)*100/SUM(auctions) AS fillRate, SUM(impressions)*100/SUM(wins) AS efficiency, SUM(impressions)*100/SUM(auctions) AS utilization, SUM(demand_revenue)*1000000/SUM(auctions) AS revPerMillionAuctions FROM (SELECT minute(auction.timestamp) AS time, auction.device.ifa as device_id, SUM(IF(tracker.measure = 'dsp_win' AND auction.bidResponses.responseStatus='WON', auction.bidResponses.winPriceDemand, 0))/1000 AS demand_revenue, SUM(IF(tracker.measure = 'dsp_win'AND auction.bidResponses.responseStatus='WON', auction.bidResponses.winPriceSupply, 0))/1000 AS supply_revenue, SUM(IF(tracker.measure = 'dsp_win' AND auction.bidResponses.responseStatus='WON', 1, 0)) AS impressions FROM (SELECT * FROM (SELECT * FROM TABLE_QUERY(chocolate_raw, \"table_id CONTAINS 'auctions_served' AND table_id CONTAINS \'"+ datefinal +"\' \")) AS auction JOIN (SELECT * FROM TABLE_QUERY(chocolate_raw, \"table_id CONTAINS 'trackers_essential_' AND table_id CONTAINS \'"+datefinal+"\' \")) AS tracker ON auction.id = tracker.auctionId ) GROUP BY time, device_id ), (SELECT minute(timestamp) AS time, device.ifa as device_id, COUNT(1) AS auctions, SUM(IF(bidResponses.responseStatus = 'WON', 1, 0) ) AS wins FROM TABLE_QUERY(chocolate_raw, \"table_id CONTAINS 'auctions' AND table_id CONTAINS \'"+ datefinal+"\' \") GROUP BY time, device_id ) GROUP BY time,device_id having device_id is not null and impressions > " + str(threshold) + "ORDER BY impressions desc"
    # [START async_query]
    print(query_string)

    
    def async_query(
            bigquery, project_id, query,
            batch=False, num_retries=5, use_legacy_sql=True):
        # Generate a unique job ID so retries
        # don't accidentally duplicate query
        job_data = {
            'jobReference': {
                'projectId': project_id,
                'jobId': str(uuid.uuid4())
            },
            'configuration': {
                'query': {
                    'query': query,
                    'priority': 'BATCH' if batch else 'INTERACTIVE',
                    # Set to False to use standard SQL syntax. See:
                    # https://cloud.google.com/bigquery/sql-reference/enabling-standard-sql
                    'useLegacySql': use_legacy_sql
                }
            }
        }
        return bigquery.jobs().insert(
            projectId=project_id,
            body=job_data).execute(num_retries=num_retries)
    # [END async_query]


    # [START poll_job]
    def poll_job(bigquery, job):
        """Waits for a job to complete."""

        print('Waiting for job to finish...')

        request = bigquery.jobs().get(
            projectId=job['jobReference']['projectId'],
            jobId=job['jobReference']['jobId'])

        while True:
            result = request.execute(num_retries=2)

            if result['status']['state'] == 'DONE':
                if 'errorResult' in result['status']:
                    raise RuntimeError(result['status']['errorResult'])
                print('Job complete.')
                return

            time.sleep(1)
    # [END poll_job]


    # [START run]
    def main(
            project_id, batch, num_retries, interval,
            use_legacy_sql):
        # [START build_service]
        # Construct the service object for interacting with the BigQuery API.
        bigquery = googleapiclient.discovery.build('bigquery', 'v2')
        # [END build_service]
        


        #query_string = "SELECT domain, date FROM (SELECT domain, date, bundle, SUM(available_inventory) AS auctionss, SUM(demand_side_revenue) AS demand_side_revenue FROM TABLE_DATE_RANGE(reporting_hudson_views.stats_daily_, TIMESTAMP('2018-03-01'), TIMESTAMP('2018-03-25')) GROUP BY date, domain, bundle ) GROUP BY domain, bundle, date HAVING bundle IS NULL order by demand_side_revenue DESC"
        # Submit the job and wait for it to complete.
        use_legacy_sql
        query_job = async_query(
            bigquery,
            project_id,
            query_string,
            batch,
            num_retries,
            use_legacy_sql)
        df3=pd.read_csv("AllDevice.csv")
        df = gbq.read_gbq(query_string, project_id=project_id)
        print(df.shape)
        frames = [df, df3]
        df5=pd.concat(frames)
        print(df5.shape)
        df5.to_csv("AllDevice.csv", index=False)

        df4=df5["device_id"]
        df4=df4

        df4.to_csv(filename, header= False, index=False)

        """myFTP = ftplib.FTP("ec2-204-236-207-77.compute-1.amazonaws.com", "kunal", "kunal123")
        ec2-204-236-207-77.compute-1.amazonaws.comkunalkunal@123

        myFTP.set_pasv(False)
        file=open(filename,"rb")
        filename1='STOR ' + filename
        myFTP.storbinary(filename1,file)"""


        #print(helper_config['ftp']['host'])
        host = helper_config['ftp']['host']
        username = helper_config['ftp']['user_name']
        password = helper_config['ftp']['password']

        print(host + username + password)

        myFTP = ftplib.FTP(host,username,password)
        myFTP.set_pasv(False)
        file=open(filename,"rb")
        filename1='STOR ' + filename
        myFTP.storbinary(filename1,file)





        poll_job(bigquery, query_job)

        # Page through the result set and print all results.
        page_token = None
        while True:
            page = bigquery.jobs().getQueryResults(
                pageToken=page_token,
                **query_job['jobReference']).execute(num_retries=2)

            page_token = page.get('pageToken')
            if not page_token:
                break
    # [END run]


    # [START main]
    if __name__ == '__main__':
        parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter)
        ##parser.add_argument('project_id', help='Your Google Cloud project ID.', default = 'divine-builder-586')
        parser.add_argument(
            '-b', '--batch', help='Run query in batch mode.', action='store_true')
        parser.add_argument(
            '-r', '--num_retries',
            help='Number of times to retry in case of 500 error.',
            type=int,
            default=5)
        parser.add_argument(
            '-p', '--poll_interval',
            help='How often to poll the query for completion (seconds).',
            type=int,
            default=1)
        parser.add_argument(
            '-l', '--use_legacy_sql',
            help='Use legacy BigQuery SQL syntax instead of standard SQL syntax.',
            type=bool,
            default=True)

        args = parser.parse_args()
        pro = "divine-builder-586"

        main(
            ##args.project_id,
            pro,
            args.batch,
            args.num_retries,
            args.poll_interval,
            args.use_legacy_sql)
    # [END main]
