import argparse
import concurrent.futures
from datetime import datetime, timedelta
import os

from cloudwatch import CloudwatchLog, CloudwatchStats


def create_logs(cw_log: CloudwatchLog, cw_stats: CloudwatchStats):
    start_day = 0
    end_day = 14
    
    # Full day is 1440 minutes
    multipler = 16

    # Short test period
    # minutes_in_day = 1
    # minutes_increment = 0.5

    # Less threads
    minutes_in_day = 1440/multipler
    # minutes_increment = 15
    minutes_increment = minutes_in_day/10
    
    # More threads
    # minutes_in_day = 1440
    # minutes_increment = 60

    end_date = datetime.now()
    start_date = end_date
    max_threads = 10
    for day in range(start_day, end_day*multipler):
        logs = []
        futures = []
        day_minutes = day * minutes_in_day
        minutes = 0
        start = end_date - timedelta(minutes=minutes_in_day)
        end = end_date
        print(f"day: {day}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            thread_count = 0
            while minutes < minutes_in_day:
                end_date = start_date
                start_date -= timedelta(minutes=minutes_increment)
                # start_date = datetime.now() - timedelta(seconds=minutes+30)
                # end_date = start_date + timedelta(seconds=30)
                print(f"Spinning thread for: {start_date} - {end_date}\n")
                futures.append(
                    executor.submit(
                        cw_log.ingest,
                        cw_log.log_streams,
                        start_date,
                        end_date,
                        thread_count,
                    )
                )
                thread_count += 1
                minutes += minutes_increment
    
        for future in futures:
            logs.extend(future.result())

        print(f"\nlength of logs for day {day}: {len(logs)}")
        if not os.path.exists("./basic_logs"):
            os.makedirs("./basic_logs")
        cw_stats.write_basic_timing_log(logs, f"timing_log_day_{start}_to_{end}", header=["timestamp", "request_uuid", "concierge service", "latency"])
        stats = cw_stats.get_log_stats(logs)
        if not os.path.exists("./full_logs"):
            os.makedirs("./full_logs")
        cw_stats.write_full_log(stats, f"full_log_day_{start}_to_{end}", header=["request_uuid", "hotel_code", "downstream service", "concierge service", "latency", "logs"])


def parse_logs(cw_log: CloudwatchLog, cw_stats: CloudwatchStats):
    cw_stats.parse_logs(cw_log)

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--create-logs', type=bool, help='If set to true, will create logs from cloudwatch and save to csv', default=False)
    arg_parser.add_argument('--parse-logs', type=bool, help='If set to true, will parse logs from csv files', default=False)
    arg_parser.add_argument('--filter-logs', type=str, help='Filters the logs by the given hotel_code', default='')
    cw_log = CloudwatchLog("concierge-prod")
    cw_stats = CloudwatchStats(basic_logs_path="./basic_logs")
    start_date =  - timedelta(minutes=30)
    end_date = datetime.now()
    if arg_parser.parse_args().create_logs:
        create_logs(cw_log, cw_stats)
    if arg_parser.parse_args().parse_logs:
        parse_logs(cw_log, cw_stats)
    # if arg_parser.parse_args().filter_logs:
    #     filter_logs(arg_parser.parse_args().filter_logs)