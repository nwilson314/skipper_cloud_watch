import argparse
import concurrent.futures
import csv
import sys
import dataclasses
from datetime import datetime, timedelta
import os
from typing import Optional
from memory_profiler import profile

from skipper_cloud_watch_parser import CloudwatchLog, Log, FormattedMessage, LogStats, extract_time_from_log

@dataclasses.dataclass
class EndPoint:
    name: str
    avg_latency: float = 0
    latency: list[float] = dataclasses.field(default_factory=lambda : [])
    count: int = 0
    errors: int = 0

@dataclasses.dataclass
class Service:
    name: str
    rate_calendar: EndPoint
    availability: EndPoint
    room_details: EndPoint
    addons: EndPoint
    package: EndPoint
    room_rate: EndPoint
    make_reservation: EndPoint
    get_reservation: EndPoint
    cancel_reservation: EndPoint
    modify_reservation: EndPoint
    avg_latency: float = 0
    latency: list[float] = dataclasses.field(default_factory=lambda : [])
    count: int = 0
    errors: int = 0

def write_basic_timing_log(logs: list[Log], name: str, header: list[str]) -> None:

    with open(f'./basic_logs/{name}.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for log in logs:
            if "Took" in log.formatted_message.message:
                time = extract_time_from_log(log.formatted_message.message)
                row = [
                    log.time,
                    log.formatted_message.request_uuid,
                    log.formatted_message.service,
                    time
                ]
                writer.writerow(row)


def write_full_log(stats: list[LogStats], name: str, header: list[str]) -> None:

    with open(f'./full_logs/{name}.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)

        for stat in stats:
            row = [
                stat.request_uuid,
                stat.hotel_code,
                stat.service,
                stat.api_type,
                stat.end_to_end_latency,
                stat.downstream_messages
            ]
            writer.writerow(row)

@profile
def create_logs(cw_log: CloudwatchLog):
    start_day = 0
    end_day = 5
    
    # Short test period
    # minutes_in_day = 20
    # minutes_increment = 20

    # Less threads
    minutes_increment = 120
    minutes_in_day = 720
    
    # More threads
    # minutes_in_day = 1440
    # minutes_increment = 60

    end_date = datetime.now()
    start_date = end_date
    max_threads = 10
    for day in range(start_day, end_day):
        logs = []
        futures = []
        day_minutes = day * minutes_in_day
        minutes = 0
        start = end_date - timedelta(minutes=minutes_in_day)
        end = end_date
        print(f"day: {day}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
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
                        end_date
                    )
                )
                minutes += minutes_increment
    
        for future in futures:
            logs.extend(future.result())
        print(f"\nlength of logs for day {day}: {len(logs)}")
        if not os.path.exists("./basic_logs"):
            os.makedirs("./basic_logs")
        write_basic_timing_log(logs, f"timing_log_day_{start}_to_{end}", header=["timestamp", "request_uuid", "concierge service", "latency"])
        stats = cw_log.get_log_stats(logs)
        if not os.path.exists("./full_logs"):
            os.makedirs("./full_logs")
        write_full_log(stats, f"full_log_day_{start}_to_{end}", header=["request_uuid", "hotel_code", "downstream service", "concierge service" "latency", "logs"])
        
def parse_logs():
    csv.field_size_limit(sys.maxsize)
    ws = Service(
        name="windsurfer",
        rate_calendar=EndPoint(name="rate_calendar"),
        availability=EndPoint(name="availability"),
        room_details=EndPoint(name="room_details"),
        addons=EndPoint(name="addons"),
        package=EndPoint(name="package"),
        room_rate=EndPoint(name="room_rate"),
        make_reservation=EndPoint(name="make_reservation"),
        get_reservation=EndPoint(name="get_reservation"),
        cancel_reservation=EndPoint(name="cancel_reservation"),
        modify_reservation=EndPoint(name="modify_reservation")
    )
    sabre = Service(
        name="sabre",
        rate_calendar=EndPoint(name="rate_calendar"),
        availability=EndPoint(name="availability"),
        room_details=EndPoint(name="room_details"),
        addons=EndPoint(name="addons"),
        package=EndPoint(name="package"),
        room_rate=EndPoint(name="room_rate"),
        make_reservation=EndPoint(name="make_reservation"),
        get_reservation=EndPoint(name="get_reservation"),
        cancel_reservation=EndPoint(name="cancel_reservation"),
        modify_reservation=EndPoint(name="modify_reservation")
    )

    services = {
        "windsurfer": ws,
        "sabre": sabre
    }
    hotel_codes = dict()
    for file in os.listdir("./full_logs"):
        with open(f"./full_logs/{file}", newline='') as f:
            reader = csv.reader(f, delimiter=',')
            file = []
            for row in reader:
                file.append(row)

            header = file[0]
            for i in range(1, len(file)):
                if file[i][2] not in services or not file[i][2]:
                    continue
                service = services[file[i][2]]
                service.count += 1
                hotel_code = file[i][1]

                if hotel_code not in hotel_codes:
                    hotel_codes[hotel_code] = Service(
                        name=service.name,
                        rate_calendar=EndPoint(name="rate_calendar"),
                        availability=EndPoint(name="availability"),
                        room_details=EndPoint(name="room_details"),
                        addons=EndPoint(name="addons"),
                        package=EndPoint(name="package"),
                        room_rate=EndPoint(name="room_rate"),
                        make_reservation=EndPoint(name="make_reservation"),
                        get_reservation=EndPoint(name="get_reservation"),
                        cancel_reservation=EndPoint(name="cancel_reservation"),
                        modify_reservation=EndPoint(name="modify_reservation")
                    )
                
                hotel_codes[hotel_code].count += 1
                if hotel_code == "USAMEBCK":
                    print(file[i])
                match file[i][3]:
                    case "GetRateCalendar":
                        service.rate_calendar.count += 1
                        hotel_codes[hotel_code].rate_calendar.count += 1
                        if float(file[i][4]) > 0.000001:
                            service.latency.append(float(file[i][4]))
                            service.rate_calendar.latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].rate_calendar.latency.append(float(file[i][4]))
                        else:
                            service.errors += 1
                            service.rate_calendar.errors += 1
                            hotel_codes[hotel_code].errors += 1
                            hotel_codes[hotel_code].rate_calendar.errors += 1
                    case "GetAvailability":
                        service.availability.count += 1
                        hotel_codes[hotel_code].availability.count += 1
                        if float(file[i][4]) > 0.000001:
                            service.latency.append(float(file[i][4]))
                            service.availability.latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].availability.latency.append(float(file[i][4])) 
                        else:
                            service.errors += 1
                            service.availability.errors += 1
                            hotel_codes[hotel_code].errors += 1
                            hotel_codes[hotel_code].availability.errors += 1
                    case "GetRoomDetails":
                        service.room_details.count += 1
                        hotel_codes[hotel_code].room_details.count += 1
                        if float(file[i][4]) > 0.000001:
                            service.latency.append(float(file[i][4]))
                            service.room_details.latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].room_details.latency.append(float(file[i][4]))
                        else:
                            service.errors += 1
                            service.room_details.errors += 1
                            hotel_codes[hotel_code].errors += 1
                            hotel_codes[hotel_code].room_details.errors += 1
                    case "GetAddons":
                        service.addons.count += 1
                        hotel_codes[hotel_code].addons.count += 1
                        if float(file[i][4]) > 0.000001:
                            service.latency.append(float(file[i][4]))
                            service.addons.latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].addons.latency.append(float(file[i][4]))
                        else:
                            service.errors += 1
                            service.addons.errors += 1
                            hotel_codes[hotel_code].errors += 1
                            hotel_codes[hotel_code].addons.errors += 1
                    case "GetPackage":
                        service.package.count += 1
                        hotel_codes[hotel_code].package.count += 1
                        if float(file[i][4]) > 0.000001:
                            service.latency.append(float(file[i][4]))
                            service.package.latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].package.latency.append(float(file[i][4]))
                        else:
                            service.errors += 1
                            service.package.errors += 1
                            hotel_codes[hotel_code].errors += 1
                            hotel_codes[hotel_code].package.errors += 1
                    case "GetRoomRate":
                        service.room_rate.count += 1
                        hotel_codes[hotel_code].room_rate.count += 1
                        if float(file[i][4]) > 0.000001:
                            service.latency.append(float(file[i][4]))
                            service.room_rate.latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].room_rate.latency.append(float(file[i][4]))
                        else:
                            service.errors += 1
                            service.room_rate.errors += 1
                            hotel_codes[hotel_code].errors += 1
                            hotel_codes[hotel_code].room_rate.errors += 1
                    case "MakeReservation":
                        service.make_reservation.count += 1
                        hotel_codes[hotel_code].make_reservation.count += 1
                        if float(file[i][4]) > 0.000001:
                            service.latency.append(float(file[i][4]))
                            service.make_reservation.latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].make_reservation.latency.append(float(file[i][4]))
                        else:
                            service.errors += 1
                            service.make_reservation.errors += 1
                            hotel_codes[hotel_code].errors += 1
                            hotel_codes[hotel_code].make_reservation.errors += 1
                    case "GetReservation":
                        service.get_reservation.count += 1
                        hotel_codes[hotel_code].get_reservation.count += 1
                        if float(file[i][4]) > 0.000001:
                            service.latency.append(float(file[i][4]))
                            service.get_reservation.latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].get_reservation.latency.append(float(file[i][4]))
                        else:
                            service.errors += 1
                            service.get_reservation.errors += 1
                            hotel_codes[hotel_code].errors += 1
                            hotel_codes[hotel_code].get_reservation.errors += 1
                    case "CancelReservation":
                        service.cancel_reservation.count += 1
                        hotel_codes[hotel_code].cancel_reservation.count += 1
                        if float(file[i][4]) > 0.000001:
                            service.latency.append(float(file[i][4]))
                            service.cancel_reservation.latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].cancel_reservation.latency.append(float(file[i][4]))
                        else:
                            service.errors += 1
                            service.cancel_reservation.errors += 1
                            hotel_codes[hotel_code].errors += 1
                            hotel_codes[hotel_code].cancel_reservation.errors += 1
                    case "ModifyReservation":
                        service.modify_reservation.count += 1
                        hotel_codes[hotel_code].modify_reservation.count += 1
                        if float(file[i][4]) > 0.000001:
                            service.latency.append(float(file[i][4]))
                            service.modify_reservation.latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].latency.append(float(file[i][4]))
                            hotel_codes[hotel_code].modify_reservation.latency.append(float(file[i][4]))
                        else:
                            service.errors += 1
                            service.modify_reservation.errors += 1
                            hotel_codes[hotel_code].errors += 1
                            hotel_codes[hotel_code].modify_reservation.errors += 1
            f.close()

    with open(f'./log_stats/stats.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow([
            "Service",
            "Total Count",
            "Error Count",
            "Average Latency",
            "Rate Calendar Count",
            "Rate Calendar Error Count",
            "Rate Calendar Average Latency",
            "Rate Calendar Requests > 2s",
            "Rate Calendar Requests > 5s",
            "Rate Calendar Requests > 10s",
            "Availability Count",
            "Availability Error Count",
            "Availability Average Latency",
            "Availability Requests > 2s",
            "Availability Requests > 5s",
            "Availability Requests > 10s",
            "Room Details Count",
            "Room Details Error Count",
            "Room Details Average Latency",
            "Room Details Requests > 2s",
            "Room Details Requests > 5s",
            "Room Details Requests > 10s",
            "Addons Count",
            "Addons Error Count",
            "Addons Average Latency",
            "Addons Requests > 2s",
            "Addons Requests > 5s",
            "Addons Requests > 10s",
            "Package Count",
            "Package Error Count",
            "Package Average Latency",
            "Package Requests > 2s",
            "Package Requests > 5s",
            "Package Requests > 10s",
            "Room Rate Count",
            "Room Rate Error Count",
            "Room Rate Average Latency",
            "Room Rate Requests > 2s",
            "Room Rate Requests > 5s",
            "Room Rate Requests > 10s",
            "Make Reservation Count",
            "Make Reservation Error Count",
            "Make Reservation Average Latency",
            "Make Reservation Requests > 2s",
            "Make Reservation Requests > 5s",
            "Make Reservation Requests > 10s",
            "Get Reservation Count",
            "Get Reservation Error Count",
            "Get Reservation Average Latency",
            "Get Reservation Requests > 2s",
            "Get Reservation Requests > 5s",
            "Get Reservation Requests > 10s",
            "Cancel Reservation Count",
            "Cancel Reservation Error Count",
            "Cancel Reservation Average Latency",
            "Cancel Reservation Requests > 2s",
            "Cancel Reservation Requests > 5s",
            "Cancel Reservation Requests > 10s",
            "Modify Reservation Count",
            "Modify Reservation Error Count",
            "Modify Reservation Average Latency"
            "Modify Reservation Requests > 2s",
            "Modify Reservation Requests > 5s",
            "Modify Reservation Requests > 10s",
        ])
        for service in services.values():
            writer.writerow([
                service.name,
                service.count,
                service.errors,
                sum(service.latency) / len(service.latency) if len(service.latency) > 0 else 0,
                service.rate_calendar.count,
                service.rate_calendar.errors,
                sum(service.rate_calendar.latency) / len(service.rate_calendar.latency) if len(service.rate_calendar.latency) > 0 else 0,
                sum(1 for e in service.rate_calendar.latency if e > 2),
                sum(1 for e in service.rate_calendar.latency if e > 5),
                sum(1 for e in service.rate_calendar.latency if e > 10),
                service.availability.count,
                service.availability.errors,
                sum(service.availability.latency) / len(service.availability.latency) if len(service.availability.latency) > 0 else 0,
                sum(1 for e in service.availability.latency if e > 2),
                sum(1 for e in service.availability.latency if e > 5),
                sum(1 for e in service.availability.latency if e > 10),
                service.room_details.count,
                service.room_details.errors,
                sum(service.room_details.latency) / len(service.room_details.latency) if len(service.room_details.latency) > 0 else 0,
                sum(1 for e in service.room_details.latency if e > 2),
                sum(1 for e in service.room_details.latency if e > 5),
                sum(1 for e in service.room_details.latency if e > 10),
                service.addons.count,
                service.addons.errors,
                sum(service.addons.latency) / len(service.addons.latency) if len(service.addons.latency) > 0 else 0,
                sum(1 for e in service.addons.latency if e > 2),
                sum(1 for e in service.addons.latency if e > 5),
                sum(1 for e in service.addons.latency if e > 10),
                service.package.count,
                service.package.errors,
                sum(service.package.latency) / len(service.package.latency) if len(service.package.latency) > 0 else 0,
                sum(1 for e in service.package.latency if e > 2),
                sum(1 for e in service.package.latency if e > 5),
                sum(1 for e in service.package.latency if e > 10),
                service.room_rate.count,
                service.room_rate.errors,
                sum(service.room_rate.latency) / len(service.room_rate.latency) if len(service.room_rate.latency) > 0 else 0,
                sum(1 for e in service.room_rate.latency if e > 2),
                sum(1 for e in service.room_rate.latency if e > 5),
                sum(1 for e in service.room_rate.latency if e > 10),
                service.make_reservation.count,
                service.make_reservation.errors,
                sum(service.make_reservation.latency) / len(service.make_reservation.latency) if len(service.make_reservation.latency) > 0 else 0,
                sum(1 for e in service.make_reservation.latency if e > 2),
                sum(1 for e in service.make_reservation.latency if e > 5),
                sum(1 for e in service.make_reservation.latency if e > 10),
                service.get_reservation.count,
                service.get_reservation.errors,
                sum(service.get_reservation.latency) / len(service.get_reservation.latency) if len(service.get_reservation.latency) > 0 else 0,
                sum(1 for e in service.get_reservation.latency if e > 2),
                sum(1 for e in service.get_reservation.latency if e > 5),
                sum(1 for e in service.get_reservation.latency if e > 10),
                service.cancel_reservation.count,
                service.cancel_reservation.errors,
                sum(service.cancel_reservation.latency) / len(service.cancel_reservation.latency) if len(service.cancel_reservation.latency) > 0 else 0,
                sum(1 for e in service.cancel_reservation.latency if e > 2),
                sum(1 for e in service.cancel_reservation.latency if e > 5),
                sum(1 for e in service.cancel_reservation.latency if e > 10),
                service.modify_reservation.count,
                service.modify_reservation.errors,
                sum(service.modify_reservation.latency) / len(service.modify_reservation.latency) if len(service.modify_reservation.latency) > 0 else 0,
                sum(1 for e in service.modify_reservation.latency if e > 2),
                sum(1 for e in service.modify_reservation.latency if e > 5),
                sum(1 for e in service.modify_reservation.latency if e > 10),
            ])

        writer.writerow([])
        writer.writerow([])
        writer.writerow([
            "HotelCode",
            "Service",
            "Total Count",
            "Error Count",
            "Average Latency",
            "Rate Calendar Count",
            "Rate Calendar Error Count",
            "Rate Calendar Average Latency",
            "Rate Calendar Requests > 2s",
            "Rate Calendar Requests > 5s",
            "Rate Calendar Requests > 10s",
            "Availability Count",
            "Availability Error Count",
            "Availability Average Latency",
            "Availability Requests > 2s",
            "Availability Requests > 5s",
            "Availability Requests > 10s",
            "Room Details Count",
            "Room Details Error Count",
            "Room Details Average Latency",
            "Room Details Requests > 2s",
            "Room Details Requests > 5s",
            "Room Details Requests > 10s",
            "Addons Count",
            "Addons Error Count",
            "Addons Average Latency",
            "Addons Requests > 2s",
            "Addons Requests > 5s",
            "Addons Requests > 10s",
            "Package Count",
            "Package Error Count",
            "Package Average Latency",
            "Package Requests > 2s",
            "Package Requests > 5s",
            "Package Requests > 10s",
            "Room Rate Count",
            "Room Rate Error Count",
            "Room Rate Average Latency",
            "Room Rate Requests > 2s",
            "Room Rate Requests > 5s",
            "Room Rate Requests > 10s",
            "Make Reservation Count",
            "Make Reservation Error Count",
            "Make Reservation Average Latency",
            "Make Reservation Requests > 2s",
            "Make Reservation Requests > 5s",
            "Make Reservation Requests > 10s",
            "Get Reservation Count",
            "Get Reservation Error Count",
            "Get Reservation Average Latency",
            "Get Reservation Requests > 2s",
            "Get Reservation Requests > 5s",
            "Get Reservation Requests > 10s",
            "Cancel Reservation Count",
            "Cancel Reservation Error Count",
            "Cancel Reservation Average Latency",
            "Cancel Reservation Requests > 2s",
            "Cancel Reservation Requests > 5s",
            "Cancel Reservation Requests > 10s",
            "Modify Reservation Count",
            "Modify Reservation Error Count",
            "Modify Reservation Average Latency"
            "Modify Reservation Requests > 2s",
            "Modify Reservation Requests > 5s",
            "Modify Reservation Requests > 10s",
        ])
        for hotel_code, service in hotel_codes.items():
            writer.writerow([
                hotel_code,
                service.name,
                service.count,
                service.errors,
                sum(service.latency) / len(service.latency) if len(service.latency) > 0 else 0,
                service.rate_calendar.count,
                service.rate_calendar.errors,
                sum(service.rate_calendar.latency) / len(service.rate_calendar.latency) if len(service.rate_calendar.latency) > 0 else 0,
                sum(1 for e in service.rate_calendar.latency if e > 2),
                sum(1 for e in service.rate_calendar.latency if e > 5),
                sum(1 for e in service.rate_calendar.latency if e > 10),
                service.availability.count,
                service.availability.errors,
                sum(service.availability.latency) / len(service.availability.latency) if len(service.availability.latency) > 0 else 0,
                sum(1 for e in service.availability.latency if e > 2),
                sum(1 for e in service.availability.latency if e > 5),
                sum(1 for e in service.availability.latency if e > 10),
                service.room_details.count,
                service.room_details.errors,
                sum(service.room_details.latency) / len(service.room_details.latency) if len(service.room_details.latency) > 0 else 0,
                sum(1 for e in service.room_details.latency if e > 2),
                sum(1 for e in service.room_details.latency if e > 5),
                sum(1 for e in service.room_details.latency if e > 10),
                service.addons.count,
                service.addons.errors,
                sum(service.addons.latency) / len(service.addons.latency) if len(service.addons.latency) > 0 else 0,
                sum(1 for e in service.addons.latency if e > 2),
                sum(1 for e in service.addons.latency if e > 5),
                sum(1 for e in service.addons.latency if e > 10),
                service.package.count,
                service.package.errors,
                sum(service.package.latency) / len(service.package.latency) if len(service.package.latency) > 0 else 0,
                sum(1 for e in service.package.latency if e > 2),
                sum(1 for e in service.package.latency if e > 5),
                sum(1 for e in service.package.latency if e > 10),
                service.room_rate.count,
                service.room_rate.errors,
                sum(service.room_rate.latency) / len(service.room_rate.latency) if len(service.room_rate.latency) > 0 else 0,
                sum(1 for e in service.room_rate.latency if e > 2),
                sum(1 for e in service.room_rate.latency if e > 5),
                sum(1 for e in service.room_rate.latency if e > 10),
                service.make_reservation.count,
                service.make_reservation.errors,
                sum(service.make_reservation.latency) / len(service.make_reservation.latency) if len(service.make_reservation.latency) > 0 else 0,
                sum(1 for e in service.make_reservation.latency if e > 2),
                sum(1 for e in service.make_reservation.latency if e > 5),
                sum(1 for e in service.make_reservation.latency if e > 10),
                service.get_reservation.count,
                service.get_reservation.errors,
                sum(service.get_reservation.latency) / len(service.get_reservation.latency) if len(service.get_reservation.latency) > 0 else 0,
                sum(1 for e in service.get_reservation.latency if e > 2),
                sum(1 for e in service.get_reservation.latency if e > 5),
                sum(1 for e in service.get_reservation.latency if e > 10),
                service.cancel_reservation.count,
                service.cancel_reservation.errors,
                sum(service.cancel_reservation.latency) / len(service.cancel_reservation.latency) if len(service.cancel_reservation.latency) > 0 else 0,
                sum(1 for e in service.cancel_reservation.latency if e > 2),
                sum(1 for e in service.cancel_reservation.latency if e > 5),
                sum(1 for e in service.cancel_reservation.latency if e > 10),
                service.modify_reservation.count,
                service.modify_reservation.errors,
                sum(service.modify_reservation.latency) / len(service.modify_reservation.latency) if len(service.modify_reservation.latency) > 0 else 0,
                sum(1 for e in service.modify_reservation.latency if e > 2),
                sum(1 for e in service.modify_reservation.latency if e > 5),
                sum(1 for e in service.modify_reservation.latency if e > 10),
            ])


def filter_logs(hotel_code: str):
    csv.field_size_limit(sys.maxsize)
    outfile = []
    for file in os.listdir("./full_logs"):
        with open(f"./full_logs/{file}", newline='') as f:
            reader = csv.reader(f, delimiter=',')
            file = []
            for row in reader:
                file.append(row)

            header = file[0]
            for i in range(1, len(file)):
                if file[i][1] == hotel_code:
                    outfile.append(file[i])
    
    with open(f'./log_stats/filtered_logs.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in outfile:
            writer.writerow(row)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--create-logs', type=bool, help='If set to true, will create logs from cloudwatch and save to csv', default=False)
    arg_parser.add_argument('--parse-logs', type=bool, help='If set to true, will parse logs from csv files', default=False)
    arg_parser.add_argument('--filter-logs', type=str, help='Filters the logs by the given hotel_code', default='')
    cw_log = CloudwatchLog("concierge-prod")
    start_date =  - timedelta(minutes=30)
    end_date = datetime.now()
    if arg_parser.parse_args().create_logs:
        create_logs(cw_log)
    if arg_parser.parse_args().parse_logs:
        parse_logs()
    if arg_parser.parse_args().filter_logs:
        filter_logs(arg_parser.parse_args().filter_logs)