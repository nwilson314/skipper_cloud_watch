import ast
import dataclasses
from datetime import datetime
import time
import json
import sys
import os
from enum import Enum
import csv
from collections import defaultdict

import boto3
from botocore import config

config = config.Config(
    retries = dict(
        max_attempts = 10
    )
)


class MessageType(str, Enum):
    UNDEFINED = "UNDEFINED"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    EXCEPTION = "EXCEPTION"
    CRITICAL = "CRITICAL"


@dataclasses.dataclass
class FormattedMessage:
    request_uuid: str | None = None
    service: str | None = None
    function: str | None = None
    message: str | None = None


@dataclasses.dataclass
class Log:
    time: datetime
    source: str
    message_type: MessageType
    formatted_message: FormattedMessage | None = None
    # event_id: str | None = None


class ServiceType(str, Enum):
    windsurfer = "windsurfer"
    sabre = "sabre"
    oracle = "oracle"


class ApiType(str, Enum):
    GetRateCalendar = "GetRateCalendar"
    GetAvailability = "GetAvailability"
    GetRoomDetails = "GetRoomDetails"
    GetAddons = "GetAddons"
    GetPackage = "GetPackage"
    GetRoomRate = "GetRoomRate"
    MakeReservation = "MakeReservation"
    GetReservation = "GetReservation"
    CancelReservation = "CancelReservation"
    ModifyReservation = "ModifyReservation"
    CreateConfig = "CreateConfig"
    ReadConfig = "ReadConfig"
    UpdateConfig = "UpdateConfig"
    DeleteConfig = "DeleteConfig"
    ListAllConfigs = "ListAllConfigs"

@dataclasses.dataclass
class LogStats:
    request_uuid: str
    initial_time: datetime
    service: ServiceType
    downstream_messages: list[tuple[datetime, str, MessageType]]
    api_type: ApiType
    end_to_end_latency: str
    hotel_code: str | None = None


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
    get_pricing_info: EndPoint
    avg_latency: float = 0
    latency: list[float] = dataclasses.field(default_factory=lambda : [])
    count: int = 0
    errors: int = 0


class CloudwatchLog:
    def __init__(self, log_stream_name: str=None):
        self.cloudwatch_logs = boto3.client("logs", region_name="us-east-2", config=config)
        self.log_streams = [
            stream
            for stream in self.get_log_streams()
            if log_stream_name and stream.__contains__(log_stream_name) and not stream.__contains__("gfbl")
        ]
        print(self.log_streams)
        self.services = [
            "windsurfer",
            "sabre",
            "oracle"
        ]

    def get_log_streams(self) -> list[str]:
        response = self.cloudwatch_logs.describe_log_groups()
        return [group["logGroupName"] for group in response["logGroups"]]
    
    def _parse_formatted_message(self, json_message: dict) -> FormattedMessage:

        calling_function = json_message.get("calling_function", None)
        service = None
        if calling_function:
            for service_ in self.services:
                if service_ in calling_function:
                    service = service_
                    break
        
        return FormattedMessage(
            request_uuid=json_message.get("request_uuid", json_message.get("requestUuid", None)),
            service=service,
            function=calling_function,
            message=json_message
        )

    def _parse_message(self, log_event: dict, source: str) -> Log:
        message = log_event.get("message", "")
        json_message = json.loads(message)

        formatted_message = self._parse_formatted_message(json_message)
        log = Log(
            time=datetime.utcfromtimestamp(log_event["timestamp"] / 1000),
            source=source,
            message_type=MessageType(json_message.get("level", MessageType.UNDEFINED)),
            formatted_message=formatted_message,
        )
        
        return log

    def ingest(
        self, sources: tuple, start_time: datetime, end_time: datetime = None, thread_count=0,
    ) -> list[Log]:
        logs: list[Log] = []
        end_time = end_time or datetime.now()
        for source in sources:
            first = True
            next_token = None
            i = 0
            while first or next_token:
                response = None
                sleep_time = 0.1
                if first:
                    response = self.cloudwatch_logs.filter_log_events(
                        logGroupName=source, startTime=int(start_time.timestamp() * 1000), endTime=int(end_time.timestamp() * 1000)
                    )
                else:
                    while not response:
                        try:
                            response = self.cloudwatch_logs.filter_log_events(
                                logGroupName=source, startTime=int(start_time.timestamp() * 1000), endTime=int(end_time.timestamp() * 1000), nextToken=next_token
                            )
                        except Exception as e:
                            # Sleep to avoid throttling
                            time.sleep(sleep_time)
                            sleep_time = sleep_time * 2
                            response = None
                for log_event in response["events"]:
                    if '"GET / HTTP/1.1" 200' in log_event.get('message'):
                        continue
                    try:
                        parsed_meesage = self._parse_message(log_event, source)
                        logs.append(parsed_meesage)
                    except Exception as e:
                        # print("Failed parsing message", log_event.get("message", ""))
                        pass
                if not i % 5:
                    if response["events"]:
                        try:
                            print(f"Processing {datetime.utcfromtimestamp(log_event['timestamp'] / 1000)}", end="\r")
                        except:
                            pass
                
                next_token = response.get("nextToken", None)
                first = False
                i += 1

        return logs
    

def extract_time_from_log(message: str) -> str:
    message = message.strip()
    time = message.split(" ")[1][:-1]
    return time


def extract_hotel_code_from_log(message: str) -> str:
    # Extract the portion inside the outermost curly braces
    brace_counter = 0
    start_index = None
    end_index = None

    for i, char in enumerate(message):
        if char == '{':
            if brace_counter == 0:
                start_index = i
            brace_counter += 1
        elif char == '}':
            brace_counter -= 1
            if brace_counter == 0:
                end_index = i
                break

    if start_index is not None and end_index is not None:
        content = message[start_index:end_index+1]
    else:
        raise ValueError(f"Mismatched curly braces for content: {message}")
    
    parsed_dict = ast.literal_eval(content)

    hotel = parsed_dict.get('hotel', None)
    if hotel == None:
        # check room details
        availRequest = parsed_dict.get('availabilityRequest', None)
        if availRequest == None:
            # check reservation
            reservation = parsed_dict.get('reservationDetails', None)
            if reservation == None:
                return None
            else:
                hotel = reservation['hotel']
        else:
            hotel = availRequest["hotel"]

    ims_hotel_id = hotel['imsHotelId']

    return ims_hotel_id


class CloudwatchStats:
    def __init__(self, basic_logs_path: str="./basic_logs"):
        self.basic_logs_path = basic_logs_path

    def write_basic_timing_log(self, logs: list[Log], name: str, header: list[str]) -> None:

        with open(f'./basic_logs/{name}.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for log in logs:
                stats: dict = log.formatted_message.message.get("stats", None)
                if stats:
                    timing = stats.get("time", None)
                    if timing:
                        row = [
                            log.time,
                            log.formatted_message.request_uuid,
                            log.formatted_message.service,
                            log.formatted_message.function,
                            timing
                        ]
                        writer.writerow(row)



    def write_full_log(self, stats: list[LogStats], name: str, header: list[str]) -> None:
        with open(f'./full_logs/{name}.csv', 'w', newline='', encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=',')
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


    def get_log_stats(self, logs: list[Log]) -> list[LogStats]:
        log_dict: dict[str, list[Log]] = defaultdict(list)
        for log in logs:
            if log.formatted_message.request_uuid:
                log_dict[log.formatted_message.request_uuid].append(log)

        log_stats: list[LogStats] = []
        for request_uuid, log_list in log_dict.items():
            downstream_messages = []
            latency = "0"
            hotel_code = "0"
            for log in log_list:
                stats: dict = log.formatted_message.message.get("stats", {})
                if stats:
                    latency = str(stats.get("time", "0"))
                if "GRPC Request" in log.formatted_message.message["message"]:
                    hotel_code = extract_hotel_code_from_log(log.formatted_message.message["message"])
                downstream_messages.append((log.time, log.formatted_message.service, log.message_type, log.formatted_message.function))
            service = ""
            api_type = ""
            for message in downstream_messages:
                if service and api_type:
                    break
                service_ = message[1]
                function_ = message[3]
                if not service and service_:
                    if "windsurfer" in service_:
                        service = "windsurfer"
                    if "sabre" in service_:
                        service = "sabre"
                    if "oracle" in service_:
                        service = "oracle"
                if not api_type and function_:
                    if "GetRateCalendar" in function_:
                        api_type = "GetRateCalendar"
                    if "GetAvailability" in function_:
                        api_type = "GetAvailability"
                    if "GetRoomDetails" in function_:
                        api_type = "GetRoomDetails"
                    if "GetAddons" in function_:
                        api_type = "GetAddons"
                    if "GetPackage" in function_:
                        api_type = "GetPackage"
                    if "GetRoomRate" in function_:
                        api_type = "GetRoomRate"
                    if "MakeReservation" in function_:
                        api_type = "MakeReservation"
                    if "GetReservation" in function_:
                        api_type = "GetReservation"
                    if "CancelReservation" in function_:
                        api_type = "CancelReservation"
                    if "ModifyReservation" in function_:
                        api_type = "ModifyReservation"
                    if "CreateConfig" in function_:
                        api_type = "CreateConfig"
                    if "ReadConfig" in function_:
                        api_type = "ReadConfig"
                    if "UpdateConfig" in function_:
                        api_type = "UpdateConfig"
                    if "DeleteConfig" in function_:
                        api_type = "DeleteConfig"
                    if "ListAllConfigs" in function_:
                        api_type = "ListAllConfigs"
                    if "GetPricingInfo" in function_:
                        api_type = "GetPricingInfo"
                
            try:
                log_stat = LogStats(
                    request_uuid=request_uuid,
                    initial_time=downstream_messages[0][0],
                    service=service,
                    downstream_messages=downstream_messages,
                    end_to_end_latency=latency,
                    api_type=api_type,
                    hotel_code=hotel_code
                )
                log_stats.append(
                    log_stat
                )
            except Exception as e:
                print(e)
                pass

        return log_stats
    

    def parse_logs(self, cw_log: CloudwatchLog):
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
            modify_reservation=EndPoint(name="modify_reservation"),
            get_pricing_info=EndPoint(name="get_pricing_info")
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
            modify_reservation=EndPoint(name="modify_reservation"),
            get_pricing_info=EndPoint(name="get_pricing_info")
        )
        oracle = Service(
            name="oracle",
            rate_calendar=EndPoint(name="rate_calendar"),
            availability=EndPoint(name="availability"),
            room_details=EndPoint(name="room_details"),
            addons=EndPoint(name="addons"),
            package=EndPoint(name="package"),
            room_rate=EndPoint(name="room_rate"),
            make_reservation=EndPoint(name="make_reservation"),
            get_reservation=EndPoint(name="get_reservation"),
            cancel_reservation=EndPoint(name="cancel_reservation"),
            modify_reservation=EndPoint(name="modify_reservation"),
            get_pricing_info=EndPoint(name="get_pricing_info")
        )

        services = {
            "windsurfer": ws,
            "sabre": sabre,
            "oracle": oracle
        }
        hotel_codes = dict()
        for file_ in os.listdir("./full_logs"):
            if not file_.endswith(".csv"):
                continue
            with open(f"./full_logs/{file_}", newline='', encoding="utf-8") as f:
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
                            modify_reservation=EndPoint(name="modify_reservation"),
                            get_pricing_info=EndPoint(name="get_pricing_info")
                        )
                    
                    hotel_codes[hotel_code].count += 1
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
                        case "GetPricingInfo":
                            service.get_pricing_info.count += 1
                            hotel_codes[hotel_code].get_pricing_info.count += 1
                            if float(file[i][4]) > 0.000001:
                                service.latency.append(float(file[i][4]))
                                service.get_pricing_info.latency.append(float(file[i][4]))
                                hotel_codes[hotel_code].latency.append(float(file[i][4]))
                                hotel_codes[hotel_code].get_pricing_info.latency.append(float(file[i][4]))
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
                "Get Pricing Info Count",
                "Get Pricing Info Error Count",
                "Get Pricing Info Average Latency"
                "Get Pricing Info Requests > 2s",
                "Get Pricing Info Requests > 5s",
                "Get Pricing Info Requests > 10s",
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
                    service.get_pricing_info.count,
                    service.get_pricing_info.errors,
                    sum(service.get_pricing_info.latency) / len(service.get_pricing_info.latency) if len(service.get_pricing_info.latency) > 0 else 0,
                    sum(1 for e in service.get_pricing_info.latency if e > 2),
                    sum(1 for e in service.get_pricing_info.latency if e > 5),
                    sum(1 for e in service.get_pricing_info.latency if e > 10),
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
                "Get Pricing Info Count",
                "Get Pricing Info Error Count",
                "Get Pricing Info Average Latency"
                "Get Pricing Info Requests > 2s",
                "Get Pricing Info Requests > 5s",
                "Get Pricing Info Requests > 10s",
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
                    service.get_pricing_info.count,
                    service.get_pricing_info.errors,
                    sum(service.get_pricing_info.latency) / len(service.get_pricing_info.latency) if len(service.get_pricing_info.latency) > 0 else 0,
                    sum(1 for e in service.get_pricing_info.latency if e > 2),
                    sum(1 for e in service.get_pricing_info.latency if e > 5),
                    sum(1 for e in service.get_pricing_info.latency if e > 10),
                ])

    