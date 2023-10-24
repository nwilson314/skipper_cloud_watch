import ast
from collections import defaultdict
import dataclasses
from datetime import datetime, timedelta
from enum import Enum
import time

import boto3
from botocore import config

config = config.Config(
    retries = dict(
        max_attempts = 10
    )
)

class MessageType(str, Enum):
    DEBUG = "DEBUG"
    ERROR = "ERROR"


class ServiceType(str, Enum):
    windsurfer = "windsurfer"
    sabre = "sabre"


class DownstreamType(str, Enum):
    request = "request"
    response = "response"
    unknown = "unknown"

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
class FormattedMessage:
    request_uuid: str | None = None
    service: str | None = None
    message: str | None = None



@dataclasses.dataclass
class Log:
    time: datetime
    source: str
    message: str
    message_type: MessageType
    formatted_message: FormattedMessage | None = None
    event_id: str | None = None


@dataclasses.dataclass
class LogStats:
    request_uuid: str
    service: ServiceType
    downstream_messages: list[tuple[datetime, str, MessageType]]
    api_type: ApiType
    end_to_end_latency: str
    hotel_code: str | None = None


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


class CloudwatchLog:
    def __init__(self, log_stream_name: str=None):
        self.cloudwatch_logs = boto3.client("logs", region_name="us-east-2", config=config)
        self.log_streams = [
            stream
            for stream in self.get_log_streams()
            if log_stream_name and stream.__contains__(log_stream_name)
        ]

    def _parse_formatted_message(self, message: str) -> str:
        service_lst = []
        for k, char in enumerate(message):
            if char != "-":
                service_lst.append(char)
            else:
                break
        service = "".join(service_lst)
        stack = []
        request_uuid = []
        req_id = ""
        for i in range(k, len(message)):
            if not stack and message[i] != "{": continue
            if message[i] == "{":
                stack.append("{")
                request_uuid.append(message[i])
            elif message[i] == "}":
                stack.pop()
                request_uuid.append(message[i])
                expr = ast.literal_eval("".join(request_uuid))
                if not stack and isinstance(expr, dict):
                    if 'request_uuid' in expr:
                        req_id = expr['request_uuid']
                    if 'requestUuid' in expr:
                        req_id = expr['requestUuid']
                    break
                else:
                    raise ValueError(f"Invalid Message: {expr}")
            else:
                request_uuid.append(message[i])

        formatted_message = message[i+1:]

        return FormattedMessage(
            request_uuid=req_id,
            service=service.strip(),
            message=formatted_message,
        )

    def _parse_message(self, log_event: dict, source: str) -> Log:
        message = log_event.get("message", "")
        slices = message.split("|")
        if len(slices) < 3:
            raise ValueError(f"Invalid Log Message: {message}")
        if len(slices) > 3:
            slices[2] = "|".join(slices[2:])
        
        formatted_message = self._parse_formatted_message(slices[2].strip())

        return Log(
            time=datetime.utcfromtimestamp(log_event["timestamp"] / 1000),
            source=source,
            message_type=MessageType(slices[1].strip()),
            message=message,
            formatted_message=formatted_message,
            event_id=log_event["eventId"],
        )

    def get_log_streams(self) -> list[str]:
        response = self.cloudwatch_logs.describe_log_groups()
        return [group["logGroupName"] for group in response["logGroups"]]
    
    def ingest(
        self, sources: tuple, start_time: datetime, end_time: datetime = None
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
                            time.sleep(sleep_time)
                            sleep_time = sleep_time * 2
                            response = None
                for log_event in response["events"]:
                    if '"GET / HTTP/1.1" 200' in log_event.get('message'):
                        continue
                    try:
                        logs.append(self._parse_message(log_event, source))
                        # logs.append(log_event)
                    except Exception as e:
                        # print("Failed parsing message", log_event.get("message", ""))
                        pass
                if i % 100000:
                    print(f"Processing {datetime.utcfromtimestamp(log_event['timestamp'] / 1000)}", end="\r")
                
                next_token = response.get("nextToken", None)
                first = False
                i += 1

        return logs
    
    def get_log_stats(self, logs: list[Log]) -> list[LogStats]:
        log_dict: dict[str, list[Log]] = defaultdict(list)
        for log in logs:
            log_dict[log.formatted_message.request_uuid].append(log)

        log_stats = []
        for request_uuid, log_list in log_dict.items():
            downstream_messages = []
            latency = "0"
            for log in log_list:
                if "Took" in log.formatted_message.message: 
                    latency = extract_time_from_log(log.formatted_message.message)
                if "GRPC Request" in log.formatted_message.message:
                    hotel_code = extract_hotel_code_from_log(log.formatted_message.message)
                downstream_messages.append((log.time, log.formatted_message.service, log.message_type))
            service = ""
            api_type = ""
            for message in downstream_messages:
                service_ = message[1]
                if "windsurfer" in service_:
                    service = "windsurfer"
                if "sabre" in service_:
                    service = "sabre"
                if "oracle" in service_:
                    service = "oracle"
                if "GetRateCalendar" in service_:
                    api_type = "GetRateCalendar"
                if "GetAvailability" in service_:
                    api_type = "GetAvailability"
                if "GetRoomDetails" in service_:
                    api_type = "GetRoomDetails"
                if "GetAddons" in service_:
                    api_type = "GetAddons"
                if "GetPackage" in service_:
                    api_type = "GetPackage"
                if "GetRoomRate" in service_:
                    api_type = "GetRoomRate"
                if "MakeReservation" in service_:
                    api_type = "MakeReservation"
                if "GetReservation" in service_:
                    api_type = "GetReservation"
                if "CancelReservation" in service_:
                    api_type = "CancelReservation"
                if "ModifyReservation" in service_:
                    api_type = "ModifyReservation"
                if "CreateConfig" in service_:
                    api_type = "CreateConfig"
                if "ReadConfig" in service_:
                    api_type = "ReadConfig"
                if "UpdateConfig" in service_:
                    api_type = "UpdateConfig"
                if "DeleteConfig" in service_:
                    api_type = "DeleteConfig"
                if "ListAllConfigs" in service_:
                    api_type = "ListAllConfigs"
            
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
            except:
                pass

        return log_stats