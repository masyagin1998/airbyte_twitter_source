#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from datetime import datetime
from time import sleep
from typing import Any, Iterable, List, Mapping, Tuple, MutableMapping, Optional

import requests
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

URL_BASE = "https://api.twitter.com"


class Tweets(HttpStream):
    url_base = URL_BASE
    cursor_field = "ds"
    primary_key = "ds"

    @staticmethod
    def __datetime_to_str(d: datetime) -> str:
        return d.strftime("%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def __str_to_datetime(d: str) -> datetime:
        return datetime.strptime(d, "%Y-%m-%dT%H:%M:%S")

    # noinspection PyUnusedLocal
    def __init__(self, config, **kwargs):
        super().__init__(**kwargs)
        self.__config = config

        self.__logger = AirbyteLogger()

        self.__count_in_req = 0
        self.__count = 0
        self.__total_count = 0

        self.__cursor_value = self.__str_to_datetime(self.__config["start_time"])
        self.__tmp_cursor_value = self.__str_to_datetime(self.__config["start_time"])

        self.__logger.info("Read latest review timestamp from config: {}".format(self.__config["start_time"]))

    @property
    def state(self) -> Mapping[str, Any]:
        ds = self.__datetime_to_str(self.__tmp_cursor_value)
        self.__logger.info("Saved latest review timestamp to file: {}".format(ds))
        return {self.cursor_field: ds}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        ds = value.get(self.cursor_field)
        if ds is not None:
            self.__logger.info("Read latest review timestamp from file: {}".format(ds))
            self.__cursor_value = self.__str_to_datetime(ds)
            self.__tmp_cursor_value = self.__str_to_datetime(ds)

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            ds = record.get("ds")
            if ds is not None:
                self.__tmp_cursor_value = max(self.__tmp_cursor_value, ds)
            yield record

    http_method = "GET"

    def path(
            self,
            *,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return "/2/tweets/search/recent"

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        mentions = ["@" + m for m in self.__config["mentions"]]
        params = {
            "query": " OR ".join(mentions),
            "tweet.fields": "author_id,created_at,lang",
            "start_time": self.__datetime_to_str(self.__cursor_value) + "Z"
        }
        max_results = self.__config.get("max_tweets_per_req")
        if max_results is not None:
            params["max_results"] = max_results
        if next_page_token is not None:
            params["pagination_token"] = next_page_token
        return params

    def request_headers(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {
            "User-Agent": "v2TweetLookupPython"
        }

    @staticmethod
    def __fetch_tweets(response: requests.Response):
        return response.json().get("data", [])

    @staticmethod
    def __transform(v: dict):
        v1 = {
            "source": "Twitter",
            "lang": v["lang"],
            "country": "unknown",
            "ticket_id": v["id"],
            "user_id": v["author_id"],
            "ds": datetime.strptime(v["created_at"][:19], "%Y-%m-%dT%H:%M:%S"),
            "version": "unknown",
            "message": v["text"],
            "score": "unknown",
        }

        return v1

    def parse_response(
            self,
            response: requests.Response,
            *,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        result = []
        tweets = self.__fetch_tweets(response)
        for tweet in tweets:
            v = self.__transform(tweet)
            ds = v.get("ds")
            if (ds is None) or (ds > self.__cursor_value):
                result.append(v)
        self.__count_in_req = len(result)
        self.__count += self.__count_in_req
        return result

    def __fetch_next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        token = response.json().get("meta", {}).get("next_token")
        if token is None:
            self.__logger.info("Totally fetched {} reviews".format(self.__total_count + self.__count))
        return token

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        timeout_ms = self.__config.get("timeout_ms")
        if timeout_ms is not None:
            sleep(timeout_ms / 1000.0)
        return self.__fetch_next_page_token(response)


class SourceTwitter(AbstractSource):
    @staticmethod
    def __get_user(bearer_token: str, name: str) -> bool:
        path = "/2/users/by"
        params = {"usernames": name}

        def bearer_oauth(r):
            r.headers["Authorization"] = f"Bearer {bearer_token}"
            r.headers["User-Agent"] = "v2UserLookupPython"
            return r

        response = requests.get(URL_BASE + path, params=params, auth=bearer_oauth)
        return response.ok

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        logger.info("Checking connection configuration...")

        logger.info("Checking \"bearer_token\"...")
        ok = self.__get_user(config["bearer_token"], "TwitterAPI")
        if not ok:
            error_text = "\"bearer_token\" is invalid!"
            logger.error(error_text)
            return False, {"key": "bearer_token", "error_text": error_text}
        logger.info("\"bearer_token\" is valid")

        logger.info("Checking \"mentions\" values...")
        for ind, m in enumerate(config["mentions"]):
            logger.info("Checking \"mentions\" {}'th value \"{}\"".format(ind, m))
            ok = self.__get_user(config["bearer_token"], m)
            if not ok:
                error_text = "\"mentions\" {}'th value \"{}\" is invalid!".format(ind, m)
                logger.error(error_text)
                return False, {"key": "mentions", "value": config["mentions"], "error_text": error_text}
            logger.info("\"mentions\" {}'th value \"{}\" is valid".format(ind, m))
        logger.info("\"mentions\" values are valid")

        logger.info("Connection configuration is valid!")
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(config["bearer_token"])
        return [Tweets(authenticator=auth, config=config)]
