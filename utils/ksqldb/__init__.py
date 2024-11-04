import sys
import requests

from urllib.parse import urlencode
from requests.auth import HTTPBasicAuth

from utils import log_exception

class KsqlDB:
    def __init__(
            self,
            end_point:str = "http://localhost:8088",
            username:str = "admin",
            password:str = "admin"
           
    ):
        self.end_point = end_point.strip("/")
        if None not in (username, password):
            self.auth = HTTPBasicAuth(username, password)
        else:
            self.auth = None
    
    def _request(
            self,
            method:str = "GET",
            path: str = "",
            query: dict = None,
            headers: dict = None,
            json: dict = None
    ) -> tuple:
        if not isinstance(headers, dict):
            headers = dict()
        headers_request = {
            "Content-Type": "application/vnd.ksql.v1+json",
            "Accept": "application/vnd.ksql.v1+json",
        }
        headers_request.update(headers)
        if isinstance(query, dict):
            query_string = f"?{urlencode(query)}"
        else:
            quaery_string = ""
        url = f"""{self.end_point}/{path.lstrip("/")}{query_string}"""
        try:
            if method == "GET":
                response = requests.get(
                    url,
                    auth=self.auth,
                    headers=headers_request
                )
            elif method == "POST":
                response = requests.post(
                    url,
                    auth=self.auth,
                    headers=headers_request,
                    json=json
                )
            else:
                raise ValueError(f"Method {method} not supported")
            return response.status_code, response.json()
        except requests.exceptions.Timeout:
            response = None
            status_code = 408

        except Exception:
            response = None
            try:
                status_code = r.status_code
            except Exception:
                status_code = 502
            log_exception(
                f"Unable to send {method} request ({status_code}): {url}",
                sys.exc_info(),
            )

        finally:
            return status_code, response

    def query(self, query: dict) -> tuple:
        return self._request(
            path="ksql",
            json=query,
        )
            

