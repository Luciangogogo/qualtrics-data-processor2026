import requests
import time
import logging
from ..config.settings import get_config

logger = logging.getLogger(__name__)

class QualtricsAPI:
    def __init__(self):
        self.config = get_config()
        self.headers = {
            "x-api-token": self.config.QUALTRICS_API_TOKEN,
            "content-type": "application/json"
        }
        self.base_url = f"https://{self.config.QUALTRICS_DATA_CENTER}.qualtrics.com/API/v3"

    def start_export(self, survey_id: str, export_format: str = "csv"):
        url = f"{self.base_url}/surveys/{survey_id}/export-responses/"

        try:
            response = requests.post(
                url,
                headers=self.headers,
                json={"format": export_format},
                timeout=self.config.API_TIMEOUT
            )
            response.raise_for_status()
            return response.json()["result"]["progressId"]
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to start export for survey {survey_id}: {e}")
            raise

    def get_survey_responses(self, survey_id: str, export_format: str = "csv"):
        return self.start_export(survey_id, export_format)

    def get_survey_questions(self, survey_id: str):
        url = f"{self.base_url}/survey-definitions/{survey_id}"

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()["result"]
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get survey questions for {survey_id}: {e}")
            raise

    def test_connection(self):
        url = f"{self.base_url}/whoami"

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"API connection test failed: {e}")
            return False