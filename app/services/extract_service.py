import zipfile
import io
import pandas as pd
import logging
import time
import requests
from datetime import datetime, timezone

from .qualtrics_api import QualtricsAPI
from ..config.database import db_manager
from ..config.settings import get_config
from ..utils.file_utils import calculate_file_hash, generate_filename

logger = logging.getLogger(__name__)


class DataExtractionService:
    def __init__(self):
        self.config = get_config()
        self.api_client = QualtricsAPI()

    def extract_survey_responses(self, survey_id: str):
        """Single Response"""
        file_name = generate_filename(survey_id)
        file_path = self.config.DATA_DIR / file_name

        try:
            logger.info(f"[{survey_id}] Starting survey responses extraction...")

            file_content = self._execute_full_export(survey_id)

            # Save as CSV
            with zipfile.ZipFile(io.BytesIO(file_content)) as zip_file:
                csv_filename = zip_file.namelist()[0]
                with zip_file.open(csv_filename) as f:
                    df = pd.read_csv(f)

            # Save to data directory
            df.to_csv(file_path, index=False)
            logger.info(f"[{survey_id}] Survey responses data saved to {file_path}")

            # Success logging
            self._log_responses_extraction_result(survey_id, file_name, file_path, success=True)

            return {
                "success": True,
                "file_path": str(file_path),
                "file_name": file_name,
                "records_count": len(df)
            }

        except Exception as e:
            error_msg = f"Failed to extract survey responses: {str(e)}"
            logger.error(f"[{survey_id}] {error_msg}")

            return {
                "success": False,
                "error": error_msg
            }

    def extract_survey_definitions(self, survey_id: str):
        """Single survey definitions and mappings, only when field_mapping is null """
        try:
            logger.info(f"[{survey_id}] Checking survey definitions status...")

            if self._has_existing_field_mapping(survey_id):
                logger.info(f"[{survey_id}] Survey definitions already exist, skipping extraction")
                return {
                    "success": True,
                    "action": "skipped",
                    "reason": "field_mapping_already_exists",
                    "survey_name": None,
                    "questions": None,
                    "questions_count": 0
                }

            logger.info(f"[{survey_id}] Field mapping is empty, extracting survey definitions...")

            # Get Survey definitions from Qualtrics API (includes SurveyName and Questions)
            survey_data = self.api_client.get_survey_questions(survey_id)

            # Extract survey name and questions
            survey_name = survey_data.get("SurveyName", "")
            questions = survey_data.get("Questions", {})

            logger.info(f"[{survey_id}] Survey name: {survey_name}")
            logger.info(f"[{survey_id}] Successfully extracted {len(questions)} questions")

            return {
                "success": True,
                "action": "extracted",
                "survey_name": survey_name,
                "questions": questions,
                "questions_count": len(questions)
            }

        except Exception as e:
            error_msg = f"Failed to extract survey definitions: {str(e)}"
            logger.error(f"[{survey_id}] {error_msg}")

            return {
                "success": False,
                "action": "failed",
                "error": error_msg
            }

    def extract_all_surveys(self, organisation_id=None):
        """Multi surveys' responses"""
        try:
            # Get all active survey ids
            survey_ids = self._get_all_survey_ids_from_db(organisation_id)

            if not survey_ids:
                return {"success": False, "error": "No surveys found in database"}

            results = {}
            logger.info(
                f"Starting responses extraction for {len(survey_ids)} surveys from database: {', '.join(survey_ids)}")

            for survey_id in survey_ids:
                results[survey_id] = self.extract_survey_responses(survey_id)

            successful = sum(1 for result in results.values() if result["success"])
            total = len(survey_ids)

            logger.info(f"Responses extraction completed: {successful}/{total} successful")

            return {
                "success": True,
                "data": {
                    "total_surveys": total,
                    "successful_extractions": successful,
                    "failed_extractions": total - successful,
                    "details": results,
                    "survey_ids": survey_ids
                }
            }

        except Exception as e:
            logger.error(f"Failed to extract surveys from database: {e}")
            return {"success": False, "error": str(e)}

    def extract_all_surveys_definitions(self, organisation_id=None):
        """Get all surveys' definitions and mappings (only if field_mapping is null)"""
        try:
            survey_ids = self._get_all_survey_ids_from_db(organisation_id)

            if not survey_ids:
                return {"success": False, "error": "No surveys found in database"}

            results = {}
            logger.info(
                f"Starting definitions extraction for {len(survey_ids)} surveys from database: {', '.join(survey_ids)}")

            for survey_id in survey_ids:
                results[survey_id] = self.extract_survey_definitions(survey_id)

            successful = sum(1 for result in results.values() if result["success"])
            extracted = sum(1 for result in results.values() if result.get("action") == "extracted")
            skipped = sum(1 for result in results.values() if result.get("action") == "skipped")
            total = len(survey_ids)

            logger.info(
                f"Definitions extraction completed: {successful}/{total} successful ({extracted} extracted, {skipped} skipped)")

            return {
                "success": True,
                "data": {
                    "total_surveys": total,
                    "successful_extractions": successful,
                    "extracted_count": extracted,
                    "skipped_count": skipped,
                    "failed_extractions": total - successful,
                    "details": results,
                    "survey_ids": survey_ids
                }
            }

        except Exception as e:
            logger.error(f"Failed to extract survey definitions from database: {e}")
            return {"success": False, "error": str(e)}

    def extract_specific_surveys(self, survey_ids):
        """Get responses for specific surveys"""
        if not survey_ids:
            return {"success": False, "error": "No survey IDs provided"}

        results = {}
        logger.info(f"Starting responses extraction for {len(survey_ids)} specified surveys: {', '.join(survey_ids)}")

        for survey_id in survey_ids:
            results[survey_id] = self.extract_survey_responses(survey_id)

        successful = sum(1 for result in results.values() if result["success"])
        total = len(survey_ids)

        logger.info(f"Specified surveys responses extraction completed: {successful}/{total} successful")

        return {
            "success": True,
            "data": {
                "total_surveys": total,
                "successful_extractions": successful,
                "failed_extractions": total - successful,
                "details": results,
                "survey_ids": survey_ids
            }
        }

    def _execute_full_export(self, survey_id: str):
        """Full export process"""
        try:
            logger.info(f"[{survey_id}] Starting full export process...")

            # Step 1: Launch progress
            progress_id = self.api_client.start_export(survey_id)
            logger.info(f"[{survey_id}] Export started, progress_id: {progress_id}")

            # Step 2: Wait for export completion
            logger.info(f"[{survey_id}] Waiting for export completion...")
            file_id = self._wait_for_export_completion(survey_id, progress_id)
            logger.info(f"[{survey_id}] Export completed, file_id: {file_id}")

            # Step 3: Download files
            logger.info(f"[{survey_id}] Downloading file...")
            file_content = self._download_export_file(survey_id, file_id)
            logger.info(f"[{survey_id}] File downloaded successfully")

            return file_content

        except Exception as e:
            logger.error(f"[{survey_id}] Full export process failed: {e}")
            raise

    def _wait_for_export_completion(self, survey_id: str, progress_id: str):
        waited = 0
        while waited < self.config.EXPORT_POLL_MAX_SECONDS:
            try:
                result = self._check_export_status(survey_id, progress_id)

                if result["status"] == "complete":
                    return result["fileId"]
                elif result["status"] in {"failed", "error"}:
                    raise Exception(f"Export failed: {result}")

                time.sleep(self.config.EXPORT_POLL_INTERVAL)
                waited += self.config.EXPORT_POLL_INTERVAL

            except Exception as e:
                logger.error(f"Error while waiting for export completion: {e}")
                raise

        raise TimeoutError(f"Export timed out after {self.config.EXPORT_POLL_MAX_SECONDS} seconds")

    def _check_export_status(self, survey_id: str, progress_id: str):
        url = f"{self.api_client.base_url}/surveys/{survey_id}/export-responses/{progress_id}"

        try:
            response = requests.get(url, headers=self.api_client.headers)
            response.raise_for_status()
            return response.json()["result"]
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to check export status for survey {survey_id}: {e}")
            raise

    def _download_export_file(self, survey_id: str, file_id: str):
        url = f"{self.api_client.base_url}/surveys/{survey_id}/export-responses/{file_id}/file"

        try:
            response = requests.get(url, headers=self.api_client.headers)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download export file for survey {survey_id}: {e}")
            raise

    def get_export_progress(self, survey_id: str, progress_id: str):
        try:
            result = self._check_export_status(survey_id, progress_id)
            return {
                "survey_id": survey_id,
                "progress_id": progress_id,
                "status": result["status"],
                "percent_complete": result.get("percentComplete", 0),
                "file_id": result.get("fileId")
            }
        except Exception as e:
            logger.error(f"Failed to get export progress for {survey_id}: {e}")
            raise

    def _has_existing_field_mapping(self, survey_id):
        try:
            with db_manager.get_cursor() as cursor:
                query = """
                        SELECT field_mapping
                        FROM surveys
                        WHERE qualtrics_survey_id = %s
                          AND field_mapping IS NOT NULL
                          AND field_mapping != '{}'::jsonb
                    AND field_mapping != 'null'::jsonb \
                        """
                cursor.execute(query, (survey_id,))
                result = cursor.fetchone()

                if result:
                    logger.info(f"[{survey_id}] Field mapping already exists")
                    return True
                else:
                    logger.info(f"[{survey_id}] Field mapping is empty or null")
                    return False

        except Exception as e:
            logger.error(f"Failed to check field mapping for survey {survey_id}: {e}")
            return False

    def _get_all_survey_ids_from_db(self, organisation_id=None):
        """Get all survey ids"""
        try:
            with db_manager.get_cursor() as cursor:
                if organisation_id:
                    # Get all active survey ids for specific organisation
                    query = """
                            SELECT DISTINCT qualtrics_survey_id
                            FROM surveys
                            WHERE organisation_id = %s and status = 'active'
                            ORDER BY qualtrics_survey_id \
                            """
                    cursor.execute(query, (organisation_id,))
                else:
                    # Get all active survey ids
                    query = """
                            SELECT DISTINCT qualtrics_survey_id
                            FROM surveys
                            WHERE status = 'active'
                            ORDER BY qualtrics_survey_id \
                            """
                    cursor.execute(query)

                results = cursor.fetchall()
                return [row['qualtrics_survey_id'] for row in results]

        except Exception as e:
            logger.error(f"Failed to get survey IDs from database: {e}")
            raise

    def _log_responses_extraction_result(self, survey_id, file_name, file_path, success=True, error_message=None):
        """Success download process log"""
        if not success:
            logger.info(f"[{survey_id}] Skipping log for failed extraction")
            return None

        try:
            with db_manager.get_cursor() as cursor:
                if file_path.exists():
                    file_size = file_path.stat().st_size
                    file_hash = calculate_file_hash(file_path)
                else:
                    logger.warning(f"[{survey_id}] File does not exist, skipping log")
                    return None

                insert_query = """
                               INSERT INTO survey_responses_extraction_log
                                   (survey_id, file_name, file_size, file_hash, extracted_at)
                               VALUES (%s, %s, %s, %s, %s) RETURNING id \
                               """
                cursor.execute(insert_query, (
                    survey_id,
                    file_name,
                    file_size,
                    file_hash,
                    datetime.now(timezone.utc)
                ))

                log_id = cursor.fetchone()['id']
                logger.info(f"Responses extraction success log recorded with ID: {log_id}")
                return log_id

        except Exception as e:
            logger.error(f"Failed to log responses extraction result: {e}")
            return None