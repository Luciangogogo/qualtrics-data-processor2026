import pandas as pd
import logging
from typing import Dict, Any

from ..config.settings import get_config
from ..utils.file_utils import find_latest_csv
from ..config.database import db_manager
from .load_service import DataLoadService

logger = logging.getLogger(__name__)


class DataTransformService:
    def __init__(self):
        self.config = get_config()
        self.load_service = DataLoadService()

        self.key_fields = ["Facility", "Satisfaction", "EndDate", "NPS", "NPS_NPS_GROUP", "Gender", "ParticipantType"]
        self.key_fields_prefixes = ["Ab_"]
        self.allowed_keys_dict = ["ServiceType", "Facility", "Satisfaction", "Gender", "ParticipantType"]
        self.allowed_prefixes = ["Ab_"]

    def transform_and_load_all(self, organisation_id=None, force_mappings_update=False):
        try:
            survey_ids = self._get_all_survey_ids_from_db(organisation_id)

            if not survey_ids:
                return {"success": False, "error": "No surveys found in database"}

            return self.transform_specific_surveys(survey_ids, force_mappings_update)

        except Exception as e:
            logger.error(f"Failed to transform and load all surveys: {e}")
            return {"success": False, "error": str(e)}

    def transform_specific_surveys(self, survey_ids, force_mappings_update=False):
        if not survey_ids:
            return {"success": False, "error": "No survey IDs provided"}

        results = {}
        logger.info(f"Starting transform and load for {len(survey_ids)} surveys: {', '.join(survey_ids)}")

        for survey_id in survey_ids:
            try:
                mappings_result = self._process_survey_mappings(survey_id, force_mappings_update)

                responses_result = self._process_survey_responses(survey_id)

                results[survey_id] = {
                    "mappings": mappings_result,
                    "responses": responses_result,
                    "overall_success": mappings_result.get("success", False) and responses_result.get("success", False)
                }

            except Exception as e:
                logger.error(f"[{survey_id}] Transform and load failed: {e}")
                results[survey_id] = {
                    "mappings": {"success": False, "error": str(e)},
                    "responses": {"success": False, "error": "Skipped due to mappings failure"},
                    "overall_success": False
                }

        successful = sum(1 for result in results.values() if result["overall_success"])
        total = len(survey_ids)

        logger.info(f"Transform and load completed: {successful}/{total} successful")

        return {
            "success": True,
            "data": {
                "total_surveys": total,
                "successful_transforms": successful,
                "failed_transforms": total - successful,
                "details": results,
                "survey_ids": survey_ids
            }
        }

    def transform_survey_mappings(self, survey_id: str, survey_name: str, questions: Dict[str, Any]):
        try:
            if not questions or (isinstance(questions, dict) and len(questions) == 0):
                logger.info(f"[{survey_id}] No questions provided from extract stage, skip mappings transform.")
                return {
                    "success": True,
                    "survey_id": survey_id,
                    "action": "skipped",
                    "reason": "no_questions_provided",
                    "mappings_data": {"survey_name": survey_name, "mappings": {}, "key_fields": {}},
                    "mappings_count": 0,
                    "key_fields_count": 0
                }

            logger.info(f"[{survey_id}] Transforming mappings")
            mappings_data = self._extract_mappings_from_questions(questions)

            mappings_data["survey_name"] = survey_name

            return {
                "success": True,
                "survey_id": survey_id,
                "mappings_data": mappings_data,
                "mappings_count": len(mappings_data.get("mappings", {})),
                "key_fields_count": len(mappings_data.get("key_fields", {}))
            }

        except Exception as e:
            logger.error(f"[{survey_id}] Failed to transform mappings: {e}")
            return {"success": False, "error": str(e)}

    def transform_survey_responses(self, survey_id: str):
        try:
            dup_check = self._is_latest_duplicate_download(survey_id)
            if dup_check.get("is_duplicate"):
                logger.info(f"[{survey_id}] Latest download hash equals previous one; skip transform & load.")
                return {
                    "success": True,
                    "survey_id": survey_id,
                    "action": "skipped_duplicate",
                    "reason": "latest_two_file_hash_equal",
                    "transformed_count": 0,
                    "responses_data": [],
                    "total_records_in_csv": 0,
                    "hash": dup_check.get("latest_hash"),
                }

            logger.info(f"[{survey_id}] Transforming responses")

            csv_file = find_latest_csv(self.config.DATA_DIR, survey_id)
            df_responses = pd.read_csv(csv_file)

            responses_data = self._transform_responses_data(df_responses)

            return {
                "success": True,
                "survey_id": survey_id,
                "transformed_count": len(responses_data),
                "responses_data": responses_data,
                "total_records_in_csv": len(df_responses)
            }

        except FileNotFoundError:
            error_msg = f"CSV file not found for survey {survey_id}"
            logger.error(f"[{survey_id}] {error_msg}")
            return {"success": False, "error": error_msg}
        except Exception as e:
            logger.error(f"[{survey_id}] Failed to transform responses: {e}")
            return {"success": False, "error": str(e)}

    def _process_survey_mappings(self, survey_id: str, force_update=False):
        try:
            if not force_update and self.load_service.check_survey_mappings_exist(survey_id):
                logger.info(f"[{survey_id}] Mappings already exist, skipping")
                return {
                    "success": True,
                    "action": "skipped",
                    "reason": "mappings_already_exist"
                }

            logger.info(f"[{survey_id}] Need to extract questions for mappings")

            from .extract_service import DataExtractionService
            extract_service = DataExtractionService()

            questions_result = extract_service.extract_survey_definitions(survey_id)

            if not questions_result.get("success"):
                return {
                    "success": False,
                    "error": f"Failed to extract questions: {questions_result.get('error')}"
                }

            if questions_result.get("action") == "skipped":
                return {
                    "success": True,
                    "action": "skipped",
                    "reason": "questions_already_exist"
                }

            survey_name = questions_result.get("survey_name", "")
            questions = questions_result.get("questions", {})

            transform_result = self.transform_survey_mappings(survey_id, survey_name, questions)

            if not transform_result.get("success"):
                return transform_result

            mappings_data = transform_result.get("mappings_data", {})
            load_result = self.load_service.load_survey_mappings(survey_id, mappings_data, force_update)

            return load_result

        except Exception as e:
            logger.error(f"[{survey_id}] Failed to process mappings: {e}")
            return {"success": False, "error": str(e)}

    def _process_survey_responses(self, survey_id: str):
        try:
            transform_result = self.transform_survey_responses(survey_id)

            if not transform_result.get("success"):
                return transform_result

            if transform_result.get("action") == "skipped_duplicate":
                return transform_result

            responses_data = transform_result.get("responses_data", [])
            load_result = self.load_service.load_survey_responses(survey_id, responses_data)

            combined_result = {
                **transform_result,
                **load_result
            }

            return combined_result

        except Exception as e:
            logger.error(f"[{survey_id}] Failed to process responses: {e}")
            return {"success": False, "error": str(e)}

    def _is_latest_duplicate_download(self, survey_id: str) -> dict:
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute(
                    """
                    SELECT file_hash, extracted_at
                    FROM survey_responses_extraction_log
                    WHERE survey_id = %s
                    ORDER BY extracted_at DESC LIMIT 2
                    """,
                    (survey_id,)
                )
                rows = cursor.fetchall() or []

            if len(rows) < 2:
                return {"is_duplicate": False}

            latest_hash = rows[0]["file_hash"]
            prev_hash = rows[1]["file_hash"]

            if latest_hash and prev_hash and latest_hash == prev_hash:
                return {"is_duplicate": True, "latest_hash": latest_hash}

            return {"is_duplicate": False}
        except Exception as e:
            logger.warning(f"[{survey_id}] Failed to check duplicate download, will proceed with transform. Error: {e}")
            return {"is_duplicate": False}

    def _extract_mappings_from_questions(self, questions):
        transformed_fields = {
            "key_fields": {},
            "mappings": {}
        }

        for question in questions.values():
            outer_key = question.get("DataExportTag")
            if not outer_key:
                continue

            if (outer_key not in self.allowed_keys_dict and
                    not any(outer_key.startswith(p) for p in self.allowed_prefixes)):
                continue

            choices = question.get("Choices")
            if not choices:
                logger.debug(f"[{outer_key}] No Choices found")
                continue

            # ServiceType requires special handling
            if outer_key == "ServiceType":
                service_type_name = ""

                # Get the first choice's display value
                if "1" in choices:
                    choice_1 = choices["1"]
                    if isinstance(choice_1, dict):
                        service_type_name = choice_1.get("Display", "")
                    else:
                        service_type_name = str(choice_1)

                # Fallback: get any first available display value
                if not service_type_name:
                    for choice_val in choices.values():
                        if isinstance(choice_val, dict):
                            service_type_name = choice_val.get("Display", "")
                        else:
                            service_type_name = str(choice_val)
                        if service_type_name:
                            break

                transformed_fields["key_fields"][outer_key] = service_type_name
                logger.info(f"[ServiceType] Extracted: '{service_type_name}'")
                continue  # Skip to next question, don't create mappings for ServiceType

            # For all other fields, create mappings using RecodeValues
            recode_values = question.get("RecodeValues", {})
            inner_mapping = {}

            for choice_key, choice_value in choices.items():
                # Get display label
                if isinstance(choice_value, dict):
                    display = choice_value.get("Display", "")
                else:
                    display = str(choice_value)

                # Get the recode value for this choice key
                recode_val = recode_values.get(choice_key)

                # Use recode value as mapping key if available
                if recode_val is not None:
                    mapping_key = str(recode_val)
                else:
                    # Fallback to choice_key if no recode value
                    mapping_key = str(choice_key)

                inner_mapping[mapping_key] = display

            if not inner_mapping:
                logger.warning(f"[{outer_key}] No mappings created")
                continue

            # Add to regular mappings
            transformed_fields["mappings"][outer_key] = inner_mapping

        return transformed_fields

    def _transform_responses_data(self, df):
        prefix_cols = [col for col in df.columns
                       if any(col.startswith(p) for p in self.key_fields_prefixes)]
        available_cols = [col for col in (self.key_fields + prefix_cols)
                          if col in df.columns]

        df_selected = df[available_cols]

        data = df_selected.to_dict(orient='records')[2:]
        return data

    def _get_all_survey_ids_from_db(self, organisation_id=None):
        try:
            with db_manager.get_cursor() as cursor:
                if organisation_id:
                    query = """
                            SELECT DISTINCT qualtrics_survey_id
                            FROM surveys
                            WHERE organisation_id = %s \
                              and status = 'active'
                            ORDER BY qualtrics_survey_id
                            """
                    cursor.execute(query, (organisation_id,))
                else:
                    query = """
                            SELECT DISTINCT qualtrics_survey_id
                            FROM surveys
                            WHERE status = 'active'
                            ORDER BY qualtrics_survey_id
                            """
                    cursor.execute(query)

                results = cursor.fetchall()
                return [row['qualtrics_survey_id'] for row in results]

        except Exception as e:
            logger.error(f"Failed to get survey IDs from database: {e}")
            raise