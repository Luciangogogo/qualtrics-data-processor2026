import json
import pandas as pd
import logging

from ..config.database import db_manager

logger = logging.getLogger(__name__)


class DataLoadService:
    def __init__(self):
        pass

    def load_survey_mappings(self, survey_id, mappings_data, force_update=False):
        try:
            logger.info(f"Loading mappings for survey {survey_id}")

            # Get UUID of the survey by Qualtrics survey id
            survey_uuid = self._get_survey_uuid_by_qualtrics_id(survey_id)
            if not survey_uuid:
                return {
                    "success": False,
                    "error": f"Survey with qualtrics_survey_id {survey_id} not found in database",
                    "action": "skipped"
                }

            # Check if any existing mappings then skip the insert
            if not force_update and self._has_existing_mappings(survey_uuid):
                logger.info(f"Survey {survey_id} already has mappings, skipping update")
                return {
                    "success": True,
                    "action": "skipped",
                    "reason": "mappings_already_exist",
                    "mappings_count": len(mappings_data.get("mappings", {})),
                    "key_fields_count": len(mappings_data.get("key_fields", {}))
                }

            success = self._update_survey_mappings(survey_uuid, mappings_data)

            if success:
                return {
                    "success": True,
                    "action": "updated" if force_update else "created",
                    "mappings_count": len(mappings_data.get("mappings", {})),
                    "key_fields_count": len(mappings_data.get("key_fields", {}))
                }
            else:
                return {
                    "success": False,
                    "error": "Failed to update mappings in database",
                    "action": "failed"
                }

        except Exception as e:
            logger.error(f"Failed to load mappings for survey {survey_id}: {e}")
            return {
                "success": False,
                "error": str(e),
                "action": "failed"
            }

    def load_survey_responses(self, survey_id, responses_data, replace_existing=True):
        try:
            logger.info(f"Loading responses for survey {survey_id}")

            # Get UUID of the survey by Qualtrics survey id
            survey_uuid = self._get_survey_uuid_by_qualtrics_id(survey_id)
            if not survey_uuid:
                return {
                    "success": False,
                    "error": f"Survey with qualtrics_survey_id {survey_id} not found in database"
                }

            deleted_count = 0
            if replace_existing:
                deleted_count = self._clear_survey_responses(survey_uuid)

            inserted_count = self._insert_survey_responses(survey_uuid, responses_data)

            return {
                "success": True,
                "deleted_count": deleted_count,
                "inserted_count": inserted_count,
                "total_input_records": len(responses_data) if responses_data else 0
            }

        except Exception as e:
            logger.error(f"Failed to load responses for survey {survey_id}: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def check_survey_mappings_exist(self, survey_id):
        try:
            survey_uuid = self._get_survey_uuid_by_qualtrics_id(survey_id)
            if not survey_uuid:
                return False

            return self._has_existing_mappings(survey_uuid)

        except Exception as e:
            logger.error(f"Failed to check mappings existence for survey {survey_id}: {e}")
            return False

    def get_survey_mappings(self, survey_id):
        try:
            survey_uuid = self._get_survey_uuid_by_qualtrics_id(survey_id)
            if not survey_uuid:
                return None

            with db_manager.get_cursor() as cursor:
                query = "SELECT field_mapping FROM surveys WHERE id = %s"
                cursor.execute(query, (survey_uuid,))
                result = cursor.fetchone()

                if result and result['field_mapping']:
                    return result['field_mapping']
                else:
                    return None

        except Exception as e:
            logger.error(f"Failed to get mappings for survey {survey_id}: {e}")
            return None

    def _get_survey_uuid_by_qualtrics_id(self, qualtrics_survey_id):
        try:
            with db_manager.get_cursor() as cursor:
                query = "SELECT id FROM surveys WHERE qualtrics_survey_id = %s"
                cursor.execute(query, (qualtrics_survey_id,))
                result = cursor.fetchone()
                if result:
                    return result['id']
                else:
                    logger.warning(f"Survey with qualtrics_survey_id {qualtrics_survey_id} not found")
                    return None
        except Exception as e:
            logger.error(f"Failed to get survey UUID: {e}")
            raise

    def _has_existing_mappings(self, survey_uuid):
        try:
            with db_manager.get_cursor() as cursor:
                query = """
                        SELECT field_mapping
                        FROM surveys
                        WHERE id = %s
                          AND field_mapping IS NOT NULL
                          AND field_mapping != '{}'::jsonb
                        """
                cursor.execute(query, (survey_uuid,))
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            logger.error(f"Failed to check existing mappings: {e}")
            raise

    def _update_survey_mappings(self, survey_uuid, mappings_data):
        try:
            with db_manager.get_cursor() as cursor:
                field_mappings = mappings_data.get("mappings", {})
                key_fields = mappings_data.get("key_fields", {})
                survey_name = mappings_data.get("survey_name", "")

                service_type = key_fields.get("ServiceType", "")

                field_mapping_data = field_mappings

                update_query = """
                               UPDATE surveys
                               SET field_mapping = %s,
                                   name          = %s,
                                   service_type  = %s
                               WHERE id = %s
                               """

                cursor.execute(update_query, (
                    json.dumps(field_mapping_data),
                    survey_name,
                    service_type,
                    survey_uuid
                ))

                logger.info(f"Updated survey mappings, name, and service_type for survey UUID {survey_uuid}")
                logger.info(f"Service Type set to: {service_type}")
                logger.info(f"Field mappings count: {len(field_mapping_data)}")

                return True

        except Exception as e:
            logger.error(f"Failed to update survey mappings: {e}")
            return False

    def _clear_survey_responses(self, survey_uuid):
        try:
            with db_manager.get_cursor() as cursor:
                delete_query = "DELETE FROM survey_responses WHERE survey_id = %s"
                cursor.execute(delete_query, (survey_uuid,))
                deleted_count = cursor.rowcount
                logger.info(f"Deleted {deleted_count} existing responses for survey {survey_uuid}")
                return deleted_count
        except Exception as e:
            logger.error(f"Failed to clear survey responses: {e}")
            raise

    def _insert_survey_responses(self, survey_uuid, responses_data):
        if not responses_data:
            logger.warning("No response data to insert")
            return 0

        try:
            with db_manager.get_cursor() as cursor:
                insert_query = """
                               INSERT INTO survey_responses
                               (survey_id, submitted_at, period_year, period_month,
                                response_data)
                               VALUES (%s, %s, %s, %s, %s)
                               """

                inserted_count = 0
                for idx, response in enumerate(responses_data):
                    try:
                        submitted_at = None
                        period_year = None
                        period_month = None

                        if 'EndDate' in response and response['EndDate']:
                            try:
                                submitted_at = pd.to_datetime(response['EndDate'])

                                from datetime import timedelta
                                time_str = str(response['EndDate']).strip()
                                if ',' in time_str:
                                    time_str = time_str.split(',')[0]

                                utc_dt = pd.to_datetime(time_str).to_pydatetime()
                                perth_dt = utc_dt + timedelta(hours=8)

                                period_year = perth_dt.year
                                period_month = perth_dt.month

                                logger.debug(f"Time conversion - UTC: {time_str} -> Perth: {perth_dt.strftime('%Y-%m-%d %H:%M:%S')} -> Period: {period_year}-{period_month:02d}")

                            except Exception as e:
                                logger.warning(f"Failed to parse EndDate '{response['EndDate']}': {e}")

                        cursor.execute(insert_query, (
                            survey_uuid,
                            submitted_at,
                            period_year,
                            period_month,
                            json.dumps(response)
                        ))
                        inserted_count += 1

                    except Exception as row_error:
                        logger.warning(f"Failed to insert response {idx}: {row_error}")
                        continue

                logger.info(f"Successfully inserted {inserted_count} responses using survey UUID {survey_uuid}")
                return inserted_count

        except Exception as e:
            logger.error(f"Failed to insert survey responses: {e}")
            raise