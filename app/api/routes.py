from flask import Blueprint, jsonify, request, current_app
from datetime import datetime
import logging
import traceback
import os

from ..services.extract_service import DataExtractionService
from ..services.transform_service import DataTransformService
from ..config.database import db_manager

api_bp = Blueprint('api', __name__, url_prefix='/api')
health_bp = Blueprint('health', __name__)

logger = logging.getLogger(__name__)


def create_response(success, data=None, error=None, status_code=200):
    response_data = {
        "success": success,
        "timestamp": datetime.now().isoformat(),
    }

    if success:
        response_data["data"] = data or {}
    else:
        response_data["error"] = error or "Unknown error"
        if status_code == 200:
            status_code = 400

    return jsonify(response_data), status_code


@health_bp.route('/health', methods=['GET'])
def health_check():
    try:
        with db_manager.get_cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()

        return create_response(
            success=True,
            data={
                "status": "healthy",
                "database": "connected",
                "service": current_app.config.get('APP_NAME', 'data-processing-api'),
                "version": current_app.config.get('APP_VERSION', '1.0.0')
            }
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return create_response(
            success=False,
            error=f"Database connection failed: {str(e)}",
            status_code=503
        )


@api_bp.route('/extract-data', methods=['POST'])
def extract_data():
    try:
        logger.info("=== Extract Data API Called ===")

        request_data = request.get_json() if request.is_json else {}
        survey_ids = request_data.get('survey_ids') if request_data else None
        organisation_id = request_data.get('organisation_id') if request_data else None

        extraction_service = DataExtractionService()

        if survey_ids:
            result = extraction_service.extract_specific_surveys(survey_ids)
        else:
            result = extraction_service.extract_all_surveys(organisation_id)

        if result.get("success"):
            logger.info("Extract data API completed successfully")
            return create_response(
                success=True,
                data=result.get("data", {})
            )
        else:
            logger.error(f"Extract data API failed: {result.get('error')}")
            return create_response(
                success=False,
                error=result.get("error"),
                status_code=500
            )

    except Exception as e:
        logger.error(f"Extract data API exception: {e}")
        logger.error(traceback.format_exc())
        return create_response(
            success=False,
            error=f"Internal server error: {str(e)}",
            status_code=500
        )


@api_bp.route('/extract-definitions', methods=['POST'])
def extract_definitions():
    try:
        logger.info("=== Extract Definitions API Called ===")

        request_data = request.get_json() if request.is_json else {}
        survey_ids = request_data.get('survey_ids') if request_data else None
        organisation_id = request_data.get('organisation_id') if request_data else None

        extraction_service = DataExtractionService()

        if survey_ids:
            results = {}
            for survey_id in survey_ids:
                results[survey_id] = extraction_service.extract_survey_definitions(survey_id)

            successful = sum(1 for result in results.values() if result["success"])
            extracted = sum(1 for result in results.values() if result.get("action") == "extracted")
            skipped = sum(1 for result in results.values() if result.get("action") == "skipped")

            result = {
                "success": True,
                "data": {
                    "total_surveys": len(survey_ids),
                    "successful_extractions": successful,
                    "extracted_count": extracted,
                    "skipped_count": skipped,
                    "failed_extractions": len(survey_ids) - successful,
                    "details": results,
                    "survey_ids": survey_ids
                }
            }
        else:
            result = extraction_service.extract_all_surveys_definitions(organisation_id)

        if result.get("success"):
            logger.info("Extract definitions API completed successfully")
            return create_response(
                success=True,
                data=result.get("data", {})
            )
        else:
            logger.error(f"Extract definitions API failed: {result.get('error')}")
            return create_response(
                success=False,
                error=result.get("error"),
                status_code=500
            )

    except Exception as e:
        logger.error(f"Extract definitions API exception: {e}")
        logger.error(traceback.format_exc())
        return create_response(
            success=False,
            error=f"Internal server error: {str(e)}",
            status_code=500
        )


@api_bp.route('/transform-and-load', methods=['POST'])
def transform_and_load():
    try:
        logger.info("=== Transform and Load API Called ===")

        request_data = request.get_json() if request.is_json else {}
        survey_ids = request_data.get('survey_ids') if request_data else None
        organisation_id = request_data.get('organisation_id') if request_data else None
        force_mappings_update = request_data.get('force_mappings_update', False) if request_data else False

        transform_service = DataTransformService()

        if survey_ids:
            result = transform_service.transform_specific_surveys(survey_ids, force_mappings_update)
        else:
            result = transform_service.transform_and_load_all(organisation_id, force_mappings_update)

        if result.get("success"):
            logger.info("Transform and load API completed successfully")
            return create_response(
                success=True,
                data=result.get("data", {})
            )
        else:
            logger.error(f"Transform and load API failed: {result.get('error')}")
            return create_response(
                success=False,
                error=result.get("error"),
                status_code=500
            )

    except Exception as e:
        logger.error(f"Transform and load API exception: {e}")
        logger.error(traceback.format_exc())
        return create_response(
            success=False,
            error=f"Internal server error: {str(e)}",
            status_code=500
        )


@api_bp.route('/full-pipeline', methods=['POST'])
def full_pipeline():
    try:
        logger.info("=== Full Pipeline API Called ===")

        request_data = request.get_json() if request.is_json else {}
        survey_ids = request_data.get('survey_ids') if request_data else None
        organisation_id = request_data.get('organisation_id') if request_data else None
        force_mappings_update = request_data.get('force_mappings_update', False) if request_data else False

        pipeline_result = {
            "extract_phase": None,
            "transform_phase": None,
            "overall_success": False
        }

        # Phase 1
        logger.info("Starting extract phase...")
        extraction_service = DataExtractionService()

        if survey_ids:
            extract_result = extraction_service.extract_specific_surveys(survey_ids)
        else:
            extract_result = extraction_service.extract_all_surveys(organisation_id)

        pipeline_result["extract_phase"] = extract_result

        if not extract_result.get("success"):
            logger.error("Extract phase failed, stopping pipeline")
            return create_response(
                success=False,
                data=pipeline_result,
                error="Extract phase failed"
            )

        # Phase 2
        logger.info("Starting transform and load phase...")
        transform_service = DataTransformService()

        if survey_ids:
            transform_result = transform_service.transform_specific_surveys(survey_ids, force_mappings_update)
        else:
            transform_result = transform_service.transform_and_load_all(organisation_id, force_mappings_update)

        pipeline_result["transform_phase"] = transform_result

        if transform_result.get("success"):
            pipeline_result["overall_success"] = True
            logger.info("Full pipeline completed successfully")
            return create_response(
                success=True,
                data=pipeline_result
            )
        else:
            logger.error("Transform phase failed")
            return create_response(
                success=False,
                data=pipeline_result,
                error="Transform and load phase failed"
            )

    except Exception as e:
        logger.error(f"Full pipeline API exception: {e}")
        logger.error(traceback.format_exc())
        return create_response(
            success=False,
            error=f"Internal server error: {str(e)}",
            status_code=500
        )


@api_bp.route('/status', methods=['GET'])
def get_status():
    try:
        total_surveys = 0
        survey_list = []

        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("SELECT COUNT(DISTINCT qualtrics_survey_id) as total FROM surveys")
                total_surveys = cursor.fetchone()['total']

                cursor.execute("""
                               SELECT DISTINCT qualtrics_survey_id
                               FROM surveys
                               ORDER BY qualtrics_survey_id
                               """)
                results = cursor.fetchall()
                survey_list = [row['qualtrics_survey_id'] for row in results]

        except Exception as e:
            logger.warning(f"Failed to fetch surveys info: {e}")

        recent_extractions = []
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                               SELECT survey_id, extracted_at, file_name, file_size, file_hash
                               FROM survey_responses_extraction_log
                               ORDER BY extracted_at DESC LIMIT 10
                               """)
                recent_extractions = [dict(row) for row in cursor.fetchall()]

                for extraction in recent_extractions:
                    if extraction['extracted_at']:
                        extraction['extracted_at'] = extraction['extracted_at'].isoformat()

        except Exception as e:
            logger.warning(f"Failed to fetch recent extractions: {e}")

        return create_response(
            success=True,
            data={
                "surveys_info": {
                    "total_surveys": total_surveys,
                    "survey_ids": survey_list
                },
                "recent_extractions": recent_extractions,
                "data_center": current_app.config.get('QUALTRICS_DATA_CENTER', 'not_configured'),
                "data_dir": str(current_app.config.get('DATA_DIR', 'not_configured')),
                "app_version": current_app.config.get('APP_VERSION', '1.0.0')
            }
        )

    except Exception as e:
        logger.error(f"Status API exception: {e}")
        return create_response(
            success=False,
            error=f"Failed to get status: {str(e)}",
            status_code=500
        )


@api_bp.errorhandler(404)
def api_not_found(error):
    return create_response(
        success=False,
        error="API endpoint not found",
        status_code=404
    )


@api_bp.errorhandler(500)
def api_internal_error(error):
    return create_response(
        success=False,
        error="Internal server error",
        status_code=500
    )