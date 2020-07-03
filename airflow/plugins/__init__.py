from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.S3UploadOperator,
        operators.RawDataQualityOperator,
        operators.CustomEmrCreateJobFlowOperator,
        operators.ProcessedDataQualityOperator,
        operators.AthenaDataQuality,
        operators.AthenaPartitionInsert
    ]
    helpers = [
        helpers.SqlQueries
    ]
