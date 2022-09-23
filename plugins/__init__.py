from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import custom_operators
import helpers

# Defining the plugin class
class SamarPlugin(AirflowPlugin):
    name = "samar_plugin"
    operators = [
        custom_operators.LoadTableOperator,
        custom_operators.DataQualityOperator,
        custom_operators.CreateTableOperator
    ]
    helpers = [
        helpers.SqlQueries,
        helpers.SQLCreateTables
    ]
