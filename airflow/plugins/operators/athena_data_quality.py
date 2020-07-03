from airflow.models import Variable
from helpers import SqlQueries
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.providers.amazon.aws.hooks.athena import AWSAthenaHook


class AthenaDataQuality(AWSAthenaOperator):
    # source: https://bit.ly/3eRlSZF
    ui_color = '#89DA59'

    def execute(self, context):

        self.hook = self.get_hook()

        self.query_execution_context['Database'] = self.database
        self.result_configuration['OutputLocation'] = self.output_location
        self.log.info(
            self.query.format(context['ds_nodash'], context['ds_nodash']))
        self.query_execution_id = self.hook.run_query(
            self.query.format(context['ds_nodash'], context['ds_nodash']),
            self.query_execution_context,
            self.result_configuration,
            self.client_request_token,
            self.workgroup)
        query_status = self.hook.poll_query_status(
            self.query_execution_id, self.max_tries)

        if query_status in AWSAthenaHook.FAILURE_STATES:
            error_message = self.hook.get_state_change_reason(
                self.query_execution_id)
            s_msg = 'Final state of Athena job is {}, query_execution_id is '
            s_msg += '{}. Error: {}'
            raise Exception(s_msg.format(
                query_status, self.query_execution_id, error_message))
        elif not query_status or (
                query_status in AWSAthenaHook.INTERMEDIATE_STATES):
            raise Exception(
                'Final state of Athena job is {}. '
                'Max tries of poll status exceeded, query_execution_id is {}.'
                .format(query_status, self.query_execution_id))

        res = self.hook.get_query_results(self.query_execution_id)
        # self.log.info(f'Type( {type(res)}), Results {res}...')
        i_tot = int(res['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
        s_msg = f"... {i_tot} lines found in {context['ds_nodash']}"
        self.log.info(s_msg)
        if i_tot == 0:
            raise ValueError("!! At least one data point should be present "
                             "in this day")

        return self.query_execution_id
