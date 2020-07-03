from airflow.models import Variable
from helpers import SqlQueries
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.providers.amazon.aws.hooks.athena import AWSAthenaHook


class AthenaPartitionInsert(AWSAthenaOperator):
    # source: https://bit.ly/3eRlSZF
    ui_color = '#3486eb'

    def execute(self, context):

        self.hook = self.get_hook()

        self.query_execution_context['Database'] = self.database
        self.result_configuration['OutputLocation'] = self.output_location
        s_sql = self.query.format(*[int(x) for x in context['ds'].split('-')])
        self.log.info(s_sql)

        self.query_execution_id = self.hook.run_query(
            s_sql,
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

        return self.query_execution_id
