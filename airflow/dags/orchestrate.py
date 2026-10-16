from airflow.sdk import dag,task   
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import time
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

@dag(
        dag_id="orchestrate",
        schedule="0 11 * * *",
        catchup=False,
        start_date=pendulum.datetime(year=2026,month=10,day=16,tz='Asia/Kolkata')
)
def orchestrate():

    @task
    def ingest_cdc():

        ws = WorkspaceClient(
        host="your_databricks_host",
        token="your_databricks_token"

    )

        job_trigger = ws.jobs.run_now(job_id='your_job_id')

        while True:
            job_run = ws.jobs.get_run(job_trigger.run_id)

            print(f"Job run status: {job_run.state.life_cycle_state}, result state:{job_run.state.result_state}")
            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print('Job completed successfully')
                    break

                else:
                    raise Exception (f'Job failed with state:{job_run.state.result_state}')

            time.sleep(5)

        return "CDC Ingestion Completed"

    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_project/target && rm -rf /opt/airflow/walmart_project/logs"

    @task.bash
    def source_freshness():
        # Manually set the working directory
        return "cd /opt/airflow/walmart_project && dbt source freshness"

    silver_technical = BashOperator(
        task_id = "silver_technical",
        bash_command = "cd /opt/airflow/walmart_project && dbt run --select silver_t"
       
    )

    silver_technical_test = BashOperator(
    task_id = "silver_technical_test",
    bash_command = "cd /opt/airflow/walmart_project && dbt test --select silver_t"
    
    )

    silver_business = BashOperator(
        task_id='silver_business',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select silver_b'
    )

    silver_business_tests = BashOperator(
        task_id='silver_business_tests',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt test --select silver_b'
    )
    
    gold_ephemeral = BashOperator(
        task_id='gold_ephemeral',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select gold/ephemeral'
    )

    gold_dimension = BashOperator(
        task_id = "gold_dimension",
        cwd = "/opt/airflow/walmart_project",
        bash_command = "dbt snapshot"
    )

    gold_fact = BashOperator(
        task_id = "gold_fact",
        cwd = "/opt/airflow/walmart_project",
        bash_command = "dbt run --select gold/facts"
    )


    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_test >> silver_business >> silver_business_tests >> gold_ephemeral >> gold_dimension >> gold_fact

orchestrate_dag = orchestrate()
    
        
