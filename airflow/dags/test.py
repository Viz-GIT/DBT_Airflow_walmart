from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import time

ws = WorkspaceClient(
        host="your_databricks_host",
        token="your_databricks_token"
)

job_trigger = ws.jobs.run_now(job_id=922057600959682)

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

# @dag(
#         dag_id="orchestrate",
#         schedule="0 11 * * *",
#         catchup=False,
#         start_date=pendulum.datetime(year=2026,month=10,day=16,tz='Asia/kolkata')
# )