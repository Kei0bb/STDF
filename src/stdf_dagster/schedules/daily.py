"""Daily schedule for analytics refresh."""

from dagster import (
    schedule,
    ScheduleEvaluationContext,
    RunRequest,
    DefaultScheduleStatus,
)

from stdf_dagster.jobs import full_pipeline_job


@schedule(
    cron_schedule="0 6 * * *",  # 毎朝 6:00 JST
    job=full_pipeline_job,
    description="毎朝6時にフルパイプラインを実行する日次スケジュール",
    default_status=DefaultScheduleStatus.STOPPED,  # 手動で有効化
)
def daily_refresh_schedule(context: ScheduleEvaluationContext):
    """Daily schedule to run the full pipeline.

    Triggers a full pipeline run every morning to ensure
    all data and analytics are up-to-date.
    """
    return RunRequest(
        run_key=f"daily-{context.scheduled_execution_time.strftime('%Y-%m-%d')}",
    )
