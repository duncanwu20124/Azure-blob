from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from . import assets

all_assets = load_assets_from_modules([assets])

# Define a job that will materialize the assets
my_job = define_asset_job("test_job", selection=AssetSelection.all())
#another_job=define_asset_job("456", selection=AssetSelection.all())

# Addition: a ScheduleDefinition the job it should run and a cron schedule of how frequently to run it
test_schedule = ScheduleDefinition(
    job=my_job,
    cron_schedule="* * * * *",  # every hour
)

#another_schedule = ScheduleDefinition(
#    job=another_job,
#    cron_schedule="0 * * * *",  # every hour
#)

defs = Definitions(
    assets=all_assets,
    schedules=[test_schedule],
)


