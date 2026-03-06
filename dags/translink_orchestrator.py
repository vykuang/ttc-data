import os

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag
from docker.types import Mount


def docker_mounts() -> list:
    """
    Mount local AWS credentials for development.
    In production, credentials come from the EC2/ECS IAM role.
    'nonroot' is the user created inside Dockerfile.translink.
    """
    if os.getenv("AWS_ENV") == "local":
        return [
            Mount(
                source=os.getenv("AWS_DIR"),
                target="/home/nonroot/.aws",
                type="bind",
                read_only=True,
            )
        ]
    return []


def translink_env() -> dict:
    """
    Environment variables injected into each DockerOperator container.
    The Translink API key is read from Airflow's env (sourced from .env).
    """
    return {"TRANSLINK_API_KEY": os.getenv("TRANSLINK_API_KEY", "")}


IMAGE = f"translink-data:{os.getenv('ENV', 'dev')}"


@dag(
    dag_id="dag_translink_realtime",
    schedule="*/5 * * * *",
    catchup=False,
    tags=["translink", "realtime"],
)
def dag_translink_realtime():
    """
    Every 5 minutes:
      1. Fetch real-time bus positions from the Translink API → S3 raw/
      2. Transform raw snapshots into stop_arrival_fact → S3 transformed/
    """
    get_trip = DockerOperator(
        task_id="get-trip",
        image=IMAGE,
        command=["get_api.py", "--item", "trip"],
        environment=translink_env(),
        mounts=docker_mounts(),
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    get_vehicle = DockerOperator(
        task_id="get-vehicle",
        image=IMAGE,
        command=["get_api.py", "--item", "vehicle"],
        environment=translink_env(),
        mounts=docker_mounts(),
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    transform = DockerOperator(
        task_id="transform",
        image=IMAGE,
        command=["transform.py"],
        environment=translink_env(),
        mounts=docker_mounts(),
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    get_trip >> get_vehicle >> transform


@dag(
    dag_id="dag_translink_gtfs_static",
    schedule="0 4 * * 1",  # Every Monday at 04:00 UTC
    catchup=False,
    tags=["translink", "gtfs", "static"],
)
def dag_translink_gtfs_static():
    """
    Weekly: download the Translink GTFS static zip and write dimension
    tables (routes, stops, trips, stop_times, shapes) to S3 static/gtfs/.
    """
    get_gtfs = DockerOperator(
        task_id="get-gtfs-static",
        image=IMAGE,
        command=["get_gtfs_static.py"],
        environment=translink_env(),
        mounts=docker_mounts(),
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    get_gtfs


dag_translink_realtime()
dag_translink_gtfs_static()
