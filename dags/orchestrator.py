import os

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag
from docker.types import Mount


def docker_mounts() -> list:
    """
    mounts local aws cred for development
    else creds will come from EC2 profile/IAM role if prod
    'nonroot' is the user created inside the base Dockerfile
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


@dag(
    schedule="*/5 * * * *",
    catchup=False,
)
def dag_get_api():
    get_trip = DockerOperator(
        task_id="get-trip",
        image=f"ttc-data:{os.getenv('ENV')}",
        # required for initiated container to access host creds
        mounts=docker_mounts(),
        auto_remove="success",
        # required for docker-in-airflow to initiate new containers
        docker_url="unix://var/run/docker.sock",
        # REQUIRED for docker-in-docker
        mount_tmp_dir=False,
    )

    get_vehicle = DockerOperator(
        task_id="get-vehicle",
        image=f"ttc-data:{os.getenv('ENV')}",
        mounts=docker_mounts(),
        command=["--item", "vehicle"],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    get_trip >> get_vehicle


dag_get_api()
