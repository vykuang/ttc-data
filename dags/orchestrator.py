from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag
from docker.types import Mount


@dag(
    schedule="*/5 * * * *",
    catchup=False,
)
def dag_get_api():
    get_trip = DockerOperator(
        task_id="get-trip",
        image="ttc-data:dev",
        mounts=[
            Mount(
                source="/home/kohada/.aws",
                target="/home/nonroot/.aws",
                type="bind",
                read_only=True,
            )
        ],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    get_vehicle = DockerOperator(
        task_id="get-vehicle",
        image="ttc-data:dev",
        mounts=[
            Mount(
                source="/home/kohada/.aws",
                target="/home/nonroot/.aws",
                type="bind",
                read_only=True,
            )
        ],
        command=["--item", "vehicle"],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    get_trip >> get_vehicle


dag_get_api()
