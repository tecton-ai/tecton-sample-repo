from tecton.framework.configs import DataprocJsonClusterConfig

from google.cloud import dataproc_v1 as dataproc
from google.protobuf.json_format import MessageToJson

KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2"
DELTA = "io.delta:delta-core_2.12:1.2.1"
BIGQUERY = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar"
SECRETMANAGER = "google-cloud-secret-manager==2.16.2"

def make_custom_config(jar_packages, jar_file_uris, pip_packages):
    template = dataproc.types.WorkflowTemplate()
    job = dataproc.types.OrderedJob()

    # Additional dependent jars dependencies are used in the sample repo. They may not be necessary for all data sources.
    if jar_packages or jar_file_uris:
        # The step_id must be named "tecton-materialization" for Tecton to merge the additional dependencies.
        job.step_id = "tecton-materialization"
        # The job type must be pyspark.
        if jar_file_uris:
            job.pyspark_job.jar_file_uris.extend(jar_file_uris)
        if jar_packages:
            job.pyspark_job.properties.update({"spark.jars.packages": ",".join(jar_packages)})
        template.jobs.append(job)

    # Optionally add a name or label
    template.placement.managed_cluster.cluster_name = "my-tecton-cluster"
    template.labels.update({'cost-center': 'tecton-development'})

    # Required: The only supported image version by Tecton is 2.1
    software_config = template.placement.managed_cluster.config.software_config
    software_config.image_version = "2.1"
    # Optionally, configure additional pip packages
    if pip_packages:
        software_config.properties.update({"dataproc:pip.packages":",".join(pip_packages)})

    # Required: Tecton needs to know which network and service account to run jobs in
    gce_cluster_config = template.placement.managed_cluster.config.gce_cluster_config
    gce_cluster_config.subnetwork_uri = "tecton-dataproc-subnet"
    gce_cluster_config.service_account = "itest-675@databricks-poc-376321.iam.gserviceaccount.com"
    return DataprocJsonClusterConfig(MessageToJson(template._pb))

dataproc_config = make_custom_config(
        jar_packages=[KAFKA, DELTA],
        jar_file_uris=[BIGQUERY],
        pip_packages=[SECRETMANAGER],
        )
