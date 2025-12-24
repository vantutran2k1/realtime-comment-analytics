from diagrams import Diagram, Cluster, Edge
from diagrams.custom import Custom
from diagrams.generic.storage import Storage
from diagrams.onprem.analytics import Spark
from diagrams.onprem.client import Users
from diagrams.onprem.compute import Server
from diagrams.onprem.database import MongoDB
from diagrams.onprem.queue import Kafka
from diagrams.onprem.workflow import Airflow
from diagrams.programming.language import Python

graph_attr = {
    "fontsize": "24",
    "bgcolor": "white",
    "splines": "ortho",
    "nodesep": "1.5",
    "ranksep": "2.0",
    "pad": "2.0",
}

with Diagram(
    show=False,
    filename="system_architecture_diagram",
    direction="LR",
    graph_attr=graph_attr,
):
    with Cluster("Data Sources (Nguồn Dữ liệu)"):
        users = Users("Viewers")
        yt_platform = Custom("YouTube Live\nStream API", "youtube_logo.png")
        kaggle = Storage("Kaggle Dataset\n(Initial Training)")

    with Cluster("Real-time Processing Pipeline"):

        with Cluster("Ingestion Layer"):
            ingest_service = Python("Ingestion Script\n(Polling API)")

        with Cluster("Streaming Layer (Spark & Kafka)"):
            kafka_raw = Kafka("Topic:\nevents.raw_messages")
            stream_app = Spark("Stream App\n(ETL/Cleaner)")
            kafka_clean = Kafka("Topic:\nevents.transformed")
            analytics_service = Spark("Analytics Service\n(Inference Model)")

    with Cluster("Storage & Serving Layer"):
        mongo_db = MongoDB("MongoDB\n(Logs & History)")
        action_handler = Server("Action Handler\n(Block/Hide)")

    with Cluster("MLOps & Retraining (Batch Layer)"):
        airflow = Airflow("Airflow Scheduler\n(Weekly)")
        spark_train = Spark("Spark Training Job")
        model_registry = Storage("HDFS/S3\n(Model Registry)")

    (
        users
        >> Edge(label="Post Comments", minlen="0.5", constraint="false")
        >> yt_platform
    )

    (
        yt_platform
        >> Edge(color="darkgreen", xlabel="Polling (5s)", minlen="2")
        >> ingest_service
    )
    ingest_service >> Edge(label="JSON Raw") >> kafka_raw

    kafka_raw >> Edge(label="Consume") >> stream_app
    stream_app >> Edge(label="Clean/Tokenize") >> kafka_clean

    kafka_clean >> Edge(label="Consume") >> analytics_service
    (
        model_registry
        >> Edge(style="dashed", color="purple", label="Load Model")
        >> analytics_service
    )

    analytics_service >> Edge(xlabel="Store Logs", minlen="3") >> mongo_db
    (
        analytics_service
        >> Edge(color="red", style="bold", xlabel="Decision > 0.8", minlen="3")
        >> action_handler
    )

    (
        kaggle
        >> Edge(style="dashed", xlabel="Base Data", constraint="true")
        >> spark_train
    )
    (mongo_db >> spark_train)
    airflow >> Edge(style="dotted", label="Trigger") >> spark_train
    spark_train >> Edge(color="purple", label="Save New Model") >> model_registry
