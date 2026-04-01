
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On Unix/macOS
# OR
venv\Scripts\activate     # On Windows

docker-compose exec kafka kafka-console-consumer \
    --bootstrap-server localhost:29092 \
    --topic continuous-stock-market-realtime \
    --from-beginning

docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092


#spark jars
"org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901"

docker exec stockmarketdatapipeline-spark-master-1 \
      spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
        /opt/spark/jobs/spark_batch_processor.py {{ ds }}

docker exec stockmarketdatapipeline-spark-master-1 \
      spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
        /opt/spark/jobs/spark_stream_processor.py


# For batch processor:
docker exec stockmarketdatapipeline-spark-client-1 \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
    /opt/spark/jobs/spark_batch_processor.py

# For stream processor:
docker exec stockmarketdatapipeline-spark-client-1 \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
    /opt/spark/jobs/spark_stream_processor.py

# For stream batch processor:
docker exec stockmarketdatapipeline-spark-client-1 \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
    /opt/spark/jobs/spark_stream_batch_processor.py