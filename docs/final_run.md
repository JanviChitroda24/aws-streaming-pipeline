# 1. Create Kinesis
aws kinesis create-stream --stream-name stock-trades-stream --shard-count 1 --region us-east-1

# 2. Wait 10 seconds, verify
aws kinesis describe-stream-summary --stream-name stock-trades-stream --region us-east-1 --query 'StreamDescriptionSummary.StreamStatus'

# 3. Clear checkpoints
aws s3 rm s3://stock-streaming-pipeline-jc/glue-checkpoints/bronze-raw/ --recursive
aws s3 rm s3://stock-streaming-pipeline-jc/glue-checkpoints/silver-vwap-1min/ --recursive
aws s3 rm s3://stock-streaming-pipeline-jc/glue-checkpoints/silver-vwap-5min/ --recursive
aws s3 rm s3://stock-streaming-pipeline-jc/glue-checkpoints/gold-anomaly/ --recursive

# 4. Open CloudWatch dashboard in browser to watch live

# 5. Start producer
python3 src/producer.py --mode simulated --eps 20 --duration 700

# 6. Execute Step Functions
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:366447947905:stateMachine:stock-streaming-pipeline \
  --region us-east-1

# 7. Watch Step Functions console + CloudWatch dashboard
# 8. Wait for "✅ Stock Pipeline — Complete Success" email


# 9. Delete Kinesis
aws kinesis delete-stream --stream-name stock-trades-stream --region us-east-1