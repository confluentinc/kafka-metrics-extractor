# -*- coding: utf-8 -*-
"""
Script to fetch AWS MSK metrics, cost data, and export them to an Excel file.

Refactored for improved readability and maintainability.
"""

import boto3
from datetime import datetime, timedelta, timezone
import os
import pandas as pd


# Constants
METRIC_COLLECTION_PERIOD_DAYS = 7
AGGREGATION_DURATION_SECONDS = 3600  # 1 Hour

# Metrics
CLUSTER_INFO = ["Region", 'ClusterName', 'Availability', 'Authentication', "KafkaVersion", "EnhancedMonitoring"]
INSTANCE_INFO = ["NodeId", "NodeType", "VolumeSize (GB)"]
AVERAGE_METRICS = ['BytesInPerSec', 'BytesOutPerSec', 'MessagesInPerSec', 'KafkaDataLogsDiskUsed']
PEAK_METRICS = AVERAGE_METRICS + [
    'ClientConnectionCount', 'PartitionCount', 'GlobalTopicCount',
    'LeaderCount', 'ReplicationBytesOutPerSec', 'ReplicationBytesInPerSec'
]
AVERAGE_METRICS_SERVERLESS = ['BytesInPerSec', 'BytesOutPerSec', 'MessagesInPerSec']
PEAK_METRICS_SERVERLESS = ['BytesInPerSec', 'BytesOutPerSec', 'MessagesInPerSec']


def get_msk_clusters(session):
    """
    Retrieves active MSK clusters using the AWS Kafka client.

    Args:
        session (boto3.Session): The AWS session to use.

    Returns:
        dict: A dictionary containing active MSK clusters,
              or an empty dictionary if no active clusters are found.
    """
    kafka_client = session.client('kafka')
    clusters = {}
    paginator = kafka_client.get_paginator('list_clusters_v2')
    for page in paginator.paginate():
        for cluster in page['ClusterInfoList']:
            if cluster['State'] == 'ACTIVE':
                clusters[cluster['ClusterName']] = cluster
    return {'msk_running_instances': clusters}


def get_cloudwatch_serverless_metric(
        cloudwatch_client,
        cluster_id: str,  # For MSK Serverless, this should be the Cluster ARN
        metric_name: str,
        is_peak: bool,
        time_period: int = METRIC_COLLECTION_PERIOD_DAYS
):
    """
    Collects CloudWatch metrics for a serverless MSK cluster at the topic level
    and sums the results to reflect the metric for the entire cluster.

    The function sums the 'Average' statistic for each topic if 'is_peak' is False,
    and sums the 'Maximum' statistic for each topic if 'is_peak' is True.

    Args:
        cloudwatch_client: Initialized boto3 CloudWatch client.
                           Example: boto3.client('cloudwatch', region_name='your-region')
        cluster_id: The ARN of the MSK Serverless cluster.
                    Example: 'arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/abcdef12-3456-7890-abcd-ef1234567890'
        metric_name: The name of the metric to collect (e.g., 'BytesInPerSec', 'MessagesInPerSec').
        is_peak: Boolean.
                 If True, the function sums the peak value (Maximum statistic) of the metric from each topic.
                 If False, the function sums the average value (Average statistic) of the metric from each topic.
        time_period: The number of days in the past to collect metrics for.
                     Defaults to METRIC_COLLECTION_PERIOD_DAYS (e.g., 7 days).

    Returns:
        A float representing the summed metric value for the cluster.
        Returns 0.0 if no topics are found, no data is available for any topic, or an error occurs.
    """
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=time_period)
    period = int(time_period * 24 * 60 * 60)  # time_period in seconds
    # end_time = datetime.date.today() + datetime.timedelta(days=1)
    # start_time = end_time - datetime.timedelta(days=time_period)

    # For MSK Serverless, the dimension key for the cluster identifier is 'Cluster Arn'.
    # The 'cluster_id' parameter is expected to be this ARN.
    cluster_dimension_key = 'Cluster Name'

    # --- 1. Discover topics by listing metrics for the cluster ---
    # We list metrics matching the metric_name and cluster_id, then extract topic names
    # from the dimensions of these metrics.
    topics = set()
    paginator = cloudwatch_client.get_paginator('list_metrics')

    try:
        list_metrics_params = {
            'Namespace': 'AWS/Kafka',  # MSK Serverless metrics are in this namespace
            'MetricName': metric_name,
            'Dimensions': [{'Name': cluster_dimension_key, 'Value': cluster_id}]
            # 'Dimensions': [{'Name': cluster_dimension_key, 'Value': 'arn:aws:kafka:us-east-1:829250931565:cluster/ohad-serverless/a1bace2f-cab4-4e7c-b0e2-5fafec1ee0ce-s2'}]
        }

        # print(f"DEBUG: Listing metrics with params: {list_metrics_params}") # Uncomment for debugging

        for page in paginator.paginate(**list_metrics_params):
            for metric in page.get('Metrics', []):
                # A metric entry can have multiple dimensions. We are looking for 'Topic'.
                has_topic_dimension = False
                topic_value = None
                # Verify it's for the correct cluster (list_metrics filter should handle this, but good to be sure)
                is_correct_cluster = False

                for dim in metric.get('Dimensions', []):
                    if dim['Name'] == cluster_dimension_key and dim['Value'] == cluster_id:
                        is_correct_cluster = True
                    if dim['Name'] == 'Topic':
                        has_topic_dimension = True
                        topic_value = dim['Value']

                if is_correct_cluster and has_topic_dimension and topic_value:
                    topics.add(topic_value)

    except Exception as e:
        # More specific error handling (e.g., botocore.exceptions.ClientError) is recommended in production.
        print(f"Error listing metrics to discover topics for cluster {cluster_id}, metric {metric_name}: {e}")
        return 0.0

    if not topics:
        print(f"No topics found for cluster {cluster_id} and metric {metric_name} with a 'Topic' dimension.")
        return 0.0

    # print(f"DEBUG: Found topics for cluster {cluster_id}: {topics}") # Uncomment for debugging

    # --- 2. Prepare MetricDataQuery for each topic ---
    # We will request a single data point (Average or Maximum) for the entire time_period for each topic.
    metric_data_queries = []

    # The 'Period' for MetricStat should cover the entire time_period to get a single aggregated value.
    # It must be in seconds and a multiple of 60.
    statistic_to_fetch = 'Maximum' if is_peak else 'Average'

    for i, topic_name in enumerate(list(topics)):  # Convert set to list for unique ID generation
        # MetricDataQuery Id must start with a lowercase letter and contain only lowercase letters, numbers, and underscore.
        query_id = f"query_{metric_name.lower().replace('-', '_').replace('.', '_')}_{i}"
        metric_data_queries.append({
            'Id': query_id,
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/Kafka',
                    'MetricName': metric_name,
                    'Dimensions': [
                        {'Name': cluster_dimension_key, 'Value': cluster_id},
                        {'Name': 'Topic', 'Value': topic_name}
                    ]
                },
                'Period': period,
                'Stat': statistic_to_fetch,
            },
            'ReturnData': True,  # We want data back for this query
        })

    if not metric_data_queries:  # Should be caught by "if not topics"
        return 0.0

    # --- 3. Fetch metric data using GetMetricData ---
    # GetMetricData can handle up to 500 MetricDataQuery objects in a single call.
    # Batching is implemented for robustness if there are many topics.
    aggregated_sum_of_metric_values = 0.0
    max_queries_per_call = 500

    for i in range(0, len(metric_data_queries), max_queries_per_call):
        batch_queries = metric_data_queries[i:i + max_queries_per_call]
        try:
            # print(f"DEBUG: Calling GetMetricData with StartTime={start_time}, EndTime={end_time}") # Uncomment for debugging
            # for q_debug in batch_queries: print(f"DEBUG: Query: {q_debug}") # Uncomment for debugging

            response = cloudwatch_client.get_metric_data(
                MetricDataQueries=batch_queries,
                StartTime=start_time,
                EndTime=end_time,
                ScanBy='TimestampAscending'  # Order doesn't matter much for a single datapoint result
            )

            # print(f"DEBUG: GetMetricData response: {response}") # Uncomment for debugging

            for result in response.get('MetricDataResults', []):
                if result.get('Values') and len(result['Values']) > 0:
                    # We expect one value because 'Period' covers the whole time range.
                    value_from_topic = result['Values'][0]
                    aggregated_sum_of_metric_values += value_from_topic

                    # # For detailed debugging of which topic yielded what value:
                    # current_query_id = result['Id']
                    # topic_for_result = "Unknown"
                    # for q_assoc in batch_queries:
                    #     if q_assoc['Id'] == current_query_id:
                    #         for dim_assoc in q_assoc['MetricStat']['Metric']['Dimensions']:
                    #             if dim_assoc['Name'] == 'Topic':
                    #                 topic_for_result = dim_assoc['Value']
                    #                 break
                    #         break
                    # print(f"DEBUG: Data for QueryId {result['Id']} (Topic: {topic_for_result}, Stat: {statistic_to_fetch}): Value = {value_from_topic}")

                else:
                    # No data found for this specific topic/metric combination in the time range.
                    # This can happen if a topic had no activity for that metric during the period.
                    current_query_id = result['Id']
                    topic_for_result = "Unknown"  # Fallback
                    for q_debug_nd in batch_queries:
                        if q_debug_nd['Id'] == current_query_id:
                            for dim_nd in q_debug_nd['MetricStat']['Metric']['Dimensions']:
                                if dim_nd['Name'] == 'Topic':
                                    topic_for_result = dim_nd['Value']
                                    break
                            break
                    print(
                        f"Info: No data found for QueryId {result['Id']} (Topic: {topic_for_result}, Stat: {statistic_to_fetch}) for period {start_time} to {end_time}.")

        except Exception as e:  # More specific error handling is better in production
            print(f"Error during get_metric_data call for a batch: {e}")
            # Depending on requirements, you might want to:
            # - Continue and sum whatever data was retrieved.
            # - Return the partial sum obtained so far.
            # - Return 0.0 or None to indicate failure.
            # - Raise the exception.
            # Current behavior: sums what it can, prints error, continues.
            # If a call fails, the sum might be incomplete. For critical applications,
            # consider more robust error handling like retries or returning an error indicator.

    return aggregated_sum_of_metric_values


def get_cloudwatch_metric(cloudwatch_client, cluster_id, metric_name, is_peak, node, time_period=METRIC_COLLECTION_PERIOD_DAYS):
    """
    Fetches CloudWatch metrics for a given MSK cluster (and optionally node).

    Args:
        cloudwatch_client (boto3.client): The CloudWatch client.
        cluster_id (str): The ID of the MSK cluster.
        metric_name (str): The name of the CloudWatch metric to retrieve.
        is_peak (bool):  If True, retrieve peak value, otherwise retrieve average.
        node (int, optional): The ID of the broker node (for PROVISIONED clusters). Defaults to None.
        time_period (int): The time period in days over which to collect metrics.

    Returns:
        float: The peak or average value of the metric, or 0 if no data is available.
    """
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=time_period)

    # Calculate period dynamically
    period = int(time_period * 24 * 60 * 60)  # time_period in seconds

    dimensions = [{'Name': 'Cluster Name', 'Value': cluster_id}]
    if node is not None and metric_name != 'GlobalTopicCount':
        dimensions.append({'Name': 'Broker ID', 'Value': str(node)})

    statistics = ['Maximum'] if is_peak else ['Average']

    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/Kafka',
        MetricName=metric_name,
        Dimensions=dimensions,
        StartTime=start_time.isoformat(),
        EndTime=end_time.isoformat(),
        Period=period,
        Statistics=statistics
    )
    # Check for empty Datapoints
    if not response.get('Datapoints'):
        return 0

    if is_peak:
        return response['Datapoints'][0]['Maximum']
    else:
        return response['Datapoints'][0]['Average']


def create_dataframe():
    """
    Creates an empty Pandas DataFrame with the specified columns.

    Returns:
        pd.DataFrame: An empty DataFrame with columns for cluster info,
                      instance info, and metrics.
    """
    columns = CLUSTER_INFO.copy()
    columns += INSTANCE_INFO
    columns += [f"{metric} (avg)" for metric in AVERAGE_METRICS]
    columns += [f"{metric} (max)" for metric in PEAK_METRICS]
    return pd.DataFrame(columns=columns)



def get_msk_cluster_data(session, region):
    """
    Retrieves MSK cluster information and metrics, and organizes it into a Pandas DataFrame.

    Args:
        session (boto3.Session): The AWS session to use.
        region (str): The AWS region.

    Returns:
        pd.DataFrame: A DataFrame containing MSK cluster data.
    """
    cloudwatch_client = session.client('cloudwatch')
    running_instances = get_msk_clusters(session)['msk_running_instances'] # corrected function call
    cluster_df = create_dataframe()
    rows = []

    for cluster_id, details in running_instances.items():
        cluster_type = details.get('ClusterType', 'CLUSTERLESS').upper()
        print(f'Processing cluster account: {cluster_id}')
        cluster_info_written = False
        base_info = []

        if not cluster_info_written:
            auth_string = "N/A"  # Default value
            az_distribution = "N/A"
            kafka_version = "N/A"
            enhanced_monitoring = "N/A"
            number_of_broker_nodes = 1  # for serverless
            instance_type = "N/A"
            volume_size = 0

            # Get the cluster's auth configuration
            if cluster_type == 'PROVISIONED':
                auth_config = details['Provisioned']['ClientAuthentication']
                az_distribution = details['Provisioned']["BrokerNodeGroupInfo"]["BrokerAZDistribution"]
                if az_distribution == "DEFAULT":
                    az_distribution = "Multiple AZ"
                elif az_distribution == "SINGLE":
                    az_distribution = "Single AZ"
                kafka_version = details['Provisioned']['CurrentBrokerSoftwareInfo']['KafkaVersion'],
                enhanced_monitoring = details['Provisioned']['EnhancedMonitoring']
                number_of_broker_nodes = details['Provisioned']['NumberOfBrokerNodes']
                instance_type = details.get('Provisioned', {}).get('BrokerNodeGroupInfo', {}).get('InstanceType', "N/A")
                volume_size =  details.get('Provisioned', {}).get('BrokerNodeGroupInfo', {}).get('StorageInfo', {}).get('EbsStorageInfo', {}).get('VolumeSize', 0)
            else:
                auth_config = details['Serverless']['ClientAuthentication']
                az_distribution = "Multiple AZ"

            # Simplify auth info into a string
            auth_types = []
            if auth_config.get('Sasl', {}).get('Iam', {}).get('Enabled'):
                auth_types.append("SASL/IAM")
            if auth_config.get('Sasl', {}).get('Scram', {}).get('Enabled'):
                auth_types.append("SASL/SCRAM")
            if auth_config.get('Tls', {}).get('Enabled'):
                auth_types.append("TLS")
            auth_string = ', '.join(auth_types) if auth_types else "None"

            # Shared info to write only once
            base_info = [
                region,
                cluster_id,
                az_distribution,
                auth_string,
                kafka_version,
                enhanced_monitoring
            ]
        number_of_nodes = number_of_broker_nodes if cluster_type == 'PROVISIONED' else 1
        for node_id in range(1, number_of_nodes + 1):
            # Empty version of the same size
            row = []
            if cluster_info_written:
                row += [""] * len(CLUSTER_INFO)
            else:
                row += base_info
                cluster_info_written = True

            row += [
                node_id,
                instance_type,
                volume_size
            ]

            # only works for PROVISIONED
            if cluster_type == 'PROVISIONED':
                pass
                row += [get_cloudwatch_metric(cloudwatch_client, cluster_id, metric, False, node_id) for metric in
                        AVERAGE_METRICS]
                row += [get_cloudwatch_metric(cloudwatch_client, cluster_id, metric, True, node_id) for metric in
                        PEAK_METRICS]
            else:
                row += [get_cloudwatch_serverless_metric(cloudwatch_client, cluster_id, metric, False, node_id) for metric in
                        AVERAGE_METRICS]
                row += [get_cloudwatch_serverless_metric(cloudwatch_client, cluster_id, metric, True, node_id) for metric in
                        PEAK_METRICS]

            rows.append(row)

    return pd.DataFrame(rows, columns=cluster_df.columns)



def get_aws_costs(session):
    """
    Fetches AWS MSK cost data from Cost Explorer.

    Args:
        session (boto3.Session): The AWS session to use.

    Returns:
        pd.DataFrame: A DataFrame containing MSK cost data,
                      or an empty DataFrame on error.
    """
    try:
        cost_explorer = session.client('ce')
        now = datetime.now()
        start = (now.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
        end = (now.replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')

        pricing_data = cost_explorer.get_cost_and_usage(
            TimePeriod={'Start': start, 'End': end},
            Granularity='MONTHLY',
            Filter={
                "And": [
                    {"Dimensions": {'Key': 'REGION', 'Values': [session.region_name]}},
                    {"Dimensions": {'Key': 'SERVICE', 'Values': ['Amazon Managed Streaming for Apache Kafka']}}
                ]
            },
            Metrics=['UnblendedCost'],
            GroupBy=[{'Type': 'DIMENSION', 'Key': 'USAGE_TYPE'}]
        )

        data = [
            {"time_period": res["TimePeriod"]["Start"], "usage_type": group["Keys"][0],
             "cost": float(group["Metrics"]["UnblendedCost"]["Amount"])}
            for res in pricing_data['ResultsByTime'] for group in res['Groups']
        ]
        cost_df = pd.DataFrame(data)
        total_cost = cost_df["cost"].sum()
        total_row = pd.DataFrame([{"time_period": "TOTAL", "usage_type": "ALL", "cost": total_cost}])
        cost_df = pd.concat([cost_df, total_row], ignore_index=True)
        return cost_df

    except Exception as e:
        print(f"Error querying AWS Cost Explorer: {e}")
        return pd.DataFrame()



def process_aws_account(section, output_dir):
    """
    Processes MSK metrics and cost data for an AWS account, and saves it to an Excel file.

    Args:
        section (str):  The section/account identifier.
        output_dir (str): The directory where the Excel file should be saved.
    """
    print(f'Processing AWS account: {section}')
    session = boto3.Session()
    output_file = os.path.join(output_dir, f"{section}-{session.region_name}.xlsx")
    excel_writer = pd.ExcelWriter(output_file, engine='xlsxwriter')

    cluster_df = get_msk_cluster_data(session, session.region_name)
    cluster_df.to_excel(excel_writer=excel_writer, sheet_name='ClusterData', index=False)

    costs_df = get_aws_costs(session)
    if not costs_df.empty:
        costs_df.to_excel(excel_writer=excel_writer, sheet_name='Costs', index=False)

    excel_writer.close()
    print(f'Results saved to {output_file}')

