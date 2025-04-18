# -*- coding: utf-8 -*-
"""
Script to fetch AWS MSK metrics, cost data, and export them to an Excel file.
"""
import sys

import boto3
import datetime
import os
import pandas as pd

# Constants
METRIC_COLLECTION_PERIOD_DAYS = 7
AGGREGATION_DURATION_SECONDS = 3600

# Metrics
CLUSTER_INFO = ["Region", 'ClusterName', 'Authentication', "KafkaVersion", "EnhancedMonitoring"]
INSTANCE_INFO = ["NodeId", "NodeType", "VolumeSize (GB)"]
AVERAGE_METRICS = ['BytesInPerSec', 'BytesOutPerSec', 'MessagesInPerSec', 'CpuUser']
PEAK_METRICS = AVERAGE_METRICS + [
    'ConnectionCount', 'PartitionCount', 'GlobalTopicCount', 'EstimatedMaxTimeLag',
    'LeaderCount', 'ReplicationBytesOutPerSec', 'ReplicationBytesInPerSec'
]


def get_clusters_info(session):
    """Retrieve active MSK clusters."""
    conn = session.client('kafka')
    clusters = {}
    paginator = conn.get_paginator('list_clusters')
    for page in paginator.paginate():
        for instance in page['ClusterInfoList']:
            if instance['State'] == 'ACTIVE':
                clusters[instance['ClusterName']] = instance
    return {'msk_running_instances': clusters}


def get_metric(cloud_watch, cluster_id, node, metric, is_peak, time_period=METRIC_COLLECTION_PERIOD_DAYS):
    """Fetch CloudWatch metrics for a given MSK broker node."""
    end_time = datetime.date.today() + datetime.timedelta(days=1)
    start_time = end_time - datetime.timedelta(days=time_period)
    period = AGGREGATION_DURATION_SECONDS if is_peak else AGGREGATION_DURATION_SECONDS * 24 * time_period

    dimensions = [{'Name': 'Cluster Name', 'Value': cluster_id}]
    if metric != 'GlobalTopicCount':
        dimensions.append({'Name': 'Broker ID', 'Value': str(node)})

    response = cloud_watch.get_metric_statistics(
        Namespace='AWS/Kafka',
        MetricName=metric,
        Dimensions=dimensions,
        StartTime=start_time.isoformat(),
        EndTime=end_time.isoformat(),
        Period=period,
        Statistics=['Average']
    )

    return max((rec['Average'] for rec in response.get('Datapoints', [])), default=0)


def create_data_frame():
    """Create an empty DataFrame with required columns."""
    columns = CLUSTER_INFO.copy()
    columns += INSTANCE_INFO
    columns += [f"{metric} (avg)" for metric in AVERAGE_METRICS]
    columns += [f"{metric} (max)" for metric in PEAK_METRICS]
    return pd.DataFrame(columns=columns)


def write_clusters_info(df, clusters_info, session, region):
    """Populate DataFrame with MSK cluster data."""
    cloud_watch = session.client('cloudwatch')
    running_instances = clusters_info['msk_running_instances']

    rows = []
    for cluster_id, details in running_instances.items():
        print(f'Processing cluster account: {cluster_id}')
        cluster_info_written = False
        base_info = []

        if not cluster_info_written:
            # Get the cluster's auth configuration
            try:
                auth_config = running_instances[cluster_id]['ClientAuthentication']
                # Simplify auth info into a string
                auth_types = []
                if auth_config.get('Sasl', {}).get('Iam', {}).get('Enabled'):
                    auth_types.append("SASL/IAM")
                if auth_config.get('Sasl', {}).get('Scram', {}).get('Enabled'):
                    auth_types.append("SASL/SCRAM")
                if auth_config.get('Tls', {}).get('Enabled'):
                    auth_types.append("TLS")
                auth_string = ', '.join(auth_types) if auth_types else "None"
            except Exception as e:
                auth_string = "Unknown"

            # Shared info to write only once
            base_info = [
                region,
                cluster_id,
                auth_string,
                running_instances[cluster_id]['CurrentBrokerSoftwareInfo']['KafkaVersion'],
                running_instances[cluster_id]['EnhancedMonitoring']
            ]

        for node_id in range(1, details['NumberOfBrokerNodes'] + 1):
            # Empty version of the same size
            row = []
            if cluster_info_written:
                row += [""] * len(CLUSTER_INFO)
            else:
                row+=base_info
                cluster_info_written = True

            row += [
                node_id,
                details['BrokerNodeGroupInfo']['InstanceType'],
                details['BrokerNodeGroupInfo']['StorageInfo']['EbsStorageInfo']['VolumeSize']
            ]

            row += [get_metric(cloud_watch, cluster_id, node_id, metric, False) for metric in AVERAGE_METRICS]
            row += [get_metric(cloud_watch, cluster_id, node_id, metric, True) for metric in PEAK_METRICS]
            rows.append(row)

    return pd.DataFrame(rows, columns=df.columns)


def get_costs(session):
    """Fetch AWS MSK cost data from Cost Explorer."""
    try:
        pr = session.client('ce')
        now = datetime.datetime.now()
        start = (now.replace(day=1) - datetime.timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
        end = (now.replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        pricing_data = pr.get_cost_and_usage(
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

        return pd.DataFrame(data)
    except Exception as e:
        print(f"Error querying AWS Cost Explorer: {e}")
        return pd.DataFrame()


def process_aws_account(section, output_dir):
    """Process MSK metrics and cost data for an AWS account."""
    print(f'Processing AWS account: {section}')
    session = boto3.Session()
    output_file = os.path.join(output_dir, f"{section}-{session.region_name}.xlsx")
    writer = pd.ExcelWriter(output_file, engine='xlsxwriter')

    clusters_info = get_clusters_info(session)
    cluster_df = write_clusters_info(create_data_frame(), clusters_info, session, session.region_name)
    cluster_df.to_excel(excel_writer=writer, sheet_name='ClusterData', index=False)

    costs_df = get_costs(session)
    if not costs_df.empty:
        costs_df = pd.concat(
            [costs_df, pd.DataFrame([{"time_period": "TOTAL", "usage_type": "ALL", "cost": costs_df["cost"].sum()}])],
            ignore_index=True)
        costs_df.to_excel(excel_writer=writer, sheet_name='Costs', index=False)

    writer.close()
    print(f'Results saved to {output_file}')
