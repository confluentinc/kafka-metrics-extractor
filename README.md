# 📊 kafka-metrics-extractor
 
`kafka-metrics-extractor` is a tool designed to pull raw usage from Kafka providers such as MSK, OSK and others (currently supports MSK).
The script for extracting MSK usage, it uses MSK permissions to list and describe the clusters only and then 
collects the usage data from CloudWatch and CostExplorer in order to avoid any cluster disruption. 
 
## 🚀 Installation and Setup
 
### 1️⃣ Clone the Repository
```bash
git clone https://github.com/confluentinc/kafka-metrics-extractor
cd kafka-metrics-extractor
```
 
### 2️⃣ Set Up a Virtual Environment
make sure your have python version is >=3.8
```bash
python3 -m venv venv
source venv/bin/activate
```
 
### 3️⃣ Install Dependencies
```bash
pip3 install -r requirements.txt
```
 
### 4️⃣ Configure the Script
Copy the example configuration file and update it as needed:
```bash
cp config.cfg.example config.cfg
```

### 🔐 Credential Setup
MSK: You can authenticate using long-term credentials or temporary session credentials (via AWS STS).
The script will extract the all clusters usage according to provided account and region
```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_SESSION_TOKEN=your_token     # Optional (if using temporary credentials)
export AWS_DEFAULT_REGION=us-east-1
```

### 🔑 Required AWS Permissions

To run the script successfully, your IAM user or role must have the following permissions:

#### ✅ Required AWS Services:
- Amazon MSK
- Amazon CloudWatch
- AWS Cost Explorer

#### 🔐 Minimum IAM Policy Permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kafka:ListClusters",
        "kafka:DescribeCluster"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:ListMetrics"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "ce:GetCostAndUsage"
      ],
      "Resource": "*"
    }
  ]
}
```

### 5️⃣ Run the Script
Execute the script with the configuration file:
```bash
python pullMSKStats.py config.cfg <output directory>
```
 
### 6️⃣ Deactivate the Virtual Environment (When Finished)
```bash
deactivate
```
 
## 🔮 Future Plans
- 🐳 Docker support (coming soon)
- Open Source Kafka
- EventHub
