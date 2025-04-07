# 📊 kafka-metrics-extractor
 
`kafka-metrics-extractor` is a tool designed to pull raw usage from Kafka providers such as MSK, OSK and others (currently supports MSK).
 
## 🚀 Installation and Setup
 
### 1️⃣ Clone the Repository
```bash
git clone https://github.com/oisraeli/kafka-metrics-extractor
cd kafka-metrics-extractor
```
 
### 2️⃣ Set Up a Virtual Environment
```bash
mkdir .env
virtualenv .env
source .env/bin/activate
```
 
### 3️⃣ Install Dependencies
```bash
pip install -r requirements.txt
```
 
### 4️⃣ Configure the Script
Copy the example configuration file and update it as needed:
```bash
cp config.cfg.example config.cfg
```

### 🔐 Credential Setup
MSK: You can authenticate using long-term credentials or temporary session credentials (via AWS STS).
```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_SESSION_TOKEN=your_token     # Optional (if using temporary credentials)
export AWS_DEFAULT_REGION=us-east-1
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
