import configparser
import sys
import pullMSKStats
import pullKafkaStats
import argparse


def main():
    # Create an ArgumentParser instance
    parser = argparse.ArgumentParser()
    parser.add_argument("cluster_type", help="Cluster type (msk\osk\eventhub)", metavar="TYPE")
    parser.add_argument("config_file", help="Path to configuration file", metavar="FILE")
    parser.add_argument("output_dir", default=".", help="Output directory", nargs='?', metavar="PATH")
    args = parser.parse_args()


    if not args.config_file or not args.cluster_type:
        print("Usage: script.py <config file> <output directory>")
        sys.exit(1)


    cluster_type = args.cluster_type
    config_file = args.config_file

    # config = configparser.ConfigParser()
    # config.read(config_file)

    if cluster_type == "msk":
        print(f"📡 Running AWS MSK stats...")
        pullMSKStats.processMSKStats(config_file, args.output_dir)
    elif cluster_type == "osk":
        print(f"📡 Running Open-Source Kafka stats...")
        pullKafkaStats.processKafkaStats(config_file, args.output_dir)
    else:
        print("❌ Invalid cluster type. Use 'msk' or 'kafka'.")
        sys.exit(1)

if __name__ == "__main__":
    main()