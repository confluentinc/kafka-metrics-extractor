"""
Script to fetch Kafka metrics, cost data, and export them to an Excel file.

Usage:
    python pullStats.py config.cfg output_directory
"""

import configparser
import os
import sys
import pullMSKStats
import argparse


def main():
    # Create an ArgumentParser instance
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file", help="Path to configuration file", metavar="FILE")
    parser.add_argument("output_dir", default=".", help="Output directory", nargs='?', metavar="PATH")
    args = parser.parse_args()


    if not args.config_file:
        print("Usage: script.py <config file> <output directory>")
        sys.exit(1)


    config_file = args.config_file

    config = configparser.ConfigParser()
    config.read(config_file)

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    for section in config.sections():
        if config.get(section, 'cluster_type') == "msk":
            pullMSKStats.process_aws_account(section, args.output_dir)
        else:
            print("❌ Invalid cluster type. currently only 'msk' is supported.")

if __name__ == "__main__":
    main()