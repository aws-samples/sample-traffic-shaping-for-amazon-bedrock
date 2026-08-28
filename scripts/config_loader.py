import os
import sys
import boto3


def load_config():
    """
    Load configuration from config.env file in parent directory.
    Returns a dictionary with all config values.
    """
    # Find config.env in parent directory (one level up from scripts/)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, '..', 'config.env')
    
    if not os.path.exists(config_file):
        print("❌ config.env not found. Run: ./setup.sh or cp config.env.template config.env")
        sys.exit(1)
    
    # Parse config file
    config = {}
    with open(config_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue
            # Parse KEY=VALUE
            if '=' in line:
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip()
    
    # Set default region for boto3 early (before any DynamoService instantiation)
    region = config.get('AWS_REGION', 'us-east-1')
    os.environ['AWS_DEFAULT_REGION'] = region

    return config


def check_aws_access(region='us-east-1'):
    """
    Verify AWS credentials are valid by making a simple API call.
    Returns True if access is valid, False otherwise.
    """
    try:
        bedrock = boto3.client('bedrock', region_name=region)
        bedrock.list_foundation_models(byProvider='anthropic')
        return True
    except Exception as e:
        print(f"❌ AWS access failed: {e}")
        return False


def get_config_with_aws_check():
    """
    Load configuration and verify AWS access.
    Sets AWS_DEFAULT_REGION so boto3 uses the correct region.
    Exits gracefully if either fails.
    Returns config dictionary.
    """
    config = load_config()
    region = config.get('AWS_REGION', 'us-east-1')

    if check_aws_access(region):
        print(f"✅ AWS access verified (Region: {region})")
    else:
        sys.exit(1)

    return config


if __name__ == "__main__":
    # Test the config loader
    print("Testing config loader...")
    config = get_config_with_aws_check()
    print(f"✅ Config loaded successfully!")
    print(f"   State Machine: {config.get('STATE_MACHINE_ARN', 'Not set')}")