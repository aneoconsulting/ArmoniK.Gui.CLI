import pytest
import grpc
import armonik_cli.admin as admin
from armonik.common.filter import Filter
import os
import sys
from dotenv import load_dotenv


def test_create_channel_insecure():
    endpoint = "localhost:5001"
    channel = admin.create_channel(endpoint)
    assert isinstance(channel, grpc.Channel)

def test_create_channel_invalid_path():
    endpoint = "localhost:5001"
    ca_file = ""
    cert_file = ""
    key_file = ""
    
    with pytest.raises(FileNotFoundError):
        admin.create_channel(endpoint, ca=ca_file, cert=cert_file, key=key_file)


# @pytest.mark.skipif(sys.version_info < (4, 10), reason="require ArmoniK Deployment in mTLS")
def test_create_channel_secure():
    load_dotenv()
    endpoint = "armonik.local:5001"
    ca_file = os.getenv('CA_PATH')
    cert_file = os.getenv('CERT_PATH')
    key_file = os.getenv('KEY_PATH')
    channel = admin.create_channel(endpoint, ca=ca_file, cert=cert_file, key=key_file)
    print(ca_file, cert_file, key_file)
    assert isinstance(channel, grpc.Channel)
    

    

def test_creating_task_filter_all():
    task_filter = admin.create_task_filter("b", True, False, False)
    assert isinstance(task_filter, Filter)

def test_parse_args_with_specifiy_endpoint():
    args = admin.parse_arguments(
        '--endpoint myendpoint list-session --all'.split()
    )
    print(args)
    assert args.endpoint == 'myendpoint'
    assert isinstance(args.filter, Filter)

def test_parse_args_with_default_endpoint():
    args = admin.parse_arguments(
        'list-session --all'.split()
    )
    print(args)
    assert args.endpoint == 'localhost:5001'
    assert isinstance(args.filter, Filter)

def test_parse_args_with_ca():
    args = admin.parse_arguments(
        '--ca path/to/ca list-session --all'.split()
    )
    assert args.endpoint == 'localhost:5001'
    assert args.ca == 'path/to/ca'

def test_parse_args_with_ca_cert_key():
    args = admin.parse_arguments(
        '--ca path/to/ca --cert path/to/cert --key path/to/key list-session --all'.split()
    )
    assert args.endpoint == 'localhost:5001'
    assert args.ca == 'path/to/ca'
    assert args.cert == 'path/to/cert'
    assert args.key == 'path/to/key'
