import boto3
from nose.tools import eq_ as eq
from nose.plugins.attrib import attr
from nose.plugins.attrib import attr
import nose
from botocore.exceptions import ClientError

from .utils import assert_raises

from . import (
    get_client,
    get_new_bucket,
    )

#TODO: move this into utils
def _get_status_and_error_code(response):
    status = response['ResponseMetadata']['HTTPStatusCode']
    error_code = response['Error']['Code']
    return status, error_code


def _setup_bad_object(headers=None, remove=None):
    """ Create a new bucket, add an object w/header customizations
    """
    bucket_name = get_new_bucket()
    client = get_client()
    key_name = 'foo'

    # pass in custom headers before PutObject call
    add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
    client.meta.events.register('before-call.s3.PutObject', add_headers)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key_name, Body='bar')

    return e

def tag(*tags):
    def wrap(func):
        for tag in tags:
            setattr(func, tag, True)
        return func
    return wrap

#
# common tests
#

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/invalid MD5')
@attr(assertion='fails 400')
#TODO: have a similar function if necessary
#@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_md5_invalid_short():
    e = _setup_bad_object({'Content-MD5':'YWJyYWNhZGFicmE='})
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)
    eq(error_code, 'InvalidDigest')

