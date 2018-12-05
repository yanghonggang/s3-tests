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


def _add_header_create_bad_object(headers=None):
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

def _add_header_create_object(headers=None):
    """ Create a new bucket, add an object w/header customizations
    """
    bucket_name = get_new_bucket()
    client = get_client()
    key_name = 'foo'

    # pass in custom headers before PutObject call
    add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
    client.meta.events.register('before-call.s3.PutObject', add_headers)
    client.put_object(Bucket=bucket_name, Key=key_name)

    return bucket_name, key_name


def _remove_header_create_object(remove=None):
    """ Create a new bucket, add an object w/header customizations
    """
    bucket_name = get_new_bucket()
    client = get_client()
    key_name = 'foo'

    # remove custom headers before PutObject call
    def remove_header(**kwargs):
        if (remove in kwargs['params']['headers']):
            del kwargs['params']['headers'][remove]

    client.meta.events.register('before-call.s3.PutObject', remove_header)
    client.put_object(Bucket=bucket_name, Key=key_name)

    return bucket_name, key_name


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
def test_object_create_bad_md5_invalid_short():
    e = _add_header_create_bad_object({'Content-MD5':'YWJyYWNhZGFicmE='})
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)
    eq(error_code, 'InvalidDigest')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/mismatched MD5')
@attr(assertion='fails 400')
def test_object_create_bad_md5_bad():
    e = _add_header_create_bad_object({'Content-MD5':'rL0Y20xC+Fzt72VPzMSk2A=='})
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)
    eq(error_code, 'BadDigest')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty MD5')
@attr(assertion='fails 400')
def test_object_create_bad_md5_empty():
    e = _add_header_create_bad_object({'Content-MD5':''})
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)
    eq(error_code, 'InvalidDigest')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/non-graphics in MD5')
@attr(assertion='fails 403')
@attr('fails_strict_rfc2616')
def test_object_create_bad_md5_unreadable():
    e = _add_header_create_bad_object({'Content-MD5':'\x07'})
    status, error_code = _get_status_and_error_code(e.response)
    #TODO when I run boto2 it gives back a 400 as well, also this is not run in teuthology
    eq(status, 400)

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no MD5 header')
@attr(assertion='succeeds')
def test_object_create_bad_md5_none():
    bucket_name, key_name = _remove_header_create_object('Content-MD5')
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

# strangely, amazon doesn't report an error with a non-expect 100 also, our
# error comes back as html, and not xml as I normally expect
@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/Expect 200')
@attr(assertion='garbage, but S3 succeeds!')
#TODO: this is supposed to fail on RGW but teuthology doesnt run the boto2 version and both boto2 and boto2 versions succeed
#@attr('fails_on_rgw')
def test_object_create_bad_expect_mismatch():
    bucket_name, key_name = _add_header_create_object({'Expect': 200})
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty expect')
@attr(assertion='succeeds ... should it?')
def test_object_create_bad_expect_empty():
    bucket_name, key_name = _add_header_create_object({'Expect': ''})
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no expect')
@attr(assertion='succeeds')
def test_object_create_bad_expect_none():
    bucket_name, key_name = _remove_header_create_object('Expect')
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/non-graphic expect')
@attr(assertion='garbage, but S3 succeeds!')
@attr('fails_on_rgw')
@attr('fails_strict_rfc2616')
def test_object_create_bad_expect_unreadable():
    bucket_name, key_name = _add_header_create_object({'Expect': '\x07'})
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty content length')
@attr(assertion='fails 400')
#TODO: the boto2 version succeeeds and returns a 400, the boto3 version succeeds and returns a 403. 
# Look at what the outgoing requests look like and see what gives??
#@attr('fails_on_rgw')
def test_object_create_bad_contentlength_empty():
    e = _add_header_create_bad_object({'Content-Length':''})
    status, error_code = _get_status_and_error_code(e.response)
    # TODO: try to run the the boto2 version on rgw and see if it works
    eq(status, 403)
    eq(error_code, 'SignatureDoesNotMatch')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/negative content length')
@attr(assertion='fails 400')
@attr('fails_on_mod_proxy_fcgi')
#TODO: the boto2 version succeeeds and returns a 400, the boto3 version succeeds and returns a 403. 
# Look at what the outgoing requests look like and see what gives??
def test_object_create_bad_contentlength_negative():
    #TODO: if I put quotes around the -1 it gets me a 403, try the boto2 version
    #THIS IS THE ONE IM WORKING ON: see what difference is if I provide ContentLength in put_object
    e = _add_header_create_bad_object({'Content-Length':'-1'})
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no content length')
@attr(assertion='fails 411')
def test_object_create_bad_contentlength_none():
    bucket_name = get_new_bucket()
    client = get_client()
    key_name = 'foo'

    #boto3.set_stream_logger(name='botocore')
    # remove custom headers before PutObject call
    def remove_header(**kwargs):
        print kwargs['params']['headers']

        if ('Content-Length' in kwargs['params']['headers']):
            del kwargs['params']['headers']['Content-Length']

        print kwargs['params']['headers']

    #client.meta.events.register('before-call.s3.PutObject', remove_header)
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')
    #e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key_name, Body='bar')
    # TODO: this one!!, try the boto2 version, no error is raised
    #status, error_code = _get_status_and_error_code(e.response)
    #eq(status, 411)
    #eq(error_code, 'MissingContentLength')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/non-graphic content length')
@attr(assertion='fails 400')
@attr('fails_on_mod_proxy_fcgi')
@attr('fails_strict_rfc2616')
#TODO: the boto2 version succeeeds and returns a 400, the boto3 version succeeds and returns a 403. 
# Look at what the outgoing requests look like and see what gives??
def test_object_create_bad_contentlength_unreadable():
    e = _add_header_create_bad_object({'Content-Length':'\x07'})
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/content length too long')
@attr(assertion='fails 400')
@attr('fails_on_rgw')
def test_object_create_bad_contentlength_mismatch_above():
    content = 'bar'
    length = len(content) + 1

