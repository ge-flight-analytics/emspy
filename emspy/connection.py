from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import sys
if sys.version_info < (3, 0):
    from future import standard_library
    standard_library.install_aliases()

from builtins import object
import gzip
import io
import json
import ssl
import urllib.error
import urllib.parse
import urllib.request

from numbers import Number
import pprint as pp
from . import common


class Connection(object):
    """
    Object for connection to EMS API
    """
    def __init__(self, user=None, pwd=None, proxies=None, verbose=False, ignore_ssl_errors=False,
                 server="prod", server_url=None, max_trials=3):
        """
        Connection initialization

        Parameters
        ----------
        user: str
            EMS username
        pwd: str
            EMS password
        proxies: dict
            proxies dictionary {'http': '', 'https': ''}
        verbose: bool
            verbose output (default False)
        ignore_ssl_errors: bool
            ignore SSL certificate errors (default False)
        server: str
            server to connect to from emspy.common.uri_root
        server_url: str
            hostname for server to connect to if different from any of the common values
        max_trials: int
            maximum number of reconnection attempts
        """
        self.__user = user
        self.__pwd = pwd
        self.__proxies = proxies
        self.__ntrials = 0
        self.__max_trials = max_trials
        self._uri_root = None
        self.__ignore_ssl_errors = ignore_ssl_errors
        self.token = None
        self.token_type = None

        # We assign the uri root to a member variable up front, and use that everywhere to
        # simplify. In order to use an alternate uri root, it must be specified in the constructor.
        if server_url is not None:
            if server_url.endswith('/api'):
                self._uri_root = server_url
            else:
                self._uri_root = server_url.rstrip('/') + '/api'
        else:
            self._uri_root = common.uri_root[server]

        if (user is not None) and (pwd is not None):
            self.connect(user, pwd, proxies, verbose)
        else:
            print("An empty connection is instantiated because credentials are not provided.\n")

    def connect(self, user, pwd, proxies=None, verbose=False):
        """
        Connect to EMS system using given credentials.

        Parameters
        ----------
        user: str
            EMS username
        pwd: str
            EMS password
        proxies: dict
            proxies dictionary (default None)
        verbose: bool
            verbose output (default False)

        Returns
        -------
        resp_h: list
            response headers
        content: dict
            response content
        """
        if proxies is not None:
            proxy_handler = urllib.request.ProxyHandler(proxies)
            opener = urllib.request.build_opener(proxy_handler, urllib.request.HTTPHandler)
            urllib.request.install_opener(opener)

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': common.user_agent
        }
        data = {
            'grant_type': 'password',
            'username': user,
            'password': pwd
        }

        resp_h, content = self.request(
            rtype="POST",
            uri_keys=('sys', 'auth'),
            data=data,
            headers=headers,
            proxies=proxies,
            verbose=verbose
        )

        # Add error handling --

        # Get the token
        self.token = content['access_token']
        self.token_type = content['token_type']
        return resp_h, content

    def reconnect(self, verbose=False):
        """
        Method to attempt reconnection to the API

        Parameters
        ----------
        verbose: bool
            verbose output (default False)

        Returns
        -------
        resp_h: list
            response headers
        content: dict
            response content
        """
        if self.__ntrials >= self.__max_trials:
            sys.exit("Stop trying to reconnect EMS API after %d trials" % self.__ntrials)
        self.__ntrials += 1
        return self.connect(self.__user, self.__pwd, self.__proxies, verbose)

    def request(self, rtype="GET", uri=None, uri_keys=None, uri_args=None, headers=None,
                body=None, data=None, jsondata=None, proxies=None, verbose=False):
        """
        Method to send a HTTP request to the API

        Parameters
        ----------
        rtype: str
            request type (UNUSED)
        uri: str
            request uri
        uri_keys: tuple
            request uri keys
        uri_args: list, tuple
            request uri arguments
        headers: dict
            request headers
        body: dict
            request body
        data: dict
            request data
        jsondata:
            JSON data
        proxies: dict
            proxies to use in request (UNUSED)
        verbose: bool
            verbose output (default False)

        Returns
        -------
        response_headers: list
            response headers
        content: dict
            response content
        """
        # If no custom headers are given, use our own
        if headers is None:
            headers = {
                'Authorization': ' '.join([self.token_type, self.token]),
                'Accept-Encoding': 'gzip',
                'User-Agent': common.user_agent
            }

        # If uri_keys are given, find the uri from the uris dictionary
        if uri_keys is not None:
            uri = self._uri_root + common.uris[uri_keys[0]][uri_keys[1]]

        # Provide the input to the request
        if uri_args is not None:
            # uri	= uri % uri_args
            # Unencoded url does not work all of sudden...

            # encode_args = lambda x: urllib.parse.quote(x) if type(x) in (str, unicode) else x
            # if type(uri_args) in (list, tuple):
            # 	uri = uri % tuple(map(encode_args, uri_args))
            # else:
            # 	uri = uri % encode_args(uri_args)

            if type(uri_args) not in (list, tuple):
                uri_args = [uri_args]

            uri_args = [x if isinstance(x, Number) else urllib.parse.quote(x) for x in uri_args]
            uri = uri % tuple(uri_args)

        # Append query to the uri if body is given
        if body is not None:
            uri = uri + "?" + urllib.parse.urlencode(body)

        # Encode the data
        if data is not None:
            data = urllib.parse.urlencode(data).encode('utf-8')

        if jsondata is not None:
            headers['Content-Type'] = 'application/json'
            data = json.dumps(jsondata).encode('utf-8')

        # uri = uri.encode('utf-8')
        request = urllib.request.Request(uri, data=data, headers=headers)
        try:
            response = self.__send_request(request)
            status_code = response.getcode()
            if status_code != 200:
                print("HTTP status code: %d" % status_code)
                verbose = True
        except:
            (exc_type, exc_value, exc_trace) = sys.exc_info()
            if exc_type is ssl.CertificateError:
                print("A certificate verification error occurred for the request to '%s'. "
                      "Certificate verification is required by default, but can be disabled by "
                      "using the ignore_ssl_errors argument for the Connection constructor." % uri)
                raise
            if exc_type is urllib.error.HTTPError:
                message_bytes = exc_value.read()
                message_str = message_bytes.decode('utf-8')
                print(exc_value.msg + '\n Details: ' + message_str)
                raise

            print("Trying to reconnect the EMS API.")
            self.reconnect()
            print("Done.")
            response = self.__send_request(request)
        response_headers = response.getheaders()

        # If the response is compressed, decompress it.
        if response.info().get('Content-Encoding') == 'gzip':
            buffer = io.BytesIO(response.read())
            file = gzip.GzipFile(fileobj=buffer)
            content = json.loads(file.read())
        else:
            content = json.loads(response.read())

        if verbose:
            print("URL: %s" % response.geturl())
            pp.pprint(response_headers)
            pp.pprint(content)

        return response_headers, content

    def __send_request(self, req):
        """
        Sends the request and returns the response, optionally ignoring ssl errors.

        Parameters
        ----------
        req: urllib.request.Request
            request object

        Returns
        -------
        request: urllib.request.Request
            request object
        """
        # Normally you do NOT want to ignore SSL errors, but this is
        # sometimes necessary on beta API endpoints without a proper cert.
        # TODO: Find a real fix for GE Mac OS machines that are having trouble verifying.
        if self.__ignore_ssl_errors:
            try:
                request = urllib.request.urlopen(req, context=None)
            except urllib.error.URLError:
                request = urllib.request.urlopen(req, context=ssl._create_unverified_context())
        else:
            request = urllib.request.urlopen(req, context=None)
        return request


def _print_resp(resp):
    for r in resp:
        pp.pprint(r)
