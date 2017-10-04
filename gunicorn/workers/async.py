# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.

from datetime import datetime
import errno
import socket
import ssl

import gunicorn.http as http
import gunicorn.http.wsgi as wsgi
import gunicorn.util as util
import gunicorn.workers.base as base
from gunicorn import six


class AsyncWorker(base.Worker):

    def __init__(self, *args, **kwargs):
        super(AsyncWorker, self).__init__(*args, **kwargs)
        self.worker_connections = self.cfg.worker_connections
        self.keep_alive = self.cfg.keepalive

    def timeout_ctx(self):
        raise NotImplementedError()

    def handle(self, listener, client, addr, _socket_error=socket.error, _next=six.next, _RequestParser=http.RequestParser,
            _NoMoreData=http.errors.NoMoreData, _SSLError=ssl.SSLError):
        req = None
        try:
            parser = _RequestParser(self.cfg, client)
            try:
                if not self.keep_alive:
                    req = _next(parser)
                    self.handle_request(listener, req, client, addr)
                else:
                    # keepalive loop
                    while True:
                        req = None
                        with self.timeout_ctx():
                            req = _next(parser)
                        if not req:
                            break
                        self.handle_request(listener, req, client, addr)
            except _NoMoreData as e:
                self.log.debug("Ignored premature client disconnection. %s", e)
            except StopIteration as e:
                self.log.debug("Closing connection. %s", e)
            except _SSLError:
                raise  # pass to next try-except level
            except _socket_error:
                raise  # pass to next try-except level
            except Exception as e:
                self.handle_error(req, client, addr, e)
        except _SSLError as e:
            if e.args[0] == ssl.SSL_ERROR_EOF:
                self.log.debug("ssl connection closed")
                client.close()
            else:
                self.log.debug("Error processing SSL request.")
                self.handle_error(req, client, addr, e)
        except _socket_error as e:
            if e.args[0] not in (errno.EPIPE, errno.ECONNRESET):
                self.log.exception("Socket error processing request.")
            else:
                if e.args[0] == errno.ECONNRESET:
                    self.log.debug("Ignoring connection reset")
                else:
                    self.log.debug("Ignoring EPIPE")
        except Exception as e:
            self.handle_error(req, client, addr, e)
        finally:
            try:
                client.close()
            except _socket_error:
                pass

    def handle_request(self, listener, req, sock, addr, _wsgi_create=wsgi.create):

        environ = {}
        response = None
        try:

            response, environ = _wsgi_create(req, sock, addr, listener.getsockname(), self.cfg)

            if not self.keep_alive:
                response.force_close()

            response_iter = self.wsgi(environ, response.start_response)

            for item in response_iter:
                response.write(item)

            response.close()

        except Exception:
            if response and response.headers_sent:
                # If the requests have already been sent, we should close the
                # connection to indicate the error.
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                    sock.close()
                except socket.error:
                    pass
                raise StopIteration()
            raise

        return True
