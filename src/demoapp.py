import tornado.ioloop
import tornado.web
import logging
from datetime import datetime
import json
from tornado import gen, ioloop
from tornado.ioloop import PeriodicCallback
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

"""
API:

/ui
/ping
/status
{ 
     id: { site:"", tier:"", name:"" }
     connections: [
       { direction: "in|out",
         target: "",
         live: true|false
         last_seen:
         message:  
        }
     ],
/dot
/png
"""
LOGGER = logging.getLogger(__name__)

# time before a host is considered dead
TIMEOUT_SECONDS = 5
# polling interval
POLLING_INTERVAL_SECONDS = 1

DIRECTION_IN = "in"
DIRECTION_OUT = "out"


class Connection(object):

    def __init__(self, myid, ip, direction):
        self.id = myid
        self.target = ip
        self.direction = direction
        self.last_seen = None
        self.message = ""
        self.live = False

    def is_in(self):
        return self.direction == DIRECTION_IN

    def seen(self, message=""):
        self.last_seen = datetime.now()
        self.live = True
        self.message = message

    def dead(self):
        self.live = False

    def age(self):
        return (datetime.now() - self.last_seen).total_seconds()

    def to_dict(self):
        return {
            "direction": self.direction,
            "target": self.target,
            "live": self.live,
            "last_seen": self.last_seen,
            "message": self.message
        }


def custom_json_encoder(o):
    """
        A custom json encoder that knows how to encode other types used by this app
    """
    if isinstance(o, datetime):
        return o.isoformat()

    if hasattr(o, "to_dict"):
        return o.to_dict()

    LOGGER.error("Unable to serialize %s", o)
    raise TypeError(repr(o) + " is not JSON serializable")


def json_encode(value):
    # see json_encode in tornado.escape
    return json.dumps(value, default=custom_json_encoder).replace("</", "<\\/")


class APP(object):

    def __init__(self, site, tier, name, port=8888, connect_to=[]):
        self.site = site
        self.tier = tier
        self.name = name
        self.port = port
        self.connect_to = connect_to

        self.connections = {}

        self.http_client = AsyncHTTPClient()

    def _get_connection(self, ip, direction, myid):
        if myid in self.connections:
            connection = self.connections[myid]
        else:
            connection = Connection(myid, ip, direction)
            self.connections[myid] = connection
        return connection

    def seen(self, site, tier, name, ip, direction=DIRECTION_IN):
        myid = (site, tier, name, direction)

        LOGGER.info("Incoming connection from %s %s %s (%s)", site, tier, name, ip)

        connection = self._get_connection(ip, direction, myid)

        connection.seen()

    def get_id(self):
        return {"site": self.site, "tier": self.tier, "name": self.name}

    def status(self):
        return {
            "id": self.get_id(),
            "connections": [x for x in self.connections.values()]
        }

    @gen.coroutine
    def check(self, url):
        connection = self._get_connection(url, DIRECTION_OUT, url)
        try:
            request = HTTPRequest(url + "/ping", "POST", headers={"Content-Type": "application/json"},
                                  body=json_encode(self.get_id()), request_timeout=0.9 * POLLING_INTERVAL_SECONDS)
            response = yield self.http_client.fetch(request)
            connection.seen(message=response.body.decode())

        except Exception as e:
            connection.message = repr(e)

    @gen.coroutine
    def run(self):
        ioloop = tornado.ioloop.IOLoop.current()

        while True:
            start = ioloop.time()

            for connection in [c for c in self.connections.values() if c.is_in() and c.live and c.age() > TIMEOUT_SECONDS]:
                connection.dead()

            yield [self.check(url) for url in self.connect_to]

            end = ioloop.time()
            duration = end - start
            sleeptime = POLLING_INTERVAL_SECONDS - duration

            LOGGER.info("Iteration done in %d time, sleeping %d" % (duration, sleeptime))

            yield gen.sleep(sleeptime)


class AppHandler(tornado.web.RequestHandler):

    def __init__(self, application, request, app: APP, **kwargs):
        super(AppHandler, self).__init__(application, request, **kwargs)
        self.app = app

    def error(self, status, msg):
        self.set_header("Content-Type", "application/json")
        self.write(tornado.escape.json_encode({"message": msg}))
        self.set_status(status, msg)


class PingHandler(AppHandler):

    def __init__(self, application, request, app: APP, **kwargs):
        super(AppHandler, self).__init__(application, request, **kwargs)
        self.app = app

    def post(self, *args, **kwargs):
        data = tornado.escape.json_decode(self.request.body)
        if not ("name" in data and "site" in data and "tier" in data):
            self.error(400, "Body should contain the fields 'name', 'site' and 'tier'")
        else:
            self.app.seen(data["site"], data["tier"], data["name"], self.request.remote_ip)
            self.set_header("Content-Type", "application/json")
            self.write(json_encode(self.app.get_id()))


class StatusHandler(AppHandler):

    def __init__(self, application, request, app: APP, **kwargs):
        super(AppHandler, self).__init__(application, request, **kwargs)
        self.app = app

    def get(self):
        self.set_header("Content-Type", "application/json")
        self.write(json_encode(self.app.status()))


def make_app():
    app = APP("Site A", "Tier A", "Alpha", connect_to=["http://127.0.0.1:8888", "http://127.0.0.1:8887"])

    tornado.ioloop.IOLoop.current().add_callback(app.run)
    return tornado.web.Application([
        (r"/ping", PingHandler, {"app": app}),
        (r"/status", StatusHandler, {"app": app}),
    ])


if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
