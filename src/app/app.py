import tornado.ioloop
import tornado.web
import logging
from datetime import datetime
import json
from tornado import gen, ioloop
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.escape import json_decode
import dateutil.parser
import argparse
import toml


LOGGER = logging.getLogger(__name__)

# time before a host is considered dead
TIMEOUT_SECONDS = 5
# time before a _topology line is considered dead
TOPOLOGY_TIMEOUT_SECONDS = 15
# polling interval
POLLING_INTERVAL_SECONDS = 1

DIRECTION_IN = "in"
DIRECTION_OUT = "out"


class Connection(object):

    def __init__(self, myid, ip, direction, location=""):
        self.id = myid
        self.target = ip
        self.direction = direction
        self.last_seen = None
        self.message = ""
        self.live = False
        self.remote_id = ""
        self.location = location

    def is_in(self):
        return self.direction == DIRECTION_IN

    def is_alive(self):
        return self.live and (datetime.now() - self.last_seen).total_seconds() < TIMEOUT_SECONDS

    def seen(self, remote_id="", message=""):
        self.last_seen = datetime.now()
        self.live = True
        self.message = message
        self.remote_id = remote_id

    def dead(self):
        self.live = False

    def age(self):
        return (datetime.now() - self.last_seen).total_seconds()

    def to_dict(self):
        return {
            "direction": self.direction,
            "target": self.target,
            "live": self.is_alive(),
            "last_seen": self.last_seen,
            "message": self.message,
            "id": self.remote_id,
            "location": self.location
        }

    def short_remote_id(self):
        x = self.remote_id
        return "%s|%s|%s" % (x["site"], x["tier"], x["name"])


class TopoNode(object):

    def __init__(self, fromid, location):
        self.fromid = fromid
        self.last_seen = datetime.now()
        self.location = location
        self.toids = []

    def is_alive(self):
        return (datetime.now() - self.last_seen).total_seconds() < TOPOLOGY_TIMEOUT_SECONDS

    def update(self, toids, last_seen, location):
        # update if newer and live
        if last_seen >= self.last_seen:
            if (datetime.now() - last_seen).total_seconds() < TOPOLOGY_TIMEOUT_SECONDS:
                LOGGER.debug("updating topo node %s for new %s", self.fromid, toids)
                self.toids = toids
                self.last_seen = last_seen
                self.location = location
            else:
                LOGGER.debug("Not updating node %s for new %s due to stale data", self.fromid, toids)
        else:
            LOGGER.debug("Not updating node %s for new %s due to not newer %s", self.fromid, toids, last_seen)

    def age(self):
        return (datetime.now() - self.last_seen).total_seconds()

    def to_dict(self):
        return {"from": self.fromid, "location": self.location, "target": self.toids, "timestamp": self.last_seen.isoformat()}


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

    def __init__(self, site, tier, name, location="", port=8888, connect_to=[]):
        self.site = site
        self.tier = tier
        self.name = name
        self.port = port
        self.location = location
        self.connect_to = connect_to

        self.connections = {}

        self._topology = {}
        self.self_topology = {}

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

    def status(self):
        return {
            "id": self.get_id(),
            "connections": [x for x in self.connections.values()]
        }

    def get_id(self):
        return {"site": self.site, "tier": self.tier, "name": self.name}

    def _get_toponode(self, myid, location):
        if myid in self._topology:
            connection = self._topology[myid]
        else:
            connection = TopoNode(myid, location)
            self._topology[myid] = connection
        return connection

    def topology(self):
        return [tn.to_dict() for tn in self._topology.values() if tn.is_alive()]

    def build_self_topo(self):
        id = "%s|%s|%s" % (self.site, self.tier, self.name)
        node = self._get_toponode(id, self.location)
        to = [x.short_remote_id() for x in self.connections.values() if not x.is_in() and x.is_alive()]
        node.update(to, datetime.now(), self.location)

    def update_topology_safe(self, topo):
        try:
            self.update_topology(topo)
        except Exception:
            LOGGER.exception("Failed to process topology update")

    def update_topology(self, topo):
        for node in topo:
            fromid = node["from"]
            target = node["target"]
            location = node["location"]
            ts = dateutil.parser.parse(node["timestamp"])
            toponode = self._get_toponode(fromid, location)
            toponode.update(target, ts, location)

    @gen.coroutine
    def check(self, url):
        connection = self._get_connection(url, DIRECTION_OUT, url)
        try:
            body = {"id": self.get_id(), "topology": self.topology()}
            request = HTTPRequest(url + "/ping", "POST", headers={"Content-Type": "application/json"},
                                  body=json_encode(body), request_timeout=0.9 * POLLING_INTERVAL_SECONDS)
            response = yield self.http_client.fetch(request)
            data = json_decode(response.body)

            remote_id = data["id"]
            # validate fields
            remote_id["site"]
            remote_id["name"]
            remote_id["tier"]

            topo = data["topology"]

            connection.seen(message="", remote_id=data["id"])
            self.update_topology_safe(topo)

        except Exception as e:
            connection.message = repr(e)
            connection.dead()

    @gen.coroutine
    def run(self):
        ioloop = tornado.ioloop.IOLoop.current()

        while True:
            start = ioloop.time()

            for toponode in [k for k, c in self._topology.items() if c.age() > TOPOLOGY_TIMEOUT_SECONDS]:
                del self._topology[toponode]

            self.build_self_topo()

            yield [self.check(url) for url in self.connect_to]

            end = ioloop.time()
            duration = end - start
            sleeptime = POLLING_INTERVAL_SECONDS - duration

            LOGGER.info("Iteration done in %d time, sleeping %d" % (duration, sleeptime))

            yield gen.sleep(sleeptime)

    def topology_dot(self):
        def clean(ident):
            return ident.replace("|", "_")
        lines = ["""digraph {"""] + ["%s -> %s;" % (clean(tnode.fromid), clean(tonode))
                                     for tnode in self._topology.values() for tonode in tnode.toids] + ["}"]
        return "\n".join(lines)


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
        if not ("id" in data and "topology" in data):
            self.error(400, "Body should contain the fields 'id and 'topology'")
            return
        mid = data["id"]
        if not ("name" in mid and "site" in mid and "tier" in mid):
            self.error(400, "ID fields should contain the fields 'name', 'site' and 'tier'")
            return

        self.app.seen(mid["site"], mid["tier"], mid["name"], self.request.remote_ip)
        self.set_header("Content-Type", "application/json")
        rdata = {"id": self.app.get_id(), "topology": self.app.topology()}
        self.write(json_encode(rdata))
        self.app.update_topology_safe(data["topology"])


class StatusHandler(AppHandler):

    def __init__(self, application, request, app: APP, **kwargs):
        super(AppHandler, self).__init__(application, request, **kwargs)
        self.app = app

    def get(self):
        self.set_header("Content-Type", "application/json")
        self.write(json_encode(self.app.status()))


class TopoHandler(AppHandler):

    def __init__(self, application, request, app: APP, **kwargs):
        super(AppHandler, self).__init__(application, request, **kwargs)
        self.app = app

    def get(self):
        self.set_header("Content-Type", "application/json")
        self.write(json_encode(self.app.topology()))


class DotHandler(AppHandler):

    def __init__(self, application, request, app: APP, **kwargs):
        super(AppHandler, self).__init__(application, request, **kwargs)
        self.app = app

    def get(self):
        self.set_header("Content-Type", "text/plain")
        self.write(self.app.topology_dot())


class PngHandler(AppHandler):

    def __init__(self, application, request, app: APP, **kwargs):
        super(AppHandler, self).__init__(application, request, **kwargs)
        self.app = app

    @gen.coroutine
    def get(self):
        self.set_header("Content-Type", "image/png")
        self.set_header("Cache-Control:", "no-store")
        dot = self.app.topology_dot()
        proc = tornado.process.Subprocess(["dot", "-Tpng"], stdin=tornado.process.Subprocess.STREAM,
                                          stdout=tornado.process.Subprocess.STREAM)
        yield proc.stdin.write(dot.encode())
        proc.stdin.close()
        ret = yield proc.wait_for_exit()
        png = yield proc.stdout.read_until_close()
        self.write(png)


class IndexHandler(tornado.web.RequestHandler):

    @gen.coroutine
    def get(self):
        self.set_header("Content-Type", "text/html")
        self.write("""<html> <head> 
<script type="text/JavaScript">
function TimedRefresh( t ) {
    setTimeout("location.reload(true);", t);
}
</script>
</head> <body onload="JavaScript:TimedRefresh(5000);"> <img src="png"/> </body> </html>
""")


def make_app(site, tier, name, location, connect_to, port=8888):
    app = APP(site=site, tier=tier, location=location, name=name, connect_to=connect_to)

    tornado.ioloop.IOLoop.current().add_callback(app.run)
    app = tornado.web.Application([
        (r"/ping", PingHandler, {"app": app}),
        (r"/status", StatusHandler, {"app": app}),
        (r"/topo", TopoHandler, {"app": app}),
        (r"/dot", DotHandler, {"app": app}),
        (r"/png", PngHandler, {"app": app}),
        (r"/", IndexHandler),
        (r"/index.html", IndexHandler)

    ])

    app.listen(port)
    LOGGER.warn("Listening on port %d", port)


log_levels = {
    0: logging.ERROR,
    1: logging.WARNING,
    2: logging.INFO,
    3: logging.DEBUG,
    4: 2
}


def main():
    parser = argparse.ArgumentParser(description='Networking test app')
    parser.add_argument("-c", "--config", dest="config_file", help='config file', default="config.toml")
    parser.add_argument('-v', '--verbose', action='count', default=0,
                        help="Log level for messages going to the console. Default is only errors,"
                        "-v warning, -vv info and -vvv debug and -vvvv trace")

    normalformatter = logging.Formatter(fmt="%(levelname)-8s%(message)s")
    stream = logging.StreamHandler()
    stream.setLevel(logging.INFO)
    stream.setFormatter(normalformatter)
    logging.root.handlers = []
    logging.root.addHandler(stream)
    logging.root.setLevel(0)

    options = parser.parse_args()

    # set the log level
    level = options.verbose
    if level >= len(log_levels):
        level = 3
    stream.setLevel(log_levels[level])

    cfg = toml.load(options.config_file)

    def get_or(name, default):
        if name in cfg:
            return cfg[name]
        else:
            return default

    make_app(get_or("site", "DEFAULTSITE"), get_or("tier", "DEFAULTTIER"),
             get_or("name", "DEFAULTNAME"), location=get_or("location", ""),
             connect_to=get_or("connect_to", []), port=int(get_or("port", 8888)))

    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
