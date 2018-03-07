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
import tempfile
from itertools import groupby
from urllib.parse import urlencode
import urllib


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

    def __init__(self, myid, ip, direction, location="", color=""):
        self.id = myid
        self.target = ip
        self.direction = direction
        self.last_seen = None
        self.message = ""
        self.live = False
        self.remote_id = ""
        self.location = location
        self.latency = -1
        self.color = color

    def is_in(self):
        return self.direction == DIRECTION_IN

    def is_alive(self):
        return self.live and (datetime.now() - self.last_seen).total_seconds() < TIMEOUT_SECONDS

    def seen(self, remote_id="", message="", latency=-1):
        self.last_seen = datetime.now()
        self.live = True
        self.message = message
        self.remote_id = remote_id
        self.latency = latency

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
            "location": self.location,
            "latency": self.latency,
            "color": self.color
        }

    def short_remote_id(self):
        x = self.remote_id
        return "%s|%s|%s" % (x["site"], x["tier"], x["name"])


class TopoNode(object):

    def __init__(self, fromid, location, color):
        self.fromid = fromid
        self.last_seen = datetime.now()
        self.location = location
        self.latencies = {}
        self.toids = []
        self.color = color

    def is_alive(self):
        return (datetime.now() - self.last_seen).total_seconds() < TOPOLOGY_TIMEOUT_SECONDS

    def update(self, toids, latencies, last_seen, location, color):
        # update if newer and live
        if last_seen >= self.last_seen:
            if (datetime.now() - last_seen).total_seconds() < TOPOLOGY_TIMEOUT_SECONDS:
                LOGGER.debug("updating topo node %s for new %s", self.fromid, toids)
                self.toids = toids
                self.latencies = latencies
                self.last_seen = last_seen
                self.location = location
                self.color = color
            else:
                LOGGER.debug("Not updating node %s for new %s due to stale data", self.fromid, toids)
        else:
            LOGGER.debug("Not updating node %s for new %s due to not newer %s", self.fromid, toids, last_seen)

    def age(self):
        return (datetime.now() - self.last_seen).total_seconds()

    def to_dict(self):
        return {"from": self.fromid, "location": self.location, "color": self.color, "latency": self.latencies, "target": self.toids, "timestamp": self.last_seen.isoformat()}


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

    def __init__(self, site, tier, name, location="", color="", port=8888, connect_to=[], colornodes=False, fastest=False):
        self.site = site
        self.tier = tier
        self.name = name
        self.port = port
        self.location = location
        self.color = color
        self.fastest = fastest
        self.connect_to = connect_to

        self.connections = {}

        self._topology = {}
        self.self_topology = {}

        self.colornodes = colornodes

        self.http_client = AsyncHTTPClient()

    def _get_connection(self, ip, direction, myid):
        if myid in self.connections:
            connection = self.connections[myid]
        else:
            connection = Connection(myid, ip, direction)
            self.connections[myid] = connection
        return connection

    def seen(self, site, tier, name, ip, direction=DIRECTION_IN, latency=-1):
        myid = (site, tier, name, direction)

        LOGGER.info("Incoming connection from %s %s %s (%s)", site, tier, name, ip)

        connection = self._get_connection(ip, direction, myid)

        connection.seen(latency)

    def status(self):
        return {
            "id": self.get_id(),
            "connections": [x for x in self.connections.values()]
        }

    def get_id(self):
        return {"site": self.site, "tier": self.tier, "name": self.name}

    def _get_toponode(self, myid, location, color):
        if myid in self._topology:
            connection = self._topology[myid]
        else:
            connection = TopoNode(myid, location, color)
            self._topology[myid] = connection
        return connection

    def topology(self):
        return [tn.to_dict() for tn in self._topology.values() if tn.is_alive()]

    def build_self_topo(self):
        id = "%s|%s|%s" % (self.site, self.tier, self.name)
        node = self._get_toponode(id, self.location, self.color)
        outgoing = [x for x in self.connections.values() if not x.is_in() and x.is_alive()]
        to = [x.short_remote_id() for x in outgoing]
        target_latency = {x.short_remote_id(): x.latency for x in outgoing}
        node.update(to, target_latency, datetime.now(), self.location, self.color)

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
            latency = node["latency"]
            color = node["color"]
            ts = dateutil.parser.parse(node["timestamp"])
            toponode = self._get_toponode(fromid, location, color=color)
            toponode.update(target, latency, ts, location, color=color)

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

            connection.seen(message="", remote_id=data["id"], latency=response.request_time)
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
            return ident.replace("|", "_").replace(" ", "_")

        lines = ["""digraph {"""]

        def render_edge(fromnode, tonode, latency):
            return "\"%s\" -> \"%s\" [label=\"%.0f\"];" % (clean(fromnode), clean(tonode), latency * 1000)

        def get_tier(nodeid):
            return nodeid.split("|")[1]

        if not self.fastest:
            lines += [render_edge(tnode.fromid, tonode, latency)
                      for tnode in self._topology.values() for tonode, latency in tnode.latencies.items()]
        else:
            node_to_latency = [(tnode.fromid, tonode, latency)
                               for tnode in self._topology.values() for tonode, latency in tnode.latencies.items()]
            node_tier_to_tier_latency = [(node, get_tier(node), tonode, get_tier(tonode), latecny)
                                         for node, tonode, latecny in node_to_latency]
            # same tier
            lines += [render_edge(fromnode, tonode, latency) for fromnode, fromtier, tonode, totier,
                      latency in node_tier_to_tier_latency if fromtier == totier]
            #not same
            node_totier_tonode_latency = [(fromnode, totier, tonode, latency) for fromnode, fromtier,
                                          tonode, totier, latency in node_tier_to_tier_latency if fromtier != totier]
            node_totier__tonode_latency = groupby(
                sorted(node_totier_tonode_latency, key=lambda t: (t[0], t[1])), lambda t: (t[0], t[1]))
            node_totier__tonode_latency = [(node[0], sorted(i, key=lambda x:x[3])) for node, i in node_totier__tonode_latency]
            lines += [render_edge(fromnode, nl[0][2], nl[0][3]) for fromnode,nl in node_totier__tonode_latency]

        node_location = {tnode: tnode.location for tnode in self._topology.values()}

        location_node = groupby(sorted(node_location.items(), key=lambda x: x[1]), lambda x: x[1])
        location_node = [(k, [l[0] for l in g]) for k, g in location_node]

        def render_node(node):
            if node.color != "" and self.colornodes:
                return "\"%s\" [color=%s];" % (clean(node.fromid), node.color)
            else:
                return "\"%s\";" % clean(node.fromid)

        def render_nodes(nodes):
            return "\n".join([render_node(node) for node in nodes])

        lines += ["subgraph \"cluster_%s\" { \n %s \n label=\"%s\";\n}" %
                  (clean(location), render_nodes(nodes), location) for (location, nodes) in location_node]

        lines += ["}"]

        return "\n".join(lines)

    def get_location_color(self):
        pairs = [(tnode.location, tnode.color) for tnode in self._topology.values()]
        lc = {}
        for l, c in pairs:
            if l not in lc:
                lc[l] = c
            elif lc[l] != c:
                LOGGER.warn("Same location with two colors: %s %s %s" % (l, lc[l], c))
        return lc


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


smaptamples = """<html> <head>
<script type="text/JavaScript">
function TimedRefresh( t ) {
    setTimeout("location.reload(true);", t);
}
</script>
</head> <body onload="JavaScript:TimedRefresh(5000);"> <img src="%s"/> </body> </html>
"""


class StaticMapHandler(AppHandler):

    def __init__(self, application, request, app: APP, **kwargs):
        super(AppHandler, self).__init__(application, request, **kwargs)
        self.app = app

    def get(self):
        location_color = self.app.get_location_color()

        color_location = groupby(sorted(location_color.items(), key=lambda x: x[1]), lambda x: x[1])
        color_location = [(k, [v[0] for v in locations]) for (k, locations) in color_location]

        def marker_for_color(color, locations):
            return ("markers", ("color:%s|" % color) + "|".join(locations))

        markers = [marker_for_color(k, v) for k, v in color_location]

        parts = [("size", "640x640"), ("key", "AIzaSyAxE4078fMnhyC1z6xYBrlbdT4JcEdseNY")] + markers
        url = "https://maps.googleapis.com/maps/api/staticmap?"
        url = url + urlencode(parts)

        self.set_header("Content-Type", "text/html")
        self.write(smaptamples % url)


class PngHandler(AppHandler):

    def __init__(self, application, request, app: APP, **kwargs):
        super(AppHandler, self).__init__(application, request, **kwargs)
        self.app = app

    @gen.coroutine
    def get(self):
        self.set_header("Content-Type", "image/png")
        self.set_header("Cache-Control:", "no-store")
        dot = self.app.topology_dot()

        out = tempfile.NamedTemporaryFile()
        proc = tornado.process.Subprocess(["dot", "-Tpng"], stdin=tornado.process.Subprocess.STREAM,
                                          stdout=out)
        yield proc.stdin.write(dot.encode())
        proc.stdin.close()
        ret = yield proc.wait_for_exit(raise_error=False)
        proc.uninitialize()
        out.seek(0)

        self.write(out.read())


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
</head> <body onload="JavaScript:TimedRefresh(5000);"> <img src="png"/> </body></html>
""")


dynamap = """
<!DOCTYPE html>
<html>

<head>
  <style>
    #map {
      height: 800px;
      width: 100%;
    }
  </style>
</head>

<body>
  <h3>My Google Maps Demo</h3>
  <div id="map"></div>
  <script>
    function initMap() {
      var markers = {}
      var bounds = new google.maps.LatLngBounds();
      var geocoder = new google.maps.Geocoder();
      var map = new google.maps.Map(document.getElementById('map'));

      function add_marker(color, place) {
        console.log(color, place)
        if (place in markers) {
          if(markers[place] != ""){
            markers[place].setIcon('http://maps.google.com/mapfiles/ms/icons/'+color+'-dot.png')
          }
        } else {
          markers[place] = ""
          geocoder.geocode({ 'address': place }, function (results, status) {
            if (status == 'OK') {
              var location = results[0].geometry.location
              var marker = new google.maps.Marker({
                map: map,
                position: location,
                icon: 'http://maps.google.com/mapfiles/ms/icons/'+color+'-dot.png'
              });
              markers[place] = marker
              bounds.extend(location)
              map.fitBounds(bounds);
            } else {
              alert('Geocode was not successful for the following reason: ' + status);
            }
          })
        }
      }

      function load() {
        var xhr = new XMLHttpRequest();
        xhr.open('GET', 'topo');
        xhr.onload = function () {
            var topo = JSON.parse(xhr.responseText);
            for (var marker in markers){
              marker = markers[marker]
              marker.setIcon('http://maps.google.com/mapfiles/ms/icons/msmarker.shadow.png')
            }

            for(var tnode in topo){
              tnode = topo[tnode]
              add_marker(tnode.color, tnode.location)
            }
        };
        xhr.send();
      }

      load()
      var intervalID = setInterval(load, 5000);


    }
  </script>
  <script async defer src="https://maps.googleapis.com/maps/api/js?key=AIzaSyBh1IcUvsdXIZZLBlKBEdchB88h96RxM5E&callback=initMap">
  </script>
</body>

</html>
"""


class MapHandler(tornado.web.RequestHandler):

    @gen.coroutine
    def get(self):
        self.set_header("Content-Type", "text/html")
        self.write(dynamap)


def make_app(site, tier, name, location, color, connect_to, port=8888, colornodes=False, fastest=False):
    app = APP(site=site, tier=tier, location=location, color=color, name=name,
              connect_to=connect_to, colornodes=colornodes, fastest=fastest)

    tornado.ioloop.IOLoop.current().add_callback(app.run)
    app = tornado.web.Application([
        (r"/ping", PingHandler, {"app": app}),
        (r"/status", StatusHandler, {"app": app}),
        (r"/topo", TopoHandler, {"app": app}),
        (r"/dot", DotHandler, {"app": app}),
        (r"/png", PngHandler, {"app": app}),
        (r"/smap", StaticMapHandler, {"app": app}),
        (r"/map", MapHandler),
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
    parser.add_argument("--colornodes", dest="colornodes",
                        help='enable to render nodes in dot in the color inidicated in the config file of that node', action='store_true')
    parser.add_argument("--fastest", dest="fastest",
                        help='only rendere fastest link from one node to other tier', action='store_true')
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
             color=get_or("color", ""),
             connect_to=get_or("connect_to", []), port=int(get_or("port", 8888)),
             colornodes=get_or("colornodes", options.colornodes), fastest=options.fastest)

    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
