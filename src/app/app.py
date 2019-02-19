# Copyright Inmanta 2018
# Contact: code@inmanta.com
# License: Apache 2.0 License
import tornado.ioloop
import tornado.web
import logging
from datetime import datetime
import json
from tornado import gen, routing
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.escape import json_decode
import dateutil.parser
import argparse
import toml
import tempfile
from itertools import groupby
from urllib.parse import urlencode
import asyncio

from typing import Tuple, Dict, Any, List, TypeVar, cast, Union, Iterator

OptT = TypeVar("OptT")

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
    last_seen: datetime

    def __init__(self, myid: Tuple[str, str, str, str], ip: str, direction: str, location: str = "", color: str = "") -> None:
        self.id = myid
        self.target = ip
        self.direction = direction
        self.message = ""
        self.live = False

        self.location = location
        self.latency: float = -1
        self.color = color
        self.remote_id: Dict[str, str] = {}

    def is_in(self) -> bool:
        return self.direction == DIRECTION_IN

    def is_alive(self) -> bool:
        return self.live and (datetime.utcnow() - self.last_seen).total_seconds() < TIMEOUT_SECONDS

    def seen(self, remote_id: Dict[str, str]={}, message: str = "", latency: float = -1) -> None:
        self.last_seen = datetime.utcnow()
        self.live = True
        self.message = message
        self.remote_id = remote_id
        self.latency = latency

    def dead(self) -> None:
        self.live = False

    def age(self) -> float:
        return (datetime.utcnow() - self.last_seen).total_seconds()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "direction": self.direction,
            "target": self.target,
            "live": self.is_alive(),
            "last_seen": self.last_seen,
            "message": self.message,
            "id": self.remote_id,
            "location": self.location,
            "latency": self.latency,
            "color": self.color,
        }

    def short_remote_id(self) -> str:
        x = self.remote_id
        return "%s|%s|%s" % (x["site"], x["tier"], x["name"])


class TopoNode(object):
    def __init__(self, fromid: str, location: str, color: str) -> None:
        self.fromid = fromid
        self.last_seen: datetime = datetime.utcnow()
        self.location = location
        self.latencies: Dict[str, float] = {}
        self.toids: List[str] = []
        self.color = color

    def is_alive(self) -> bool:
        return (datetime.utcnow() - self.last_seen).total_seconds() < TOPOLOGY_TIMEOUT_SECONDS

    def update(self, toids: List[str], latencies: Dict[str, float], last_seen: datetime, location: str, color: str) -> None:
        # update if newer and live
        if last_seen >= self.last_seen:
            if (datetime.utcnow() - last_seen).total_seconds() < TOPOLOGY_TIMEOUT_SECONDS:
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

    def age(self) -> float:
        return (datetime.utcnow() - self.last_seen).total_seconds()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "from": self.fromid,
            "location": self.location,
            "color": self.color,
            "latency": self.latencies,
            "target": self.toids,
            "timestamp": self.last_seen.isoformat(),
        }


def custom_json_encoder(o: Any) -> Union[str, Dict[str, Any]]:
    """
        A custom json encoder that knows how to encode other types used by this app
    """
    if isinstance(o, datetime):
        return cast(datetime, o).isoformat()

    if hasattr(o, "to_dict"):
        return o.to_dict()

    LOGGER.error("Unable to serialize %s", o)
    raise TypeError(repr(o) + " is not JSON serializable")


def json_encode(value: Any) -> str:
    # see json_encode in tornado.escape
    return json.dumps(value, default=custom_json_encoder).replace("</", "<\\/")


class APP(object):
    def __init__(
        self,
        site: str,
        tier: str,
        name: str,
        location: str = "",
        color: str = "",
        port: int = 8888,
        connect_to: List[str] = [],
        colornodes: bool = False,
        fastest: bool = False,
        link_delta: int = 0,
    ):
        self.site = site
        self.tier = tier
        self.name = name
        self.port = port
        self.location = location
        self.color = color
        self.connect_to = connect_to

        self.fastest = fastest
        self.link_delta = link_delta / 1000
        self.node_tier_edge_cache: Dict[Tuple[str, str], str] = {}

        self.connections: Dict[Tuple[str, str, str, str], Connection] = {}

        self._topology: Dict[str, TopoNode] = {}

        self.colornodes = colornodes

        self.http_client = AsyncHTTPClient()
        self._running = True
        self._finished = asyncio.Future()

    def _get_connection(self, ip: str, direction: str, myid: Tuple[str, str, str, str]) -> Connection:
        if myid in self.connections:
            connection = self.connections[myid]
        else:
            connection = Connection(myid, ip, direction)
            self.connections[myid] = connection
        return connection

    def seen(self, site: str, tier: str, name: str, ip: str, direction: str = DIRECTION_IN, latency: float = -1) -> None:
        myid = (site, tier, name, direction)

        LOGGER.info("Incoming connection from %s %s %s (%s)", site, tier, name, ip)

        connection = self._get_connection(ip, direction, myid)
        connection.seen(latency)

    def status(self) -> Dict[str, Any]:
        return {"id": self.get_id(), "connections": [x for x in self.connections.values()]}

    def get_id(self) -> Dict[str, str]:
        return {"site": self.site, "tier": self.tier, "name": self.name}

    def _get_toponode(self, myid: str, location: str, color: str) -> TopoNode:
        if myid in self._topology:
            node = self._topology[myid]
        else:
            node = TopoNode(myid, location, color)
            self._topology[myid] = node
        return node

    def topology(self) -> List[TopoNode]:
        return [tn for tn in self._topology.values() if tn.is_alive()]

    def build_self_topo(self) -> None:
        id = "%s|%s|%s" % (self.site, self.tier, self.name)
        node = self._get_toponode(id, self.location, self.color)
        outgoing = [x for x in self.connections.values() if not x.is_in() and x.is_alive()]
        to = [x.short_remote_id() for x in outgoing]
        target_latency = {x.short_remote_id(): x.latency for x in outgoing}
        node.update(to, target_latency, datetime.utcnow(), self.location, self.color)

    def update_topology_safe(self, topo: List[Dict[str, Any]]) -> None:
        try:
            self.update_topology(topo)
        except Exception:
            LOGGER.exception("Failed to process topology update")

    def update_topology(self, topo: List[Dict[str, Any]]) -> None:
        for node in topo:
            fromid = node["from"]
            target = node["target"]
            location = node["location"]
            latency = node["latency"]
            color = node["color"]
            ts = dateutil.parser.parse(node["timestamp"])
            toponode = self._get_toponode(fromid, location, color=color)
            toponode.update(target, latency, ts, location, color=color)

    async def check(self, url: str) -> None:
        LOGGER.info("Sending ping to %s from %s", url, self.get_id())
        connection = self._get_connection(url, DIRECTION_OUT, url)
        try:
            body = {"id": self.get_id(), "topology": self.topology()}
            request = HTTPRequest(
                url + "/ping",
                "POST",
                headers={"Content-Type": "application/json"},
                body=json_encode(body),
                request_timeout=0.9 * POLLING_INTERVAL_SECONDS,
            )
            response = await self.http_client.fetch(request)
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

    async def run(self) -> None:
        ioloop = tornado.ioloop.IOLoop.current()

        while self._running:
            start = ioloop.time()

            for toponode in [k for k, c in self._topology.items() if c.age() > TOPOLOGY_TIMEOUT_SECONDS]:
                del self._topology[toponode]

            self.build_self_topo()

            await asyncio.gather(*[self.check(url) for url in self.connect_to])

            end = ioloop.time()
            duration = end - start
            sleeptime = POLLING_INTERVAL_SECONDS - duration

            LOGGER.info("Iteration done in %d time, sleeping %d" % (duration, sleeptime))

            await asyncio.sleep(sleeptime)

        self._finished.set_result(None)

    async def stop(self) -> None:
        self._running = False
        await self._finished

    def topology_dot(self) -> str:
        def clean(ident: str) -> str:
            return ident.replace("|", "_").replace(" ", "_")

        lines: List[str] = ["""digraph {"""]

        def render_edge(fromnode: str, tonode: str, latency: float) -> str:
            return '"%s" -> "%s" [label="%.0f"];' % (clean(fromnode), clean(tonode), latency * 1000)

        def get_tier(nodeid: str) -> str:
            return nodeid.split("|")[1]

        if not self.fastest:
            lines += [
                render_edge(tnode.fromid, tonode, latency)
                for tnode in self._topology.values()
                for tonode, latency in tnode.latencies.items()
            ]
        else:
            node_to_latency: List[Tuple[str, str, float]] = [
                (tnode.fromid, tonode, latency)
                for tnode in self._topology.values()
                for tonode, latency in tnode.latencies.items()
            ]
            node_tier_to_tier_latency: List[Tuple[str, str, str, str, float]] = [
                (node, get_tier(node), tonode, get_tier(tonode), latency) for node, tonode, latency in node_to_latency
            ]
            # same tier
            lines += [
                render_edge(fromnode, tonode, latency)
                for fromnode, fromtier, tonode, totier, latency in node_tier_to_tier_latency
                if fromtier == totier
            ]

            # not same
            def get_latency(fromnode: str, totier: str, tonode: str, latency: float) -> float:
                if (fromnode, totier) in self.node_tier_edge_cache and self.node_tier_edge_cache[(fromnode, totier)] == tonode:
                    return latency - self.link_delta
                return latency

            node_totier_tonode_latency: List[Tuple[str, str, str, float, float]] = [
                (fromnode, totier, tonode, latency, get_latency(fromnode, totier, tonode, latency))
                for fromnode, fromtier, tonode, totier, latency in node_tier_to_tier_latency
                if fromtier != totier
            ]

            grouped_node_totier_tonode_latency: Iterator[Tuple[Tuple[str, str], Iterator[Tuple[str, str, str, float, float]]]] = groupby(
                sorted(node_totier_tonode_latency, key=lambda t: (t[0], t[1])), lambda t: (t[0], t[1])
            )

            node_totier__tonode_latency = [(node, sorted(i, key=lambda x: x[4])) for node, i in grouped_node_totier_tonode_latency]

            self.node_tier_edge_cache = {(fromnode[0], fromnode[1]): nl[0][2] for fromnode, nl in node_totier__tonode_latency}
            lines += [render_edge(fromnode[0], nl[0][2], nl[0][3]) for fromnode, nl in node_totier__tonode_latency]

        node_location: Dict[TopoNode, str] = {tnode: tnode.location for tnode in self._topology.values()}

        location_node_groups: Iterator[Tuple[str, Iterator[Tuple[TopoNode, str]]]] = groupby(sorted(node_location.items(), key=lambda x: x[1]), lambda x: x[1])
        location_node = [(k, [l[0] for l in g]) for k, g in location_node_groups]

        def render_node(node: TopoNode) -> str:
            if node.color != "" and self.colornodes:
                return '"%s" [color=%s];' % (clean(node.fromid), node.color)
            else:
                return '"%s";' % clean(node.fromid)

        def render_nodes(nodes: List[TopoNode]) -> str:
            return "\n".join([render_node(node) for node in nodes])

        lines += [
            'subgraph "cluster_%s" { \n %s \n label="%s";\n}' % (clean(location), render_nodes(nodes), location)
            for (location, nodes) in location_node
        ]

        lines += ["}"]

        return "\n".join(lines)

    def get_location_color(self) -> Dict[str, str]:
        pairs = [(tnode.location, tnode.color) for tnode in self._topology.values()]
        lc: Dict[str, str] = {}
        for l, c in pairs:
            if l not in lc:
                lc[l] = c
            elif lc[l] != c:
                LOGGER.warning("Same location with two colors: %s %s %s" % (l, lc[l], c))
        return lc


class AppHandler(tornado.web.RequestHandler):
    def __init__(
        self, application: tornado.web.Application, request: tornado.httputil.HTTPServerRequest, app: APP
    ) -> None:
        super().__init__(application, request, app=app)

    def initialize(self, app: APP) -> None:
        self.app = app

    def error(self, status: int, msg: str) -> None:
        self.set_header("Content-Type", "application/json")
        self.write(tornado.escape.json_encode({"message": msg}))
        self.set_status(status, msg)


class PingHandler(AppHandler):
    def post(self, *args: str, **kwargs: str) -> None:
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
    def get(self) -> None:
        self.set_header("Content-Type", "application/json")
        self.write(json_encode(self.app.status()))


class TopoHandler(AppHandler):
    def get(self) -> None:
        self.set_header("Content-Type", "application/json")
        self.write(json_encode(self.app.topology()))


class DotHandler(AppHandler):
    def get(self) -> None:
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
    def get(self) -> None:
        location_color = self.app.get_location_color()

        grouped_color_location = groupby(sorted(location_color.items(), key=lambda x: x[1]), lambda x: x[1])
        color_location = [(k, [v[0] for v in locations]) for (k, locations) in grouped_color_location]

        def marker_for_color(color, locations):
            return ("markers", ("color:%s|" % color) + "|".join(locations))

        markers = [marker_for_color(k, v) for k, v in color_location]

        parts = [("size", "640x640"), ("key", "AIzaSyAxE4078fMnhyC1z6xYBrlbdT4JcEdseNY")] + markers
        url = "https://maps.googleapis.com/maps/api/staticmap?"
        url = url + urlencode(parts)

        self.set_header("Content-Type", "text/html")
        self.write(smaptamples % url)


class PngHandler(AppHandler):
    async def get(self) -> None:
        self.set_header("Content-Type", "image/png")
        self.set_header("Cache-Control:", "no-store")
        dot = self.app.topology_dot()

        out = tempfile.NamedTemporaryFile()
        proc = tornado.process.Subprocess(["dot", "-Tpng"], stdin=tornado.process.Subprocess.STREAM, stdout=out)
        await proc.stdin.write(dot.encode())
        proc.stdin.close()
        ret = await proc.wait_for_exit(raise_error=False)
        proc.uninitialize()
        out.seek(0)

        self.write(out.read())


class IndexHandler(tornado.web.RequestHandler):
    def get(self) -> None:
        self.set_header("Content-Type", "text/html")
        self.write(
            """<html> <head>
<script type="text/JavaScript">
function TimedRefresh( t ) {
    setTimeout("location.reload(true);", t);
}
</script>
</head> <body onload="JavaScript:TimedRefresh(5000);"> <img src="png"/> </body></html>
"""
        )


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
    async def get(self) -> None:
        self.set_header("Content-Type", "text/html")
        self.write(dynamap)


def make_app(
    site: str,
    tier: str,
    name: str,
    location: str,
    color: str,
    connect_to: List[str],
    port: int = 8888,
    colornodes: bool = False,
    fastest: bool = False,
    link_delta: bool = False,
) -> APP:
    app = APP(
        site=site,
        tier=tier,
        location=location,
        color=color,
        name=name,
        connect_to=connect_to,
        colornodes=colornodes,
        fastest=fastest,
        link_delta=link_delta,
    )

    tornado.ioloop.IOLoop.current().add_callback(app.run)
    rules = [
        routing.Rule(routing.PathMatches(r"/ping"), PingHandler, {"app": app}),
        routing.Rule(routing.PathMatches(r"/status"), StatusHandler, {"app": app}),
        routing.Rule(routing.PathMatches(r"/topo"), TopoHandler, {"app": app}),
        routing.Rule(routing.PathMatches(r"/dot"), DotHandler, {"app": app}),
        routing.Rule(routing.PathMatches(r"/png"), PngHandler, {"app": app}),
        routing.Rule(routing.PathMatches(r"/smap"), StaticMapHandler, {"app": app}),
        routing.Rule(routing.PathMatches(r"/map"), MapHandler),
        routing.Rule(routing.PathMatches(r"/"), IndexHandler),
        routing.Rule(routing.PathMatches(r"/index.html"), IndexHandler),
    ]

    tornado_app = tornado.web.Application(rules)

    tornado_app.listen(port)
    LOGGER.warning("Listening on port %d", port)

    return app


log_levels = {0: logging.ERROR, 1: logging.WARNING, 2: logging.INFO, 3: logging.DEBUG, 4: 2}


def main() -> None:
    parser = argparse.ArgumentParser(description="Networking test app")
    parser.add_argument("-c", "--config", dest="config_file", help="config file", default="config.toml")
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Log level for messages going to the console. Default is only errors,"
        "-v warning, -vv info and -vvv debug and -vvvv trace",
    )
    parser.add_argument(
        "--colornodes",
        dest="colornodes",
        help="enable to render nodes in dot in the color inidicated in the config file of that node",
        action="store_true",
    )
    parser.add_argument(
        "--fastest", dest="fastest", help="only rendere fastest link from one node to other tier", action="store_true"
    )
    normalformatter = logging.Formatter(fmt="%(levelname)-8s%(message)s")
    stream = logging.StreamHandler()
    stream.setLevel(logging.INFO)
    stream.setFormatter(normalformatter)
    logging.root.handlers = []
    logging.root.addHandler(stream)
    logging.root.setLevel(0)

    options = parser.parse_args()

    # set the log level
    assert isinstance(options.verbose, int)
    level = options.verbose
    if level >= len(log_levels):
        level = 3
    stream.setLevel(log_levels[level])

    cfg = toml.load(options.config_file)

    def get_or(name: str, default: OptT) -> OptT:
        if name in cfg:
            return cast(OptT, cfg[name])
        else:
            return default

    assert isinstance(options.colornodes, bool)
    assert isinstance(options.fastest, bool)

    make_app(
        get_or("site", "DEFAULTSITE"),
        get_or("tier", "DEFAULTTIER"),
        get_or("name", "DEFAULTNAME"),
        location=get_or("location", ""),
        color=get_or("color", ""),
        connect_to=get_or("connect_to", []),
        port=int(get_or("port", 8888)),
        colornodes=get_or("colornodes", options.colornodes),
        fastest=get_or("fastest", options.fastest),
        link_delta=get_or("renderdelta", False),
    )

    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
