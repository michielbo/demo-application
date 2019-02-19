# Copyright Inmanta 2019
# Contact: code@inmanta.com
# License: Apache 2.0 License

import pytest
import contextlib
import socket
import asyncio

from app import app
from aiohttp_requests import requests

def unused_tcp_port():
    """Find an unused localhost TCP port from 1024-65535 and return it. Used from pytest-asyncio"""
    with contextlib.closing(socket.socket()) as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


async def create_app(site, tier, name, port=None, connect_to=[]):
    if port is None:
        port = unused_tcp_port()
    application = app.make_app(site, tier, name, "here", "black", port=port, connect_to=connect_to)

    return "http://localhost:%d/" % port, application


@pytest.mark.asyncio
async def test_one_node():
    url, x = await create_app("site1", "tier1", "node1")

    # test status
    response = await requests.get(url + "status")
    data = await response.json()

    assert len(data["connections"]) == 0
    assert data["id"]["site"] == x.site
    assert data["id"]["tier"] == x.tier
    assert data["id"]["name"] == x.name

    # test topo
    response = await requests.get(url + "topo")
    data = await response.json()

    assert len(data) == 1
    assert "timestamp" in data[0]

    # test map handler
    response = await requests.get(url + "map")
    data = await response.text()

    assert "initMap" in data

    # test dot graph
    response = await requests.get(url + "dot")
    data = await response.text()

    assert "digraph" in data

    # test png graph
    response = await requests.get(url + "png")
    data = await response.read()

    assert data[1:4].decode() == "PNG"

    # test static map
    response = await requests.get(url + "smap")
    data = await response.text()

    assert "staticmap" in data

    # test index
    response = await requests.get(url)
    data = await response.text()

    assert "png" in data

    # test index
    response = await requests.get(url + "index.html")
    data = await response.text()

    assert "png" in data

    await x.stop()


@pytest.mark.asyncio
async def test_3_tier():
    nodes = [
        ("site1", "web", "web1", ["app1", "app2"]),
        ("site1", "web", "web2", ["web1", "web2"]),
        ("site1", "app", "app1", ["db1", "db2"]),
        ("site1", "app", "app2", ["db1", "db2"]),
        ("site1", "db", "db1", ["db1", "db2"]),
        ("site1", "db", "db2", ["db1", "db2"]),
    ]

    ports = {node: unused_tcp_port() for site, tier, node, links in nodes}
    urls = {}
    apps = {}

    for site, tier, node, links in nodes:
        url, x = await create_app(site, tier, node, ports[node], ["http://localhost:%d" % ports[link] for link in links])
        urls[node] = url
        apps[node] = x

    url = urls["web1"]

    # wait for gossip to converge
    response = await requests.get(url + "topo")
    data = await response.json()

    while len(data) != len(nodes):
        await asyncio.sleep(0.1)
        response = await requests.get(url + "topo")
        data = await response.json()

    # do some tests
    response = await requests.get(url + "topo")
    data = await response.json()

    assert len(data) == 6
    assert "timestamp" in data[0]

    # end all tasks
    await asyncio.gather(*[x.stop() for x in apps.values()])
