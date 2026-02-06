import marimo

__generated_with = "0.19.8"
app = marimo.App()


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # TTC API

    There are several options

    - UMoIQ - XML, third party API that also happens to have access to many other regions
        - [decomissioned a month ago??](https://data.urbandatacentre.ca/en/catalogue/city-toronto-ttc-real-time-next-vehicle-arrival-nvas)
    - TTC GTFS - ~~JSON~~ protobuf, but no real info on `vehicle_id` either - is it necessary?
    """)
    return


@app.cell
def _():
    import requests
    import xml.etree.ElementTree as ET
    import polars as pl

    return ET, pl, requests


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## UMoIQ API
    """)
    return


@app.cell
def _(ET, requests):
    URL = 'https://webservices.umoiq.com/service/publicXMLFeed?command=vehicleLocations&a=ttc'

    resp = requests.get(URL, timeout=3)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    return (root,)


@app.cell
def _(root):
    root
    return


@app.cell
def _(root):
    for child in root:
        print(child.tag, child.attrib)
    return


@app.cell
def _(root):
    len(root)
    return


@app.cell
def _(root):
    root[1].keys()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Currently `list[dict]`, but XML handles lists a little differently, in that each element has `tag` and `attribute`, and there could be many elements with the same `tag`, e.g. `vehicle`

    If we're only interested in `vehicleLoc`, we'd use `root.findall(".//vehicle")` and maybe use the `id` as key, with rest of the attributes as the `dict` content
    """)
    return


@app.cell
def _(root):
    # convert to dict 
    vlocs = [{k: v.get(k) for k in v.keys()} for v in root.findall('.//vehicle')]
    return (vlocs,)


@app.cell
def _(vlocs):
    len(vlocs)
    return


@app.cell
def _(vlocs):
    vlocs.get('3628')
    return


@app.cell
def _(pl, vlocs):
    df = pl.DataFrame(vlocs)
    return (df,)


@app.cell
def _(df):
    df
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TTC GTFS-Realtime

    [From Open Data Catalogue](https://open.toronto.ca/dataset/ttc-gtfs-realtime-gtfs-rt/)

    The general transit feed specification uses **Protocol Buffer** instead of json. Also requires the `gtfs-realtime-bindings` package, which has built-in protobuf parser (and in fact has it as a dependency)
    """)
    return


@app.cell
def _():
    # from https://gtfs.org/documentation/realtime/language-bindings/python/#
    from google.transit import gtfs_realtime_pb2

    return (gtfs_realtime_pb2,)


@app.cell
def _(requests):
    url = 'https://gtfsrt.ttc.ca/vehicles/position?format=text'
    resp_1 = requests.get(url)
    try:
        resp_1.raise_for_status()
    except Exception as e:
        if '403 Client Error' in str(e):
            print(e)
    return (url,)


@app.cell
def _(requests, url):
    _headers = {'User-Agent': 'Mozilla/5.0'}
    resp_2 = requests.get(url, headers=_headers)
    resp_2.raise_for_status()
    return (resp_2,)


@app.cell
def _(resp_2):
    data = resp_2.content
    data
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Protobuf vs json

    GTFS uses `protobuf`, which differs from `json`

    - can be binary encoded
    - defined fields, avoid repeating schema in each message
    - schema enforcement
    - better suited for inter-service comm where schema contract can be enforced
    """)
    return


@app.cell
def _(gtfs_realtime_pb2, requests):
    url_1 = 'https://gtfsrt.ttc.ca/vehicles/position?format=binary'
    feed = gtfs_realtime_pb2.FeedMessage()
    _headers = {'User-Agent': 'Mozilla/5.0'}
    resp_3 = requests.get(url_1, headers=_headers, timeout=3)
    resp_3.raise_for_status()
    feed.ParseFromString(resp_3.content)
    for i, entity in enumerate(feed.entity):
        print(entity.vehicle)
        if i > 3:
            break
    return


if __name__ == "__main__":
    app.run()
