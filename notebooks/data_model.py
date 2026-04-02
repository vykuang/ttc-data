import marimo

__generated_with = "0.20.4"
app = marimo.App()


@app.cell
def _():
    import boto3

    return (boto3,)


@app.cell
def _():
    AWS_BUCKET = 'ttc-api'
    path = 'raw/{item}/{date}/{time}.pb'
    return (AWS_BUCKET,)


@app.cell
def _(boto3):
    s3 = boto3.client('s3')
    return (s3,)


@app.cell
def _(AWS_BUCKET, s3):
    try:
        folder = 'raw/trip/20260301'
        objs = s3.list_objects_v2(Bucket=AWS_BUCKET,Prefix=folder)
        print(f'{len(objs['Contents'])} objects listed')
    except Exception as e:
        print(f'error reading {folder}: {e}')
    return (objs,)


@app.cell
def _(objs):
    objs['Contents'][0]
    return


@app.cell
def _(AWS_BUCKET, objs, s3):
    obj_key = objs['Contents'][0]['Key']
    try:
        resp = s3.get_object(Bucket=AWS_BUCKET, Key=obj_key)
        obj_content = resp['Body'].read()
    except Exception as e:
        print(f'error reading {obj_key}: {e}')

    return (obj_content,)


@app.cell
def _(obj_content):
    obj_content
    return


@app.cell
def _():
    from google.transit import gtfs_realtime_pb2

    return (gtfs_realtime_pb2,)


@app.cell
def _(gtfs_realtime_pb2, obj_content):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(obj_content)
    for i, entity in enumerate(feed.entity):
        print(entity)
        if i > 3:
            break
    return (feed,)


@app.cell
def _(feed):
    feed.entity[0]
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
