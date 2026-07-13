"""
Data IO layer supporting either local FS or S3-like
"""
import os
import io
from typing import BinaryIO
from pathlib import Path

from google.transit import gtfs_realtime_pb2 as gtfs

import boto3

class S3StorageBackend(StorageBackend):
    def __init__(self, auth, bucket):
        self._auth = auth
        self._bucket = bucket
        self._client = boto3.client('s3')
    def open_reader(self, blob_path: str) -> BinaryIO:
        response = self._client.get_object(
            Bucket=self._bucket,
            Key=blob_path
        )
        # botocore.response.StreamingBody to satisfy BinaryIO
        return response['Body']

    def open_writer(self, dest_path: str, overwrite: bool = False) -> BinaryIO:
        """
        io.BytesIO as buffer since s3 doesn't natively support this
        """
        buffer = io.BytesIO()
        # patch close() to execute upload upon close
        native_close = buffer.close # to be patched
        def s3_close():
            if not buffer.closed:
                buffer.seek(0)
                self._client.upload_fileobj(buffer, self._bucket, dest_path)
                native_close()
        buffer.close = s3_close
        return buffer

class LocalStorageBackend(StorageBackend):
    def __init__(self, root: Path) -> None:
        self._root = root

    def _resolve(self, path: str) -> Path:
        if path.startswith('/'):
            return Path(path)
        if '~' in path:
            return Path(path).expanduser()
        # otherwise, relative path
        return self._root / path
    
    def open_reader(self, src_path: str) -> BinaryIO:
        return self._resolve(src_path).open('rb')

    def open_writer(self, dest_path: str, overwrite: bool = False) -> BinaryIO:
        full_path = self._resolve(dest_path)
        if full_path.exists() and not overwrite:
            raise FileExistsError(f'{dest_path} already exists')
        full_path.parent.mkdir(parents=True, exist_ok=True)
        return full_path.open('wb')

