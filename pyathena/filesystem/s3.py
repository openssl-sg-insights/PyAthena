# -*- coding: utf-8 -*-
import itertools
import logging
import re
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from multiprocessing import cpu_count
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Pattern, Tuple, cast

import botocore.exceptions
from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile

from pyathena.util import retry_api_call

if TYPE_CHECKING:
    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class S3ObjectType:

    S3_OBJECT_TYPE_DIRECTORY: str = "directory"
    S3_OBJECT_TYPE_FILE: str = "file"


class S3StorageClass:

    S3_STORAGE_CLASS_STANDARD = "STANDARD"
    S3_STORAGE_CLASS_REDUCED_REDUNDANCY = "REDUCED_REDUNDANCY"
    S3_STORAGE_CLASS_STANDARD_IA = "STANDARD_IA"
    S3_STORAGE_CLASS_ONEZONE_IA = "ONEZONE_IA"
    S3_STORAGE_CLASS_INTELLIGENT_TIERING = "INTELLIGENT_TIERING"
    S3_STORAGE_CLASS_GLACIER = "GLACIER"
    S3_STORAGE_CLASS_DEEP_ARCHIVE = "DEEP_ARCHIVE"
    S3_STORAGE_CLASS_OUTPOSTS = "OUTPOSTS"
    S3_STORAGE_CLASS_GLACIER_IR = "GLACIER_IR"

    S3_STORAGE_CLASS_BUCKET = "BUCKET"
    S3_STORAGE_CLASS_DIRECTORY = "DIRECTORY"


@dataclass
class S3Object:

    bucket: str
    key: Optional[str]
    size: int
    type: str
    storage_class: str
    etag: Optional[str]

    def __post_init__(self) -> None:
        if self.key is None:
            self.name = self.bucket
        else:
            self.name = f"{self.bucket}/{self.key}"


class S3FileSystem(AbstractFileSystem):

    DEFAULT_BLOCK_SIZE: int = 5 * 2**20  # 5MB
    PATTERN_PATH: Pattern[str] = re.compile(
        r"(^s3://|^s3a://|^)(?P<bucket>[a-zA-Z0-9.\-_]+)(/(?P<key>[^?]+)|/)?"
        r"($|\?version(Id|ID|id|_id)=(?P<version_id>.+)$)"
    )

    protocol = ["s3", "s3a"]
    _extra_tokenize_attributes = ("default_block_size",)

    def __init__(
        self,
        connection: "Connection",
        block_size: Optional[int] = None,
        max_workers: int = (cpu_count() or 1) * 5,
        *args,
        **kwargs,
    ) -> None:
        super(S3FileSystem, self).__init__(*args, **kwargs)
        self._client = connection.session.client(
            "s3", region_name=connection.region_name, **connection._client_kwargs
        )
        self._retry_config = connection.retry_config
        self.max_workers = max_workers
        self.default_block_size = block_size if block_size else self.DEFAULT_BLOCK_SIZE

    @staticmethod
    def parse_path(path: str) -> Tuple[str, Optional[str], Optional[str]]:
        match = S3FileSystem.PATTERN_PATH.search(path)
        if match:
            return match.group("bucket"), match.group("key"), match.group("version_id")
        else:
            raise ValueError(f"Invalid S3 path format {path}.")

    def _ls_buckets(self, refresh: bool = False) -> List[Dict[Any, Any]]:
        if "" not in self.dircache or refresh:
            try:
                response = retry_api_call(
                    self._client.list_buckets,
                    config=self._retry_config,
                    logger=_logger,
                )
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "AccessDenied":
                    return []
                raise
            buckets = [
                S3Object(
                    bucket=b["Name"],
                    key=None,
                    size=0,
                    type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                    storage_class=S3StorageClass.S3_STORAGE_CLASS_BUCKET,
                    etag=None,
                ).__dict__
                for b in response["Buckets"]
            ]
            self.dircache[""] = buckets
        else:
            buckets = self.dircache[""]
        return buckets

    def _ls_dirs(
        self,
        path: str,
        prefix: str = "",
        delimiter: str = "/",
        next_token=None,
        max_keys: Optional[int] = None,
        refresh: bool = False,
    ) -> List[Dict[Any, Any]]:
        bucket, key, _ = self.parse_path(path)
        if key:
            prefix = f"{key}/{prefix if prefix else ''}"
        if path not in self.dircache or refresh:
            files: List[Dict[Any, Any]] = []
            while True:
                request: Dict[Any, Any] = {
                    "Bucket": bucket,
                    "Prefix": prefix,
                    "Delimiter": delimiter,
                }
                if next_token:
                    request.update({"ContinuationToken": next_token})
                if max_keys:
                    request.update({"MaxKeys": max_keys})
                response = retry_api_call(
                    self._client.list_objects_v2,
                    config=self._retry_config,
                    logger=_logger,
                    **request,
                )
                files.extend(
                    S3Object(
                        bucket=bucket,
                        key=c["Prefix"][:-1],
                        size=0,
                        type=S3ObjectType.S3_OBJECT_TYPE_DIRECTORY,
                        storage_class=S3StorageClass.S3_STORAGE_CLASS_DIRECTORY,
                        etag=None,
                    ).__dict__
                    for c in response.get("CommonPrefixes", [])
                )
                files.extend(
                    S3Object(
                        bucket=bucket,
                        key=c["Key"],
                        size=c["Size"],
                        type=S3ObjectType.S3_OBJECT_TYPE_FILE,
                        storage_class=c["StorageClass"],
                        etag=c["ETag"],
                    ).__dict__
                    for c in response.get("Contents", [])
                )
                next_token = response.get("NextContinuationToken", None)
                if not next_token:
                    break
            if files:
                self.dircache[path] = files
        else:
            files = self.dircache[path]
        return files

    def _head_object(self, path: str, refresh: bool = False) -> Optional[Dict[Any, Any]]:
        bucket, key, _ = self.parse_path(path)
        if path not in self.dircache or refresh:
            try:
                response = retry_api_call(
                    self._client.head_object,
                    config=self._retry_config,
                    logger=_logger,
                    Bucket=bucket,
                    Key=key,
                )
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    return None
                raise
            file = S3Object(
                bucket=bucket,
                key=key,
                size=response["ContentLength"],
                type=S3ObjectType.S3_OBJECT_TYPE_FILE,
                storage_class=response.get(
                    "StorageClass", S3StorageClass.S3_STORAGE_CLASS_STANDARD
                ),
                etag=response["ETag"],
            ).__dict__
            self.dircache[path] = file
        else:
            file = self.dircache[path]
        return file

    def ls(self, path, detail=False, refresh=False, **kwargs):
        path = self._strip_protocol(path).rstrip("/")
        if path in ["", "/"]:
            files = self._ls_buckets(refresh)
        else:
            files = self._ls_dirs(path, refresh=refresh)
            if not files and "/" in path:
                file = self._head_object(path, refresh=refresh)
                if file:
                    files = [file]
        return files if detail else [f["name"] for f in files]

    def cp_file(self, path1, path2, **kwargs):
        raise NotImplementedError  # pragma: no cover

    def _rm(self, path):
        raise NotImplementedError  # pragma: no cover

    def created(self, path):
        raise NotImplementedError  # pragma: no cover

    def modified(self, path):
        raise NotImplementedError  # pragma: no cover

    def sign(self, path, expiration=100, **kwargs):
        raise NotImplementedError  # pragma: no cover

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        # TODO cache settings?
        """
        if fill_cache is None:
            fill_cache = self.default_fill_cache
        if cache_type is None:
            cache_type = self.default_cache_type
        """
        return S3File(
            self,
            path,
            mode,
            version_id=None,
            max_workers=self.max_workers,
            block_size=block_size if block_size else self.default_block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs,
        )

    def _get_object(
        self,
        bucket: str,
        key: str,
        ranges: Tuple[int, int],
        version_id: Optional[str] = None,
        kwargs: Dict[Any, Any] = None,
    ) -> Tuple[int, bytes]:
        range_ = f"bytes={ranges[0]}-{ranges[1]}"
        request = {"Bucket": bucket, "Key": key, "Range": range_}
        if version_id:
            request.update({"versionId": version_id})
        kwargs = kwargs if kwargs else {}

        _logger.debug(f"Get object: s3://{bucket}/{key}?versionId={version_id} {range_}")
        response = retry_api_call(
            self._client.get_object,
            config=self._retry_config,
            logger=_logger,
            **request,
            **kwargs,
        )
        return ranges[0], cast(bytes, response["Body"].read())


class S3File(AbstractBufferedFile):
    def __init__(
        self,
        fs: S3FileSystem,
        path: str,
        mode: str = "rb",
        version_id: Optional[str] = None,
        max_workers: int = (cpu_count() or 1) * 5,
        block_size: int = S3FileSystem.DEFAULT_BLOCK_SIZE,
        autocommit: bool = True,
        cache_type: str = "bytes",
        cache_options: Optional[Dict[Any, Any]] = None,
        size: Optional[int] = None,
    ):
        super(S3File, self).__init__(
            fs=fs,
            path=path,
            mode=mode,
            block_size=block_size * max_workers,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            size=size,
        )
        self.fs = fs
        bucket, key, path_version_id = S3FileSystem.parse_path(path)
        self.bucket = bucket
        if not key:
            raise ValueError("The path does not contain a key.")
        self.key = key
        if version_id and path_version_id:
            if version_id != path_version_id:
                raise ValueError(
                    f"The version_id: {version_id} specified in the argument and "
                    f"the version_id: {path_version_id} specified in the path do not match."
                )
            self.version_id: Optional[str] = version_id
        elif path_version_id:
            self.version_id = path_version_id
        else:
            self.version_id = version_id
        self.max_workers = max_workers
        self.worker_block_size = block_size
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        if "r" in mode and "etag" in self.details:
            self.request_kwargs = {"IfMatch": self.details["etag"]}

    def close(self):
        super(S3File, self).close()
        self._executor.shutdown()

    def _initiate_upload(self):
        raise NotImplementedError  # pragma: no cover

    def _fetch_range(self, start, end):
        ranges = self._get_ranges(start, end)
        if len(ranges) > 1:
            object_ = self._merge_objects(
                list(
                    self._executor.map(
                        self.fs._get_object,
                        itertools.repeat(self.bucket),
                        itertools.repeat(self.key),
                        ranges,
                        itertools.repeat(self.version_id),
                        itertools.repeat(self.request_kwargs),
                    )
                )
            )
        else:
            object_ = self.fs._get_object(
                self.bucket, self.key, ranges[0], self.version_id, self.request_kwargs
            )[1]
        return object_

    def _get_ranges(self, start: int, end: int) -> List[Tuple[int, int]]:
        ranges = []
        range_size = end - start
        if self.max_workers > 1 and range_size > (self.worker_block_size + 1):
            range_start = start
            while True:
                range_end = range_start + self.worker_block_size
                if range_end > end:
                    ranges.append((range_start, end))
                    break
                else:
                    ranges.append((range_start, range_end))
                    range_start += self.worker_block_size + 1
        else:
            ranges.append((start, end))
        return ranges

    def _merge_objects(self, objects: List[Tuple[int, bytes]]) -> bytes:
        objects.sort(key=lambda x: x[0])
        return b"".join([obj for start, obj in objects])
