# -*- coding: utf-8 -*-
import logging
from concurrent.futures import Future
from multiprocessing import cpu_count
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

from pyathena import ProgrammingError
from pyathena.arrow.converter import (
    DefaultArrowTypeConverter,
    DefaultArrowUnloadTypeConverter,
)
from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.async_cursor import AsyncCursor
from pyathena.common import CursorIterator
from pyathena.converter import Converter
from pyathena.formatter import Formatter
from pyathena.model import AthenaCompression, AthenaFileFormat
from pyathena.util import RetryConfig

if TYPE_CHECKING:
    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class AsyncArrowCursor(AsyncCursor):
    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        formatter: Formatter,
        retry_config: RetryConfig,
        s3_staging_dir: Optional[str] = None,
        schema_name: Optional[str] = None,
        catalog_name: Optional[str] = None,
        work_group: Optional[str] = None,
        poll_interval: float = 1,
        encryption_option: Optional[str] = None,
        kms_key: Optional[str] = None,
        kill_on_interrupt: bool = True,
        max_workers: int = (cpu_count() or 1) * 5,
        arraysize: int = CursorIterator.DEFAULT_FETCH_SIZE,
        unload: bool = False,
    ) -> None:
        super(AsyncArrowCursor, self).__init__(
            connection=connection,
            converter=converter,
            formatter=formatter,
            retry_config=retry_config,
            s3_staging_dir=s3_staging_dir,
            schema_name=schema_name,
            catalog_name=catalog_name,
            work_group=work_group,
            poll_interval=poll_interval,
            encryption_option=encryption_option,
            kms_key=kms_key,
            kill_on_interrupt=kill_on_interrupt,
            max_workers=max_workers,
            arraysize=arraysize,
        )
        self._unload = unload

    @staticmethod
    def get_default_converter(
        unload: bool = False,
    ) -> Union[DefaultArrowTypeConverter, DefaultArrowUnloadTypeConverter, Any]:
        if unload:
            return DefaultArrowUnloadTypeConverter()
        else:
            return DefaultArrowTypeConverter()

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0:
            raise ProgrammingError("arraysize must be a positive integer value.")
        self._arraysize = value

    def _collect_result_set(
        self,
        query_id: str,
        unload_location: Optional[str] = None,
        kwargs: Dict[str, Any] = None,
    ) -> AthenaArrowResultSet:
        if kwargs is None:
            kwargs = dict()
        query_execution = self._poll(query_id)
        return AthenaArrowResultSet(
            connection=self._connection,
            converter=self._converter,
            query_execution=query_execution,
            arraysize=self._arraysize,
            retry_config=self._retry_config,
            unload=self._unload,
            unload_location=unload_location,
            **kwargs,
        )

    def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        cache_size: int = 0,
        cache_expiration_time: int = 0,
        **kwargs,
    ) -> Tuple[str, "Future[Union[AthenaArrowResultSet, Any]]"]:
        if self._unload:
            s3_staging_dir = s3_staging_dir if s3_staging_dir else self._s3_staging_dir
            assert s3_staging_dir, "If the unload option is used, s3_staging_dir is required."
            operation, unload_location = self._formatter.wrap_unload(
                operation,
                s3_staging_dir=s3_staging_dir,
                format_=AthenaFileFormat.FILE_FORMAT_PARQUET,
                compression=AthenaCompression.COMPRESSION_SNAPPY,
            )
        else:
            unload_location = None
        query_id = self._execute(
            operation,
            parameters=parameters,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
            cache_expiration_time=cache_expiration_time,
        )
        return (
            query_id,
            self._executor.submit(
                self._collect_result_set,
                query_id,
                unload_location,
                kwargs,
            ),
        )
