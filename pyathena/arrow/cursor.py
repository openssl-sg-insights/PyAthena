# -*- coding: utf-8 -*-
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, TypeVar, Union, cast

from pyathena.arrow.converter import (
    DefaultArrowTypeConverter,
    DefaultArrowUnloadTypeConverter,
)
from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.common import BaseCursor, CursorIterator
from pyathena.converter import Converter
from pyathena.error import OperationalError, ProgrammingError
from pyathena.formatter import Formatter
from pyathena.model import AthenaCompression, AthenaFileFormat, AthenaQueryExecution
from pyathena.result_set import WithResultSet
from pyathena.util import RetryConfig, synchronized

if TYPE_CHECKING:
    from pyarrow import Table

    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore
_T = TypeVar("_T", bound="ArrowCursor")


class ArrowCursor(BaseCursor, CursorIterator, WithResultSet):
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
        unload: bool = False,
        **kwargs,
    ) -> None:
        super(ArrowCursor, self).__init__(
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
            **kwargs,
        )
        self._unload = unload
        self._query_id: Optional[str] = None
        self._result_set: Optional[AthenaArrowResultSet] = None

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

    @property
    def result_set(self) -> Optional[AthenaArrowResultSet]:
        return self._result_set

    @result_set.setter
    def result_set(self, val) -> None:
        self._result_set = val

    @property
    def query_id(self) -> Optional[str]:
        return self._query_id

    @query_id.setter
    def query_id(self, val) -> None:
        self._query_id = val

    @property
    def rownumber(self) -> Optional[int]:
        return self.result_set.rownumber if self.result_set else None

    def close(self) -> None:
        if self.result_set and not self.result_set.is_closed:
            self.result_set.close()

    @synchronized
    def execute(
        self: _T,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        cache_size: int = 0,
        cache_expiration_time: int = 0,
        **kwargs,
    ) -> _T:
        self._reset_state()
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
        self.query_id = self._execute(
            operation,
            parameters=parameters,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
            cache_expiration_time=cache_expiration_time,
        )
        query_execution = self._poll(self.query_id)
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self.result_set = AthenaArrowResultSet(
                connection=self._connection,
                converter=self._converter,
                query_execution=query_execution,
                arraysize=self.arraysize,
                retry_config=self._retry_config,
                unload=self._unload,
                unload_location=unload_location,
                **kwargs,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self

    def executemany(
        self, operation: str, seq_of_parameters: List[Optional[Dict[str, Any]]]
    ) -> None:
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
        # Operations that have result sets are not allowed with executemany.
        self._reset_state()

    @synchronized
    def cancel(self) -> None:
        if not self.query_id:
            raise ProgrammingError("QueryExecutionId is none or empty.")
        self._cancel(self.query_id)

    @synchronized
    def fetchone(
        self,
    ) -> Optional[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaArrowResultSet, self.result_set)
        return result_set.fetchone()

    @synchronized
    def fetchmany(
        self, size: Optional[int] = None
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaArrowResultSet, self.result_set)
        return result_set.fetchmany(size)

    @synchronized
    def fetchall(
        self,
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaArrowResultSet, self.result_set)
        return result_set.fetchall()

    @synchronized
    def as_arrow(self) -> "Table":
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaArrowResultSet, self.result_set)
        return result_set.as_arrow()
