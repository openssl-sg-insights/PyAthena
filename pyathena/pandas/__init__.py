# -*- coding: utf-8 -*-
import fsspec

fsspec.register_implementation("s3", "pyathena.filesystem.s3.S3FileSystem")
fsspec.register_implementation("s3a", "pyathena.filesystem.s3.S3FileSystem")
