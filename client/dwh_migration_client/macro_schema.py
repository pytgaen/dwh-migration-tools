# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A processor to handle macros in the query files during the pre-processing and
post-processing stages of a Batch Sql Translation job.
"""

import fnmatch
import logging
import os
import re
import shutil
from argparse import Namespace
from os.path import abspath, dirname, isfile, join
from pprint import pformat
from typing import Dict, Pattern, Tuple

import yaml
from marshmallow import Schema, ValidationError, fields
from yaml.loader import SafeLoader


class MacrosSchema(Schema):
    macros = fields.Dict(
        keys=fields.String(),
        values=fields.Dict(keys=fields.String(), values=fields.String(), required=True),
        required=True,
    )