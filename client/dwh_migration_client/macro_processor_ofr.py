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

from ast import ExceptHandler
from curses.ascii import isalnum
from datetime import datetime, timedelta
import decimal
import fnmatch
from functools import lru_cache
import logging
import os
from pathlib import Path
import pstats
import random
import re
import shutil
import string
import uuid
from argparse import Namespace
from os.path import abspath, dirname, isfile, join
from pprint import pformat
from typing import Dict, Pattern, Tuple
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml
from marshmallow import Schema, ValidationError, fields
from yaml.loader import SafeLoader

from dwh_migration_client.macro_schema import MacrosSchema
from dateutil.parser import parse


@lru_cache
def global_parse_macros_config_file(yaml_file_path) -> Dict[str, Dict[str, str]]:
    """Parses the macros mapping yaml file.

    Return:
        macros_replacement_maps: mapping from macros to the replacement string for
            each file.  {file_name: {macro: replacement}}. File name supports
            wildcard, e.g., with "*.sql", the method will apply the macro map to all
            the files with extension of ".sql".
    """
    # logging.info("Parsing macros file: %s.", yaml_file_path)
    with open(yaml_file_path, encoding="utf-8") as file:
        data = yaml.load(file, Loader=SafeLoader)
    try:
        validated_data: Dict[str, Dict[str, Dict[str, str]]] = MacrosSchema().load(data)
    except ValidationError as error:
        logging.error("Invalid macros file: %s: %s.", yaml_file_path, error)
        raise
    # logging.info(
    #     "Finished parsing macros file: %s:\n%s.",
    #     yaml_file_path,
    #     pformat(validated_data),
    # )
    return validated_data["macros"]


class FileExpansioning:
    # Make many assumption
    # Inputs mainly is a la bash with ${..} or $..
    # Output look like python f-str

    def __init__(self, file_path: str, macros: Dict[str, Any]):
        self.file_path = file_path
        self.macros = macros
        self.expansions = {}

        self.unix_var = re.compile(
            r"(?<!\\)(\$(?P<vars>[a-zA-Z_][a-zA-Z_0-9]*))|(\${(?P<varm>[a-zA-Z_][a-zA-Z_0-9]*)})"
        )

    def macro_name(self, var_name: str) -> str:
        res = var_name
        if res.startswith("${"):
            res = res[2:-1]
        elif res.startswith("$"):
            res = res[1:]

        return res

    def translate_macro_ouput(self, var_name: str) -> str:
        return "{" + self.macro_name(var_name) + "}"

    def _used_macros(self, text: str) -> Set[str]:
        env_var = set()
        for m in self.unix_var.finditer(text):
            if m.groupdict()["vars"] is None:
                env_var.add(m.groupdict()["varm"])
            else:
                env_var.add(m.groupdict()["vars"])

        if "HEADER" in env_var:
            env_var.remove("HEADER")

        if "Workfile" in env_var:
            env_var.remove("Workfile")

        return env_var

    def used_macros(self) -> List[str]:
        text = Path(self.file_path).read_text()

        return list(self._used_macros(text))

    def unexpand(self, text: str) -> str:
        for expansion in self.expansions:
            text = text.replace(
                str(expansion[1].expansion), self.translate_macro_ouput(expansion[0])
            )

        return text

    def __repr__(self):
        return f"<FileExpansioning> ({self.file_path}, {self.expansions})"


class OfrMapBasedExpander:
    """An util class to handle map based yaml file."""

    def __init__(self, yaml_file_path: str) -> None:
        self.yaml_file_path = yaml_file_path
        self.macro_expansion_maps = global_parse_macros_config_file(yaml_file_path)
        self.generic_macro_expansion_maps = self.macro_expansion_maps["*.sql"]
        # self.reversed_maps = self._get_reversed_maps()

        self.macros_def: Dict[str, MacroDef] = {}

    def expand(self, text: str, path_dir: str, path_name: str) -> str:
        """Expands the macros in the text with the corresponding values defined in the
        macros_substitution_map file.

        Returns the text after macro substitution.
        """
        self.macro_problem = []
        self.file_path = path_dir + "/" + path_name

        _text_orig = Path(self.file_path).read_text()
        _text = _text_orig

        _file_expa = FileExpansioning(self.file_path, self.macro_expansion_maps)

        for _macro_file_expa in _file_expa.used_macros():
            if "${" + _macro_file_expa + "}" not in self.generic_macro_expansion_maps:
                rnd_str = "".join(
                    random.choice(
                        string.ascii_lowercase
                        + string.digits  # string.ascii_uppercase +
                    )
                    for _ in range(16)
                )
                logging.warning(
                    "!!! OFR macro ${"
                    + _macro_file_expa
                    + "} not exist in macro.yaml, defaulting to random string "
                    + rnd_str
                )
                self.macro_problem.append(
                    f"""macro not defined: {_macro_file_expa} use random string {rnd_str}"""
                )
                self.generic_macro_expansion_maps[
                    "${" + _macro_file_expa + "}"
                ] = rnd_str

            macrod = MacroDef(
                _macro_file_expa,
                self.generic_macro_expansion_maps["${" + _macro_file_expa + "}"],
            )
            nb_try = 100
            while re.search(re.escape(macrod.expansion), _text, re.IGNORECASE):
                nb_try -= 1
                if macrod.macro_type == "database" or nb_try == 0:
                    break
                macrod.try_uncollide()

            if re.search(re.escape(macrod.expansion), _text, re.IGNORECASE):
                if macrod.macro_type == "database":
                    obscured = []
                    for dn in _file_expa.used_macros():
                        if dn == _macro_file_expa:
                            continue # skip myself
                        if "${" + dn + "}" in self.generic_macro_expansion_maps:
                            d = MacroDef(
                                dn,
                                self.generic_macro_expansion_maps["${" + dn + "}"],
                            )
                            if (
                                d.macro_type == "database"
                                and macrod.expansion == d.expansion
                            ):
                                obscured.append(dn)
                    self.macro_problem.append(
                        f"""macro database collide: {_macro_file_expa} value {macrod.expansion} collide with {obscured}"""
                    )
                else:
                    self.macro_problem.append(
                        f"""macro collide: {_macro_file_expa} value {macrod.expansion}"""
                    )

            self.macros_def[macrod.name] = macrod

            _text = _text.replace("${" + _macro_file_expa + "}", macrod.expansion)
            _text = _text.replace("$" + _macro_file_expa, macrod.expansion)

        return _text

    def unexpand(self, text: str, path_dir: str, path_name: str) -> str:
        """Reverts the macros substitution by replacing the values with macros defined
        in the macros_substitution_map file.

        Returns the text after replacing the values with macros.
        """
        _text = text
        macros_to_order = list(self.macros_def.values())
        macros_ordered = sorted(
            macros_to_order, key=lambda m: len(m.expansion), reverse=True
        )

        for macrod in macros_ordered:
            # if macrod.macro_type == "database":
            #     _text = _text.replace(macrod.expansion, "{" + macrod.name + "}")
            #     _text = _text.replace(macrod.expansion.lower(), "{" + macrod.name + "}")
            #     _text = _text.replace(macrod.expansion.upper(), "{" + macrod.name + "}")
            # else:
            #     _text = _text.replace(macrod.expansion, "{" + macrod.name + "}")

            _text = _text.replace(macrod.expansion, "{" + macrod.name + "}")
            _text = _text.replace(macrod.expansion.lower(), "{" + macrod.name + "}")
            _text = _text.replace(macrod.expansion.upper(), "{" + macrod.name + "}")

        _text = _text.replace("__DEFAULT_DATABASE__.", "")

        u_vars = ", ".join(f'"{m}"' for m in self.macros_def.keys())
        header = f"""-- generated by macro processor_ofr at {datetime.now().isoformat()}
-- USED_VARS = [{u_vars}]

"""

        if self.macro_problem:
            pbs = [
                f"""-- macro processor_ofr warning: {m}""" for m in self.macro_problem
            ]
            header = header + "\n".join(pbs) + "\n\n"

        return header + _text


class MacroDef:
    def __init__(
        self,
        name: str,
        expansion_def: Optional[str] = None,
        macro_type: Optional[str] = None,
    ):
        self.name = name
        self.expansion_def = expansion_def.strip()
        self.macro_type = macro_type
        self.quote = self.expansion_def.startswith("'") and self.expansion_def.endswith(
            "'"
        )
        self.expansion = expansion_def.strip()

        self.guess_macro_type()

    def infer_date_format(self, datetime_str):
        _type = ""
        try:
            _ = parse(datetime_str)
            _type = "string"  # datetime:unknow"
            is_datetime = True
        except Exception as e:
            raise ValueError("Not a datetime") from e

        valid_date_formats = [
            "%Y-%m-%d",
            "%Y-%m-%d",
            "%y-%m-%d",
            "%d/%m/%Y",
            "%Y/%m/%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
        ]
        for date_format in valid_date_formats:
            try:
                _ = datetime.strptime(datetime_str, date_format)
                _type = "datetime:" + date_format
            except ValueError:
                pass

        if not _type.startswith("datetime:"):
            raise ValueError("Not a datetime")

        return _type

    def guess_macro_type(self):
        if self.macro_type:
            return None

        _type = "string"

        try:
            _ = int(self.expansion_def)
            _type = "integer"
        except Exception as e:
            pass

        if _type == "string":
            try:
                _ = decimal.Decimal(self.expansion_def)
                _type = "decimal"
            except Exception as e:
                pass

        if _type == "string":
            try:
                _datetime_fmt = self.infer_date_format(self.expansion_def)
                _type = _datetime_fmt
            except Exception as e:
                pass

        if _type == "string":
            if self.expansion_def.endswith("_MM2_CY2"):
                _type = "database"

        self.macro_type = _type

    def try_uncollide_datetime(self, datetime_str, datetime_fmt):
        dt = datetime.strptime(datetime_str, datetime_fmt)
        if dt.year == 9999:
            dt = dt - timedelta(days=random.randint(0, 120))
        else:
            dt = dt + timedelta(days=random.randint(0, 120))

        return datetime.strftime(dt, datetime_fmt)

    def try_uncollide_int_or_decimal(self, value):
        if value < 32000:
            value = value - random.randint(0, 120)
        else:
            value = value + random.randint(0, 120)

        return str(value)

    def try_uncollide_string(self, value, join="xxx"):
        for ci, cv in enumerate(value):
            if cv.isalnum():
                rnd_part = "".join(
                    random.choice(
                        string.ascii_lowercase
                        + string.digits  # string.ascii_uppercase +
                    )
                    for _ in range(16)
                )
                return value[:ci] + join + rnd_part + join + value[ci:]

        rnd_part = "".join(
            random.choice(
                string.ascii_lowercase + string.digits  # string.ascii_uppercase +
            )
            for _ in range(16)
        )
        return join + rnd_part + join + value

    def try_uncollide(self):
        _un = self.expansion_def
        if self.macro_type in ["integer"]:
            _un = self.try_uncollide_int_or_decimal(int(self.expansion_def))

        if self.macro_type in ["decimal"]:
            _un = self.try_uncollide_int_or_decimal(decimal.Decimal(self.expansion_def))

        if self.macro_type.startswith("datetime:"):
            fmt = self.macro_type.split(":", 1)[1]
            _un = self.try_uncollide_datetime(self.expansion_def, fmt)

        if self.macro_type in ["string"]:
            _un = self.try_uncollide_string(self.expansion_def)

        self.expansion = _un
        return _un

    def __repr__(self):
        return f"<MacroDef> (name={self.name}, expansion_def={self.expansion_def}, macro_type={self.macro_type}, expansion={self.expansion}, quote={self.quote})"  # decollide={self.decollide},
