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
from collections import defaultdict
from curses.ascii import isalnum
from datetime import datetime, timedelta
import decimal
import fnmatch
from functools import lru_cache
import json
import logging
import os
from pathlib import Path
import pstats
import random
import re
import shutil
import string
import unicodedata
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


CHAR_CPLX = []
for i in range(257, 674):
    chr_i = chr(i).lower()
    if len(unicodedata.decomposition(chr_i[0])) == 0 and chr_i not in CHAR_CPLX:
        CHAR_CPLX.append(chr_i)

with open("char_top.json") as f_char_top:
    CHAR_TOP = json.load(f_char_top)

CHAR_CPLX = [c["char"] for c in CHAR_TOP.values()]


MACRO_STATS = {"macro_used":0, "macro_ok":0, "macro_ko":0}

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

    def _used_macros(self, text: str) -> Dict[str, int]:
        env_var = {}
        for m in self.unix_var.finditer(text):
            if m.groupdict()["vars"] is None:
                env_var.setdefault(m.groupdict()["varm"], 0)
                env_var[m.groupdict()["varm"]] += 1
            else:
                env_var.setdefault(m.groupdict()["vars"], 0)
                env_var[m.groupdict()["vars"]] += 1

        if "HEADER" in env_var:
            del env_var["HEADER"]

        if "Workfile" in env_var:
            del env_var["Workfile"]

        return env_var

    def used_macros(self) -> List[str]:
        text = Path(self.file_path).read_text()

        return self._used_macros(text)

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

    char_rnd_complex = "".join(CHAR_CPLX)
    char_rnd_alpha = string.ascii_lowercase + string.digits  # string.ascii_uppercase +

    def __init__(self, yaml_file_path: str) -> None:
        self.yaml_file_path = yaml_file_path
        self.macro_expansion_maps = global_parse_macros_config_file(yaml_file_path)
        self.generic_macro_expansion_maps = self.macro_expansion_maps["*.sql"]
        # self.reversed_maps = self._get_reversed_maps()

        self.macros_used_def: Dict[str, MacroDef] = {}
        self.char_rnd = self.char_rnd_complex

        self.stats_statement = {"update": 0, "insert": 0}

    def try_macro_expa_un(self, _text, macrod):
        _text_prev = _text
        _test_try_expa = _text.replace("${" + macrod.name + "}", macrod.expansion)
        _test_try_unexpa = _test_try_expa.replace(
            macrod.expansion, "${" + macrod.name + "}"
        )
        return _test_try_unexpa == _text_prev

    def expand(self, text: str, path_dir: str, path_name: str) -> str:
        """Expands the macros in the text with the corresponding values defined in the
        macros_substitution_map file.

        Returns the text after macro substitution.
        """
        self.macro_problem = []
        self.file_path = f"{path_dir}/{path_name}"
        _text_orig = Path(self.file_path).read_text()
        _text = _text_orig
        _file_expa = FileExpansioning(self.file_path, self.macro_expansion_maps)
        self.used_macro = _file_expa.used_macros()
        for _macro_file_expa in sorted(self.used_macro.keys()):
            if "${" + _macro_file_expa + "}" not in self.generic_macro_expansion_maps:
                if _macro_file_expa == "KNB_BATCH_DATE":
                    rnd_str = "2021-01-02 11:22:33"
                elif _macro_file_expa == "KNB_BATCH_NAME":
                    rnd_str = "".join(random.choice(self.char_rnd) for _ in range(8))
                else:
                    rnd_str = "".join(random.choice(self.char_rnd) for _ in range(1))
                logging.warning(
                    (
                        (
                            ("!!! OFR macro ${" + _macro_file_expa)
                            + "} not exist in macro.yaml, defaulting to random string "
                        )
                        + rnd_str
                    )
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

            nb_try_max = 100
            nb_try = 0
            try_accept = True  # self.try_macro_expa_un(_text, macrod)
            while (
                re.search(re.escape(macrod.expansion), _text, re.IGNORECASE)
                or not try_accept
            ):
                nb_try += 1
                if macrod.macro_type == "database" or nb_try > nb_try_max:
                    break
                cplx = nb_try / 10 if nb_try < 50 else nb_try / 5
                macrod.try_uncollide(round(cplx) + 1)
                # try_accept = self.try_macro_expa_un(_text, macrod)
            if re.search(re.escape(macrod.expansion), _text, flags=re.IGNORECASE):
                if macrod.macro_type == "database":
                    obscured = []
                    for dn in self.used_macro.keys():
                        if dn == _macro_file_expa:
                            continue
                        if "${" + dn + "}" in self.generic_macro_expansion_maps:
                            d = MacroDef(
                                dn, self.generic_macro_expansion_maps["${" + dn + "}"]
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

            self.stats_statement = {
                "update": len(re.findall("update[^;]+set", _text, flags=re.IGNORECASE)),
                "insert": len(re.findall("insert +into", _text, flags=re.IGNORECASE)),
            }

            self.macros_used_def[macrod.name] = macrod
            _text = _text.replace("${" + _macro_file_expa + "}", macrod.expansion)
            _text = _text.replace(f"${_macro_file_expa}", macrod.expansion)
        return _text

    def unexpand(self, text: str, path_dir: str, path_name: str) -> str:
        """Reverts the macros substitution by replacing the values with macros defined
        in the macros_substitution_map file.

        Returns the text after replacing the values with macros.
        """
        _text = text
        macros_to_order = list(self.macros_used_def.values())
        macros_ordered = sorted(
            macros_to_order, key=lambda m: len(m.expansion), reverse=True
        )

        for macrod in macros_ordered:
            if macrod.name == "KNB_BATCH_NAME":
                pass
            _text = re.sub(
                r"'" + re.escape(macrod.expansion) + r" *'",
                "'{" + macrod.name + "}'",
                _text,
                flags=re.IGNORECASE,
            )
            _text = _text.replace(macrod.expansion, "{" + macrod.name + "}")
            _text = _text.replace(macrod.expansion.lower(), "{" + macrod.name + "}")
            _text = _text.replace(macrod.expansion.upper(), "{" + macrod.name + "}")
            if "','" in macrod.expansion:
                _text = _text.replace(
                    macrod.expansion.replace("','", "', '"), "{" + macrod.name + "}"
                )

        _text = _text.replace("__DEFAULT_DATABASE__.", "")

        pbs = []

        # check macro count on output:
        for m_name, m_count in self.used_macro.items():
            macro_ok = True
            MACRO_STATS['macro_used'] += 1
            this_macro_def = self.macros_used_def[m_name]
            m_ouput_count = _text.count("{" + m_name + "}")
            if m_ouput_count < m_count:
                if this_macro_def.macro_type == "database":
                    pbs.append(
                        f"-- macro processor_ofr warning: sorry i'm lost macro {m_name} should get {m_count} used but output {m_ouput_count} maybe ok because link to collect stats "
                    )
                else:
                    pbs.append(
                        f"-- macro processor_ofr error: sorry i'm lost macro {m_name} should get {m_count} used but output {m_ouput_count} translate expansion {this_macro_def.expansion}"
                    )
                    macro_ok = False
                    MACRO_STATS['macro_ko'] += 1
            elif m_ouput_count > m_count:
                pbs.append(
                    f"-- macro processor_ofr warning: macro {m_name} should get {m_count} used but output {m_ouput_count} maybe ok"
                )

            if macro_ok:
                MACRO_STATS['macro_ok'] += 1
                chrs_odr = [
                    (ord(c), c) for c in this_macro_def.expansion if ord(c) > 257
                ]

                for chr_odr in chrs_odr:
                    chr_odr_repr = str(chr_odr[0])
                    CHAR_TOP.setdefault(chr_odr_repr, {"char": chr_odr[1], "nb": 0})
                    CHAR_TOP[chr_odr_repr]["nb"] += 1

        macros_name = sorted(self.macros_used_def.keys())
        u_vars = ", ".join(f'"{m}"' for m in macros_name)
        header = f"""-- generated by macro processor_ofr at {datetime.now().isoformat()}
-- USED_VARS = [{u_vars}]

"""
        # check macro count on output:
        stats_statement = {
            "update": len(re.findall("update[^;]+set", _text, flags=re.IGNORECASE)),
            "insert": len(re.findall("insert +into", _text, flags=re.IGNORECASE)),
        }
        if stats_statement["insert"] < self.stats_statement["insert"]:
            pbs.append(f"-- macro processor_ofr warning: sorry i'm lost some insert")

        if self.macro_problem:
            pbs.extend(
                [f"""-- macro processor_ofr warning: {m}""" for m in self.macro_problem]
            )

        header = header + "\n".join(pbs) + "\n\n"

        return header + _text


class MacroDef:

    char_rnd_complex = "".join(CHAR_CPLX)
    char_rnd_alpha = string.ascii_lowercase + string.digits  # string.ascii_uppercase +

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
        self.char_rnd = self.char_rnd_complex

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

    def try_uncollide_string(self, value, cplx=1, join=""):
        for ci, cv in enumerate(value):
            if cv.isalnum():
                rnd_part = "".join(random.choice(self.char_rnd) for _ in range(cplx))
                return value[:ci] + join + rnd_part + join + value[ci + len(rnd_part) :]

        rnd_part = "".join(random.choice(self.char_rnd) for _ in range(cplx))
        return join + rnd_part + join + value

    def try_uncollide(self, cplx):
        _un = self.expansion_def
        if self.macro_type in ["integer"]:
            _un = self.try_uncollide_int_or_decimal(int(self.expansion_def))

        if self.macro_type in ["decimal"]:
            _un = self.try_uncollide_int_or_decimal(decimal.Decimal(self.expansion_def))

        if self.macro_type.startswith("datetime:"):
            fmt = self.macro_type.split(":", 1)[1]
            _un = self.try_uncollide_datetime(self.expansion_def, fmt)

        if self.macro_type in ["string"]:
            _un = self.try_uncollide_string(self.expansion_def, cplx)

        self.expansion = _un
        return _un

    def __repr__(self):
        return f"<MacroDef> (name={self.name}, expansion_def={self.expansion_def}, macro_type={self.macro_type}, expansion={self.expansion}, quote={self.quote})"  # decollide={self.decollide},
