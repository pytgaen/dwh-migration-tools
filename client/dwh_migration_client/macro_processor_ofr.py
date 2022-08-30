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
import uuid
from argparse import Namespace
from os.path import abspath, dirname, isfile, join
from pprint import pformat
from typing import Dict, Pattern, Tuple
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml
from marshmallow import Schema, ValidationError, fields
from yaml.loader import SafeLoader


class FileExpansioning:
    # Make many assumption
    # Inputs mainly is a la bash with ${..} or $..
    # Output look like python f-str

    def __init__(self, file_path: str, expansions: List[Tuple[str, Any]]):
        self.file_path = file_path
        self.expansions = expansions

    def macro_name(self, var_name: str) -> str:
        res = var_name
        if res.startswith("${"):
            res = res[2:-1]
        elif res.startswith("$"):
            res = res[1:]

        return res

    def translate_macro_ouput(self, var_name: str) -> str:
        return "{" + self.macro_name(var_name) + "}"

    def used_macros(self) -> List[str]:

# unix_var = re.compile(
#     r"(?<!\\)(\$(?P<vars>[a-zA-Z_][a-zA-Z_0-9]*))|(\${(?P<varm>[a-zA-Z_][a-zA-Z_0-9]*)})"
# )
# unix_var_s = re.compile(r"(?<!\\)(\$([a-zA-Z_][a-zA-Z_0-9]*))")
# unix_var_m = re.compile(r"(?<!\\)(\${([a-zA-Z_][a-zA-Z_0-9]*)})")
# unix_escape_dollar = re.compile(r"(\\\$)")

        return [self.macro_name(expansion[0]) for expansion in self.expansions]

    def unexpand(self, text: str) -> str:
        for expansion in self.expansions:
            text = text.replace(
                str(expansion[1].expansion), self.translate_macro_ouput(expansion[0])
            )

        return text

    def __repr__(self):
        return f"<FileExpansioning> ({self.file_path}, {self.expansions})"


class MacroDef:
    def __init__(
        self,
        name: str,
        expansion_def: str,
        expansion: Optional[str] = None,
        decollide: bool = False,
        macro_type: Optional[str] = None,
        quote: Optional[str] = None,
    ):
        self.name = name
        self.expansion = expansion if expansion else expansion_def
        self.decollide = decollide
        self.macro_type = macro_type
        self.quote = quote
        self.expansion_def = expansion_def

    def __repr__(self):
        return f"<MacroDef> (name={self.name}, expansion_def={self.expansion_def}, decollide={self.decollide}, macro_type={self.macro_type}, expansion={self.expansion}, quote={self.quote})"


class MacrosSchema(Schema):
    macros = fields.Dict(
        keys=fields.String(),
        values=fields.Dict(keys=fields.String(), values=fields.String(), required=True),
        required=True,
    )


class DecollideMapBasedExpander:
    """An util class to handle map based yaml file."""

    def __init__(self, yaml_file_path: str) -> None:
        self.yaml_file_path = yaml_file_path
        self.macro_expansion_maps = self._parse_macros_config_file()
        # self.reversed_maps = self.__get_reversed_maps()

    def check_macro_collision(self, expansion: List[Tuple[str, Any]], info_ref: str):
        dict_exp = {
            macro_name: macro_expand for (macro_name, macro_expand) in expansion
        }
        rever = {}
        for macro_name, macro_expand in dict_exp.items():
            v = rever.setdefault(macro_expand.expansion, [])
            v.append(macro_name)

        for k, expa in rever.items():
            if len(expa) > 1:
                for i, ex in enumerate(expa):
                    expa_unquote = (
                        (
                            dict_exp[ex]
                            .expansion.removeprefix(dict_exp[ex].quote)
                            .removesuffix(dict_exp[ex].quote)
                        )
                        if dict_exp[ex].quote
                        else dict_exp[ex].expansion
                    )
                    if dict_exp[ex].decollide and any(
                        x.isalpha() or x.isspace() for x in str(expa_unquote)
                    ):
                        dict_exp[ex].expansion = (
                            k[:-1]
                            + "xy"
                            + str(uuid.uuid4()).split("-")[0]
                            + "yx"
                            + k[-1]
                        )
                    if dict_exp[ex].decollide and all(
                        x.isnumeric() for x in str(expa_unquote)
                    ):
                        dict_exp[ex].expansion = str(k)[:-1] + str(i) + str(k)[-1]
                    print(
                        f"Collision {info_ref} {ex}: {dict_exp[ex].expansion_def} -> {dict_exp[ex].expansion}"
                    )
                # if dict_exp

        return dict_exp

    def expand(self, text: str, path: str) -> str:
        """Expands the macros in the text with the corresponding values defined in the
        macros_substitution_map file.

        Returns the text after macro substitution.
        """
        reg_pattern_map, patterns = self._get_all_regex_pattern_mapping(path)
        if len(reg_pattern_map) == 0:
            return text
        return patterns.sub(lambda m: reg_pattern_map[re.escape(m.group(0))], text)

    def unexpand(self, text: str, path: str) -> str:
        """Reverts the macros substitution by replacing the values with macros defined
        in the macros_substitution_map file.

        Returns the text after replacing the values with macros.
        """
        reg_pattern_map, patterns = self._get_all_regex_pattern_mapping(path, True)
        if len(reg_pattern_map) == 0:
            return text
        return patterns.sub(lambda m: reg_pattern_map[re.escape(m.group(0))], text)

    def _get_reversed_maps(self) -> Dict[str, Dict[str, str]]:
        """Swaps key and value in the macro maps and return the new map."""
        reversed_maps = {}
        for file_key, macro_map in self.macro_expansion_maps.items():
            reversed_maps[file_key] = dict((v, k) for k, v in macro_map.items())
        return reversed_maps

    def _parse_macros_config_file(self) -> Dict[str, Dict[str, str]]:
        """Parses the macros mapping yaml file.

        Return:
            macros_replacement_maps: mapping from macros to the replacement string for
                each file.  {file_name: {macro: replacement}}. File name supports
                wildcard, e.g., with "*.sql", the method will apply the macro map to all
                the files with extension of ".sql".
        """
        logging.info("Parsing macros file: %s.", self.yaml_file_path)
        with open(self.yaml_file_path, encoding="utf-8") as file:
            data = yaml.load(file, Loader=SafeLoader)
        try:
            validated_data: Dict[str, Dict[str, Dict[str, str]]] = MacrosSchema().load(
                data
            )
        except ValidationError as error:
            logging.error("Invalid macros file: %s: %s.", self.yaml_file_path, error)
            raise
        logging.info(
            "Finished parsing macros file: %s:\n%s.",
            self.yaml_file_path,
            pformat(validated_data),
        )
        macro = validated_data["macros"]

        res = {}
        for patt, patt_macro in macro.items():
            res[patt] = {}
            for macro_name, expa in patt_macro.items():
                if isinstance(expa, dict):
                    res[patt][macro_name] = MacroDef(
                        macro_name,
                        expa.get("value"),
                        decollide=expa.get("decollide"),
                        macro_type=expa.get("type"),
                    )
                else:
                    res[patt][macro_name] = MacroDef(
                        macro_name, expa, decollide=False, macro_type=None
                    )

    def _get_all_regex_pattern_mapping(
        self, file_path: str  # , use_reversed_map: bool = False
    ) -> Tuple[Dict[str, str], Pattern[str]]:
        """Compiles all the macros matched with the file path into a single regex
        pattern."""
        # macro_subst_maps = (
        #     self.reversed_maps if use_reversed_map else self.macro_expansion_maps
        # )
        reg_pattern_map = {}
        for file_map_key, token_map in self.macro_expansion_maps.items():
            if fnmatch.fnmatch(file_path, file_map_key):
                for key, value in token_map.items():
                    reg_pattern_map[re.escape(key)] = value
        all_patterns = re.compile("|".join(reg_pattern_map.keys()))
        return reg_pattern_map, all_patterns
