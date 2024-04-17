# this code is inspired by https://github.com/dmgass/baseline/ but uses separate JSON
from __future__ import annotations

# files and supports parameterized tests
import json
import os.path
import traceback
from pathlib import Path
from typing import Any

import structlog


class BaselineStore:
    def __init__(self, *args, **kwargs):
        self.caller_frame = traceback.extract_stack(limit=2)[-2]
        self.compare_file = Path(self.caller_frame.filename).with_suffix(".json")
        if kwargs:
            self.parameters = kwargs.copy()
            if args:
                self.parameters.update({f"_arg{i}": args[i] for i in range(len(args))})
        else:
            self.parameters = list(args)
        self.last_cmp = None
        self.logger = structlog.get_logger(
            self.__class__.__name__, parameters=self.parameters
        )

    def __eq__(self, other):
        self.last_cmp = None
        exists = False
        if self.compare_file.is_file():
            exists = True
            with open(self.compare_file) as f:
                baseline = json.load(f)
            updated_baseline = baseline.copy()
        else:
            updated_baseline = {}
        if self.parameters:
            cmp = self.swap(
                key_path=self.parameters, value=other, inout=updated_baseline
            )
        else:
            cmp = updated_baseline if updated_baseline else None
            updated_baseline = other
        updated_file = self.compare_file.with_suffix(".updated.json")
        if (
            exists
            and updated_file.is_file()
            and os.path.getmtime(updated_file) > os.path.getmtime(self.compare_file)
        ):
            # Update the update_file instead of self.compare_file since it is newer.
            # Like this, parameterized tests can accumulate their joint update.
            with open(updated_file) as f:
                updated_baseline2 = json.load(f)
            self.swap(key_path=self.parameters, value=other, inout=updated_baseline2)
            updated_baseline = updated_baseline2
        with open(updated_file, "w") as f:
            json.dump(updated_baseline, f, indent=4)
        if not exists:
            # write a base file that can be easily diffed against updated file
            with open(self.compare_file, "w") as f:
                if isinstance(updated_baseline, dict):
                    json.dump({}, f, indent=4)
                else:
                    json.dump(None, f, indent=4)
        self.last_cmp = cmp
        if not (cmp == other):
            self.logger.info(
                f"Comparison is false: BaselineStore({self.parameters}):\n"
                f"   '{cmp}'\n!= '{other}'"
            )
        return cmp == other

    def __repr__(self):
        if self.last_cmp:
            return f"BaselineStore({self.parameters})={self.last_cmp}"
        else:
            return f"BaselineStore({self.parameters})"

    @staticmethod
    def swap(key_path: list | dict[str, Any], value, *, inout):
        key_path = key_path.copy()
        mutable_dict = inout
        if not key_path:
            return value
        if isinstance(key_path, dict):
            try_key = list(mutable_dict.keys())[0] if mutable_dict else None
            if try_key in key_path:
                key_value = str(key_path.pop(try_key))
                if key_value in mutable_dict[try_key]:
                    if key_path:
                        return BaselineStore.swap(
                            key_path, value, inout=mutable_dict[try_key][key_value]
                        )
                    return BaselineStore._swap_value(
                        key_value, value, inout=mutable_dict[try_key]
                    )
                return BaselineStore._add_dict_key(
                    try_key, key_value, key_path, value, inout=mutable_dict
                )
            else:
                key, key_value = key_path.popitem()
                key, key_value = str(key), str(key_value)
                return BaselineStore._add_dict_key(
                    key, key_value, key_path, value, inout=mutable_dict
                )
        elif isinstance(key_path, list):
            key = str(key_path.pop(0))
            if key in mutable_dict:
                if key_path:
                    return BaselineStore.swap(key_path, value, inout=mutable_dict[key])
                else:
                    return BaselineStore._swap_value(key, value, inout=mutable_dict)
            else:
                if key_path:
                    mutable_dict[key] = {}
                    return BaselineStore.swap(key_path, value, inout=mutable_dict[key])
                else:
                    mutable_dict[key] = value
                    return None
        else:
            raise TypeError(
                "Fatal error: BaselineStore messed up self.parameters to include type: "
                f"{type(key_path)}"
            )

    @staticmethod
    def _swap_value(key, value, *, inout):
        mutable_dict = inout
        save = mutable_dict[key]
        mutable_dict[key] = value
        return save

    @staticmethod
    def _add_dict_key(key, key_value, key_path, value, *, inout):
        mutable_dict = inout
        if key_path:
            mutable_dict[key][key_value] = {}
            return BaselineStore.swap(key_path, value, mutable_dict[key][key_value])
        mutable_dict[key][key_value] = value
        return None  # no match found
