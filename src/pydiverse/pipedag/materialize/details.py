from __future__ import annotations

import copy
import dataclasses
from abc import ABC, abstractmethod
from typing import TypeVar

from pydiverse.pipedag import Table

_T = TypeVar("_T", bound="BaseMaterializationDetails")


@dataclasses.dataclass(frozen=True)
class BaseMaterializationDetails(ABC):
    @abstractmethod
    def __post_init__(self):
        """
        The __post_init__ method should validate the values.
        """
        return

    @classmethod
    def from_dict(
        cls: type[_T], d: dict[str, dict[str | list[str]]], strict: bool, logger
    ) -> _T:
        unsupported_arguments = set(d.keys()) - {
            field.name for field in dataclasses.fields(cls)
        }
        if unsupported_arguments:
            error_msg = (
                f"The materialization arguments {unsupported_arguments} "
                f"are not supported for {cls.__name__}."
            )
            if strict:
                raise TypeError(
                    f"{error_msg} To silence this exception set"
                    f" strict_materialization_details=False"
                )
            logger.error(error_msg)
        details = cls(**{k: v for k, v in d.items() if k not in unsupported_arguments})
        return details

    @classmethod
    def create_materialization_details_dict(
        cls: type[_T],
        materialization_details_cfg: dict[str, dict[str | list[str]]] | None,
        strict: bool,
        default_materialization_details: str | None,
        logger,
    ) -> dict[str, _T]:
        materialization_details_cfg = (
            materialization_details_cfg
            if materialization_details_cfg is not None
            else dict()
        )
        materialization_details: dict[str, _T] = dict()
        materialization_details["__any__"] = (
            cls.from_dict(materialization_details_cfg["__any__"], strict, logger)
            if "__any__" in materialization_details_cfg
            else cls()
        )
        any_dict = dataclasses.asdict(materialization_details["__any__"])
        for k, v in materialization_details_cfg.items():
            if k == "__any__":
                continue
            detail_dict = copy.deepcopy(any_dict)
            detail_dict.update(v)
            materialization_details[k] = cls.from_dict(detail_dict, strict, logger)

        if (
            default_materialization_details
            and default_materialization_details not in materialization_details
        ):
            raise ValueError(
                f"default_materialization_details {default_materialization_details}"
                f" not in materialization_details"
            )
        return materialization_details

    @classmethod
    def get_attribute_from_dict(
        cls: type[_T],
        d: dict[str, _T],
        label: str,
        default_label: str | None,
        attribute: str,
        strict: bool,
        logger,
    ):
        label = (
            label
            if label is not None
            else default_label
            if default_label is not None
            else "__any__"
        )
        if label not in d:
            error_msg = f"{label} is an unknown materialization details label."
            if strict:
                raise ValueError(
                    f"{error_msg} To silence this exception set"
                    f" strict_materialization_details=False"
                )
            else:
                logger.error(f"{error_msg} Using __any__ instead.")
                label = "__any__"
        return getattr(d[label], attribute)


def resolve_materialization_details_label(table: Table) -> str | None:
    if table.materialization_details is not None:
        return table.materialization_details
    if table.stage is not None:
        return table.stage.materialization_details
    return None
