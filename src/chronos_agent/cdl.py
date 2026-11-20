"""Parse the text-only CDL emitted by Chronos Control."""

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping
from xml.etree.ElementTree import ParseError, fromstring


class CdlError(ValueError):
    """The job's CDL cannot be used by a lifecycle hook."""


@dataclass(frozen=True)
class Cdl:
    system_id: str
    setup: Mapping[str, str]
    evaluation: Mapping[str, str]

    @classmethod
    def parse(cls, xml: str) -> "Cdl":
        # Chronos emits neither a DTD nor entities beyond standard XML escapes.
        if "<!DOCTYPE" in xml:
            raise CdlError("CDL document types are forbidden.")
        try:
            root = fromstring(xml)
        except (ParseError, UnicodeError, LookupError) as error:
            raise CdlError(f"Malformed CDL: {error}.") from error
        if root.tag != "chronos" or [section.tag for section in root] != ["setup", "evaluation"]:
            raise CdlError("CDL requires chronos with setup and evaluation sections.")
        setup, evaluation = root
        system_id = evaluation.get("system")
        if not system_id:
            raise CdlError("CDL evaluation requires a system attribute.")
        return cls(system_id, _parameters(setup), _parameters(evaluation))


def _parameters(section) -> Mapping[str, str]:
    parameters = {}
    for parameter in section:
        if (parameter.tag in parameters or len(parameter) or parameter.attrib
                or parameter.tag.startswith("{")):
            raise CdlError("CDL parameters must be unique, unqualified text elements.")
        parameters[parameter.tag] = parameter.text or ""
    return MappingProxyType(parameters)
