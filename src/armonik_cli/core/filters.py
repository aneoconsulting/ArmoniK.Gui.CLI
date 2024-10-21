import operator

import jinja2

from datetime import datetime, timedelta
from enum import IntEnum
from functools import partial, reduce
from pathlib import Path
from typing import Any, List, Tuple, Callable, Union

from armonik.common import (
    Filter,
    Partition,
    Result,
    ResultStatus,
    Session,
    SessionStatus,
    Task,
    TaskStatus,
)
from armonik.common.filter import PartitionFilter, ResultFilter, SessionFilter, TaskFilter
from armonik.common.filter.filter import FType, StringFilter
from lark import Lark, Transformer, Token

from armonik_cli.utils import parse_time_delta


class FilterParser:
    """
    A parser for processing and validating filter expressions.

    Attributes:
        obj: The ArmoniK API object associated with the filter to be built.
        filter: The ARmoniK API filter corresponding to the object.
        status_enum: The object status enumeration.
    """

    _grammar_template_path = Path(__file__).parent / "filter_grammar.jinja"

    def __init__(
        self,
        obj: Union[Partition, Result, Session, Task],
        filter: Union[PartitionFilter, ResultFilter, SessionFilter, TaskFilter],
        status_enum: Union[ResultStatus, SessionStatus, TaskStatus],
    ) -> None:
        self.obj = obj
        self.filter = filter
        self.status_enum = status_enum

    def get_fields(self) -> List[Tuple[str, str, str]]:
        """
        Retrieve the filterable fields and their corresponding comparison methods and types.

        Returns:
            A list of tuples containing field name, comparison method, and field type.
        """
        fields = []
        for field_name, filed_spec in self.filter._fields.items():
            field_comp = None
            field_type = None
            if filed_spec[0] == FType.STR:
                field_comp = "string_comp"
                field_type = "STRING"
            elif filed_spec[0] == FType.BOOL:
                field_comp = "bool_comp"
                field_type = "BOOL"
            elif filed_spec[0] == FType.NUM:
                field_comp = "generic_comp"
                field_type = "SIGNED_INT"
            elif filed_spec[0] == FType.DATE:
                field_comp = "generic_comp"
                field_type = "DATETIME"
            elif filed_spec[0] == FType.DURATION:
                field_comp = "generic_comp"
                field_type = "DURATION"
            elif filed_spec[0] == FType.STATUS:
                field_comp = "status_comp"
                field_type = "STATUS"
            else:
                continue
            fields.append((field_name, field_comp, field_type))
        return fields

    def get_status(self) -> List[str]:
        """
        Retrieve the status values for the considered object.

        Returns:
            A list of status names in lowercase.
        """
        return [m.lower() for m in self.status_enum.__members__.keys()]

    def generate_parser(self) -> Lark:
        """
        Generate a Lark parser for the grammar associated with the object's filter.
        The grammar is generated on-the-fly based on the filterable fields, their type
        and authorized comparators.

        Returns:
            A Lark parser instance.
        """
        with self._grammar_template_path.open() as template_file:
            template_content = template_file.read()

        template = jinja2.Template(template_content)
        grammar = template.render(
            fields=self.get_fields(),
            status=self.get_status(),
        )
        return Lark(grammar, start="start", parser="lalr")

    def parse(self, expression: str) -> Filter:
        """
        Parse a filter expression into a Filter object.

        Args:
            expression: The filter expression as a string.

        Returns:
            A Filter object constructed from the parsed expression.
        """
        tree = self.generate_parser().parse(expression)
        filter = FilterTransformer(
            obj=self.obj, filter=self.filter, status_enum=self.status_enum
        ).transform(tree)
        return filter


class FilterTransformer(Transformer):
    """
    A transformer to convert parsed filter expressions into Filter objects.

    Attributes:
        obj: The ArmoniK API object associated with the filter to be built.
        filter: The ARmoniK API filter corresponding to the object.
        status_enum: The object status enumeration.
    """

    def __init__(self, obj, filter, status_enum):
        super().__init__(visit_tokens=True)
        self._obj = obj
        self._filter = filter
        self._status_enum = status_enum

    def expr(self, args):
        """
        Process an OR expression.

        Args:
            args: A list of subfilters.

        Returns:
            The filter resulting of combining subfilters using OR.
        """
        return reduce(operator.or_, args)

    def term(self, args: List[Filter]) -> Filter:
        """
        Process an AND expression.

        Args:
            args: A list of subfilters.

        Returns:
            The filter resulting of combining subfilters using AND.
        """
        return reduce(operator.and_, args)

    def DATETIME(self, tok: Token) -> datetime:
        """
        Convert a DATETIME token into a datetime object.

        Args:
            tok: The DATETIME token.

        Returns:
            A datetime object.
        """
        raise NotImplementedError()

    def DURATION(self, tok: Token) -> timedelta:
        """
        Convert a DURATION token into a timedelta object.

        Args:
            tok: The DURATION token.

        Returns:
            A timedelta object.
        """
        return parse_time_delta(tok.value)

    def BOOL(self, tok: Token) -> bool:
        """
        Convert a BOOL token into a boolean value.

        Args:
            tok: The BOOL token.

        Returns:
            A boolean value.
        """
        return bool(tok.value)

    def STATUS(self, tok: Token) -> IntEnum:
        """
        Convert a STATUS token into an enumeration value.

        Args:
            tok: The STATUS token.

        Returns:
            An enumeration value corresponding to the token.
        """
        return getattr(self._status_enum, tok.value.upper())

    def STRING(self, tok: Token) -> str:
        """
        Convert a STRING token into a string.

        Args:
            tok: The STRING token.

        Returns:
            A string.
        """
        return tok.value

    def SIGNED_INT(self, tok: Token) -> int:
        """
        Convert a SIGNED_INT token into an integer.

        Args:
            tok: The SIGNED_INT token.

        Returns:
            An integer.
        """
        return int(tok.value)

    def EQ(self, tok: Token) -> Callable:
        """Return the equality operator."""
        return operator.eq

    def NEQ(self, tok: Token) -> Callable:
        """Return the inequality operator."""
        return operator.ne

    def LT(self, tok: Token) -> Callable:
        """Return the less-than operator."""
        return operator.lt

    def LTE(self, tok: Token) -> Callable:
        """Return the less-than-or-equal-to operator."""
        return operator.le

    def GT(self, tok: Token) -> Callable:
        """Return the greater-than operator."""
        return operator.gt

    def GTE(self, tok: Token) -> Callable:
        """Return the greater-than-or-equal-to operator."""
        return operator.ge

    def IN(self, tok: Token) -> Callable:
        """Return a function to check containment."""

        def contains(a: StringFilter, b: str) -> bool:
            return a.contains(b)

        return contains

    def NOTIN(self, tok: Token) -> Callable:
        """Return a function to check non-containment."""

        def notcontains(a: StringFilter, b: str) -> bool:
            return - a.contains(b)

        return notcontains

    def STARTSWITH(self, tok: Token) -> Callable:
        """Return a function to check if a string starts with another."""

        def startswith(a: StringFilter, b: str) -> bool:
            return a.startswith(b)

        return startswith

    def ENDSWIDTH(self, tok: Token) -> Callable:
        """Return a function to check if a string ends with another."""

        def endswith(a: StringFilter, b: str) -> bool:
            return a.endswith(b)

        return endswith

    def field(self, args: List[Any], *, name: str) -> Filter:
        """
        Create a filter for a specific field.

        Args:
            args: A list of arguments.
            name: The field name.

        Returns:
            A Filter object.
        """
        return args[0](getattr(self._obj, name), args[1])

    def __getattr__(self, name: str) -> Callable:
        """
        Dynamically create a field filter for a field name.

        Args:
            name: The field name.

        Raises:
            ValueError: If the field does not exist in the filter.

        Returns:
            A partial function to create the field filter.
        """
        if name not in self._filter._fields:
            raise ValueError(f"Filter {self._filter} has no field {name}.")
        return partial(self.field, name=name)
