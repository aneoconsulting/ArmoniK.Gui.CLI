import ast
import operator

import armonik.common
import pytest
import rich_click as click

from datetime import datetime
from functools import reduce

from armonik.common import Session, Task
import armonik.common.filter as filters


DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class FilterParam(click.ParamType):
    name = "filter"

    def __init__(self, filter_wrapper) -> None:
        self._filter_wrapper = filter_wrapper

    def convert(self, value: str, param, ctx) -> filters.Filter:
        try:
            if value:
                value = value.replace("==", "=").replace("=", "==")
                return _build_filter(ast.parse(value), obj_type=self._filter_wrapper)
            return None
        except SyntaxError as error:
            self.fail(
                f"{value} is not a valid filter. Syntax error: {str(error)}.",
                param,
                ctx,
            )


def _build_filter(node, obj_type=None, target_type=None):
    try:
        match type(node):
            case ast.Module:
                if not len(node.body) == 1:
                    raise SyntaxError("Filter definition must be a single expression.")
                return _build_filter(node.body[0], obj_type)
            case ast.Expr:
                return _build_filter(node.value, obj_type)
            case ast.BoolOp:
                match type(node.op):
                    case ast.And:
                        return reduce(
                            operator.and_, [_build_filter(val, obj_type) for val in node.values]
                        )
                    case ast.Or:
                        return reduce(
                            operator.or_, [_build_filter(val, obj_type) for val in node.values]
                        )
                    case _:
                        raise SyntaxError("Invalid boolean operator.")
            case ast.Compare:
                if (
                    len(node.comparators) != 1
                    or not (isinstance(node.left, ast.Name) or isinstance(node.left, ast.Attribute))
                    or not isinstance(node.comparators[0], ast.Constant)
                ):
                    raise SyntaxError()
                filter_descriptor = _build_filter(node.left, obj_type)
                return _build_filter(node.ops[0])(
                    filter_descriptor,
                    _build_filter(node.comparators[0], obj_type, type(filter_descriptor)),
                )
            case ast.Name:
                if (
                    node.id in obj_type._fields.keys()
                    and obj_type._fields[node.id][0] != filters.filter.FType.NA
                    or filters.filter.FType.UNKNOWN
                ):
                    return getattr(obj_type, node.id)
                raise SyntaxError()
            case ast.Constant:
                match target_type:
                    case filters.StringFilter:
                        return str(node.value)
                    case filters.StatusFilter:
                        try:
                            return getattr(
                                getattr(armonik.common, f"{obj_type}Status"),
                                str(node.value).upper(),
                            )
                        except AttributeError:
                            raise SyntaxError()
                    case filters.DateFilter:
                        try:
                            return datetime.strptime(str(node.value), DATETIME_FORMAT)
                        except ValueError:
                            raise SyntaxError()
                    case filters.DurationFilter:
                        raise NotImplementedError()
                    case filters.NumberFilter:
                        if isinstance(node.value, int):
                            return node.value
                        raise SyntaxError()
                    case filters.BooleanFilter:
                        match str(node.value).capitalize():
                            case "True":
                                return True
                            case "False":
                                return False
                            case _:
                                raise SyntaxError()
                    case filters.ArrayFilter:
                        if isinstance(node.value, list):
                            return node.value
                        raise SyntaxError()
            case ast.Attribute:
                match obj_type:
                    case "Session":
                        return Session.options[node.attr]
                    case "Task":
                        return Task.options[node.attr]
                raise SyntaxError()
            case ast.Eq:
                return operator.eq
            case ast.NotEq:
                return operator.ne
            case ast.Lt:
                return operator.lt
            case ast.LtE:
                return operator.le
            case ast.Gt:
                return operator.gt
            case ast.GtE:
                return operator.ge
            case ast.In:
                return operator.contains
            case ast.NotIn:
                return lambda a, b: not operator.contains(a, b)
            case _:
                raise SyntaxError("Invalid")
    except InterruptedError:  # filters.FilterError:
        raise SyntaxError()


# TODO : test array, int, duration filter types
@pytest.mark.parametrize(
    ("obj_type", "filter_str", "filter"),
    [
        (filters.TaskFilter, "parent_task_ids == 'id'", Task.parent_task_ids == "id"),
        # ("Session", "session_id == 'id'", Session.session_id == "id"),
        # ("Session", "status == 'Running'", Session.status == SessionStatus.RUNNING),
        # ("Session", "created_at > '2024-10-26 18:49:47'", Session.created_at > datetime(year=2024, month=10, day=26, hour=18, minute=49, second=47)),
        # ("Session", "client_submission == 'true'", Session.client_submission == True),
        # ("Session", "options.key == 'value'", Session.options["key"] == "value"),
        # ("Session", "(session_id == 'id') and (status == 'Cancelled')", (Session.session_id == "id") & (Session.status == SessionStatus.CANCELLED)),
        # ("Session", "(session_id == 'id') and (status == 'Cancelled') and (options.key == 'value')", (Session.session_id == "id") & (Session.status == SessionStatus.CANCELLED) & (Session.options["key"] == "value")),
        # ("Session", "(session_id == 'id') and ((status == 'Cancelled') or (options.key == 'value'))", (Session.session_id == "id") & ((Session.status == SessionStatus.CANCELLED) | (Session.options["key"] == "value"))),
    ],
)
def test_build_filter(obj_type, filter_str, filter):
    assert _build_filter(ast.parse(filter_str), obj_type=obj_type).to_dict() == filter.to_dict()
