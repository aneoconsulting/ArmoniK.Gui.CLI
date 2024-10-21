import operator

import pytest

from functools import reduce

from armonik.common import Session
from armonik.common.filter import SessionFilter, Filter
from armonik.common.filter.filter import FType
from lark import Lark, Transformer

SessionFilter._fields


grammar = """
?start: expr

expr: term ("or" term)*

term: factor ("and" factor)*

factor: "(" expr ")"
       | "session_id" "=" STRING -> session_id
       | "status" "=" STATUS
       | "client_submission" "=" BOOL
       | "worker_submission" "=" BOOL
       | "partition_ids" "=" array
       | "options" "." STRING "=" STRING
       | "created_at" "=" DATETIME
       | "cancelled_at" "=" DATETIME
       | "closed_at" "=" DATETIME
       | "purged_at" "=" DATETIME
       | "deleted_at" "=" DATETIME
       | "duration" "=" DURATION

array: "[" [STRING ("," STRING)*] "]"
     // Comma-separated list of values enclosed in square brackets       


DATETIME: /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})/ 
        // ISO 8601 format (e.g., 2024-12-28T14:00:00Z)

DURATION: /P(?:\d+Y)?(?:\d+M)?(?:\d+W)?(?:\d+D)?(?:T(?:\d+H)?(?:\d+M)?(?:\d+S)?)?/ 
        // ISO 8601 duration (e.g., P1Y2M3DT4H5M6S)

BOOL: "true" | "false"

STRING: /[^\s]+/
       // Matches any string without spaces


STATUS: "RUNNING" | "COMPLETED"

%ignore " "
"""


generic_grammar = """
?start: expr

expr: term ("or" term)*

term: factor ("and" factor)*

factor: "(" expr ")"
"""


def parser_generator(filter: Filter) -> Lark:
    grammar = generic_grammar
    for field_name, (field_type, _) in filter._fields.itesm():
        grammar += f'\n\t"{field_name}" {operators[field_type]} \n'
    grammar += '\n%ignore " "'
    return Lark(grammar, start="start", parser="lalr")


class SessionFilterTransformer(Transformer):
    def __init__(self, obj, filter):
        super().__init__(visit_tokens=False)
        self._obj = obj
        self._filter = filter

    def expr(self, args):
        return reduce(operator.or_, args)

    def term(self, args):
        return reduce(operator.and_, args)

    def __getattr__(self, name):
        if name in getattr(self._filter, "_fields"):
            match SessionFilter._fields[name][0]:
                case FType.NUM:

                    def handler(self, args):
                        return getattr(self._obj, name) == int(args[0].value)

                    return handler
                case FType.STR:

                    def handler(self, args):
                        return getattr(self._obj, name) == args[0].value

                    return handler
                case FType.ARRAY:
                    raise NotImplementedError()
                case FType.DURATION:
                    raise NotImplementedError()
                case FType.DATE:
                    raise NotImplementedError()
                case FType.STATUS:
                    raise NotImplementedError()
                case FType.BOOL:

                    def handler(self, args):
                        return getattr(self._obj, name) == bool(args[0].value)

                    return handler
                case _:
                    raise ValueError()
        raise AttributeError()


def parse_expression(expression):
    parse_tree = parser.parse(expression)
    transformed_tree = SessionFilterTransformer().transform(parse_tree)
    return transformed_tree


@pytest.mark.parametrize(
    ("obj_type", "expr", "filter"),
    [
        (Session, "session_id = id", Session.session_id == "id"),
        # (Session, "status = 'Running'", Session.status == SessionStatus.RUNNING),
        # (Session, "created_at > '2024-10-26 18:49:47'", Session.created_at > datetime(year=2024, month=10, day=26, hour=18, minute=49, second=47)),
        # (Session, "client_submission = 'true'", Session.client_submission == True),
        # (Session, "options.key = 'value'", Session.options["key"] == "value"),
        # (Session, "(session_id = 'id') and (status = 'Cancelled')", (Session.session_id == "id") & (Session.status == SessionStatus.CANCELLED)),
        # (Session, "(session_id = 'id') and (status = 'Cancelled') and (options.key = 'value')", (Session.session_id == "id") & (Session.status == SessionStatus.CANCELLED) & (Session.options["key"] == "value")),
        # (Session, "(session_id = 'id') and ((status = 'Cancelled') or (options.key = 'value'))", (Session.session_id == "id") & ((Session.status == SessionStatus.CANCELLED) | (Session.options["key"] == "value"))),
    ],
)
def test_parse_expression(obj_type, expr, filter):
    result = parse_expression(expr)
    assert result.to_dict() == filter.to_dict()
