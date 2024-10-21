import ast

import armonik.common as ak_obj

from armonik.common import Filter


class FilterTransformer(ast.NodeTransformer):
    def __init__(self, api_obj_type: str):
        self._api_obj_type = api_obj_type
        self._const_types = {}

    def generic_visit(self, node):
        super().generic_visit(node)
        match type(node):
            case ast.Module:
                assert len(node.body) == 1
                assert isinstance(node.body[0], ast.Expr) or isinstance(node.body[0], ast.BinOp)
                return node
            case ast.Expr:
                assert isinstance(node.value, ast.Compare)
                return node
            case ast.Compare:
                assert isinstance(node.left, ast.Name)
                assert any([isinstance(node.ops[0], op_type) for op_type in [ast.Eq, ast.Lt, ast.LtE, ast.Gt, ast.GtE]])
                assert len(node.ops) == 1
                assert isinstance(node.comparators[0], ast.Constant)

                obj = getattr(ak_obj, self._api_obj_type)
                assert node.left.id in [m for m in obj.__dict__.keys() if isinstance(getattr(obj, m), Filter)]
                self._const_types[node.comparators[0].value] = getattr(obj, node.left.id)
                return node
            case ast.Name:
                return ast.Attribute(
                    value=ast.Name(id=self._api_obj_type, ctx=ast.Load()),
                    attr=node.id,
                    ctx=ast.Load()
                )
            case ast.Constant:
                from armonik.common.filter import StringFilter, StatusFilter, DateFilter, DurationFilter, NumberFilter, BooleanFilter, ArrayFilter
                filter_type = self._const_types[node.value]
            case ast.Load:
                return node
            case _:
                raise SyntaxError(f"Invalid node {node}".)
        return 


def from_str_to_filter(filter_str: str) -> Filter:
    tree = ast.parse(filter_str)
    tree = FilterTransformer("Session").visit(tree)
    global_ctx = {}
    local_ctx = {}
    exec(f"filter={compile(tree, mode="exec")}", global_ctx, local_ctx)
    return local_ctx["filter"]


assert from_str_to_filter("session_id == 'id'") == (Session.session_id == "id")
