"""Restricted expression evaluator — replaces eval() for user-supplied expressions.

Only allows safe AST nodes: comparisons, boolean ops, arithmetic, string
methods, attribute access, and literal values.  Blocks imports, function
calls (except whitelisted), __dunder__ attributes, exec/eval/compile.
"""

from __future__ import annotations

import ast
import operator
from typing import Any

# Whitelisted string methods callable on values
_SAFE_STR_METHODS = frozenset({
    "lower", "upper", "strip", "startswith", "endswith",
    "lstrip", "rstrip", "title", "replace", "split",
})

# Whitelisted free-standing functions
_SAFE_BUILTINS: dict[str, Any] = {
    "len": len,
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "abs": abs,
    "min": min,
    "max": max,
    "round": round,
    "True": True,
    "False": False,
    "None": None,
}

_CMP_OPS: dict[type, Any] = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.Is: operator.is_,
    ast.IsNot: operator.is_not,
    ast.In: lambda a, b: a in b,
    ast.NotIn: lambda a, b: a not in b,
}

_BIN_OPS: dict[type, Any] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
}

_UNARY_OPS: dict[type, Any] = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
    ast.Not: operator.not_,
}

_BOOL_OPS: dict[type, str] = {
    ast.And: "and",
    ast.Or: "or",
}


class _SafeEval(ast.NodeVisitor):
    """Walk an AST and evaluate it with a restricted set of operations."""

    def __init__(self, variables: dict[str, Any]) -> None:
        self.variables = variables

    # ------------------------------------------------------------------
    # Literals
    # ------------------------------------------------------------------

    def visit_Constant(self, node: ast.Constant) -> Any:
        if isinstance(node.value, (int, float, str, bool, type(None))):
            return node.value
        raise ValueError(f"Unsupported constant type: {type(node.value).__name__}")

    # ------------------------------------------------------------------
    # Variables & attribute access
    # ------------------------------------------------------------------

    def visit_Name(self, node: ast.Name) -> Any:
        name = node.id
        if name in _SAFE_BUILTINS:
            return _SAFE_BUILTINS[name]
        if name in self.variables:
            return self.variables[name]
        raise NameError(f"Undefined name: {name!r}")

    def visit_Attribute(self, node: ast.Attribute) -> Any:
        if node.attr.startswith("__"):
            raise ValueError(f"Access to dunder attribute '{node.attr}' is not allowed")
        value = self.visit(node.value)
        return getattr(value, node.attr)

    def visit_Subscript(self, node: ast.Subscript) -> Any:
        value = self.visit(node.value)
        key = self.visit(node.slice)
        return value[key]

    # ------------------------------------------------------------------
    # Operators
    # ------------------------------------------------------------------

    def visit_BinOp(self, node: ast.BinOp) -> Any:
        op_func = _BIN_OPS.get(type(node.op))
        if op_func is None:
            raise ValueError(f"Unsupported binary operator: {type(node.op).__name__}")
        return op_func(self.visit(node.left), self.visit(node.right))

    def visit_UnaryOp(self, node: ast.UnaryOp) -> Any:
        op_func = _UNARY_OPS.get(type(node.op))
        if op_func is None:
            raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")
        return op_func(self.visit(node.operand))

    def visit_BoolOp(self, node: ast.BoolOp) -> Any:
        op_kind = _BOOL_OPS.get(type(node.op))
        if op_kind is None:
            raise ValueError(f"Unsupported boolean operator: {type(node.op).__name__}")
        if op_kind == "and":
            result: Any = True
            for val_node in node.values:
                result = self.visit(val_node)
                if not result:
                    return result
            return result
        else:  # or
            result = False
            for val_node in node.values:
                result = self.visit(val_node)
                if result:
                    return result
            return result

    def visit_Compare(self, node: ast.Compare) -> Any:
        left = self.visit(node.left)
        for op, comparator in zip(node.ops, node.comparators):
            op_func = _CMP_OPS.get(type(op))
            if op_func is None:
                raise ValueError(f"Unsupported comparison: {type(op).__name__}")
            right = self.visit(comparator)
            if not op_func(left, right):
                return False
            left = right
        return True

    # ------------------------------------------------------------------
    # Function calls — only whitelisted builtins and string methods
    # ------------------------------------------------------------------

    def visit_Call(self, node: ast.Call) -> Any:
        # Method call: value.method(args)
        if isinstance(node.func, ast.Attribute):
            if node.func.attr.startswith("__"):
                raise ValueError(
                    f"Call to dunder method '{node.func.attr}' is not allowed"
                )
            obj = self.visit(node.func.value)
            method_name = node.func.attr
            if isinstance(obj, str) and method_name in _SAFE_STR_METHODS:
                args = [self.visit(a) for a in node.args]
                return getattr(obj, method_name)(*args)
            raise ValueError(
                f"Method call '.{method_name}()' is not allowed on {type(obj).__name__}"
            )

        # Free-standing call: func(args)
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
            if func_name in ("eval", "exec", "compile", "type",
                             "__import__", "globals", "locals",
                             "getattr", "setattr", "delattr",
                             "open", "input", "breakpoint"):
                raise ValueError(f"Call to '{func_name}' is not allowed")
            func = _SAFE_BUILTINS.get(func_name)
            if func is not None and callable(func):
                args = [self.visit(a) for a in node.args]
                return func(*args)
            raise ValueError(f"Call to '{func_name}' is not allowed")

        raise ValueError("Unsupported call expression")

    # ------------------------------------------------------------------
    # Containers
    # ------------------------------------------------------------------

    def visit_List(self, node: ast.List) -> list[Any]:
        return [self.visit(el) for el in node.elts]

    def visit_Tuple(self, node: ast.Tuple) -> tuple[Any, ...]:
        return tuple(self.visit(el) for el in node.elts)

    def visit_Dict(self, node: ast.Dict) -> dict[Any, Any]:
        return {
            self.visit(k): self.visit(v)
            for k, v in zip(node.keys, node.values)
            if k is not None
        }

    # ------------------------------------------------------------------
    # Expression wrapper
    # ------------------------------------------------------------------

    def visit_Expression(self, node: ast.Expression) -> Any:
        return self.visit(node.body)

    # ------------------------------------------------------------------
    # Conditional expression (ternary)
    # ------------------------------------------------------------------

    def visit_IfExp(self, node: ast.IfExp) -> Any:
        return self.visit(node.body) if self.visit(node.test) else self.visit(node.orelse)

    # ------------------------------------------------------------------
    # JoinedStr (f-string)
    # ------------------------------------------------------------------

    def visit_JoinedStr(self, node: ast.JoinedStr) -> str:
        parts = []
        for value in node.values:
            parts.append(str(self.visit(value)))
        return "".join(parts)

    def visit_FormattedValue(self, node: ast.FormattedValue) -> Any:
        return self.visit(node.value)

    # ------------------------------------------------------------------
    # Catch-all
    # ------------------------------------------------------------------

    def generic_visit(self, node: ast.AST) -> None:
        raise ValueError(
            f"Unsupported expression node: {type(node).__name__}"
        )


def safe_eval(expression: str, variables: dict[str, Any]) -> Any:
    """Evaluate *expression* in a restricted environment.

    Only arithmetic, comparisons, boolean ops, string methods, and
    whitelisted builtins (len, str, int, float, abs, min, max, round)
    are permitted.  Raises ``ValueError`` on disallowed constructs.
    """
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as e:
        raise ValueError(f"Invalid expression syntax: {e}") from e

    evaluator = _SafeEval(variables)
    return evaluator.visit(tree)
