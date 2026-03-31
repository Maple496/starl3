"""
StarL3 安全表达式评估模块
"""

import ast
import operator
from typing import Any, Dict, Optional


class SafeEvalError(Exception):
    """安全评估错误"""
    pass


class SafeEvaluator:
    """安全表达式评估器"""
    
    _ast_cache = {}
    _cache_max_size = 128
    
    ALLOWED_OPERATORS = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.FloorDiv: operator.floordiv,
        ast.Mod: operator.mod,
        ast.Pow: operator.pow,
        ast.USub: operator.neg,
        ast.UAdd: operator.pos,
        ast.Eq: operator.eq,
        ast.NotEq: operator.ne,
        ast.Lt: operator.lt,
        ast.LtE: operator.le,
        ast.Gt: operator.gt,
        ast.GtE: operator.ge,
        ast.Not: operator.not_,
        ast.In: lambda x, y: x in y,
        ast.NotIn: lambda x, y: x not in y,
        ast.Is: operator.is_,
        ast.IsNot: operator.is_not,
    }
    
    ALLOWED_FUNCTIONS = {
        'int': int, 'float': float, 'str': str, 'bool': bool,
        'list': list, 'tuple': tuple, 'dict': dict, 'set': set,
        'abs': abs, 'round': round, 'min': min, 'max': max,
        'sum': sum, 'len': len, 'ord': ord, 'chr': chr,
        'range': range, 'enumerate': enumerate, 'zip': zip,
        'map': map, 'filter': filter, 'sorted': sorted,
    }
    
    def __init__(self, extra_vars: Optional[Dict] = None):
        self.extra_vars = extra_vars or {}
        self.builtins = self.ALLOWED_FUNCTIONS.copy()
    
    def _get_cached_ast(self, expression: str):
        """获取缓存的 AST"""
        if expression in SafeEvaluator._ast_cache:
            return SafeEvaluator._ast_cache[expression]
        
        try:
            tree = ast.parse(expression, mode='eval')
        except SyntaxError as e:
            raise SafeEvalError(f"语法错误: {e}")
        
        if len(SafeEvaluator._ast_cache) >= SafeEvaluator._cache_max_size:
            keys = list(SafeEvaluator._ast_cache.keys())[:64]
            for k in keys:
                del SafeEvaluator._ast_cache[k]
        
        SafeEvaluator._ast_cache[expression] = tree
        return tree
    
    def eval(self, expression: str, variables: Optional[Dict] = None) -> Any:
        """安全地评估表达式"""
        if not isinstance(expression, str):
            raise SafeEvalError(f"表达式必须是字符串")
        
        expression = expression.strip()
        if not expression:
            raise SafeEvalError("表达式不能为空")
        
        tree = self._get_cached_ast(expression)
        return self._eval_node(tree.body, variables or {})
    
    def _eval_node(self, node: ast.AST, variables: Dict) -> Any:
        """递归评估 AST 节点"""
        
        if isinstance(node, ast.Constant):
            return node.value
        
        if isinstance(node, ast.Name):
            if node.id in variables:
                return variables[node.id]
            if node.id in self.builtins:
                return self.builtins[node.id]
            raise SafeEvalError(f"未定义的变量: '{node.id}'")
        
        if isinstance(node, ast.BinOp):
            left = self._eval_node(node.left, variables)
            right = self._eval_node(node.right, variables)
            op_type = type(node.op)
            if op_type not in self.ALLOWED_OPERATORS:
                raise SafeEvalError(f"不允许的操作符: {op_type.__name__}")
            return self.ALLOWED_OPERATORS[op_type](left, right)
        
        if isinstance(node, ast.UnaryOp):
            operand = self._eval_node(node.operand, variables)
            op_type = type(node.op)
            if op_type not in self.ALLOWED_OPERATORS:
                raise SafeEvalError(f"不允许的操作符: {op_type.__name__}")
            return self.ALLOWED_OPERATORS[op_type](operand)
        
        if isinstance(node, ast.Compare):
            left = self._eval_node(node.left, variables)
            result = None
            for op, comparator in zip(node.ops, node.comparators):
                right = self._eval_node(comparator, variables)
                op_type = type(op)
                if op_type not in self.ALLOWED_OPERATORS:
                    raise SafeEvalError(f"不允许的比较操作符: {op_type.__name__}")
                cmp_result = self.ALLOWED_OPERATORS[op_type](left, right)
                result = cmp_result if result is None else (result & cmp_result)
                left = right
            return result
        
        if isinstance(node, ast.BoolOp):
            values = [self._eval_node(v, variables) for v in node.values]
            if isinstance(node.op, ast.And):
                return all(values)
            elif isinstance(node.op, ast.Or):
                return any(values)
            raise SafeEvalError(f"不允许的布尔操作符")
        
        if isinstance(node, ast.Call):
            func = self._eval_node(node.func, variables)
            args = [self._eval_node(arg, variables) for arg in node.args]
            kwargs = {kw.arg: self._eval_node(kw.value, variables) for kw in node.keywords}
            if func not in self.builtins.values() and not callable(func):
                raise SafeEvalError(f"不允许的函数调用")
            return func(*args, **kwargs)
        
        if isinstance(node, ast.Attribute):
            obj = self._eval_node(node.value, variables)
            attr_name = node.attr
            if hasattr(obj, attr_name):
                return getattr(obj, attr_name)
            raise SafeEvalError(f"'{type(obj).__name__}' 没有属性 '{attr_name}'")
        
        if isinstance(node, ast.List):
            return [self._eval_node(e, variables) for e in node.elts]
        if isinstance(node, ast.Tuple):
            return tuple(self._eval_node(e, variables) for e in node.elts)
        if isinstance(node, ast.Dict):
            return {self._eval_node(k, variables): self._eval_node(v, variables) 
                    for k, v in zip(node.keys, node.values)}
        
        if isinstance(node, ast.IfExp):
            return self._eval_node(node.body, variables) if self._eval_node(node.test, variables) else self._eval_node(node.orelse, variables)
        
        if isinstance(node, ast.Subscript):
            obj = self._eval_node(node.value, variables)
            slice_val = self._eval_node(node.slice, variables)
            return obj[slice_val]
        
        if isinstance(node, ast.Slice):
            lower = self._eval_node(node.lower, variables) if node.lower else None
            upper = self._eval_node(node.upper, variables) if node.upper else None
            step = self._eval_node(node.step, variables) if node.step else None
            return slice(lower, upper, step)
        
        raise SafeEvalError(f"不支持的表达式类型: {type(node).__name__}")


def safe_eval(expression: str, variables: Optional[Dict] = None, extra_vars: Optional[Dict] = None) -> Any:
    """安全地评估表达式"""
    evaluator = SafeEvaluator(extra_vars=extra_vars)
    return evaluator.eval(expression, variables)


def eval_condition(expression: str, context: Dict, allowed_vars: Optional[set] = None) -> bool:
    """评估条件表达式"""
    try:
        if allowed_vars is not None:
            filtered_context = {k: v for k, v in context.items() if k in allowed_vars or not k.startswith('_')}
        else:
            filtered_context = {k: v for k, v in context.items() if not k.startswith('_')}
        
        result = safe_eval(expression, filtered_context)
        return bool(result)
    except Exception as e:
        raise SafeEvalError(f"条件表达式评估失败 '{expression}': {e}")


def eval_dataframe_expression(df, expression: str, extra_funcs: Optional[Dict] = None) -> Any:
    """评估 DataFrame 相关的表达式"""
    import pandas as pd
    import numpy as np
    
    variables = {c: df[c] for c in df.columns}
    variables['df'] = df
    variables['pd'] = pd
    variables['np'] = np
    
    extra = {'str': str, 'int': int, 'float': float, 'len': len, 
             'abs': abs, 'round': round, 'min': min, 'max': max}
    if extra_funcs:
        extra.update(extra_funcs)
    
    return safe_eval(expression, variables, extra)
