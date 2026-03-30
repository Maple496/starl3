"""
StarL3 安全表达式评估模块
用于替代裸 eval，提供受限的表达式执行环境
"""

import ast
import operator
import re
from typing import Any, Dict, Optional, Set, Union


class SafeEvalError(Exception):
    """安全评估错误"""
    pass


class UnsafeExpressionError(SafeEvalError):
    """不安全的表达式错误"""
    pass


class ExpressionSyntaxError(SafeEvalError):
    """表达式语法错误"""
    pass


class SafeEvaluator:
    """安全表达式评估器
    
    支持的操作：
    - 算术运算: +, -, *, /, //, %, **
    - 比较运算: ==, !=, <, <=, >, >=
    - 逻辑运算: and, or, not
    - 成员运算: in, not in
    - 调用受限的函数（白名单）
    - 访问变量（通过 vars 传入）
    """
    
    # 类级别的 AST 缓存（LRU风格）
    _ast_cache = {}
    _cache_max_size = 128
    
    # 允许的操作符
    ALLOWED_OPERATORS = {
        # 算术
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.FloorDiv: operator.floordiv,
        ast.Mod: operator.mod,
        ast.Pow: operator.pow,
        ast.USub: operator.neg,
        ast.UAdd: operator.pos,
        
        # 比较
        ast.Eq: operator.eq,
        ast.NotEq: operator.ne,
        ast.Lt: operator.lt,
        ast.LtE: operator.le,
        ast.Gt: operator.gt,
        ast.GtE: operator.ge,
        
        # 逻辑
        ast.Not: operator.not_,
        
        # 包含
        ast.In: lambda x, y: x in y,
        ast.NotIn: lambda x, y: x not in y,
        ast.Is: operator.is_,
        ast.IsNot: operator.is_not,
    }
    
    # 允许的内置函数
    ALLOWED_FUNCTIONS = {
        # 类型转换
        'int': int,
        'float': float,
        'str': str,
        'bool': bool,
        'list': list,
        'tuple': tuple,
        'dict': dict,
        'set': set,
        
        # 数学函数
        'abs': abs,
        'round': round,
        'min': min,
        'max': max,
        'sum': sum,
        'len': len,
        
        # 字符串方法（通过属性访问）
        'ord': ord,
        'chr': chr,
        'repr': repr,
        
        # 其他
        'range': range,
        'enumerate': enumerate,
        'zip': zip,
        'map': map,
        'filter': filter,
        'sorted': sorted,
        'reversed': reversed,
    }
    
    # 允许的属性访问（白名单）
    ALLOWED_ATTRIBUTES = {
        # 字符串方法
        'str': {'lower', 'upper', 'strip', 'lstrip', 'rstrip', 'split', 'join', 
                'replace', 'startswith', 'endswith', 'contains', 'find', 'count',
                'isdigit', 'isalpha', 'isalnum', 'isspace', 'format'},
        'list': {'append', 'extend', 'insert', 'remove', 'pop', 'clear', 
                 'index', 'count', 'sort', 'reverse', 'copy'},
        'dict': {'get', 'keys', 'values', 'items', 'pop', 'popitem', 'update',
                 'clear', 'setdefault'},
    }
    
    def __init__(self, extra_vars: Optional[Dict[str, Any]] = None):
        self.extra_vars = extra_vars or {}
        self.builtins = self.ALLOWED_FUNCTIONS.copy()
    
    def _get_cached_ast(self, expression: str):
        """获取缓存的 AST，如果不存在则解析并缓存"""
        if expression in SafeEvaluator._ast_cache:
            return SafeEvaluator._ast_cache[expression]
        
        # 解析 AST
        try:
            tree = ast.parse(expression, mode='eval')
        except SyntaxError as e:
            raise ExpressionSyntaxError(f"语法错误: {e}")
        
        # 缓存管理：如果超过最大大小，清空一半缓存
        if len(SafeEvaluator._ast_cache) >= SafeEvaluator._cache_max_size:
            # 简单的 LRU：保留最近的 64 个
            keys_to_remove = list(SafeEvaluator._ast_cache.keys())[:64]
            for key in keys_to_remove:
                del SafeEvaluator._ast_cache[key]
        
        SafeEvaluator._ast_cache[expression] = tree
        return tree
    
    def eval(self, expression: str, variables: Optional[Dict[str, Any]] = None) -> Any:
        """安全地评估表达式"""
        if not isinstance(expression, str):
            raise ExpressionSyntaxError(f"表达式必须是字符串，得到 {type(expression)}")
        
        expression = expression.strip()
        if not expression:
            raise ExpressionSyntaxError("表达式不能为空")
        
        # 使用缓存的 AST
        tree = self._get_cached_ast(expression)
        
        # 合并变量（只使用传入的变量，不合并 extra_vars 以避免污染）
        vars_dict = {**(variables or {})}
        
        # 评估 AST
        return self._eval_node(tree.body, vars_dict)
    
    def _eval_node(self, node: ast.AST, variables: Dict[str, Any]) -> Any:
        """递归评估 AST 节点"""
        
        # 字面量 (Python 3.8+ 使用 Constant 节点)
        if isinstance(node, ast.Constant):
            return node.value
        
        # 变量名
        if isinstance(node, ast.Name):
            if node.id in variables:
                return variables[node.id]
            if node.id in self.builtins:
                return self.builtins[node.id]
            raise SafeEvalError(f"未定义的变量或函数: '{node.id}'")
        
        # 二元运算
        if isinstance(node, ast.BinOp):
            left = self._eval_node(node.left, variables)
            right = self._eval_node(node.right, variables)
            op_type = type(node.op)
            
            if op_type not in self.ALLOWED_OPERATORS:
                raise UnsafeExpressionError(f"不允许的二元操作符: {op_type.__name__}")
            
            return self.ALLOWED_OPERATORS[op_type](left, right)
        
        # 一元运算
        if isinstance(node, ast.UnaryOp):
            operand = self._eval_node(node.operand, variables)
            op_type = type(node.op)
            
            if op_type not in self.ALLOWED_OPERATORS:
                raise UnsafeExpressionError(f"不允许的一元操作符: {op_type.__name__}")
            
            return self.ALLOWED_OPERATORS[op_type](operand)
        
        # 比较运算
        if isinstance(node, ast.Compare):
            left = self._eval_node(node.left, variables)
            
            result = None
            for op, comparator in zip(node.ops, node.comparators):
                right = self._eval_node(comparator, variables)
                op_type = type(op)
                
                if op_type not in self.ALLOWED_OPERATORS:
                    raise UnsafeExpressionError(f"不允许的比较操作符: {op_type.__name__}")
                
                cmp_result = self.ALLOWED_OPERATORS[op_type](left, right)
                
                # 处理 Pandas Series/DataFrame 比较结果
                if result is None:
                    result = cmp_result
                else:
                    # 对于多个比较条件，使用逻辑与
                    result = result & cmp_result
                
                left = right
            
            return result
        
        # 布尔运算 (and, or)
        if isinstance(node, ast.BoolOp):
            values = [self._eval_node(v, variables) for v in node.values]
            
            if isinstance(node.op, ast.And):
                return all(values)
            elif isinstance(node.op, ast.Or):
                return any(values)
            else:
                raise UnsafeExpressionError(f"不允许的布尔操作符: {type(node.op).__name__}")
        
        # 一元非运算
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            return not self._eval_node(node.operand, variables)
        
        # 函数调用
        if isinstance(node, ast.Call):
            func = self._eval_node(node.func, variables)
            
            # 评估参数
            args = [self._eval_node(arg, variables) for arg in node.args]
            kwargs = {
                kw.arg: self._eval_node(kw.value, variables)
                for kw in node.keywords
            }
            
            # 检查是否是允许的函数
            if func not in self.builtins.values() and not callable(func):
                raise UnsafeExpressionError(f"不允许的函数调用: {node.func}")
            
            return func(*args, **kwargs)
        
        # 属性访问
        if isinstance(node, ast.Attribute):
            obj = self._eval_node(node.value, variables)
            attr_name = node.attr
            
            # 检查是否是允许的属性
            type_name = type(obj).__name__
            allowed = self.ALLOWED_ATTRIBUTES.get(type_name, set())
            
            # 特殊处理：DataFrame, Series 等常用类型
            if hasattr(obj, attr_name):
                attr = getattr(obj, attr_name)
                
                # 如果是方法，返回绑定方法；如果是属性，返回值
                return attr
            
            raise SafeEvalError(f"'{type_name}' 对象没有属性 '{attr_name}'")
        
        # 列表/元组/集合字面量
        if isinstance(node, ast.List):
            return [self._eval_node(e, variables) for e in node.elts]
        if isinstance(node, ast.Tuple):
            return tuple(self._eval_node(e, variables) for e in node.elts)
        if isinstance(node, ast.Set):
            return {self._eval_node(e, variables) for e in node.elts}
        if isinstance(node, ast.Dict):
            return {
                self._eval_node(k, variables): self._eval_node(v, variables)
                for k, v in zip(node.keys, node.values)
            }
        
        # 条件表达式 (a if b else c)
        if isinstance(node, ast.IfExp):
            test = self._eval_node(node.test, variables)
            if test:
                return self._eval_node(node.body, variables)
            else:
                return self._eval_node(node.orelse, variables)
        
        # 下标访问 (obj[index])
        if isinstance(node, ast.Subscript):
            obj = self._eval_node(node.value, variables)
            slice_val = self._eval_node(node.slice, variables)
            return obj[slice_val]
        
        # 索引（用于下标）
        if isinstance(node, ast.Index):
            return self._eval_node(node.value, variables)
        
        # 切片
        if isinstance(node, ast.Slice):
            lower = self._eval_node(node.lower, variables) if node.lower else None
            upper = self._eval_node(node.upper, variables) if node.upper else None
            step = self._eval_node(node.step, variables) if node.step else None
            return slice(lower, upper, step)
        
        # 列表/字典/集合推导式（限制使用）
        if isinstance(node, (ast.ListComp, ast.DictComp, ast.SetComp)):
            raise UnsafeExpressionError("推导式暂不支持，请使用标准函数")
        
        # 生成器表达式
        if isinstance(node, ast.GeneratorExp):
            raise UnsafeExpressionError("生成器表达式不被允许")
        
        # Lambda
        if isinstance(node, ast.Lambda):
            raise UnsafeExpressionError("Lambda 表达式不被允许")
        
        # 不允许的节点类型
        raise UnsafeExpressionError(
            f"不支持的表达式类型: {type(node).__name__}"
        )


# 便捷函数
def safe_eval(
    expression: str,
    variables: Optional[Dict[str, Any]] = None,
    extra_vars: Optional[Dict[str, Any]] = None
) -> Any:
    """安全地评估表达式
    
    Args:
        expression: 要评估的表达式字符串
        variables: 表达式中可用的变量
        extra_vars: 额外的全局变量
    
    Returns:
        表达式的计算结果
    
    Raises:
        SafeEvalError: 评估错误
        UnsafeExpressionError: 表达式包含不安全操作
        ExpressionSyntaxError: 表达式语法错误
    """
    evaluator = SafeEvaluator(extra_vars=extra_vars)
    return evaluator.eval(expression, variables)


def validate_expression(expression: str) -> bool:
    """验证表达式是否安全（不执行）
    
    Returns:
        True 如果表达式安全，False 否则
    """
    try:
        tree = ast.parse(expression, mode='eval')
        # 基础 AST 检查
        for node in ast.walk(tree):
            if isinstance(node, (
                ast.Import, ast.ImportFrom,  # 导入
                ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef,  # 定义
                ast.While, ast.For, ast.With, ast.Try,  # 控制流
                ast.Raise, ast.Assert, ast.Delete,  # 其他语句
            )):
                return False
        return True
    except Exception:
        return False


# 针对 DataFrame 的特殊处理（用于 elt_ops）
def eval_dataframe_expression(
    df,
    expression: str,
    extra_funcs: Optional[Dict[str, Any]] = None
) -> Any:
    """评估 DataFrame 相关的表达式
    
    支持:
    - 列访问: col_name 或 df['col_name']
    - 方法调用: col.str.lower(), col.astype(int)
    - 数学运算: col1 + col2, col * 2
    
    Args:
        df: DataFrame 对象
        expression: 表达式
        extra_funcs: 额外可用的函数
    """
    import pandas as pd
    import numpy as np
    
    # 构建变量环境
    variables = {c: df[c] for c in df.columns}
    variables['df'] = df
    variables['pd'] = pd
    variables['np'] = np
    
    # 添加常用函数
    extra = {
        'str': str,
        'int': int,
        'float': float,
        'len': len,
        'abs': abs,
        'round': round,
        'min': min,
        'max': max,
    }
    if extra_funcs:
        extra.update(extra_funcs)
    
    return safe_eval(expression, variables, extra)


# 用于条件判断的表达式评估（用于 pipeline condition）
def eval_condition(
    expression: str,
    context: Dict[str, Any],
    allowed_vars: Optional[set] = None
) -> bool:
    """评估条件表达式
    
    返回布尔值结果，用于 pipeline 的 condition 操作
    
    Args:
        expression: 条件表达式字符串
        context: 变量上下文
        allowed_vars: 允许访问的变量名集合（白名单），None 表示允许所有
        
    Returns:
        布尔值结果
        
    Raises:
        SafeEvalError: 表达式评估失败或访问了不允许的变量
    """
    try:
        # 如果指定了白名单，过滤上下文
        if allowed_vars is not None:
            filtered_context = {
                k: v for k, v in context.items() 
                if k in allowed_vars or not k.startswith('_')
            }
        else:
            # 默认过滤掉私有变量（以下划线开头）
            filtered_context = {
                k: v for k, v in context.items() 
                if not k.startswith('_')
            }
        
        result = safe_eval(expression, filtered_context)
        return bool(result)
    except Exception as e:
        raise SafeEvalError(f"条件表达式评估失败 '{expression}': {e}")
