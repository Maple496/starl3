"""
Web 框架模块
提供完整后端功能：路由、认证、REST响应、数据库、缓存
"""

import os
import json
import re
import hashlib
import time
import threading
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse

from core.pipeline_engine import PipelineEngine
from core.constants import BASE_DIR, DATA_DIR
from core.logger import get_logger
from core.registry import op

logger = get_logger("web_framework_ops")

# 可选依赖
try:
    import jwt
    HAS_JWT = True
except ImportError:
    HAS_JWT = False

try:
    import redis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

try:
    import sqlite3
    HAS_SQLITE = True
except ImportError:
    HAS_SQLITE = False

# ==================== 路由系统 ====================

class Router:
    """路由管理器"""
    
    def __init__(self):
        self.routes: Dict[str, Dict] = {}  # path -> {method, config_file, params}
        self.middlewares: List[Dict] = []
    
    def add_route(self, path: str, config_file: str, methods: List[str] = None, 
                  params: Dict = None):
        """添加路由"""
        self.routes[path] = {
            "config_file": config_file,
            "methods": methods or ["GET", "POST"],
            "params": params or {}
        }
        logger.info(f"【路由注册】{path} -> {config_file}")
    
    def match(self, path: str, method: str) -> Optional[Dict]:
        """匹配路由"""
        # 精确匹配
        if path in self.routes:
            route = self.routes[path]
            if method in route["methods"]:
                return route
        
        # 通配符匹配（如 /api/users/:id）
        for route_path, route_info in self.routes.items():
            if self._match_pattern(route_path, path) and method in route_info["methods"]:
                result = route_info.copy()
                result["params"] = self._extract_params(route_path, path)
                return result
        
        return None
    
    def _match_pattern(self, pattern: str, path: str) -> bool:
        """匹配模式（支持 :param 通配）"""
        pattern_parts = pattern.split("/")
        path_parts = path.split("/")
        
        if len(pattern_parts) != len(path_parts):
            return False
        
        for p, part in zip(pattern_parts, path_parts):
            if p.startswith(":"):
                continue  # 通配符匹配任何值
            if p != part:
                return False
        
        return True
    
    def _extract_params(self, pattern: str, path: str) -> Dict:
        """提取路径参数"""
        params = {}
        pattern_parts = pattern.split("/")
        path_parts = path.split("/")
        
        for p, part in zip(pattern_parts, path_parts):
            if p.startswith(":"):
                param_name = p[1:]
                params[param_name] = part
        
        return params


# 全局路由实例
router = Router()


# ==================== 认证系统 ====================

class AuthManager:
    """认证管理器"""
    
    def __init__(self):
        self.api_keys: Dict[str, Dict] = {}  # key -> {name, permissions, expires}
        self.sessions: Dict[str, Dict] = {}  # session_id -> {user, expires}
        self.jwt_secret: str = os.environ.get("JWT_SECRET", "starl3_default_secret")
    
    def add_api_key(self, key: str, name: str = None, permissions: List[str] = None):
        """添加 API Key"""
        self.api_keys[key] = {
            "name": name or key[:8],
            "permissions": permissions or ["*"],
            "created_at": datetime.now().isoformat()
        }
    
    def validate_api_key(self, key: str) -> bool:
        """验证 API Key"""
        return key in self.api_keys
    
    def create_session(self, user_id: str, data: Dict = None, expires_hours: int = 24) -> str:
        """创建会话"""
        session_id = hashlib.sha256(f"{user_id}{time.time()}".encode()).hexdigest()[:32]
        expires = datetime.now() + timedelta(hours=expires_hours)
        
        self.sessions[session_id] = {
            "user_id": user_id,
            "data": data or {},
            "expires": expires.isoformat(),
            "created_at": datetime.now().isoformat()
        }
        
        return session_id
    
    def validate_session(self, session_id: str) -> Optional[Dict]:
        """验证会话"""
        if session_id not in self.sessions:
            return None
        
        session = self.sessions[session_id]
        expires = datetime.fromisoformat(session["expires"])
        
        if datetime.now() > expires:
            del self.sessions[session_id]
            return None
        
        return session
    
    def generate_jwt(self, payload: Dict, expires_hours: int = 24) -> str:
        """生成 JWT Token"""
        if not HAS_JWT:
            raise RuntimeError("PyJWT 未安装，请运行: pip install pyjwt")
        
        payload_copy = payload.copy()
        payload_copy["exp"] = datetime.utcnow() + timedelta(hours=expires_hours)
        payload_copy["iat"] = datetime.utcnow()
        
        return jwt.encode(payload_copy, self.jwt_secret, algorithm="HS256")
    
    def validate_jwt(self, token: str) -> Optional[Dict]:
        """验证 JWT Token"""
        if not HAS_JWT:
            raise RuntimeError("PyJWT 未安装")
        
        try:
            payload = jwt.decode(token, self.jwt_secret, algorithms=["HS256"])
            return payload
        except jwt.ExpiredSignatureError:
            return None
        except jwt.InvalidTokenError:
            return None


# 全局认证实例
auth_manager = AuthManager()


# ==================== 缓存系统 ====================

class CacheManager:
    """缓存管理器（支持内存和 Redis）"""
    
    def __init__(self):
        self.memory_cache: Dict[str, Dict] = {}  # key -> {value, expires}
        self.redis_client = None
        self.default_ttl = 3600  # 默认1小时
    
    def connect_redis(self, host: str = "localhost", port: int = 6379, 
                      db: int = 0, password: str = None):
        """连接 Redis"""
        if not HAS_REDIS:
            raise RuntimeError("redis-py 未安装，请运行: pip install redis")
        
        self.redis_client = redis.Redis(
            host=host, port=port, db=db, password=password,
            decode_responses=True
        )
        logger.info(f"【缓存】Redis 连接成功 {host}:{port}")
    
    def get(self, key: str) -> Any:
        """获取缓存"""
        # 优先 Redis
        if self.redis_client:
            try:
                value = self.redis_client.get(key)
                if value:
                    return json.loads(value)
            except Exception as e:
                logger.error(f"【缓存】Redis get 失败: {e}")
        
        # 内存缓存
        if key in self.memory_cache:
            item = self.memory_cache[key]
            if datetime.now().timestamp() < item["expires"]:
                return item["value"]
            else:
                del self.memory_cache[key]
        
        return None
    
    def set(self, key: str, value: Any, ttl: int = None):
        """设置缓存"""
        ttl = ttl or self.default_ttl
        
        # Redis
        if self.redis_client:
            try:
                self.redis_client.setex(key, ttl, json.dumps(value))
                return
            except Exception as e:
                logger.error(f"【缓存】Redis set 失败: {e}")
        
        # 内存缓存
        self.memory_cache[key] = {
            "value": value,
            "expires": datetime.now().timestamp() + ttl
        }
    
    def delete(self, key: str):
        """删除缓存"""
        if self.redis_client:
            try:
                self.redis_client.delete(key)
            except:
                pass
        
        if key in self.memory_cache:
            del self.memory_cache[key]
    
    def clear(self):
        """清空缓存"""
        if self.redis_client:
            try:
                self.redis_client.flushdb()
            except:
                pass
        
        self.memory_cache.clear()


# 全局缓存实例
cache_manager = CacheManager()


# ==================== 数据库连接池 ====================

class DatabaseManager:
    """数据库管理器（支持 SQLite、MySQL、PostgreSQL）"""
    
    def __init__(self):
        self.connections: Dict[str, Any] = {}
    
    def connect_sqlite(self, name: str, db_path: str):
        """连接 SQLite"""
        if not HAS_SQLITE:
            raise RuntimeError("SQLite3 不可用")
        
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        self.connections[name] = conn
        logger.info(f"【数据库】SQLite 连接成功: {db_path}")
        return conn
    
    def get_connection(self, name: str = "default"):
        """获取连接"""
        if name not in self.connections:
            raise ValueError(f"数据库连接不存在: {name}")
        return self.connections[name]
    
    def execute(self, name: str, sql: str, params: tuple = None) -> List[Dict]:
        """执行 SQL"""
        conn = self.get_connection(name)
        cursor = conn.cursor()
        
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            conn.commit()
            
            # 查询结果
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            return [{"affected_rows": cursor.rowcount}]
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
    
    def close(self, name: str = None):
        """关闭连接"""
        if name:
            if name in self.connections:
                self.connections[name].close()
                del self.connections[name]
        else:
            for conn in self.connections.values():
                conn.close()
            self.connections.clear()


# 全局数据库实例
db_manager = DatabaseManager()


# ==================== 响应格式化 ====================

@op("http_response", category="web_framework", description="发送 HTTP 响应")
def op_http_response(ctx, params):
    """
    发送格式化的 HTTP 响应
    
    参数:
        status_code: int — HTTP 状态码，默认 200
        body: any — 响应体数据
        headers: dict — 自定义响应头
        content_type: str — 内容类型，默认 application/json
        
    注意:
        这个操作会设置 ctx['http_response']，需要配合 http_router 使用
    """
    response = {
        "status_code": params.get("status_code", 200),
        "body": params.get("body", {}),
        "headers": params.get("headers", {}),
        "content_type": params.get("content_type", "application/json")
    }
    
    ctx["http_response"] = response
    return response


@op("rest_success", category="web_framework", description="发送成功响应")
def op_rest_success(ctx, params):
    """
    发送标准 REST 成功响应
    
    参数:
        data: any — 响应数据
        message: str — 提示消息
        code: int — 业务代码，默认 0
        
    返回格式:
        {"success": true, "code": 0, "message": "", "data": {...}}
    """
    response_body = {
        "success": True,
        "code": params.get("code", 0),
        "message": params.get("message", "success"),
        "data": params.get("data"),
        "timestamp": datetime.now().isoformat()
    }
    
    return op_http_response(ctx, {
        "status_code": 200,
        "body": response_body
    })


@op("rest_error", category="web_framework", description="发送错误响应")
def op_rest_error(ctx, params):
    """
    发送标准 REST 错误响应
    
    参数:
        message: str — 错误消息
        status_code: int — HTTP 状态码，默认 400
        code: int — 业务错误代码，默认 -1
    """
    response_body = {
        "success": False,
        "code": params.get("code", -1),
        "message": params.get("message", "error"),
        "data": None,
        "timestamp": datetime.now().isoformat()
    }
    
    return op_http_response(ctx, {
        "status_code": params.get("status_code", 400),
        "body": response_body
    })


# ==================== 路由操作 ====================

@op("http_route_add", category="web_framework", description="添加 HTTP 路由")
def op_http_route_add(ctx, params):
    """
    添加 HTTP 路由规则
    
    参数:
        path: str — 路由路径，如 "/api/users" 或 "/api/users/:id"
        config_file: str — 匹配的 pipeline 文件
        methods: list — 允许的 HTTP 方法，默认 ["GET", "POST"]
        
    路径参数:
        使用 :param 定义路径参数，如 "/api/users/:user_id"
        可在处理脚本中通过 ctx.route_params.user_id 获取
    """
    path = params.get("path")
    config_file = params.get("config_file")
    
    if not path or not config_file:
        raise ValueError("path 和 config_file 参数必填")
    
    base_dir = ctx.get("base_dir", DATA_DIR)
    config_file = os.path.abspath(os.path.join(base_dir, config_file))
    
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"配置文件不存在: {config_file}")
    
    methods = params.get("methods", ["GET", "POST"])
    
    router.add_route(path, config_file, methods)
    
    return {
        "path": path,
        "methods": methods,
        "config_file": config_file
    }


@op("http_router_start", category="web_framework", description="启动 HTTP 路由服务器")
def op_http_router_start(ctx, params):
    """
    启动带有路由功能的 HTTP 服务器
    
    参数:
        port: int — 监听端口，默认 8080
        host: str — 绑定地址，默认 "0.0.0.0"
        timeout: int/float — 运行超时，0=永久
        cors: bool — 是否启用跨域，默认 true
        
    注意:
        需要先使用 http_route_add 添加路由规则
    """
    port = params.get("port", 8080)
    host = params.get("host", "0.0.0.0")
    cors = params.get("cors", True)
    
    if not router.routes:
        raise ValueError("没有路由规则，请先使用 http_route_add 添加路由")
    
    # 创建自定义 HTTP 处理器
    from http.server import HTTPServer, BaseHTTPRequestHandler
    
    class RouterHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            logger.info(f"【HTTP】{self.address_string()} - {format % args}")
        
        def do_OPTIONS(self):
            """处理预检请求（CORS）"""
            self.send_response(200)
            if cors:
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
            self.end_headers()
        
        def do_GET(self):
            self._handle_request("GET")
        
        def do_POST(self):
            self._handle_request("POST")
        
        def do_PUT(self):
            self._handle_request("PUT")
        
        def do_DELETE(self):
            self._handle_request("DELETE")
        
        def _handle_request(self, method: str):
            """处理请求"""
            from urllib.parse import urlparse
            
            parsed = urlparse(self.path)
            path = parsed.path
            query_params = parse_qs(parsed.query)
            # 简化参数
            for k, v in query_params.items():
                if isinstance(v, list) and len(v) == 1:
                    query_params[k] = v[0]
            
            # 匹配路由
            route = router.match(path, method)
            
            if not route:
                self._send_error(404, "Not Found")
                return
            
            # 读取请求体
            body = ""
            content_length = self.headers.get('Content-Length')
            if content_length:
                body = self.rfile.read(int(content_length)).decode('utf-8', errors='ignore')
            
            json_data = None
            if body:
                try:
                    json_data = json.loads(body)
                except:
                    pass
            
            # 构建上下文
            base_dir = ctx.get("base_dir", DATA_DIR)
            trigger_ctx = {
                "trigger_type": "http",
                "client_ip": self.address_string().split(':')[0],
                "method": method,
                "path": path,
                "route_params": route.get("params", {}),
                "query_params": query_params,
                "headers": dict(self.headers),
                "body": body,
                "json": json_data
            }
            
            # 执行 pipeline
            try:
                from main_starl3 import run_pipeline
                result = run_pipeline(route["config_file"], base_dir, trigger_ctx=trigger_ctx)
                
                # 获取响应
                http_response = result.get("http_response", {}) if isinstance(result, dict) else {}
                
                if http_response:
                    status_code = http_response.get("status_code", 200)
                    response_body = http_response.get("body", {})
                    headers = http_response.get("headers", {})
                else:
                    status_code = 200
                    response_body = result if result else {"success": True}
                    headers = {}
                
                # 发送响应
                self.send_response(status_code)
                self.send_header("Content-Type", "application/json")
                if cors:
                    self.send_header("Access-Control-Allow-Origin", "*")
                for k, v in headers.items():
                    self.send_header(k, v)
                self.end_headers()
                
                if isinstance(response_body, dict):
                    self.wfile.write(json.dumps(response_body).encode())
                else:
                    self.wfile.write(str(response_body).encode())
                    
            except Exception as e:
                logger.error(f"【路由处理错误】{e}")
                self._send_error(500, str(e))
        
        def _send_error(self, code: int, message: str):
            """发送错误响应"""
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            if cors:
                self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            
            error_body = {
                "success": False,
                "code": -1,
                "message": message,
                "timestamp": datetime.now().isoformat()
            }
            self.wfile.write(json.dumps(error_body).encode())
    
    # 启动服务器
    server = HTTPServer((host, port), RouterHandler)
    logger.info(f"【HTTP路由服务器】启动 http://{host}:{port}")
    logger.info(f"【路由表】")
    for path, info in router.routes.items():
        logger.info(f"  {info['methods']} {path} -> {info['config_file']}")
    
    try:
        timeout = params.get("timeout", 0)
        if timeout and timeout > 0:
            server.socket.settimeout(timeout)
        
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("收到停止信号")
    finally:
        server.shutdown()
    
    return {"status": "stopped", "port": port}


# ==================== 认证操作 ====================

@op("auth_apikey_add", category="web_framework", description="添加 API Key")
def op_auth_apikey_add(ctx, params):
    """
    添加 API Key 用于认证
    
    参数:
        key: str — API Key（必填）
        name: str — 名称描述
        permissions: list — 权限列表，如 ["read", "write"]
    """
    key = params.get("key")
    if not key:
        raise ValueError("key 参数必填")
    
    auth_manager.add_api_key(
        key=key,
        name=params.get("name"),
        permissions=params.get("permissions", ["*"])
    )
    
    logger.info(f"【认证】API Key 已添加: {params.get('name', key[:8])}")
    return {"status": "added", "name": params.get("name", key[:8])}


@op("auth_apikey_validate", category="web_framework", description="验证 API Key")
def op_auth_apikey_validate(ctx, params):
    """
    验证 API Key
    
    参数:
        key: str — 要验证的 API Key（必填）
        
    返回:
        {"valid": true/false}
    """
    key = params.get("key")
    if not key:
        raise ValueError("key 参数必填")
    
    # 从请求头中获取
    if key == "${headers.Authorization}" or key.startswith("${"):
        headers = ctx.get("headers", {})
        auth_header = headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            key = auth_header[7:]
        elif auth_header.startswith("ApiKey "):
            key = auth_header[7:]
        else:
            key = auth_header
    
    is_valid = auth_manager.validate_api_key(key)
    
    if not is_valid and params.get("reject", True):
        return op_rest_error(ctx, {
            "message": "Invalid API Key",
            "status_code": 401,
            "code": 1001
        })
    
    return {"valid": is_valid}


@op("auth_session_create", category="web_framework", description="创建会话")
def op_auth_session_create(ctx, params):
    """
    创建用户会话
    
    参数:
        user_id: str — 用户标识（必填）
        data: dict — 附加数据
        expires_hours: int — 过期时间（小时），默认 24
        
    返回:
        {"session_id": "..."}
    """
    user_id = params.get("user_id")
    if not user_id:
        raise ValueError("user_id 参数必填")
    
    session_id = auth_manager.create_session(
        user_id=user_id,
        data=params.get("data"),
        expires_hours=params.get("expires_hours", 24)
    )
    
    return {"session_id": session_id, "user_id": user_id}


# ==================== 缓存操作 ====================

@op("cache_set", category="web_framework", description="设置缓存")
def op_cache_set(ctx, params):
    """
    设置缓存数据
    
    参数:
        key: str — 缓存键（必填）
        value: any — 缓存值（必填）
        ttl: int — 过期时间（秒），默认 3600
        
    示例:
        {"key": "user:${user_id}", "value": "${user_data}", "ttl": 7200}
    """
    key = params.get("key")
    value = params.get("value")
    
    if key is None or value is None:
        raise ValueError("key 和 value 参数必填")
    
    ttl = params.get("ttl", 3600)
    
    cache_manager.set(key, value, ttl)
    
    return {"key": key, "ttl": ttl}


@op("cache_get", category="web_framework", description="获取缓存")
def op_cache_get(ctx, params):
    """
    获取缓存数据
    
    参数:
        key: str — 缓存键（必填）
        default: any — 默认值（不存在时返回）
    """
    key = params.get("key")
    if not key:
        raise ValueError("key 参数必填")
    
    value = cache_manager.get(key)
    
    if value is None and "default" in params:
        value = params["default"]
    
    return {"key": key, "value": value, "exists": value is not None}


@op("cache_delete", category="web_framework", description="删除缓存")
def op_cache_delete(ctx, params):
    """删除缓存"""
    key = params.get("key")
    if key:
        cache_manager.delete(key)
    return {"key": key, "deleted": True}


@op("cache_connect_redis", category="web_framework", description="连接 Redis")
def op_cache_connect_redis(ctx, params):
    """
    连接 Redis 服务器
    
    参数:
        host: str — 主机，默认 localhost
        port: int — 端口，默认 6379
        db: int — 数据库，默认 0
        password: str — 密码（可选）
    """
    cache_manager.connect_redis(
        host=params.get("host", "localhost"),
        port=params.get("port", 6379),
        db=params.get("db", 0),
        password=params.get("password")
    )
    
    return {"status": "connected", "host": params.get("host", "localhost")}


# ==================== 数据库操作 ====================

@op("db_connect_sqlite", category="web_framework", description="连接 SQLite 数据库")
def op_db_connect_sqlite(ctx, params):
    """
    连接 SQLite 数据库
    
    参数:
        name: str — 连接名称，默认 "default"
        db_path: str — 数据库文件路径（必填）
    """
    name = params.get("name", "default")
    db_path = params.get("db_path")
    
    if not db_path:
        raise ValueError("db_path 参数必填")
    
    # 解析路径
    if not os.path.isabs(db_path):
        db_path = os.path.join(DATA_DIR, db_path)
    
    # 确保目录存在
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    db_manager.connect_sqlite(name, db_path)
    
    return {"status": "connected", "name": name, "path": db_path}


@op("db_execute", category="web_framework", description="执行 SQL")
def op_db_execute(ctx, params):
    """
    执行 SQL 语句
    
    参数:
        name: str — 连接名称，默认 "default"
        sql: str — SQL 语句（必填）
        params: list/tuple — SQL 参数（可选）
        
    示例:
        {"sql": "SELECT * FROM users WHERE id = ?", "params": ["${user_id}"]}
    """
    name = params.get("name", "default")
    sql = params.get("sql")
    sql_params = params.get("params", [])
    
    if not sql:
        raise ValueError("sql 参数必填")
    
    result = db_manager.execute(name, sql, tuple(sql_params) if sql_params else None)
    
    return {"result": result, "row_count": len(result)}


@op("db_query", category="web_framework", description="查询数据")
def op_db_query(ctx, params):
    """
    查询数据（db_execute 的别名，语义更清晰）
    
    参数同 db_execute
    """
    return op_db_execute(ctx, params)


@op("db_insert", category="web_framework", description="插入数据")
def op_db_insert(ctx, params):
    """
    插入数据
    
    参数:
        name: str — 连接名称
        table: str — 表名（必填）
        data: dict — 要插入的数据（必填）
        
    示例:
        {"table": "users", "data": {"name": "张三", "email": "zhangsan@example.com"}}
    """
    name = params.get("name", "default")
    table = params.get("table")
    data = params.get("data")
    
    if not table or not data:
        raise ValueError("table 和 data 参数必填")
    
    columns = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    
    result = db_manager.execute(name, sql, tuple(data.values()))
    
    return {"inserted": True, "id": result[0].get("lastrowid") if result else None}


# ==================== OP_MAP ====================

OP_MAP = {
    # 响应
    "http_response": op_http_response,
    "rest_success": op_rest_success,
    "rest_error": op_rest_error,
    
    # 路由
    "http_route_add": op_http_route_add,
    "http_router_start": op_http_router_start,
    
    # 认证
    "auth_apikey_add": op_auth_apikey_add,
    "auth_apikey_validate": op_auth_apikey_validate,
    "auth_session_create": op_auth_session_create,
    
    # 缓存
    "cache_set": op_cache_set,
    "cache_get": op_cache_get,
    "cache_delete": op_cache_delete,
    "cache_connect_redis": op_cache_connect_redis,
    
    # 数据库
    "db_connect_sqlite": op_db_connect_sqlite,
    "db_execute": op_db_execute,
    "db_query": op_db_query,
    "db_insert": op_db_insert,
}


def run(config_path=None):
    """模块测试入口"""
    PipelineEngine.main(OP_MAP, cfg=config_path, init_ctx=lambda: {"base_dir": DATA_DIR})


if __name__ == '__main__':
    run()
