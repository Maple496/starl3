"""
网络端口监听模块
提供 TCP/HTTP/UDP 端口监听功能，当端口收到数据时触发执行
"""

import os
import json
import time
import socket
import threading
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime
from urllib.parse import parse_qs, urlparse
import re

from core.pipeline_engine import PipelineEngine
from core.constants import BASE_DIR, DATA_DIR
from core.logger import get_logger
from core.registry import op

logger = get_logger("network_listener_ops")

# 可选依赖
try:
    from http.server import HTTPServer, BaseHTTPRequestHandler
    HAS_HTTP = True
except ImportError:
    HAS_HTTP = False

# ==================== 全局网络监听状态 ====================

class NetworkListenerState:
    """网络监听状态管理（单例模式）"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init()
        return cls._instance
    
    def _init(self):
        self.is_running = False
        self.servers: Dict[str, Any] = {}  # 运行的服务器 {name: server_obj}
        self.threads: Dict[str, threading.Thread] = {}
        self.stop_event = threading.Event()
    
    def register_server(self, name: str, server_obj: Any, thread: threading.Thread):
        """注册服务器"""
        self.servers[name] = server_obj
        self.threads[name] = thread
        logger.info(f"服务器已注册: {name}")
    
    def start(self):
        """启动所有监听"""
        if self.is_running:
            return
        self.is_running = True
        self.stop_event.clear()
        logger.info(f"网络监听已启动，共 {len(self.servers)} 个服务")
    
    def stop(self):
        """停止所有监听"""
        if not self.is_running:
            return
        
        self.is_running = False
        self.stop_event.set()
        
        # 关闭所有服务器
        for name, server in self.servers.items():
            try:
                if hasattr(server, 'shutdown'):
                    server.shutdown()
                if hasattr(server, 'close'):
                    server.close()
                logger.info(f"服务器已停止: {name}")
            except Exception as e:
                logger.error(f"停止服务器失败 {name}: {e}")
        
        self.servers.clear()
        logger.info("网络监听已停止")
    
    def wait(self, timeout: Optional[float] = None):
        """等待运行"""
        if timeout:
            self.stop_event.wait(timeout)
        else:
            while self.is_running:
                time.sleep(1)


network_listener_state = NetworkListenerState()


# ==================== TCP 服务器 ====================

class TCPHandlerThread(threading.Thread):
    """TCP 连接处理线程"""
    
    def __init__(self, client_socket: socket.socket, client_addr: tuple, 
                 config_file: str, base_dir: str, data_format: str = "raw"):
        super().__init__(daemon=True)
        self.client_socket = client_socket
        self.client_addr = client_addr
        self.config_file = config_file
        self.base_dir = base_dir
        self.data_format = data_format
    
    def run(self):
        """处理连接"""
        try:
            # 接收数据
            data = b""
            self.client_socket.settimeout(30)  # 30秒超时
            
            while True:
                try:
                    chunk = self.client_socket.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                    # 如果数据较小，可能已全部接收
                    if len(chunk) < 4096:
                        break
                except socket.timeout:
                    break
            
            if not data:
                return
            
            # 解析数据
            received_text = data.decode('utf-8', errors='ignore')
            
            logger.info(f"【TCP接收】来自 {self.client_addr}: {received_text[:200]}...")
            
            # 触发执行
            self._trigger_pipeline(received_text)
            
            # 可选：发送响应
            try:
                response = json.dumps({"status": "ok", "received": len(data)})
                self.client_socket.send(response.encode())
            except:
                pass
                
        except Exception as e:
            logger.error(f"【TCP处理错误】{e}")
        finally:
            self.client_socket.close()
    
    def _trigger_pipeline(self, data: str):
        """触发执行 pipeline"""
        try:
            from main_starl3 import run_pipeline
            
            # 解析数据格式
            parsed_data = self._parse_data(data)
            
            trigger_ctx = {
                "trigger_type": "tcp",
                "trigger_time": datetime.now().isoformat(),
                "client_ip": self.client_addr[0],
                "client_port": self.client_addr[1],
                "raw_data": data,
                "parsed_data": parsed_data
            }
            
            logger.info(f"【执行】TCP触发: {self.config_file}")
            run_pipeline(self.config_file, self.base_dir, trigger_ctx=trigger_ctx)
            
        except Exception as e:
            logger.error(f"【执行失败】{e}")
    
    def _parse_data(self, data: str) -> Dict:
        """解析数据"""
        parsed = {"text": data, "json": None, "params": {}}
        
        if self.data_format == "json":
            try:
                parsed["json"] = json.loads(data)
            except:
                pass
        elif self.data_format == "urlencoded":
            try:
                parsed["params"] = dict(parse_qs(data))
                # 将列表转为单个值
                for k, v in parsed["params"].items():
                    if isinstance(v, list) and len(v) == 1:
                        parsed["params"][k] = v[0]
            except:
                pass
        
        return parsed


class TCPServerThread(threading.Thread):
    """TCP 服务器线程"""
    
    def __init__(self, host: str, port: int, config_file: str, base_dir: str, 
                 data_format: str = "raw"):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.config_file = config_file
        self.base_dir = base_dir
        self.data_format = data_format
        self.server_socket = None
        self.is_running = False
    
    def run(self):
        """运行服务器"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.is_running = True
            
            logger.info(f"【TCP服务】启动 {self.host}:{self.port}")
            
            while self.is_running and network_listener_state.is_running:
                try:
                    self.server_socket.settimeout(1.0)  # 1秒超时，便于检查停止
                    client_socket, client_addr = self.server_socket.accept()
                    
                    # 创建处理线程
                    handler = TCPHandlerThread(
                        client_socket, client_addr,
                        self.config_file, self.base_dir, self.data_format
                    )
                    handler.start()
                    
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.is_running:
                        logger.error(f"【TCP接受连接错误】{e}")
                    
        except Exception as e:
            logger.error(f"【TCP服务启动失败】{e}")
        finally:
            if self.server_socket:
                self.server_socket.close()
    
    def stop(self):
        """停止服务器"""
        self.is_running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass


# ==================== HTTP 服务器 ====================

class PipelineHTTPHandler(BaseHTTPRequestHandler):
    """HTTP 请求处理器"""
    
    config_file: str = ""
    base_dir: str = ""
    
    def log_message(self, format, *args):
        """自定义日志"""
        logger.info(f"【HTTP】{self.address_string()} - {format % args}")
    
    def do_GET(self):
        """处理 GET 请求"""
        self._handle_request("GET")
    
    def do_POST(self):
        """处理 POST 请求"""
        self._handle_request("POST")
    
    def _handle_request(self, method: str):
        """处理请求"""
        try:
            # 解析路径和参数
            parsed_path = urlparse(self.path)
            path = parsed_path.path
            query_params = parse_qs(parsed_path.query)
            # 简化参数
            for k, v in query_params.items():
                if isinstance(v, list) and len(v) == 1:
                    query_params[k] = v[0]
            
            # 读取 body
            body = ""
            content_length = self.headers.get('Content-Length')
            if content_length:
                body = self.rfile.read(int(content_length)).decode('utf-8', errors='ignore')
            
            # 解析 JSON body
            json_data = None
            content_type = self.headers.get('Content-Type', '')
            if 'application/json' in content_type:
                try:
                    json_data = json.loads(body)
                except:
                    pass
            elif 'application/x-www-form-urlencoded' in content_type:
                try:
                    form_data = parse_qs(body)
                    for k, v in form_data.items():
                        if isinstance(v, list) and len(v) == 1:
                            form_data[k] = v[0]
                    json_data = dict(form_data)
                except:
                    pass
            
            logger.info(f"【HTTP接收】{method} {path} from {self.client_address}")
            
            # 构建触发上下文
            trigger_ctx = {
                "trigger_type": "http",
                "trigger_time": datetime.now().isoformat(),
                "client_ip": self.client_address[0],
                "client_port": self.client_address[1],
                "method": method,
                "path": path,
                "headers": dict(self.headers),
                "query_params": query_params,
                "body": body,
                "json": json_data
            }
            
            # 触发执行（在新线程中，避免阻塞HTTP响应）
            threading.Thread(
                target=self._run_pipeline,
                args=(trigger_ctx,),
                daemon=True
            ).start()
            
            # 立即返回响应
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            response = {
                "status": "ok",
                "message": "Request received and processing",
                "timestamp": datetime.now().isoformat()
            }
            self.wfile.write(json.dumps(response).encode())
            
        except Exception as e:
            logger.error(f"【HTTP处理错误】{e}")
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": str(e)}).encode())
    
    def _run_pipeline(self, trigger_ctx: dict):
        """执行 pipeline"""
        try:
            from main_starl3 import run_pipeline
            logger.info(f"【执行】HTTP触发: {self.config_file}")
            run_pipeline(self.config_file, self.base_dir, trigger_ctx=trigger_ctx)
        except Exception as e:
            logger.error(f"【执行失败】{e}")


class HTTPServerThread(threading.Thread):
    """HTTP 服务器线程"""
    
    def __init__(self, host: str, port: int, config_file: str, base_dir: str):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.config_file = config_file
        self.base_dir = base_dir
        self.server = None
        self.is_running = False
    
    def run(self):
        """运行服务器"""
        try:
            # 设置处理器类属性
            PipelineHTTPHandler.config_file = self.config_file
            PipelineHTTPHandler.base_dir = self.base_dir
            
            self.server = HTTPServer((self.host, self.port), PipelineHTTPHandler)
            self.is_running = True
            
            logger.info(f"【HTTP服务】启动 http://{self.host}:{self.port}")
            
            while self.is_running and network_listener_state.is_running:
                self.server.handle_request()
                
        except Exception as e:
            logger.error(f"【HTTP服务错误】{e}")
    
    def stop(self):
        """停止服务器"""
        self.is_running = False
        if self.server:
            try:
                self.server.shutdown()
            except:
                pass


# ==================== UDP 服务器 ====================

class UDPServerThread(threading.Thread):
    """UDP 服务器线程"""
    
    def __init__(self, host: str, port: int, config_file: str, base_dir: str):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.config_file = config_file
        self.base_dir = base_dir
        self.sock = None
        self.is_running = False
    
    def run(self):
        """运行服务器"""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind((self.host, self.port))
            self.sock.settimeout(1.0)  # 1秒超时，便于检查停止
            self.is_running = True
            
            logger.info(f"【UDP服务】启动 {self.host}:{self.port}")
            
            while self.is_running and network_listener_state.is_running:
                try:
                    data, addr = self.sock.recvfrom(65535)
                    if data:
                        self._handle_data(data, addr)
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.is_running:
                        logger.error(f"【UDP接收错误】{e}")
                        
        except Exception as e:
            logger.error(f"【UDP服务启动失败】{e}")
        finally:
            if self.sock:
                self.sock.close()
    
    def _handle_data(self, data: bytes, addr: tuple):
        """处理收到的数据"""
        try:
            received_text = data.decode('utf-8', errors='ignore')
            logger.info(f"【UDP接收】来自 {addr}: {received_text[:200]}...")
            
            # 触发执行
            threading.Thread(
                target=self._run_pipeline,
                args=(received_text, addr),
                daemon=True
            ).start()
            
        except Exception as e:
            logger.error(f"【UDP处理错误】{e}")
    
    def _run_pipeline(self, data: str, addr: tuple):
        """执行 pipeline"""
        try:
            from main_starl3 import run_pipeline
            
            trigger_ctx = {
                "trigger_type": "udp",
                "trigger_time": datetime.now().isoformat(),
                "client_ip": addr[0],
                "client_port": addr[1],
                "raw_data": data
            }
            
            logger.info(f"【执行】UDP触发: {self.config_file}")
            run_pipeline(self.config_file, self.base_dir, trigger_ctx=trigger_ctx)
            
        except Exception as e:
            logger.error(f"【执行失败】{e}")
    
    def stop(self):
        """停止服务器"""
        self.is_running = False
        if self.sock:
            try:
                self.sock.close()
            except:
                pass


# ==================== 操作函数 ====================

@op("tcp_listen_start", category="network_listener", description="启动 TCP 端口监听")
def op_tcp_listen_start(ctx, params):
    """
    启动 TCP 端口监听，收到数据时触发执行
    
    参数:
        port: int — 监听端口（必填）
        host: str — 绑定地址，默认 "0.0.0.0"（所有接口）
        config_file: str — 收到数据时执行的 pipeline 文件（必填）
        data_format: str — 数据解析格式，可选 "raw"/"json"/"urlencoded"，默认 "raw"
        timeout: int/float — 监听超时（秒），0=永久，默认 0
        
    触发上下文:
        - trigger_type: "tcp"
        - client_ip: 客户端IP
        - client_port: 客户端端口
        - raw_data: 原始文本数据
        - parsed_data: 解析后的数据
    """
    port = params.get("port")
    if not port:
        raise ValueError("port 参数必填")
    
    host = params.get("host", "0.0.0.0")
    config_file = params.get("config_file")
    
    if not config_file:
        raise ValueError("config_file 参数必填")
    
    base_dir = ctx.get("base_dir", DATA_DIR)
    config_file = os.path.abspath(os.path.join(base_dir, config_file))
    
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"配置文件不存在: {config_file}")
    
    # 创建服务器
    server_name = f"tcp_{host}_{port}"
    server = TCPServerThread(
        host=host,
        port=port,
        config_file=config_file,
        base_dir=base_dir,
        data_format=params.get("data_format", "raw")
    )
    
    server.start()
    network_listener_state.register_server(server_name, server, server)
    network_listener_state.start()
    
    logger.info(f"【TCP监听】启动 {host}:{port} -> {config_file}")
    
    # 运行
    timeout = params.get("timeout", 0)
    try:
        if timeout and timeout > 0:
            network_listener_state.wait(timeout)
        else:
            logger.info("按 Ctrl+C 停止监听")
            network_listener_state.wait()
    except KeyboardInterrupt:
        logger.info("收到停止信号")
    finally:
        server.stop()
        network_listener_state.stop()
    
    return {"status": "stopped", "port": port}


@op("http_listen_start", category="network_listener", description="启动 HTTP 端口监听（Webhook）")
def op_http_listen_start(ctx, params):
    """
    启动 HTTP 端口监听，收到请求时触发执行
    
    参数:
        port: int — 监听端口（必填）
        host: str — 绑定地址，默认 "0.0.0.0"
        config_file: str — 收到请求时执行的 pipeline 文件（必填）
        timeout: int/float — 监听超时（秒），0=永久，默认 0
        
    触发上下文:
        - trigger_type: "http"
        - client_ip: 客户端IP
        - method: 请求方法 GET/POST
        - path: 请求路径
        - headers: 请求头
        - query_params: URL参数
        - body: 原始请求体
        - json: 解析后的JSON数据（如果是JSON请求）
        
    示例:
        发送 POST 请求到 http://localhost:8080/webhook
        数据: {"event": "order_created", "order_id": "123"}
        触发脚本执行，ctx["json"] 包含解析后的数据
    """
    port = params.get("port")
    if not port:
        raise ValueError("port 参数必填")
    
    host = params.get("host", "0.0.0.0")
    config_file = params.get("config_file")
    
    if not config_file:
        raise ValueError("config_file 参数必填")
    
    base_dir = ctx.get("base_dir", DATA_DIR)
    config_file = os.path.abspath(os.path.join(base_dir, config_file))
    
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"配置文件不存在: {config_file}")
    
    # 创建服务器
    server_name = f"http_{host}_{port}"
    server = HTTPServerThread(
        host=host,
        port=port,
        config_file=config_file,
        base_dir=base_dir
    )
    
    server.start()
    network_listener_state.register_server(server_name, server, server)
    network_listener_state.start()
    
    logger.info(f"【HTTP监听】启动 http://{host}:{port} -> {config_file}")
    logger.info(f"  测试: curl -X POST http://localhost:{port}/webhook -d '{{\"test\":true}}'")
    
    # 运行
    timeout = params.get("timeout", 0)
    try:
        if timeout and timeout > 0:
            network_listener_state.wait(timeout)
        else:
            logger.info("按 Ctrl+C 停止监听")
            network_listener_state.wait()
    except KeyboardInterrupt:
        logger.info("收到停止信号")
    finally:
        server.stop()
        network_listener_state.stop()
    
    return {"status": "stopped", "port": port}


@op("udp_listen_start", category="network_listener", description="启动 UDP 端口监听")
def op_udp_listen_start(ctx, params):
    """
    启动 UDP 端口监听，收到数据时触发执行
    
    参数:
        port: int — 监听端口（必填）
        host: str — 绑定地址，默认 "0.0.0.0"
        config_file: str — 收到数据时执行的 pipeline 文件（必填）
        timeout: int/float — 监听超时（秒），0=永久，默认 0
        
    触发上下文:
        - trigger_type: "udp"
        - client_ip: 客户端IP
        - client_port: 客户端端口
        - raw_data: 原始数据文本
    """
    port = params.get("port")
    if not port:
        raise ValueError("port 参数必填")
    
    host = params.get("host", "0.0.0.0")
    config_file = params.get("config_file")
    
    if not config_file:
        raise ValueError("config_file 参数必填")
    
    base_dir = ctx.get("base_dir", DATA_DIR)
    config_file = os.path.abspath(os.path.join(base_dir, config_file))
    
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"配置文件不存在: {config_file}")
    
    # 创建服务器
    server_name = f"udp_{host}_{port}"
    server = UDPServerThread(
        host=host,
        port=port,
        config_file=config_file,
        base_dir=base_dir
    )
    
    server.start()
    network_listener_state.register_server(server_name, server, server)
    network_listener_state.start()
    
    logger.info(f"【UDP监听】启动 {host}:{port} -> {config_file}")
    
    # 运行
    timeout = params.get("timeout", 0)
    try:
        if timeout and timeout > 0:
            network_listener_state.wait(timeout)
        else:
            logger.info("按 Ctrl+C 停止监听")
            network_listener_state.wait()
    except KeyboardInterrupt:
        logger.info("收到停止信号")
    finally:
        server.stop()
        network_listener_state.stop()
    
    return {"status": "stopped", "port": port}


@op("network_listen_stop", category="network_listener", description="停止网络监听")
def op_network_listen_stop(ctx, params):
    """停止所有网络监听"""
    network_listener_state.stop()
    return {"status": "stopped"}


@op("tcp_send", category="network_listener", description="发送 TCP 数据")
def op_tcp_send(ctx, params):
    """
    发送 TCP 数据到指定地址
    
    参数:
        host: str — 目标地址（必填）
        port: int — 目标端口（必填）
        data: str — 发送的数据（必填）
        timeout: int — 连接超时（秒），默认 10
        
    返回:
        发送结果
    """
    host = params.get("host")
    port = params.get("port")
    data = params.get("data")
    
    if not all([host, port, data]):
        raise ValueError("host, port, data 参数必填")
    
    timeout = params.get("timeout", 10)
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))
        sock.send(data.encode('utf-8'))
        
        # 尝试接收响应
        try:
            sock.settimeout(5)
            response = sock.recv(4096).decode('utf-8', errors='ignore')
        except:
            response = None
        
        sock.close()
        
        return {
            "status": "success",
            "sent_bytes": len(data),
            "response": response
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


@op("http_request", category="network_listener", description="发送 HTTP 请求")
def op_http_request(ctx, params):
    """
    发送 HTTP 请求（简单的客户端功能）
    
    参数:
        url: str — 请求URL（必填）
        method: str — 请求方法，默认 GET
        headers: dict — 请求头
        data: str — 请求体数据
        json: dict — JSON数据（与data二选一）
        timeout: int — 超时（秒），默认 30
        
    返回:
        响应数据
    """
    try:
        import urllib.request
        import urllib.parse
    except ImportError:
        raise RuntimeError("urllib 模块不可用")
    
    url = params.get("url")
    if not url:
        raise ValueError("url 参数必填")
    
    method = params.get("method", "GET").upper()
    timeout = params.get("timeout", 30)
    headers = params.get("headers", {})
    
    # 处理数据
    data = None
    if "json" in params:
        import json
        data = json.dumps(params["json"]).encode('utf-8')
        headers.setdefault('Content-Type', 'application/json')
    elif "data" in params:
        data = params["data"].encode('utf-8')
    
    try:
        req = urllib.request.Request(
            url=url,
            data=data,
            headers=headers,
            method=method
        )
        
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return {
                "status_code": response.status,
                "headers": dict(response.headers),
                "body": response.read().decode('utf-8', errors='ignore')
            }
            
    except Exception as e:
        return {
            "status_code": None,
            "error": str(e)
        }


# ==================== 便捷封装 ====================

@op("webhook_server", category="network_listener", description="启动 Webhook 服务器（HTTP监听便捷封装）")
def op_webhook_server(ctx, params):
    """
    简化的 Webhook 服务器启动
    
    参数:
        port: int — 端口，默认 8080
        run: str — 收到请求时执行的脚本（必填）
        
    示例:
        {
            "port": 8080,
            "run": "configs/handle_webhook.json"
        }
    """
    return op_http_listen_start(ctx, {
        "port": params.get("port", 8080),
        "config_file": params.get("run"),
        "timeout": params.get("timeout", 0)
    })


# ==================== OP_MAP ====================

OP_MAP = {
    "tcp_listen_start": op_tcp_listen_start,
    "http_listen_start": op_http_listen_start,
    "udp_listen_start": op_udp_listen_start,
    "network_listen_stop": op_network_listen_stop,
    "tcp_send": op_tcp_send,
    "http_request": op_http_request,
    "webhook_server": op_webhook_server,
}


def run(config_path=None):
    """模块测试入口"""
    PipelineEngine.main(OP_MAP, cfg=config_path, init_ctx=lambda: {"base_dir": DATA_DIR})


if __name__ == '__main__':
    run()
