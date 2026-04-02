"""
StarL3 系统托盘程序入口（单例模式）

功能：
1. 确保只有一个 StarL3Tray 实例在运行
2. 如果已有实例在运行，新启动的实例会激活已有实例的任务管理器，然后退出
"""
import socket
import sys
import threading

# IPC 配置
IPC_HOST = '127.0.0.1'
IPC_PORT = 55555
IPC_COMMAND_SHOW_TASK_MANAGER = b'SHOW_TASK_MANAGER'


def check_and_activate_existing_instance() -> bool:
    """
    检查是否已有实例在运行。
    如果有，发送激活命令并返回 True。
    如果没有，返回 False。
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        sock.connect((IPC_HOST, IPC_PORT))
        sock.sendall(IPC_COMMAND_SHOW_TASK_MANAGER)
        sock.close()
        return True
    except Exception:
        # 连接失败，说明没有实例在运行
        return False


def start_ipc_server(tray_app):
    """
    启动 TCP 服务器（在后台线程中）监听其他实例的连接请求。
    """
    def handle_client(conn, addr):
        """处理客户端连接"""
        try:
            data = conn.recv(1024)
            if data == IPC_COMMAND_SHOW_TASK_MANAGER:
                print("[INFO] 收到激活请求，打开任务管理器")
                # 在主线程中打开任务管理器
                tray_app.open_task_manager()
        except Exception as e:
            print(f"[WARNING] IPC 客户端处理错误: {e}")
        finally:
            conn.close()
    
    def server_loop():
        """服务器主循环"""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server.bind((IPC_HOST, IPC_PORT))
            server.listen(5)
            print(f"[INFO] IPC 服务器已启动，监听 {IPC_HOST}:{IPC_PORT}")
            while True:
                try:
                    conn, addr = server.accept()
                    # 为每个连接创建新线程
                    client_thread = threading.Thread(
                        target=handle_client, 
                        args=(conn, addr), 
                        daemon=True
                    )
                    client_thread.start()
                except Exception as e:
                    print(f"[WARNING] IPC 服务器 accept 错误: {e}")
        except Exception as e:
            print(f"[ERROR] IPC 服务器启动失败: {e}")
        finally:
            server.close()
    
    # 在后台线程中启动服务器
    server_thread = threading.Thread(target=server_loop, daemon=True)
    server_thread.start()


def main():
    """主入口"""
    # 步骤 1: 检查是否已有实例在运行
    if check_and_activate_existing_instance():
        # 已有实例在运行，已发送激活命令，本实例退出
        sys.exit(0)
    
    # 步骤 2: 没有现有实例，正常启动
    from app.tray import StarL3TrayApp
    
    # 检查依赖
    try:
        import flask
        from flask_cors import CORS
    except ImportError:
        print("[ERROR] 请先安装依赖: pip install flask flask-cors")
        sys.exit(1)
    
    # 步骤 3: 启动 Web 服务器（必须在 IPC 服务器之前，因为 IPC 需要 web_port）
    from app.web import run_web_server
    web_thread, web_port = run_web_server(
        host='127.0.0.1',
        port=0,  # 自动选择端口
        open_browser=False
    )
    
    # 步骤 4: 创建应用实例并设置 Web 服务器
    app = StarL3TrayApp()
    app.web_thread = web_thread
    app.web_port = web_port
    
    print(f"[INFO] StarL3 系统托盘程序已启动")
    print(f"[INFO] 管理界面: http://127.0.0.1:{app.web_port}")
    
    # 步骤 5: 启动 IPC 服务器监听其他实例
    start_ipc_server(app)
    
    # 步骤 6: 运行托盘程序（使用外部 Web 服务器）
    app.run(web_thread=web_thread, web_port=web_port, skip_web_server=True)


if __name__ == "__main__":
    main()
