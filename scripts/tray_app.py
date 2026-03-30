"""
StarL3 系统托盘常驻程序
主入口文件
"""

import os
import sys
import tkinter as tk
from tkinter import filedialog
import threading
import webbrowser

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import pystray
    from PIL import Image, ImageDraw
except ImportError:
    print("[ERROR] 请先安装依赖: pip install pystray pillow")
    sys.exit(1)

from task_manager import task_manager
from web_server import run_web_server


class StarL3TrayApp:
    """StarL3 系统托盘应用程序"""
    
    def __init__(self):
        self.icon = None
        self.web_port = None
        self.web_thread = None
        
    def create_icon_image(self):
        """创建托盘图标"""
        # 创建一个简单的图标（紫色圆形+S字母）
        width = 64
        height = 64
        image = Image.new('RGB', (width, height), color='#667eea')
        dc = ImageDraw.Draw(image)
        
        # 画一个白色圆形背景
        dc.ellipse([4, 4, width-4, height-4], fill='white')
        
        # 画紫色圆形
        dc.ellipse([8, 8, width-8, height-8], fill='#764ba2')
        
        # 添加 S 字母
        dc.text((22, 18), "S", fill='white', font=None)
        
        return image
    
    def on_select_config(self, icon, item):
        """选择配置文件并运行"""
        def select_and_run():
            # 创建隐藏的主窗口
            root = tk.Tk()
            root.withdraw()
            root.attributes('-topmost', True)
            
            # 弹出文件选择对话框
            file_path = filedialog.askopenfilename(
                title="选择 Pipeline 配置文件",
                filetypes=[("JSON 文件", "*.json"), ("所有文件", "*.*")]
            )
            
            root.destroy()
            
            if file_path:
                # 启动任务
                task_id = task_manager.start_task(file_path)
                print(f"[INFO] 已启动任务: {task_id}")
                
                # 打开管理界面
                if self.web_port:
                    webbrowser.open(f"http://127.0.0.1:{self.web_port}")
        
        # 在单独线程中运行，避免阻塞托盘
        threading.Thread(target=select_and_run, daemon=True).start()
    
    def on_new_config(self, icon, item):
        """新建配置文件 - 打开编辑器"""
        def open_editor():
            import subprocess
            import sys
            
            # 获取 main_edit.py 的路径
            base_dir = os.path.dirname(os.path.abspath(__file__))
            editor_path = os.path.join(base_dir, "main_edit.py")
            
            if not os.path.exists(editor_path):
                print(f"[ERROR] 编辑器不存在: {editor_path}")
                return
            
            # 查找可用的 Python 解释器
            python_exe = sys.executable
            if not python_exe or not os.path.exists(python_exe):
                # 尝试虚拟环境
                venv_python = os.path.join(os.path.dirname(base_dir), "venv", "Scripts", "python.exe")
                if os.path.exists(venv_python):
                    python_exe = venv_python
                else:
                    python_exe = "python"
            
            try:
                # 启动编辑器，使用新建文件模式
                cmd = [python_exe, editor_path]
                subprocess.Popen(cmd, cwd=base_dir)
                print(f"[INFO] 已启动编辑器: {editor_path}")
            except Exception as e:
                print(f"[ERROR] 启动编辑器失败: {e}")
        
        # 在单独线程中运行
        threading.Thread(target=open_editor, daemon=True).start()
    
    def on_view_tasks(self, icon, item):
        """查看任务管理界面"""
        if self.web_port:
            webbrowser.open(f"http://127.0.0.1:{self.web_port}")
    
    def on_exit(self, icon, item):
        """退出程序"""
        print("[INFO] 正在停止所有任务并退出...")
        
        # 停止所有任务
        task_manager.stop_all_tasks(timeout=3.0)
        
        # 停止托盘图标
        icon.stop()
    
    def create_menu(self):
        """创建右键菜单"""
        from pystray import Menu, MenuItem
        
        return Menu(
            MenuItem(
                "选择配置文件",
                self.on_select_config,
                default=True  # 双击图标时的默认操作
            ),
            MenuItem(
                "新建配置文件",
                self.on_new_config
            ),
            MenuItem(
                "查看任务管理",
                self.on_view_tasks
            ),
            Menu.SEPARATOR,
            MenuItem(
                "退出",
                self.on_exit
            )
        )
    
    def run(self):
        """运行系统托盘应用"""
        # 启动 Web 服务器
        self.web_thread, self.web_port = run_web_server(
            host='127.0.0.1',
            port=0,  # 自动选择端口
            open_browser=False
        )
        
        print(f"[INFO] StarL3 系统托盘程序已启动")
        print(f"[INFO] 管理界面: http://127.0.0.1:{self.web_port}")
        
        # 创建托盘图标
        self.icon = pystray.Icon(
            name="StarL3",
            icon=self.create_icon_image(),
            title="StarL3 Pipeline 管理器",
            menu=self.create_menu()
        )
        
        # 运行托盘程序（阻塞）
        self.icon.run()
        
        print("[INFO] StarL3 已退出")


def main():
    """主入口"""
    # 检查依赖
    try:
        import flask
        from flask_cors import CORS
    except ImportError:
        print("[ERROR] 请先安装依赖: pip install flask flask-cors")
        sys.exit(1)
    
    app = StarL3TrayApp()
    app.run()


if __name__ == "__main__":
    main()
