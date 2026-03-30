# 贡献指南

感谢您对 StarL3 项目的关注！本文档将指导您如何为项目做出贡献。

## 🚀 快速开始

1. **Fork 仓库**: 点击 GitHub 页面的 Fork 按钮
2. **克隆代码**: `git clone https://github.com/YOUR_USERNAME/starl3.git`
3. **创建分支**: `git checkout -b feature/your-feature-name`
4. **提交更改**: `git commit -m "feat: add some feature"`
5. **推送分支**: `git push origin feature/your-feature-name`
6. **创建 PR**: 在 GitHub 上创建 Pull Request

## 📋 开发规范

### 代码风格

- 遵循 PEP 8 Python 编码规范
- 使用 4 空格缩进
- 最大行长度 100 字符
- 使用有意义的变量名

### 提交信息规范

使用 [Conventional Commits](https://www.conventionalcommits.org/) 规范：

```
<type>(<scope>): <subject>

<body>

<footer>
```

**常用类型**:

- `feat`: 新功能
- `fix`: 修复 Bug
- `docs`: 文档更新
- `style`: 代码格式（不影响功能）
- `refactor`: 代码重构
- `test`: 测试相关
- `chore`: 构建过程或辅助工具的变动

**示例**:

```
feat(elt_ops): add support for large Excel files

- Implement chunked reading for files > 100MB
- Add progress indicator for long operations
- Update documentation

Closes #123
```

## 🔧 开发环境设置

```bash
# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或
venv\Scripts\activate  # Windows

# 安装开发依赖
pip install -r requirements-dev.txt

# 运行测试
python -m pytest tests/
```

## 📝 添加新操作

如果您想添加新的操作模块，请参考以下步骤：

1. 在 `scripts/ops/` 下创建新的操作文件
2. 使用 `@op` 装饰器注册操作
3. 添加文档字符串说明参数和返回值
4. 在 `tests/` 下添加测试用例

**示例**:

```python
from core.registry import op

@op("my_op", category="custom", description="我的操作")
def my_op(ctx, params):
    """
    操作说明
    
    Args:
        ctx: 执行上下文
        params: 参数字典
        
    Returns:
        操作结果
        
    Example:
        >>> my_op(ctx, {"key": "value"})
        result
    """
    # 实现逻辑
    pass
```

## 🐛 提交 Issue

如果您发现了 Bug 或有功能建议，请提交 Issue：

1. 检查是否已有相关 Issue
2. 使用 Issue 模板
3. 提供详细的复现步骤
4. 贴上相关日志和错误信息

## 📜 行为准则

- 尊重他人，保持友善
- 接受建设性的批评
- 关注对社区最有利的事情
- 对其他社区成员表示同理心

## ❓ 需要帮助？

如果您在贡献过程中遇到问题，可以：

- 查看 [文档](README.md)
- 提交 Issue 询问
- 联系维护者

感谢您对 StarL3 的贡献！
