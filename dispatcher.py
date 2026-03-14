#被打包成exe程序之后 可以拖动文件到这个exe程序,然后程序会按照 拖入文件的地址作为参数启动这个exe
#根据输入的参数，调用不同的函数  现在有三个程序 elt_ops.py,file_ops.py,attemper_ops.py,然后会根据输入的参数类别 参数通常是文件地址,例如c:\elt_ops_config.json 的就调用elt_ops程序,然后把参数传递给elt_ops程序,参数开头是file_ops的就调用file_ops程序,参数开头是attemper_ops的就调用attemper_ops程序,然后把参数传递给对应的程序,这样就实现了一个总调度器的功能,根据输入的参数来调用不同的模块,并且把参数传递给对应的模块,实现了模块之间的协调和调度,使得整个系统能够灵活地处理不同类型的任务,并且能够根据需要扩展新的模块和功能

import sys
import os
def main():
    path = sys.argv[1]
    name = os.path.basename(path).lower()
    if name.startswith("elt_ops"):
        from elt_ops import run
    elif name.startswith("file_ops"):
        from file_ops import run
    elif name.startswith("attemper_ops"):
        from attemper_ops import run
    elif name.startswith("ai_ops"):
        from ai_ops import run
    else:
        print(f"未知的任务类型: {name}")
        return
    run(path)
if __name__ == "__main__":
    main()