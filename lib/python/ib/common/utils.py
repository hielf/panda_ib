
import yaml
import os

def get_yaml_data(yaml_file):

    # 打开yaml文件
    print("***获取yaml文件数据***")
    file = open(yaml_file, 'r', encoding="utf-8")
    file_data = file.read()
    file.close()

    print(file_data)
    print("类型：", type(file_data))

    # 将字符串转化为字典或列表
    print('\n' * 3)
    print("***转化yaml数据为字典或列表***")
    data = yaml.safe_load(file_data)
    print(data)
    print("类型：", type(data))
    print('\n' * 3)
    return data

def get_config(source):
    current_path = os.path.abspath(".")
    yaml_path = os.path.join(current_path, source)
    my_config = get_yaml_data(yaml_path)

    return my_config