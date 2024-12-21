import json
import re


def read_json_objects(file_path):
    json_list = []
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            # 使用正则表达式匹配完整的 JSON 对象（以 { 开始，以 } 结束）
            json_objects = re.findall(r'\{.*?\}', content, re.DOTALL)
            for json_text in json_objects:
                try:
                    json_object = json.loads(json_text.strip())
                    json_list.append(json_object)
                except json.JSONDecodeError:
                    print(f"Failed to parse JSON text: {json_text}")
    except FileNotFoundError:
        print("File not found. Please check the file path.")
    return json_list

# Example usage
file_path = '/home/rings/searchEngine/data/newripepage.dat'
result_list = read_json_objects(file_path)
id_json=[]
i=0
json_str_list=[]
for result_json in result_list:
    result_json["content"] =result_json["content"].replace("\u3000", "")
    id_json.append("id"+str(i))
    i=i+1
    json_str_list.append(json.dumps(result_json))
print(result_list)

with open("/home/rings/searchEngine/data/output.json", "w", encoding="utf-8") as file:
    json.dump(result_list, file, ensure_ascii=False, indent=4)