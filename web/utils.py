import math
import pandas as pd
import re

s1 = u'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨĩŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴỵỶỷỸỹ'
s0 = u'AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYyYy'

def get_individual_from_title(input_str):
	try:
		if math.isnan(input_str):
			return "nan"
	except:
		k = 1
	s = ''
	for c in input_str:
		if c in s1:
			s += s0[s1.index(c)]
		else:
			s += c
	s = remove_special_chars_keep_punct_space(s)
	return s.replace(" ", "").lower().replace("|", "")

# print(get_individual_from_title("Tôi thấy hoa vàng trên cỏ xanh"))



data = pd.read_csv("./data.csv")
classes = []
for i in range(len(data)):
    classes.append(data["hasCategory"][i])
classes = list(set(classes))

mapping_category = {}
for classs in classes:
    mapping_category[classs] = classs
mapping_category['Optimization. Operations Research'] = 'Optimization'
mapping_category ['Popular scientific literature'] = 'PopularScientificLiterature'
mapping_category['Operating Systems'] = 'OperatingSystem'
mapping_category['Mathematical Economics'] = 'MathematicalEconomics'
mapping_category['Love, erotic'] = 'LoveErotic'
mapping_category['Databases'] = 'Database'
mapping_category['Creative Thinking'] = 'CreativeThinking'

def category_mapping():
    return mapping_category


def remove_special_chars_keep_punct_space(text):
	if text == "nan":
		return "nan"
	if type(text) is not str and  math.isnan(text):
		return "nan"
	else:
		# print(text)
		return re.sub(r"[^\w\s]", "", text)
	