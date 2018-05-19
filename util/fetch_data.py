import xml.etree.ElementTree as etree
file_names = []
bucket_path = "https://s3.eu-central-1.amazonaws.com/deutsche-boerse-xetra-pds/"
tree = etree.parse('deutsche-boerse-xetra-pds')
root = tree.getroot()


def write_files():
    import urllib.request
    import os
    for file_name in file_names:
        url = bucket_path + file_name
        response =urllib.request.urlopen(url)
        os.makedirs(os.path.dirname("../data/"+file_name), exist_ok=True)
        f = open("../data/" + file_name, 'w+')
        data =response.read()
        text = data.decode('utf-8')
        f.write(text)
        f.close()
        print(file_name + " completed!")


for element in root:
    for child in element:
        file_names.append(child.text)
        print(child.text)
        break

write_files()


