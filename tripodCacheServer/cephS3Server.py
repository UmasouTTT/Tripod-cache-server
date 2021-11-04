from boto3.session import Session
import sys, os, re

class CephS3BOTO3():

    def __init__(self):
        access_key = 'todo'
        secret_key = 'todo'
        self.session = Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        self.url = 'todo'
        self.s3_client = self.session.client('s3', endpoint_url=self.url)

    def upload(self):
        resp = self.s3_client.put_object(
            # 桶ming
            Bucket="todo",
            # 文件名
            Key='todo',
            Body=open("todo", "rb").read()
        )

    def split_file_by_KB(self, file, size):
        fp = open(file, 'rb')
        i = 0
        n = 0
        dir_put = 'split_dir/'
        if os.path.isdir(dir_put):  # os.path.isdir	判断路径是否为目录
            pass
        else:
            os.mkdir(dir_put)  # 创建dir_put文件夹
        filename_front = os.path.splitext(file)[0]  # 取到除去扩展名的文件名	os.path.splitext	分割路径，返回路径名和文件扩展名的元组
        temp = open(dir_put + filename_front + '.part' + str(i) + '.txt', 'wb')
        buf = fp.read(1024)  # file.read()	从文件中读取指定的字节数
        while 1:
            temp.write(buf)
            buf = fp.read(1024)
            try:
                if buf[0] == "":
                    n += 1
                    continue
            except IndexError:
                print(filename_front + '.part' + str(i) + '.txt')
                temp.close()
                fp.close()
                return
            n += 1
            if n == size:
                n = 0
                print(filename_front + '.part' + str(i) + '.txt')
                i += 1
                temp.close()
                temp = open(dir_put + filename_front + '.part' + str(i) + '.txt', 'wb')
        fp.close()