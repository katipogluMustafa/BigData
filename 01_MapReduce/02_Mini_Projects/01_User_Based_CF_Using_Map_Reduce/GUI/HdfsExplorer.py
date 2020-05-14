import os
import pandas


class HdfsExplorer:
    def __init__(self, hdfs_path, temp_path="./temp"):
        self.hdfs_path = hdfs_path
        self.temp_path = temp_path
        os.chdir("/home/herolenk/Project")

    def download(self, src_path, dest_path=None):
        path = self.temp_path if dest_path is None else dest_path
        os.system(f"gohdfs get {self.hdfs_path}{src_path} {dest_path}")

    def upload(self, src_path, dest_path):
        os.system(f"gohdfs put {src_path} {self.hdfs_path}{dest_path}")

    def remove(self, path):
        os.system(f"gohdfs rm {self.hdfs_path}{path}")

    def remove_folder(self, path):
        os.system(f"gohdfs rm -rf {self.hdfs_path}{path}")


os.system("gohdfs get hdfs://master:9000/user/herolenk/outputs/pearsonCorr/part-r-00000 ./pearson")

df = pandas.read_csv("./pearson", sep=",")
df.columns = ["userId1", "userId2", "corr"]
print(df)