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

os.chdir("/home/herolenk/Project/Jars")

def compile(hadoop_path="/usr/local/hadoop/hadoop-3.1.1", hadoop_version="3.1.1", exe_path="$HOME/Project/Executables/UserAvg", source_path="$HOME/Project/UserAvg/*.java"):
    """
    os.system(r"javac -cp /usr/local/hadoop/hadoop-3.1.1/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.1.1.jar:/usr/local/hadoop/hadoop-3.1.1/share/hadoop/common/hadoop-common-3.1.1.jar -d $HOME/Project/Executables $HOME/Project/Avg/*.java")
    """
    os.system(f"javac -cp {hadoop_path}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-{hadoop_version}.jar:{hadoop_path}/share/hadoop/common/hadoop-common-{hadoop_version}.jar -d {exe_path} {source_path}");


def create_jar(jar_name, exe_path="$HOME/Project/Executables/UserAvg", output_folder_path="."):
    """
    Create jar out of compiled java classes
    os.system(r"jar -cvf Mycode.jar -C $HOME/Project/Executables .")
    :param jar_name: Name of the jar to be created.
    :param exe_path: Path of compiled class files
    :param output_folder_path: Path to put the jar
    :return:
    """
    os.system(f"jar -cvf {jar_name}.jar -C {exe_path} {output_folder_path}")

# run_hadoop(hadoop_path="/usr/local/hadoop/hadoop-3.1.1", jar_name="Mycode", driver_class_name="MovieAvgRating")
def run_hadoop(hadoop_path, jar_name, driver_class_name, params=None):
    """
    Run jar on hadoop with given driver class
    os.system(r"/usr/local/hadoop/hadoop-3.1.1/bin/hadoop jar Mycode.jar MovieAvgRating")
    :param hadoop_path: Hadoop source folder path
    :param jar_name: Name of the executable jar
    :param driver_class_name: Name of the driver class
    """
    if params is None:
        os.system(f"{hadoop_path}/bin/hadoop jar {jar_name}.jar {driver_class_name}")
    else:
        os.system(f"{hadoop_path}/bin/hadoop jar {jar_name}.jar {driver_class_name} {params}")


def calculate_user_avg_ratings():
    compile()
    create_jar(jar_name="userAvg", exe_path="$HOME/Project/Executables", output_folder_path=".")
    run_hadoop(hadoop_path="/usr/local/hadoop/hadoop-3.1.1", jar_name="userAvg", driver_class_name="Project.UserAvg.UserAvgRating")


def get_user_avg_ratings():
    calculate_user_avg_ratings()
    os.system("gohdfs get hdfs://master:9000/user/herolenk/outputs/userAvgRating/part-r-00000 /home/herolenk/Project/results/userAvg")
    df = pandas.read_csv("/home/herolenk/Project/results/userAvg", sep=",")
    df.columns = ["userId", "avgRating"]
    return df


def calculate_movie_avg_ratings():
    compile(exe_path="$HOME/Project/Executables", source_path="$HOME/Project/MovieAvg/*.java")
    create_jar(jar_name="movieAvg", exe_path="$HOME/Project/Executables/", output_folder_path=".")
    run_hadoop(hadoop_path="/usr/local/hadoop/hadoop-3.1.1", jar_name="movieAvg",
               driver_class_name="Project.MovieAvg.MovieAvgRating")


def get_movie_avg_ratings():
    calculate_movie_avg_ratings()
    os.system(
        "gohdfs get hdfs://master:9000/user/herolenk/outputs/movieAvgRating/part-r-00000 /home/herolenk/Project/results/movieAvgRating")
    df = pandas.read_csv("/home/herolenk/Project/results/movieAvgRating", sep=",")
    df.columns = ["movieId", "movieAvgRating"]
    return df


def find_movie_watchers():
    compile(exe_path="$HOME/Project/Executables", source_path="$HOME/Project/MovieWatchers/*.java")
    create_jar(jar_name="movieWatchers", exe_path="$HOME/Project/Executables/", output_folder_path=".")
    run_hadoop(hadoop_path="/usr/local/hadoop/hadoop-3.1.1", jar_name="movieWatchers",
                   driver_class_name="Project.MovieWatchers.MovieRatings")


def calculate_pearson_corrs():
    compile(exe_path="$HOME/Project/Executables", source_path="$HOME/Project/Pearson/*.java")
    create_jar(jar_name="pearson", exe_path="$HOME/Project/Executables/", output_folder_path=".")
    run_hadoop(hadoop_path="/usr/local/hadoop/hadoop-3.1.1", jar_name="pearson",
                       driver_class_name="Project.Pearson.PearsonCorrelation")

def get_pearson_corrs():
    #calculate_pearson_corrs()
    os.system("gohdfs get hdfs://master:9000/user/herolenk/outputs/pearsonCorr/part-r-00000 /home/herolenk/Project/results/pearson")
    df = pandas.read_csv("/home/herolenk/Project/results/pearson", sep=",")
    df.columns = ["userId1", "userId2", "corr"]
    return df


def calculate_movie_rec():
    compile(exe_path="$HOME/Project/Executables", source_path="$HOME/Project/MoviePredict/*.java")
    create_jar(jar_name="moviePredict", exe_path="$HOME/Project/Executables/", output_folder_path=".")
    run_hadoop(hadoop_path="/usr/local/hadoop/hadoop-3.1.1", jar_name="moviePredict",
                       driver_class_name="Project.MoviePrediction.Predict")


def get_movie_rec():
    calculate_movie_rec()
    os.system("gohdfs get hdfs://master:9000/user/herolenk/outputs/movieRec/part-r-00000 /home/herolenk/Project/results/movieRec")
    df = pandas.read_csv("/home/herolenk/Project/results/movieRec", sep=",")
    return df.columns[1]


def get_knn():
    os.system("gohdfs get hdfs://master:9000/user/herolenk/outputs/knns/part-r-00000 /home/herolenk/Project/results/knns")