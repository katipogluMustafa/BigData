import os

os.chdir("/home/herolenk/Project/Jars")

def compile(hadoop_path="/usr/local/hadoop/hadoop-3.1.1", hadoop_version="3.1.1", exe_path="$HOME/Project/Executables", source_path="$HOME/Project/Avg/*.java"):
    """
    os.system(r"javac -cp /usr/local/hadoop/hadoop-3.1.1/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.1.1.jar:/usr/local/hadoop/hadoop-3.1.1/share/hadoop/common/hadoop-common-3.1.1.jar -d $HOME/Project/Executables $HOME/Project/Avg/*.java")
    """
    os.system(f"javac -cp {hadoop_path}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-{hadoop_version}.jar:{hadoop_path}/share/hadoop/common/hadoop-common-{hadoop_version}.jar -d {exe_path} {source_path}");

def create_jar(jar_name, exe_path="$HOME/Project/Executables", output_folder_path="."):
    """
    Create jar out of compiled java classes
    os.system(r"jar -cvf Mycode.jar -C $HOME/Project/Executables .")
    :param jar_name: Name of the jar to be created.
    :param exe_path: Path of compiled class files
    :param output_folder_path: Path to put the jar
    :return:
    """
    os.system(f"jar -cvf {jar_name}.jar -C {exe_path} {output_folder_path}")


def run_hadoop(hadoop_path, jar_name, driver_class_name):
    """
    Run jar on hadoop with given driver class
    os.system(r"/usr/local/hadoop/hadoop-3.1.1/bin/hadoop jar Mycode.jar MovieAvgRating")
    :param hadoop_path: Hadoop source folder path
    :param jar_name: Name of the executable jar
    :param driver_class_name: Name of the driver class
    """
    os.system(f"{hadoop_path}/bin/hadoop jar {jar_name}.jar {driver_class_name}")

# run_hadoop(hadoop_path="/usr/local/hadoop/hadoop-3.1.1", jar_name="Mycode", driver_class_name="MovieAvgRating")

#create_jar(jar_name="deneme", exe_path="$HOME/Project/Executables", output_folder_path=".")
#run_hadoop(hadoop_path="/usr/local/hadoop/hadoop-3.1.1", jar_name="Mycode", driver_class_name="MovieAvgRating")