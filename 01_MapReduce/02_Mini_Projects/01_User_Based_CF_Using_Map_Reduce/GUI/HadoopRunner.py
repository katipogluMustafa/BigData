import os


class HadoopRunner:
    """
    Run a hadoop job
    """

    def __init__(self, hadoop_path="/usr/local/hadoop/hadoop-3.1.1", hadoop_version="3.1.1",
                 exe_path="/home/herolenk/Project/Executables",
                 source_path="/home/herolenk/Project/Avg", output_jar_path=".",
                 jar_path="/home/herolenk/Project/Jars"):
        self.hadoop_path = hadoop_path
        self.hadoop_version = hadoop_version
        self.exe_path = exe_path
        self.source_path = source_path
        self.output_jar_path = output_jar_path
        os.chdir(jar_path)

    def run(self, jar_name, driver_class_name, src_path=None, exe_path=None):
        self.compile(exe_path=exe_path, src_path=src_path)
        self.create_jar(jar_name=jar_name, exe_path=exe_path)
        self.run_hadoop(jar_name=jar_name, driver_class_name=driver_class_name)

    def compile(self, exe_path=None, src_path=None):
        executable_path = self.exe_path if exe_path is None else exe_path
        source_path = self.source_path if src_path is None else src_path
        os.system(
            f"javac -cp {self.hadoop_path}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-{self.hadoop_version}.jar:{self.hadoop_path}/share/hadoop/common/hadoop-common-{self.hadoop_version}.jar -d {executable_path} {source_path}/*.java")

    def create_jar(self, jar_name, exe_path=None, output_folder_path=None):
        """
        Create jar out of compiled java classes
        os.system(r"jar -cvf Mycode.jar -C $HOME/Project/Executables .")
        :param jar_name: Name of the jar to be created.
        :param exe_path: Path of compiled class files
        :param output_folder_path: Path to put the jar
        :return:
        """
        executable_path = self.exe_path if exe_path is None else exe_path
        output_path = self.output_jar_path if output_folder_path is None else output_folder_path
        os.system(f"jar -cvf {jar_name}.jar -C {executable_path} {output_path}")

    def run_hadoop(self, jar_name, driver_class_name):
        """
        Run jar on hadoop with given driver class
        os.system(r"/usr/local/hadoop/hadoop-3.1.1/bin/hadoop jar Mycode.jar MovieAvgRating")
        :param hadoop_path: Hadoop source folder path
        :param jar_name: Name of the executable jar
        :param driver_class_name: Name of the driver class
        """
        os.system(f"{self.hadoop_path}/bin/hadoop jar {jar_name}.jar {driver_class_name}")



hr = HadoopRunner()
# hr.run(jar_name="Usr_Avg_Rating", driver_class_name="UserAvgRating",
#        src_path="/home/herolenk/Project/UserAvg", exe_path="/home/herolenk/Project/Executables/UserAvg")
hr.compile(src_path="/home/herolenk/Project/UserAvg", exe_path="/home/herolenk/Project/Executables/")
hr.create_jar(jar_name="AvgRating", exe_path="/home/herolenk/Project/Executables/Project/UserAvg")
hr.run_hadoop(jar_name="AvgRating", driver_class_name="Project.UserAvg.UserAvgRating")