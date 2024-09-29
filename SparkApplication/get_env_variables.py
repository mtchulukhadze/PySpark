
import os

os.environ["envn"] = "Dev"
os.environ["header"] = "True"
os.environ["interSchema"] = "True"

header = os.environ["header"]
inferSchema = os.environ["interSchema"]
envn = os.environ["envn"]

appName = "PySkark"
currentpath = os.getcwd()

src_olap = currentpath + '\source\olap'
src_oltp = currentpath + '\source\oltp'



