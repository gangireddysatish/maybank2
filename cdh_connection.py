import os
import jpype
import jaydebeapi
import gssapi

os.environ["KRB5_CLIENT_KTNAME"] = "C:\ProgramData\MIT\Kerberos5\80013010-cdp.keytab"

jaas_conf = "D:\CDP\cdh_Python_Connection\jaas.conf"
jdbc_jars = [ "D:\CDP\cdh_Python_Connection\jars\hive-jdbc-2.1.1-cdh6.3.4-standalone.jar" ]


hive_lib_dir = "D:\CDP\cdh_Python_Connection\jars\"

hive_jars = [
    os.path.join(hive_lib_dir, jar)
    for jar in os.listdir(hive_lib_dir)
    if jar.endswith(".jar")
]
#making sure the jdbc lib is the correct one (and all other jars are also in same classpath)
extra_jars = [
    "D:\CDP\cdh_Python_Connection\jars\hive-jdbc-2.1.1-cdh6.3.4-standalone.jar"
]

classpath = ":".join(hive_jars + extra_jars)

jdbc_url = (
    "jdbc:hive2://mbbbdauatdb001.mbbuat.local:10015/cdptesting;"
    "principal=hive/mbbbdauatdb001.mbbuat.local@MBBBDAUAT.COM;"
    "auth=KERBEROS"
)

jdbc_driver = "org.apache.hive.jdbc.HiveDriver"

jvm_args = [
    f"-Djava.security.auth.login.config={jaas_conf}",
    "-Djavax.security.auth.useSubjectCredsOnly=false",
    f"-Djava.class.path={classpath}"
]
if not jpype.isJVMStarted():
    jpype.startJVM(jpype.getDefaultJVMPath(), *jvm_args)

# 4. Connect and run a basic query
try:
    conn = jaydebeapi.connect(jdbc_driver, jdbc_url, [], classpath)
    curs = conn.cursor()
    curs.execute("SELECT current_database(), current_user()")
    result = curs.fetchall()
    print("✅ Connected:", result)
    curs.close()
    conn.close()
except Exception as e:
    print("❌ Connection failed:", e)
finally:
    if jpype.isJVMStarted():
        jpype.shutdownJVM()

