import os
import jpype
import jaydebeapi
import gssapi
import traceback

os.environ["KRB5_CLIENT_KTNAME"] = r"C:\ProgramData\MIT\Kerberos5\80013010-cdp.keytab"

jaas_conf = r"D:\CDP\cdh_Python_Connection\jaas.conf"
jdbc_jars = [r"D:\CDP\cdh_Python_Connection\jars\hive-jdbc-2.1.1-cdh6.3.4-standalone.jar"]

hive_lib_dir = r"D:\CDP\cdh_Python_Connection\jars"

hive_jars = [
    os.path.join(hive_lib_dir, jar)
    for jar in os.listdir(hive_lib_dir)
    if jar.endswith(".jar")
]
# making sure the jdbc lib is the correct one (and all other jars are also in same classpath)
extra_jars = [
    r"D:\CDP\cdh_Python_Connection\jars\hive-jdbc-2.1.1-cdh6.3.4-standalone.jar"
]

classpath = ";".join(hive_jars + extra_jars)

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

# === Logging and file existence checks ===
print(f"JVM args: {jvm_args}")
print(f"Classpath: {classpath}")
print(f"JDBC Driver: {jdbc_driver}")
print(f"JDBC URL: {jdbc_url}")
print(f"JARs used: {hive_jars + extra_jars}")
print(f"JAAS config: {jaas_conf}")
print(f"Keytab: {os.environ['KRB5_CLIENT_KTNAME']}")

required_files = hive_jars + extra_jars + [jaas_conf, os.environ["KRB5_CLIENT_KTNAME"]]
for path in required_files:
    if not os.path.exists(path):
        print(f"❌ Required file not found: {path}")
        exit(1)

print("Attempting to start JVM and connect to Hive...")

if not jpype.isJVMStarted():
    jpype.startJVM(jpype.getDefaultJVMPath(), *jvm_args)

try:
    conn = jaydebeapi.connect(jdbc_driver, jdbc_url, [], hive_jars + extra_jars)
    print("Connection established. Running test query...")
    curs = conn.cursor()
    curs.execute("SELECT current_database(), current_user()")
    result = curs.fetchall()
    print("✅ Connected:", result)
    curs.close()
    conn.close()
except Exception as e:
    print("❌ Connection failed:", e)
    traceback.print_exc()
finally:
    if jpype.isJVMStarted():
        jpype.shutdownJVM()

input("Press Enter to exit...")