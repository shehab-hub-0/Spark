import io
import sys
import subprocess
import requests
from minio import Minio
from minio.deleteobjects import DeleteObject

print("🧹 Starting Data Cleanup Script...")

# 1. تهيئة الاتصال بـ MinIO
minio_client = None
minio_endpoint = None

for endpoint in ["localhost:9005", "minio:9000"]:
    try:
        client = Minio(
            endpoint,
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        client.list_buckets()
        minio_client = client
        minio_endpoint = endpoint
        print(f"📡 Connected to MinIO at: {endpoint}")
        break
    except Exception:
        continue

if not minio_client:
    print("❌ Failed to connect to MinIO.")
    sys.exit(1)

# 2. مسح الداتا ليك (MinIO Buckets) وإعادة إنشائها
buckets = ["warehouse", "spark-logs"]

print("\n🌊 Cleaning up MinIO Data Lake...")
for bucket in buckets:
    if minio_client.bucket_exists(bucket):
        print(f"   Deleting objects from bucket '{bucket}'...")
        # جلب كل الملفات وحذفها دفعة واحدة
        try:
            objects = minio_client.list_objects(bucket, recursive=True)
            delete_list = [DeleteObject(obj.object_name) for obj in objects]
            if delete_list:
                errors = minio_client.remove_objects(bucket, delete_list)
                for err in errors:
                    print(f"   ❌ Error deleting: {err}")
            
            # حذف البوكيت نفسه
            minio_client.remove_bucket(bucket)
            print(f"   ✅ Removed bucket: '{bucket}'")
        except Exception as e:
            print(f"   ❌ Failed to remove bucket '{bucket}': {e}")
    
    # إعادة إنشاء البوكيت فارغ
    minio_client.make_bucket(bucket)
    print(f"   ✅ Re-created empty bucket: '{bucket}'")

# إعادة إنشاء مجلد event-logs/ المطلوب لـ Spark History
try:
    minio_client.put_object("spark-logs", "event-logs/", io.BytesIO(b""), 0)
    print("   ✅ Created event-logs directory structure in 'spark-logs' bucket.")
except Exception as e:
    print("   ❌ Failed to create event-logs directory:", e)


# 3. مسح الداتا ويرهاوس (ClickHouse Tables)
print("\n🏢 Cleaning up ClickHouse Data Warehouse...")
clickhouse_url = "http://localhost:8123/"
clickhouse_params = {"user": "admin", "password": "admin"}

for db in ["default", "gold"]:
    try:
        # التحقق مما إذا كانت الداتابيز موجودة أولاً
        exists_query = f"EXISTS DATABASE {db}"
        exists_res = requests.post(clickhouse_url, params=clickhouse_params, data=exists_query)
        
        if exists_res.status_code == 200 and exists_res.text.strip() == "1":
            # جلب قائمة الجداول إذا كانت موجودة
            show_query = f"SHOW TABLES FROM {db}"
            res = requests.post(clickhouse_url, params=clickhouse_params, data=show_query)
            if res.status_code == 200:
                tables = [t.strip() for t in res.text.strip().split("\n") if t.strip()]
                if tables:
                    for table in tables:
                        drop_query = f"DROP TABLE {db}.{table}"
                        drop_res = requests.post(clickhouse_url, params=clickhouse_params, data=drop_query)
                        if drop_res.status_code == 200:
                            print(f"   ✅ Dropped ClickHouse table: {db}.{table}")
                        else:
                            print(f"   ❌ Failed to drop table {db}.{table}: {drop_res.text}")
                else:
                    print(f"   ℹ️ ClickHouse database '{db}' is empty (no tables to drop).")
            else:
                print(f"   ❌ Failed to list tables for DB '{db}': {res.text}")
        else:
            print(f"   ℹ️ ClickHouse database '{db}' does not exist yet (skipping).")
    except Exception as e:
        print(f"   ❌ ClickHouse DB '{db}' cleanup error: {e}")


# 4. تصفير Nessie Catalog (عن طريق إعادة تشغيل الحاوية كونها تحفظ البيانات في الذاكرة المؤقتة In-Memory)
print("\n🌳 Resetting Nessie Catalog...")
try:
    # إعادة تشغيل حاوية نسي
    subprocess.run(["docker", "compose", "restart", "nessie"], check=True)
    print("   ✅ Nessie Catalog restarted and reset to clean state.")
except Exception as e:
    print(f"   ❌ Failed to restart Nessie container: {e}")
    print("   Manual workaround: Run 'docker compose restart nessie' in your terminal.")

print("\n✨ Clean up complete! All data has been wiped.")
