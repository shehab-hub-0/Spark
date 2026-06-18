# 🚀 Apache Spark & Data Lakehouse Labs - Student Guide

![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5.0-orange?logo=apachespark&style=for-the-badge)
![Jupyter](https://img.shields.io/badge/Jupyter_Lab-Ready-F37626?logo=jupyter&style=for-the-badge)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&style=for-the-badge)
![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible-C7202C?logo=minio&style=for-the-badge)
![ClickHouse](https://img.shields.io/badge/ClickHouse-24.3-yellow?logo=clickhouse&style=for-the-badge)

أهلاً بك في بيئة العمل الخاصة بك لتعلم هندسة البيانات (Data Engineering) ومعالجة البيانات الضخمة باستخدام **Apache Spark**. 
هذا المشروع مصمم ليكون "معمل متكامل" (Lab Environment) يشتغل على جهازك بشكل معزول تماماً، وبوفر لك كل الأدوات اللي هتحتاجها عشان تكتب وتنفذ أكواد PySpark وتتعلم مفاهيم الـ Data Lake والـ Data Warehouse.

---

## 🔬 محتويات المعمل (The Lab Environment)

المعمل بيحتوي على مجموعة من الخدمات (Services) المترابطة عشان تحاكي بيئة العمل الحقيقية في الشركات:

1. **Jupyter Workspace (`jupyter-workspace`)**: بيئة كتابة الأكواد الرئيسية (IDE). هنا هتفتح الـ Notebooks وتكتب كود PySpark بتاعك.
2. **Spark Cluster (`spark-master` & `spark-worker-1/2`)**: المحرك الرئيسي لمعالجة البيانات. متكون من مدير (Master) وعاملين (Workers) عشان ينفذوا الكود بتاعك بشكل موزع (Distributed).
3. **Spark History Server (`spark-history`)**: واجهة بتسمحلك تراجع وتحلل الـ Jobs اللي خلصت وتفهم إزاي الـ Spark نفذها خطوة بخطوة.
4. **MinIO (`minio`)**: بيمثل الـ **Data Lake** بتاعك. هو نظام تخزين متوافق مع S3، هترفع عليه الملفات الخام (Raw Data) عشان الـ Spark يقراها.
5. **PostgreSQL (`postgres`)**: قاعدة بيانات علائقية تُستخدم كخلفية لحفظ البيانات الوصفية (Metadata) للمعمل.
6. **Project Nessie (`nessie`) & Dremio (`dremio`)**: أدوات بناء الـ **Data Lakehouse** الحديثة لتنظيم وإدارة البيانات.
7. **ClickHouse (`clickhouse`)**: بيمثل الـ **Data Warehouse**. قاعدة بيانات تحليلية سريعة جداً ممكن تستخدمها عشان تخزن النتايج النهائية بعد معالجتها بالـ Spark.

---

## 📦 الحصول على المشروع وتحديثه (Clone & Pull)

للحصول على نسخة من المعمل وكل الأكواد، استخدم الأمر التالي:
```bash
git clone https://github.com/shehab-hub-0/Spark.git
cd Spark
```

عشان تحدث المعمل وتجيب آخر التغييرات اللي بتنزل عليه (زي النوت بوكس أو الإعدادات الجديدة)، افتح الـ Terminal جوة مجلد المشروع واكتب:
```bash
git pull origin main
```

---

## 🚀 طريقة التشغيل (Getting Started)

عشان تشغل المعمل على جهازك، اتبع الخطوات دي:


**أو يمكنك استخدام الأمر المباشر لبناء خدمات Spark والـ Data Lakehouse فقط:**
```bash
docker compose up -d --build
```

**أو يمكنك استخدام الأمر المباشر لتشغيل خدمات Spark والـ Data Lakehouse فقط:**
```bash
docker compose up -d
```


---

## 🌐 روابط الوصول للأدوات (Access URLs)

بعد ما المعمل يشتغل بنجاح، افتح المتصفح بتاعك وادخل على الروابط دي:

| الأداة (Service) | الرابط (URL) / المنفذ (Port) | الباسورد / المستخدم |
|---|---|---|
| **Jupyter Lab** (كتابة الكود) | [http://localhost:8889](http://localhost:8889) | `admin` |
| **Spark Master UI** (مراقبة المهام) | [http://localhost:8085](http://localhost:8085) | - |
| **Spark Worker 1** (العامل الأول) | [http://localhost:8092](http://localhost:8092) | - |
| **Spark Worker 2** (العامل الثاني) | [http://localhost:8093](http://localhost:8093) | - |
| **Spark History** (مراجعة المهام) | [http://localhost:18080](http://localhost:18080) | - |
| **MinIO Console** (واجهة المستخدم - رفع الداتا) | [http://localhost:9008](http://localhost:9008) | `minioadmin` / `minioadmin` |
| **MinIO API** (للاتصال البرمجي - S3) | `localhost:9005` | `minioadmin` / `minioadmin` |
| **PostgreSQL** (قاعدة البيانات) | `localhost:5432` | `postgres` / `postgres` |
| **Dremio** | [http://localhost:9047](http://localhost:9047) | - |
| **Nessie Catalog** | [http://localhost:19120](http://localhost:19120) | - |
| **ClickHouse UI** (واجهة الـ Play) | [http://localhost:8123/play](http://localhost:8123/play) | `admin` / `admin` |
| **ClickHouse Native** (للاتصال السريع) | `localhost:9009` | `admin` / `admin` |

---

## 📁 هيكل المجلدات (أين تضع ملفاتك؟)

كل اللي هتعمله جوة Jupyter Lab بيتحفظ مباشرة في المجلدات دي عندك على الجهاز، عشان ملفاتك متضيعش لو قفلت المعمل:

- 📂 `notebooks/` ⬅️ أي Jupyter Notebook هتعمله هيتحفظ هنا. (مربوط بـ `/home/jovyan/work` جوة Jupyter).
- 📂 `scripts/` ⬅️ لو حبيت تكتب سكريبتات Python عادية (مربوط بـ `/home/jovyan/scripts`).
- 📂 `data/` ⬅️ حط هنا أي ملفات CSV أو JSON عايز تقراها بالـ Spark (مربوط بـ `/data` جوة Jupyter).

> **💡 نصيحة:** المجلدات `notebooks/` و `scripts/` محمية ومش هتترفع على Git عشان تحافظ على حلك للتمارين خاص بيك.

---

## 🔗 تفاصيل الاتصال والإعدادات (Connectors)

جوة مجلد `connectors/` هتلاقي ملفات مهمة بتساعدك في إعداد بيئة العمل والاتصال بالأدوات المختلفة:

### 1. إعدادات VS Code (`connectors/spark_env.txt`)
الملف ده بيشرح إزاي تربط محرّر VS Code بتاعك مباشرة بالـ Jupyter Server اللي شغال جوة Docker عشان تقدر تكتب كود PySpark وتنفذه مباشرة في VS Code بدل المتصفح.
- **نوع الخادم:** Existing Jupyter Server
- **الرابط:** `http://127.0.0.1:8889/?token=admin`
- **بيئة التشغيل (Kernel):** Python 3 (ipykernel)

### 2. الاتصال وتجهيز الداتا (`connectors/ConnS.ipynb`)
النوت بوك ده بيحتوي على أكواد بايثون مساعدة بتعمل الآتي:
- **إعداد MinIO:** إنشاء الـ Buckets الأساسية زي `warehouse` و `spark-logs` وتجهيز مسار لـ Spark History.
- **ربط Dremio بـ Nessie:** كود بيعمل اتصال أوتوماتيكي بين Dremio و Nessie عبر الـ API الخاص بـ Dremio.
- **تنظيف البيانات (Clear Data):** بيحتوي على سكريبت لمسح الداتا من MinIO وجداول ClickHouse وتصفير Nessie Catalog لو احتجت تبدأ على نظافة (Clean Slate).

---

## 🛑 إيقاف المعمل (Stopping the Lab)

لما تخلص مذاكرة وتطبيق، الأفضل توقف المعمل عشان توفر موارد جهازك (البطارية والرامات).
افتح الـ Terminal في نفس المجلد واكتب:

```bash
docker compose down
```

*(أكوادك وملفاتك هتفضل محفوظة في المجلدات بتاعتها متقلقش!)*

بالتوفيق في رحلة التعلم! 🚀
