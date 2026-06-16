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

1. **Jupyter Workspace (`ide-jupyter-workspace`)**: بيئة كتابة الأكواد الرئيسية (IDE). هنا هتفتح الـ Notebooks وتكتب كود PySpark بتاعك.
2. **Spark Cluster (`compute-spark-master` & `compute-spark-worker-1/2`)**: المحرك الرئيسي لمعالجة البيانات. متكون من مدير (Master) وعاملين (Workers) عشان ينفذوا الكود بتاعك بشكل موزع (Distributed).
3. **Spark History Server (`compute-spark-history`)**: واجهة بتسمحلك تراجع وتحلل الـ Jobs اللي خلصت وتفهم إزاي الـ Spark نفذها خطوة بخطوة.
4. **MinIO (`storage-minio`)**: بيمثل الـ **Data Lake** بتاعك. هو نظام تخزين متوافق مع S3، هترفع عليه الملفات الخام (Raw Data) عشان الـ Spark يقراها.
5. **PostgreSQL (`core-postgres`)**: قاعدة بيانات علائقية تُستخدم كخلفية لحفظ البيانات الوصفية (Metadata) للمعمل.
6. **Project Nessie (`meta-nessie`) & Dremio (`query-dremio`)**: أدوات بناء الـ **Data Lakehouse** الحديثة لتنظيم وإدارة البيانات.
7. **ClickHouse (`dw-clickhouse`)**: بيمثل الـ **Data Warehouse**. قاعدة بيانات تحليلية سريعة جداً ممكن تستخدمها عشان تخزن النتايج النهائية بعد معالجتها بالـ Spark.

---

## 🚀 طريقة التشغيل (Getting Started)

عشان تشغل المعمل على جهازك، اتبع الخطوات دي:

### 1. إعداد البيئة (أول مرة فقط)
اعمل نسخة من ملف الإعدادات:
```bash
cp .env.example .env
```
*(لو إنت شغال على Linux/Mac، اتأكد إن السكريبت معاه صلاحية التشغيل: `chmod +x build_and_push.sh`)*

### 2. تشغيل المعمل
شغل السكريبت المساعد:
```bash
./build_and_push.sh
```

**السكريبت هيسألك سؤالين:**
1. `Do you want to pull and run all services or just the Spark stack? (all/spark) [all]:`
   - 👉 **اكتب `spark` واضغط Enter.** (ده هيشغل خدمات المعمل الخاصة بيك بس ويوفر موارد جهازك).
2. `Do you want to build new images or just start existing ones? (build/up) [build]:`
   - 👉 **اكتب `build` في أول مرة تشغل فيها المعمل.** (عشان يحمل ويبني الصور الأساسية).
   - 👉 **في المرات الجاية، اكتب `up` واضغط Enter.** (عشان يشغل المعمل بسرعة بدون ما يحمل من النت تاني).

---

## 🌐 روابط الوصول للأدوات (Access URLs)

بعد ما المعمل يشتغل بنجاح، افتح المتصفح بتاعك وادخل على الروابط دي:

| الأداة (Service) | الرابط (URL) | الباسورد (لو طلب) |
|---|---|---|
| **Jupyter Lab** (كتابة الكود) | [http://localhost:8889](http://localhost:8889) | `admin` |
| **Spark Master UI** (مراقبة العمال) | [http://localhost:8085](http://localhost:8085) | - |
| **Spark History** (مراجعة المهام) | [http://localhost:18080](http://localhost:18080) | - |
| **MinIO Console** (رفع الداتا) | [http://localhost:9008](http://localhost:9008) | `admin` / `admin123` |
| **Dremio** | [http://localhost:9047](http://localhost:9047) | - |
| **Nessie Catalog** | [http://localhost:19120](http://localhost:19120) | - |

---

## 📁 هيكل المجلدات (أين تضع ملفاتك؟)

كل اللي هتعمله جوة Jupyter Lab بيتحفظ مباشرة في المجلدات دي عندك على الجهاز، عشان ملفاتك متضيعش لو قفلت المعمل:

- 📂 `notebooks/` ⬅️ أي Jupyter Notebook هتعمله هيتحفظ هنا. (مربوط بـ `/home/jovyan/work` جوة Jupyter).
- 📂 `scripts/` ⬅️ لو حبيت تكتب سكريبتات Python عادية (مربوط بـ `/home/jovyan/scripts`).
- 📂 `data/` ⬅️ حط هنا أي ملفات CSV أو JSON عايز تقراها بالـ Spark (مربوط بـ `/data` جوة Jupyter).

> **💡 نصيحة:** المجلدات `notebooks/` و `scripts/` محمية ومش هتترفع على Git عشان تحافظ على حلك للتمارين خاص بيك.

---

## 🛑 إيقاف المعمل (Stopping the Lab)

لما تخلص مذاكرة وتطبيق، الأفضل توقف المعمل عشان توفر موارد جهازك (البطارية والرامات).
افتح الـ Terminal في نفس المجلد واكتب:

```bash
docker compose down
```

*(أكوادك وملفاتك هتفضل محفوظة في المجلدات بتاعتها متقلقش!)*

بالتوفيق في رحلة التعلم! 🚀
