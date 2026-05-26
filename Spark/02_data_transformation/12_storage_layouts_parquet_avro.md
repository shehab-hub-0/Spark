# 📘 صيغ التخزين: Parquet vs Avro — الهيكل الداخلي والاختيار الأمثل

> [!IMPORTANT]
> **هدف هذا الدليل:**
> بنهاية هذا الملف، ستفهم لماذا نفس البيانات بـ Parquet تحتل 0.8 GB بينما بـ CSV تحتل 4.8 GB، كيف يقفز Spark على مجموعات صفوف كاملة دون قراءتها (Row Group Skipping)، ومتى تختار كل صيغة.

---

## 1. 🎯 لماذا صيغة التخزين تُحدد 80% من تكلفة عنقودك؟

```
نفس البيانات (100 مليون سجل مبيعات):
  CSV:          4.8 GB  ← 18.5 ثانية للقراءة
  Avro:         1.9 GB  ← 5.2 ثانية للقراءة
  Parquet:      0.8 GB  ← 0.4 ثانية (عمود واحد)!

Parquet = 6x أصغر من CSV و 5x أسرع!
```

**لماذا هذا الفرق الهائل؟**

البيانات التحليلية عادةً يقرأ منها **أعمدة قليلة** عبر **ملايين الصفوف**:
```sql
SELECT SUM(amount) FROM sales WHERE region = 'MENA'
-- يحتاج عمودين فقط: amount وregion
-- من جدول يحتوي 200 عمود!
```

- **CSV**: يقرأ جميع الـ 200 عمود لكل صف ثم يُهمل الـ 198 عمود الأخرى
- **Parquet**: يقرأ بايتات عمودي `amount` و`region` فقط، يُهمل الباقي تماماً

---

## 2. 🏗️ البنية الداخلية لـ Parquet: صف بصف أم عمود بعمود؟

### التخزين العمودي (Columnar) في Parquet

```
CSV (صف بعد صف):
  [id=1, name="Ali", amount=500, region="Cairo"]
  [id=2, name="Sara", amount=750, region="Alex"]
  [id=3, name="Omar", amount=200, region="Cairo"]
  
  لقراءة "amount" فقط: يجب قراءة كل الصفوف كاملة!

Parquet (عمود بعد عمود):
  ┌──────────────────────────────────────────────┐
  │                  ROW GROUP 1                 │
  │  Column "id":     [1, 2, 3, ...]             │
  │  Column "name":   ["Ali","Sara","Omar",...]   │
  │  Column "amount": [500, 750, 200, ...]        │
  │  Column "region": ["Cairo","Alex","Cairo"...] │
  └──────────────────────────────────────────────┘
  
  لقراءة "amount" فقط: يقرأ الـ Column Chunk الخاص به فقط!
```

### هيكل ملف Parquet بالكامل

```
┌─────────────────────────────────────────────────────────┐
│                   PARQUET FILE                          │
│  [Header: PAR1]                                         │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │  Row Group 1 (128 MB افتراضياً)                  │   │
│  │  ┌──────────────────────────────────────────┐   │   │
│  │  │ Column Chunk "id"                        │   │   │
│  │  │   Page 1 [1,2,...,1000] (1 MB)           │   │   │
│  │  │   Page 2 [1001,...,2000] (1 MB)          │   │   │
│  │  ├──────────────────────────────────────────┤   │   │
│  │  │ Column Chunk "region"                    │   │   │
│  │  │   Dictionary: {0:"Cairo", 1:"Alex",...}  │   │   │
│  │  │   Page 1: [0,1,0,0,1,...] (مضغوط جداً!)  │   │   │
│  │  └──────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────┘   │
│  [ Row Group 2 ] [ Row Group 3 ] ...                   │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │  FOOTER (الأهم!)                                  │   │
│  │  • Schema الكامل                                 │   │
│  │  • مواقع كل Row Group                            │   │
│  │  • إحصاءات min/max لكل عمود في كل Row Group     │   │
│  └─────────────────────────────────────────────────┘   │
│  [Footer Length] [PAR1]                                 │
└─────────────────────────────────────────────────────────┘
```

**الـ Footer هو سر Parquet!** Spark يقرأه أولاً (بضع KB فقط) ويعرف منه:
- ما الأعمدة المتاحة وأنواعها
- أين بايتات كل عمود في الملف
- قيم min/max لكل عمود في كل Row Group

---

## 3. ⚡ Row Group Skipping: كيف يتجاهل Spark بيانات بالكامل

هذه الميزة توفر 90%+ من الـ I/O في الاستعلامات المُصفَّاة:

```python
df = spark.read.parquet("s3://sales/") \
         .filter("amount > 50000") \
         .select("store_id", "amount")

df.explain(mode="formatted")
```

```
== Physical Plan ==
*(1) Project [store_id, amount]
+- *(1) Filter (amount > 50000)
   +- *(1) Scan parquet s3://sales/
      PushedFilters: [IsNotNull(amount), GreaterThan(amount,50000.0)]
      ReadSchema: struct<store_id:int,amount:double>
```

**ما الذي يحدث فيزيائياً:**
```
Spark يقرأ الـ Footer → يجد الـ Row Groups وإحصاءاتها:
  Row Group 1: amount min=100, max=5000
               → max < 50000 → تخطّي كاملاً! ❌ لا قراءة
  
  Row Group 2: amount min=1000, max=200000
               → قد تحتوي على قيم > 50000 → قراءة ✅
  
  Row Group 3: amount min=80000, max=900000
               → كلها > 50000 → قراءة ✅

النتيجة: تجاهل 70% من الـ Row Groups دون لمس بياناتها!
```

> [!TIP]
> **Pro Tip — تحسين Row Group Skipping:**
> كلما كانت البيانات مُرتَّبة حسب عمود الفلتر، كانت قيم min/max لكل Row Group أكثر تمييزاً → skipping أفضل!
>
> ```python
> # ✅ اكتب البيانات مرتبة حسب أكثر الأعمدة استخداماً في الفلاتر
> df.sortWithinPartitions("amount", "date") \
>   .write.parquet("s3://sales_sorted/")
> # الآن Row Groups لها min/max واضحة جداً!
> ```

---

## 4. 🗜️ تقنيات الضغط في Parquet: لماذا يحتل أقل مساحة؟

### Dictionary Encoding — ضغط القيم المتكررة

```
البيانات الأصلية (عمود "region"):
  ["Cairo", "Cairo", "Alex", "Cairo", "Giza", "Cairo", "Alex"]
  = 7 × ~6 bytes = 42 bytes

بعد Dictionary Encoding:
  Dictionary: {0: "Cairo", 1: "Alex", 2: "Giza"}
  Values:     [0, 0, 1, 0, 2, 0, 1]
  = 3 strings (16 bytes) + 7 ints (7 bytes) = 23 bytes
  → توفير 45%!

مع Run-Length Encoding (RLE) أيضاً:
  [0,0,1,0,2,0,1] → [(0,2), (1,1), (0,1), (2,1), (0,1), (1,1)]
  توفير إضافي!
```

### Bit-Packing — ضغط الأعداد الصغيرة

```
عمود "month" يحتوي قيم 1-12 فقط:
  عادةً: 4 bytes لكل رقم
  Bit-Packing: 4 bits تكفي (4 bits تستوعب 0-15)
  → توفير 50%!
```

---

## 5. 🏺 Apache Avro: متى يتفوق على Parquet؟

### هيكل Avro (صف بعد صف)

```
┌─────────────────────────────────────────────────────┐
│                    AVRO FILE                        │
│  ┌──────────────────────────────────────────────┐  │
│  │  Header                                       │  │
│  │    Magic Bytes: "Obj\1"                       │  │
│  │    Schema (JSON): {"type": "record",          │  │
│  │                    "fields": [...]}            │  │
│  │    Codec: snappy                              │  │
│  └──────────────────────────────────────────────┘  │
│                                                     │
│  [Block 1: 64 KB] [Sync Marker]                    │
│  [Block 2: 64 KB] [Sync Marker]                    │
│  ...                                               │
│  كل Block: سجلات كاملة متتالية                     │
└─────────────────────────────────────────────────────┘
```

### متى يكون Avro أفضل من Parquet؟

| السيناريو | الأفضل |
| :--- | :--- |
| Kafka Event Streaming (كتابة مستمرة لسجلات كاملة) | ✅ Avro |
| Schema Evolution (تغيير الـ Schema بدون كسر القراء القدامى) | ✅ Avro |
| ETL عبر Spark (تعديل كل الأعمدة) | ✅ Avro |
| Analytics SQL (SELECT بعض الأعمدة من ملايين الصفوف) | ✅ Parquet |
| ML Training Data (قراءة أعمدة محددة لـ Features) | ✅ Parquet |
| Long-term Storage (تقليل التكلفة) | ✅ Parquet |

---

## 6. 🗜️ Compression Codecs: الاختيار الصحيح

```python
# Snappy: الأسرع، ضغط متوسط
df.write.option("compression", "snappy").parquet("path/")

# Zstandard: أفضل ضغط مع أداء جيد
df.write.option("compression", "zstd").parquet("path/")

# Gzip: أعلى ضغط، أبطأ كثيراً
df.write.option("compression", "gzip").parquet("path/")

# None: بدون ضغط (للـ Debug فقط)
df.write.option("compression", "none").parquet("path/")
```

| Codec | سرعة الكتابة | سرعة القراءة | نسبة الضغط | الاستخدام المثالي |
| :--- | :--- | :--- | :--- | :--- |
| **None** | ⚡⚡⚡ | ⚡⚡⚡ | 1x | Debug فقط |
| **Snappy** | ⚡⚡⚡ | ⚡⚡⚡ | 2-3x | ملفات Shuffle المؤقتة |
| **Zstandard** | ⚡⚡ | ⚡⚡⚡ | 4-6x | ✅ الإنتاج (أفضل خيار) |
| **Gzip** | ⚡ | ⚡⚡ | 4-5x | عندما الـ Storage أغلى من الـ CPU |

> [!TIP]
> **Pro Tip:** في Spark 3+، استخدم **Zstandard** دائماً للإنتاج. يوفر ضغطاً أفضل من Snappy مع قراءة بنفس السرعة تقريباً، مما يقلل تكاليف S3 بشكل كبير.

---

## 7. 🚨 سيناريوهات الفشل وكيفية التشخيص

### حادثة 1: Type Mismatch في Parquet

```text
ERROR Executor: Exception in task 4.0 in stage 1.0
java.lang.UnsupportedOperationException: Parquet column type mismatch:
  expected INT64 but found INT32 in column transaction_id
```

**السبب:** ملفات قديمة كتبت `transaction_id` كـ `Int` (32-bit)، وملفات جديدة كتبتها كـ `Long` (64-bit). الـ Vectorized Reader لا يُعالج هذا التباين تلقائياً.

**الحل:**
```python
# الحل 1: تعطيل الـ Vectorized Reader (يعود لـ Java Reader أكثر مرونة)
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

# الحل 2: دمج الـ Schema يدوياً
spark.conf.set("spark.sql.parquet.mergeSchema", "true")
# ← سيدمج الـ Schema من كل الملفات (أبطأ لكن أكثر أماناً)

# الحل 3 (الأفضل): تحديد الـ Schema يدوياً
from pyspark.sql.types import *
schema = StructType([
    StructField("transaction_id", LongType(), True),  # Long دائماً
    ...
])
df = spark.read.schema(schema).parquet("s3://data/")
```

### حادثة 2: Driver OOM أثناء دمج الـ Schema

```text
Driver out of memory while merging Parquet schemas from 50,000 files.
java.lang.OutOfMemoryError: GC overhead limit exceeded
```

**السبب:** `spark.sql.parquet.mergeSchema=true` مع ملايين الملفات يجبر الـ Driver على قراءة كل الـ Footers.

**الحل:**
```python
# ❌ لا تستخدم mergeSchema مع ملفات كثيرة
spark.conf.set("spark.sql.parquet.mergeSchema", "false")

# ✅ استخدم Delta Lake أو Iceberg لإدارة الـ Schema evolution
# هذه الصيغ تخزن الـ Schema في transaction log منفصل
```

---

## 8. 🧪 التمارين العملية

### التمرين 1: مقارنة حجم الملفات عبر الصيغ

```python
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .master("local[4]") \
    .appName("FormatComparison") \
    .getOrCreate()

# بيانات اختبار: مليون سجل
df = spark.range(1, 1_000_001) \
    .selectExpr(
        "id",
        "cast(id % 50 as string) as region",    # Low-cardinality string
        "cast(id % 12 + 1 as int) as month",     # Low-cardinality int
        "rand() * 10000 as amount",               # Random double
        "concat('User_', cast(id as string)) as name"  # High-cardinality string
    )

# كتابة بكل الصيغ
df.write.mode("overwrite").csv("/tmp/compare.csv")
df.write.mode("overwrite").parquet("/tmp/compare.parquet")
df.write.mode("overwrite").option("compression", "snappy").parquet("/tmp/compare_snappy.parquet")
df.write.mode("overwrite").option("compression", "zstd").parquet("/tmp/compare_zstd.parquet")

def dir_size_mb(path):
    total = 0
    for f in os.scandir(path):
        if f.is_file():
            total += f.stat().st_size
    return total / (1024 * 1024)

print(f"CSV:              {dir_size_mb('/tmp/compare.csv'):.1f} MB")
print(f"Parquet (Snappy): {dir_size_mb('/tmp/compare_snappy.parquet'):.1f} MB")
print(f"Parquet (Zstd):   {dir_size_mb('/tmp/compare_zstd.parquet'):.1f} MB")
```

### التمرين 2: مشاهدة Column Pruning وRow Group Skipping

```python
import time

# كتابة Parquet مع بيانات كبيرة
df_large = spark.range(1, 10_000_001) \
    .selectExpr("id", "id * 2.5 as amount", "rand() as score", 
                "cast(id % 100 as string) as region")

df_large.write.mode("overwrite").parquet("/tmp/large_parquet")

# القراءة 1: كل الأعمدة
start = time.time()
spark.read.parquet("/tmp/large_parquet").count()
full_read = time.time() - start

# القراءة 2: عمود واحد فقط
start = time.time()
spark.read.parquet("/tmp/large_parquet").select("amount").count()
column_read = time.time() - start

# القراءة 3: مع فلتر (Row Group Skipping)
start = time.time()
spark.read.parquet("/tmp/large_parquet").filter("amount > 20000000").count()
filtered_read = time.time() - start

print(f"قراءة كل الأعمدة:    {full_read:.2f}s")
print(f"قراءة عمود واحد:     {column_read:.2f}s  ← Column Pruning")
print(f"قراءة مع فلتر:       {filtered_read:.2f}s ← Row Group Skipping")
```

### التمرين 3: اختبار Schema Evolution في Avro

```python
# Avro يدعم Schema Evolution بشكل أفضل من Parquet
# لاحظ الفرق عند إضافة عمود جديد

# كتابة Avro v1
df_v1 = spark.createDataFrame(
    [(1, "Alice"), (2, "Bob")],
    ["id", "name"]
)
df_v1.write.format("avro").mode("overwrite").save("/tmp/avro_v1")

# إضافة عمود جديد (v2)
df_v2 = spark.createDataFrame(
    [(3, "Charlie", "Cairo"), (4, "Diana", "Alex")],
    ["id", "name", "city"]
)
df_v2.write.format("avro").mode("overwrite").save("/tmp/avro_v2")

# قراءة الاثنين معاً (Schema Evolution)
df_merged = spark.read.format("avro").load("/tmp/avro_v1", "/tmp/avro_v2")
df_merged.show()
# السجلات القديمة ستظهر city = null
```

---

## 9. 🎓 أسئلة المقابلات التقنية

### سؤال 1: لماذا Parquet أسرع من Avro في استعلامات SELECT بعض الأعمدة؟

**الإجابة النموذجية:**
Parquet تُخزّن كل عمود في Chunk مستقل (Column Chunk) مع إحداثيات بايتية محددة. عند SELECT عمودين فقط من جدول بـ 200 عمود، يقرأ Spark بايتات تلك الـ Column Chunks مباشرة. أما Avro، فتُخزّن السجلات بالكامل صفاً بصف — لقراءة عمودين، يجب قراءة كل السجل وتجاهل الـ 198 عمود الأخرى. 

### سؤال 2: ما هو Row Group Skipping وكيف يعمل؟

**الإجابة النموذجية:**
كل ملف Parquet يحتوي على Footer يُخزّن إحصاءات (min/max) لكل عمود في كل Row Group. عند تطبيق فلتر مثل `amount > 50000`، يقرأ Spark الـ Footer أولاً ويفحص min/max لعمود `amount` في كل Row Group. إذا كان max للـ Row Group = 5000، فهذا يعني أنه لا توجد قيم > 50000 فيه → يُتخطّى كاملاً دون قراءة أي بيانات. يُحسّن الأداء بشكل كبير مع الفلاتر انتقائية.

### سؤال 3 (متقدم): لماذا يُفضّل Avro لـ Kafka Streams؟

**الإجابة النموذجية:**
Kafka يتعامل مع السجلات حدثاً حدثاً (individual events). كل حدث يُسلسَل كاملاً ويُرسَل. Avro يُسلسِل كل سجل كـ binary row بكفاءة عالية، ويُخزّن الـ Schema مرة واحدة في الـ Header (أو في Schema Registry). كل حدث يحمل قيمه فقط دون تكرار أسماء الأعمدة (كما في JSON). أيضاً، Schema Evolution في Avro مُصمَّمة لهذه الحالة: الـ Producer يُرسل بـ Schema V2، والـ Consumer يقرأ بـ Schema V1 بدون انهيار.

---

## 10. 📋 ورقة الغش السريعة

### أيهما تختار؟

```
هل العمل تحليلي (OLAP)؟
  ← اختر Parquet (أو Delta/Iceberg فوق Parquet)

هل تكتب من Kafka أو تحتاج Schema Evolution؟
  ← اختر Avro

هل تتبادل البيانات مع Excel أو نظام آخر؟
  ← اختر CSV (مع Manual Schema دائماً عند القراءة)
```

### إعدادات Parquet الأساسية

```python
# تحسين Row Group Size (أصغر = skipping أدق، أكبر = scan أسرع)
spark.conf.set("spark.sql.parquet.rowGroupSize", str(128 * 1024 * 1024))  # 128 MB

# اختيار الـ Codec
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")

# Vectorized Reader (تلقائياً مُفعَّل)
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")

# تعطيله عند وجود Type Conflicts
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
```

### مقارنة الصيغ

| الصيغة | الهيكل | الأفضل لـ | Schema | الضغط |
| :--- | :--- | :--- | :--- | :--- |
| **Parquet** | عمودي | OLAP, ML | Footer | ممتاز |
| **Avro** | صفي | Kafka, Ingestion | Header JSON | جيد |
| **ORC** | عمودي | Hive compat | Footer | ممتاز |
| **CSV** | صفي نصي | التبادل مع Excel | لا | لا |
| **Delta Lake** | Parquet + Log | ACID, Updates | Transaction Log | ممتاز |

> [!TIP]
> **الخطوة القادمة:** انتقل للملف `13_basic_data_wrangling.md` لتتعلم تحويلات البيانات الأساسية وكيف تُصحح وتُنظّف البيانات بكفاءة.

<!-- START_NAVIGATION_LINKS -->
---
### 🔗 روابط التنقل السريع

| السابق (Previous) | التالي (Next) |
| :--- | :--- |
| [◀️ 📘 قراءة وكتابة الملفات: Schema Inference، الـ Manual Schemas، وتقسيم الملفات (Partitioning)](11_reading_writing_files.md) | [▶️ 📘 تنظيف وتحضير البيانات (Data Wrangling): Select، Filter، Cast، وأخطاء الـ Null الخفية](13_basic_data_wrangling.md) |
<!-- END_NAVIGATION_LINKS -->
