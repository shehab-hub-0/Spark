# 📘 Lazy Evaluation والـ Caching: فن التحكم في متى وكيف يعمل Spark

> [!IMPORTANT]
> **هدف هذا الدليل:**
> بنهاية هذا الملف، ستفهم لماذا Lazy Evaluation ميزة وليست مشكلة، متى يستحق الـ Caching الجهد، وأيّ StorageLevel تختار لكل سيناريو إنتاجي.

---

## 1. 🎯 Lazy Evaluation: لماذا لا يعمل Spark فوراً؟

```python
# هذا الكود كاملاً لا ينفذ أي عملية على البيانات:
df = spark.read.parquet("s3://data/sales")  # قراءة؟ لا!
filtered = df.filter("amount > 1000")        # فلترة؟ لا!
grouped = filtered.groupBy("city")           # تجميع؟ لا!
result = grouped.sum("amount")               # حساب؟ لا!

# الوحيد الذي يُطلق التنفيذ:
final = result.collect()  # ← الآن فقط يعمل كل شيء!
```

**لماذا هذا مفيد؟**

لأنه يُعطي Catalyst فرصة رؤية **الصورة الكاملة** قبل التنفيذ:

```
بدون Lazy Evaluation (تنفيذ فوري):
  read() → تقرأ 100 GB كاملة
  filter() → تُمرّر 100 GB، تُبقي 10 GB
  groupBy() → تعمل على 10 GB

مع Lazy Evaluation (تنفيذ مؤجل):
  Catalyst يرى: read + filter + groupBy معاً
  يُحسّن: يُرسل الفلتر لـ Parquet reader مباشرة (PushDown)
  النتيجة: يقرأ 8 GB فقط (تجاهل 92% من الملفات!)
```

> [!TIP]
> **Pro Tip:** يمكنك التحقق أن الكود "لا يعمل بعد" بوضع مسار غير موجود — Spark لن يُخطئ حتى تستدعي Action:
>
> في كثير من مصادر البيانات قد لا يظهر خطأ المسار إلا عند الـ Action، لكن بعض القراءات قد تتحقق من الـ metadata أو schema مبكراً. الفكرة الصحيحة: التنفيذ الكامل للبيانات مؤجل، لا أن كل الأخطاء مؤجلة دائماً.

---

## 2. 🏗️ Actions vs Transformations: الفرق الجوهري

### Transformations (كسولة — تبني الخطة فقط)

```python
# كل هذه تُعيد DataFrame/RDD جديداً بدون تنفيذ
df.filter()         # Narrow Transformation
df.map()            # Narrow Transformation
df.groupBy()        # Wide Transformation (ستُنشئ Shuffle عند Action)
df.join()           # Wide Transformation
df.select()         # Narrow Transformation
df.withColumn()     # Narrow Transformation
```

### Actions (تُطلق التنفيذ الفعلي)

```python
# كل هذه تُطلق تنفيذ الخطة كاملة
df.collect()        # يُعيد جميع البيانات للـ Driver ← خطر!
df.count()          # يُعيد عدداً واحداً
df.show(20)         # يُطبع 20 سجل
df.first()          # يُعيد أول سجل
df.take(100)        # يُعيد أول 100 سجل
df.write.parquet()  # يكتب للتخزين
df.cache()          # ⚠️ هذه أيضاً Transformation! (الـ Cache يتم عند أول Action)
```

> [!WARNING]
> **Common Mistake:** كثيرون يعتقدون أن `df.cache()` يخزّن البيانات فوراً.
>
> **الحقيقة:** `.cache()` تضع **علامة** على الـ DataFrame "خزّنني عند التنفيذ". البيانات لا تُحمَّل للذاكرة إلا عند تنفيذ أول Action بعدها!
>
> ```python
> df.cache()       # فقط علامة — لا بيانات محملة
> df.count()       # الآن البيانات تُحمَّل للذاكرة + تُحسب
> df.show()        # الآن القراءة من الذاكرة (سريعة)
> ```

---

## 3. 📦 أنواع الـ Caching: اختر الصحيح

### مستويات التخزين (Storage Levels)

```
MEMORY_ONLY
  ← يخزن كـ JVM Objects في الذاكرة
  ← أسرع وصول
  ← أعلى استهلاك للذاكرة (كائنات Java تأخذ ضعف حجمها الأصلي)
  ← إذا لم تكفِ الذاكرة: لا تخزين (يُعاد الحساب عند الطلب)

MEMORY_AND_DISK
  ← يخزن في الذاكرة أولاً
  ← إذا نفدت الذاكرة: يكتب على القرص
  ← آمن لكن القرص أبطأ

MEMORY_ONLY_SER
  ← يخزن كـ Serialized Bytes في الذاكرة (أقل مساحة)
  ← يحتاج وقت Serialization/Deserialization عند القراءة
  ← مثالي للبيانات الكبيرة نسبياً

MEMORY_AND_DISK_SER
  ← مثل MEMORY_ONLY_SER لكن مع fallback للقرص
  ← الأكثر أماناً في الإنتاج

OFF_HEAP
  ← خارج JVM Heap عند تفعيل off-heap memory
  ← لا GC overhead
  ← يتطلب تفعيل: spark.memory.offHeap.enabled=true
```

### مقارنة مرئية

```
بيانات 10 GB (أصلية):

MEMORY_ONLY:      قد يكون أكبر بكثير بسبب JVM Object overhead
MEMORY_ONLY_SER:  أقل مساحة غالباً مقابل كلفة serialization
OFF_HEAP:         خارج الـ Heap لكنه يحتاج إعداداً ومراقبة دقيقة
```

---

## 4. ⚡ متى يستحق الـ Cache الجهد؟

### ✅ استخدم الـ Cache هنا

**1. البيانات تُستخدم أكثر من مرة في نفس Job:**
```python
# ✅ منطقي جداً للـ Caching
clean_df = raw_df.filter("is_valid = true") \
                 .withColumn("amount_usd", col("amount") * exchange_rate)

clean_df.cache()  # سيُقرأ من الملف مرة واحدة فقط!

# استخدامات متعددة
report1 = clean_df.groupBy("city").sum("amount_usd")
report2 = clean_df.groupBy("category").avg("amount_usd")
report3 = clean_df.filter("amount_usd > 10000").count()

report1.show()
report2.show()
report3
```

**2. Iterative ML Algorithms:**
```python
from pyspark.ml.classification import LogisticRegression

# بدون Cache: الـ Training data تُقرأ من القرص في كل iteration!
training_data = spark.read.parquet("s3://training/").cache()
training_data.count()  # تجسيد الـ Cache

model = LogisticRegression(maxIter=100).fit(training_data)
# 100 iteration × قراءة من الذاكرة بدلاً من القرص → أسرع بـ 10-50x
```

### ❌ لا تستخدم الـ Cache هنا

**1. البيانات تُستخدم مرة واحدة فقط:**
```python
# ❌ هدر للذاكرة
df.cache()
result = df.write.parquet("output/")  # استخدام واحد فقط!
df.unpersist()  # يجب تنظيفه يدوياً
```

**2. البيانات أكبر من الذاكرة المتاحة:**
```python
# ❌ خطر! إذا كانت البيانات 50 GB والذاكرة 30 GB
huge_df.cache()
huge_df.count()  # سيُكتب جزء منها على القرص ببطء شديد
# الحل: لا تقم بـ Cache، أو استخدم MEMORY_AND_DISK_SER
```

---

## 5. 🔬 الـ BlockManager: من يدير الذاكرة المخزنة؟

```
┌─────────────────────────────────────────┐
│              Executor JVM               │
│                                         │
│  ┌───────────────────────────────────┐  │
│  │         MemoryManager             │  │
│  │  ┌─────────────┐ ┌─────────────┐ │  │
│  │  │  Storage    │ │  Execution  │ │  │
│  │  │  Memory     │ │  Memory     │ │  │
│  │  │  (Cache +   │ │  (Sort +    │ │  │
│  │  │  Broadcast) │ │  Hash Agg)  │ │  │
│  │  │  60% (def.) │ │  40% (def.) │ │  │
│  │  └─────────────┘ └─────────────┘ │  │
│  └───────────────────────────────────┘  │
│                                         │
│  ┌───────────────────────────────────┐  │
│  │          BlockManager             │  │
│  │  يُدير مواقع جميع الـ Blocks      │  │
│  │  (Cached Partitions + Shuffles)   │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

**آلية الإخلاء (Eviction — LRU Policy):**
```
إذا امتلأت ذاكرة الـ Storage:
  1. BlockManager يُحدد الـ Block الأقل استخداماً مؤخراً (LRU)
  2. إذا MEMORY_ONLY: يُحذف Block → يُعاد حسابه لاحقاً من الـ Lineage
  3. إذا MEMORY_AND_DISK: يُكتب Block على القرص → يُقرأ من القرص لاحقاً
```

---

## 6. 🚨 أخطاء الـ Caching الشائعة

### الخطأ 1: تراكم الـ Cache وعدم تنظيفه

```python
# ❌ كود يُسبب نفاد الذاكرة التدريجي
for batch_id in range(100):
    df = spark.read.parquet(f"s3://data/batch_{batch_id}/")
    df_clean = df.filter("is_valid = true").cache()
    df_clean.count()  # تجسيد
    
    model.transform(df_clean).write.parquet(f"s3://output/batch_{batch_id}/")
    # ⚠️ نسي unpersist! كل iteration تُضيف 5 GB للذاكرة
    # بعد 6 iterations: OOM!

# ✅ الحل: تنظيف بعد كل استخدام
for batch_id in range(100):
    df = spark.read.parquet(f"s3://data/batch_{batch_id}/")
    df_clean = df.filter("is_valid = true").cache()
    df_clean.count()
    
    model.transform(df_clean).write.parquet(f"s3://output/batch_{batch_id}/")
    df_clean.unpersist()  # ✅ تنظيف فوري
```

### الخطأ 2: الـ Caching قبل عملية مُكلفة عوضاً عن بعدها

```python
# ❌ خطأ: تخزين البيانات الخام قبل التحويل المُكلف
raw_df.cache()  # تخزين 100 GB خام
result = raw_df.groupBy("city").sum("amount")  # تُعالج 100 GB في كل مرة

# ✅ صحيح: تخزين نتيجة العملية المُكلفة
result = raw_df.groupBy("city").sum("amount")
result.cache()  # تخزين فقط النتيجة (مثلاً 1 GB)
result.count()  # تجسيد

# الآن كل استخدام لـ result يقرأ من الذاكرة (1 GB) لا من القرص (100 GB)
```

### الخطأ 3: الـ Cache في الـ Streaming Pipeline

```python
# ❌ لا تقم بـ Cache في Streaming
streaming_df.cache()  # غير منطقي! البيانات تتدفق باستمرار
# استخدم Watermarks وState management بدلاً من الـ Cache
```

---

## 7. 🧪 التمارين العملية

### التمرين 1: مشاهدة فائدة الـ Cache

```python
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.master("local[4]").appName("CacheLab").getOrCreate()

# إنشاء DataFrame كبير نسبياً
df = spark.range(1, 10_000_000) \
    .selectExpr("id", "id * 2.5 as amount", "cast(id % 50 as string) as city")

expensive_df = df.filter("amount > 1000") \
                 .withColumn("tax", df.amount * 0.15)

# --- بدون Cache ---
start = time.time()
count1 = expensive_df.count()
time1 = time.time() - start

start = time.time()
avg1 = expensive_df.agg({"amount": "avg"}).collect()
time2 = time.time() - start

print(f"بدون Cache — count: {time1:.2f}s, avg: {time2:.2f}s")
print(f"إجمالي بدون Cache: {time1 + time2:.2f}s")

# --- مع Cache ---
expensive_df.cache()

start = time.time()
count2 = expensive_df.count()  # يُجسّد الـ Cache هنا
time3 = time.time() - start

start = time.time()
avg2 = expensive_df.agg({"amount": "avg"}).collect()  # من الذاكرة!
time4 = time.time() - start

print(f"\nمع Cache — count: {time3:.2f}s, avg: {time4:.2f}s")
print(f"إجمالي مع Cache: {time3 + time4:.2f}s")
print(f"\nتسريع الـ Cache: {(time1+time2)/(time3+time4):.1f}x أسرع")

expensive_df.unpersist()
```

### التمرين 2: مراقبة الـ Cache في Spark UI

```python
from pyspark import StorageLevel

# إنشاء DataFrames بمستويات تخزين مختلفة
df_large = spark.range(1, 5_000_000).selectExpr("id", "id * 3.0 as value")

# مستوى 1: MEMORY_ONLY
df_mem = df_large.persist(StorageLevel.MEMORY_ONLY)
df_mem.count()
print("افتح http://localhost:4040/storage وشاهد الحجم المستخدم")
input("اضغط Enter للمتابعة...")

# إلغاء ثم استخدام MEMORY_AND_DISK_SER
df_mem.unpersist()
df_ser = df_large.persist(StorageLevel.MEMORY_AND_DISK_SER)
df_ser.count()
print("قارن الحجم مع المستوى السابق في Spark UI")
input("اضغط Enter للمتابعة...")

df_ser.unpersist()
```

### التمرين 3: الفرق بين Cache و Checkpoint

```python
import os

sc = spark.sparkContext

# --- Cache ---
rdd = sc.parallelize(range(1, 1000000), 10)
for i in range(10):
    rdd = rdd.map(lambda x: x + 1)

rdd.cache()
rdd.count()  # تجسيد

print("Cache — Lineage (طويل):")
print(rdd.toDebugString().decode("utf-8")[:300])

# --- Checkpoint (يقطع الـ Lineage) ---
rdd2 = sc.parallelize(range(1, 1000000), 10)
for i in range(10):
    rdd2 = rdd2.map(lambda x: x + 1)

sc.setCheckpointDir("/tmp/checkpoint_test")
rdd2.checkpoint()
rdd2.count()  # يُكتب على القرص

print("\nCheckpoint — Lineage (قصير جداً!):")
print(rdd2.toDebugString().decode("utf-8")[:200])
```

---

## 8. 🎓 أسئلة المقابلات التقنية

### سؤال 1: ما الفرق بين `cache()` و `persist(StorageLevel)` و `checkpoint()`؟

**الإجابة النموذجية:**

| المعيار | `cache()` | `persist(level)` | `checkpoint()` |
| :--- | :--- | :--- | :--- |
| الـ Default Level | في PySpark DataFrame: `MEMORY_AND_DISK_DESER` في الإصدارات الحديثة؛ راجع إصدارك | حسب ما تُحدد | - |
| يقطع الـ Lineage؟ | ❌ لا | ❌ لا | ✅ نعم |
| مكان التخزين | Executor Memory/Disk | حسب المستوى | HDFS/S3 |
| يبقى بعد إغلاق App؟ | ❌ لا | ❌ لا | ✅ نعم |
| متى تستخدم؟ | بيانات تُستخدم مرتين | تحكم في المستوى | Lineage طويل أو ML loops |

### سؤال 2: ماذا يحدث إذا نفدت ذاكرة الـ Executor أثناء الـ Cache؟

**الإجابة النموذجية:**
يعتمد على الـ StorageLevel:
- `MEMORY_ONLY`: يتم إخلاء (Evict) الـ Partitions الأقل استخداماً (LRU). الـ Partitions المُخلاة **تُحذف** ولا تُكتب على القرص. ستُعاد من الـ Lineage عند الطلب (قد يكون مُكلفاً!).
- `MEMORY_AND_DISK`: يُكتب الـ Partition الأقل استخداماً على القرص بدلاً من حذفه. أبطأ لكن لا إعادة حساب.

### سؤال 3 (متقدم): ما الفرق بين Storage Memory وExecution Memory في الـ MemoryManager؟

**الإجابة النموذجية:**
- **Storage Memory:** للـ Cache والـ Broadcast variables. إذا نفدت، يتم إخلاء الـ Cache المُخزّن.
- **Execution Memory:** للعمليات الحسابية مثل الـ Hash Joins والـ Aggregations والـ Sorting. إذا نفدت، تبدأ البيانات في **Spill** على القرص.
- الحاجز بينهما **ديناميكي** في Spark (Unified Memory Model): إذا كانت الـ Storage Memory لا تُستخدم، يمكن للـ Execution Memory استخدامها والعكس صحيح.

---

## 9. 📋 ورقة الغش السريعة

### اختيار مستوى الـ Storage

```python
from pyspark import StorageLevel

# بيانات صغيرة تُستخدم كثيراً → MEMORY_ONLY (الأسرع)
df.persist(StorageLevel.MEMORY_ONLY)

# بيانات كبيرة لكن متاحة في الذاكرة → MEMORY_ONLY_SER (أقل مساحة)
df.persist(StorageLevel.MEMORY_ONLY_SER)

# بيانات قد تتجاوز الذاكرة → MEMORY_AND_DISK (الآمن)
df.cache()  # أو: df.persist(StorageLevel.MEMORY_AND_DISK)

# بيئة Spot Instances (خطر انهيار مفاجئ) → MEMORY_AND_DISK_SER
df.persist(StorageLevel.MEMORY_AND_DISK_SER)

# تحرير الـ Cache بعد الانتهاء (دائماً!)
df.unpersist()
```

### Actions التي تُجسّد الـ Cache

```python
df.cache()

# هذه تُجسّد الـ Cache (تختار أسرعها):
df.count()          # ✅ الأسرع والأخف
df.first()          # ✅ سريع جداً
df.take(1)          # ✅ سريع

# هذه تعمل لكن أبطأ:
df.show()           # يُطبع + يُجسّد
df.collect()        # ⚠️ خطر OOM على البيانات الكبيرة
```

### فحص الـ Cache عبر Spark UI

```
http://localhost:4040/storage

يُظهر:
- اسم الـ RDD/DataFrame المُخزَّن
- نسبة الـ Partitions المُخزَّنة (Cached Partitions / Total)
- حجم الذاكرة المستخدمة
- حجم القرص المستخدم (إذا Spilled)
```

> [!TIP]
> **الخطوة القادمة:** انتقل للملف `10_deploy_modes_networking.md` لتفهم الفرق بين Client Mode وCluster Mode وكيف تؤثر شبكة الاتصال على أداء التطبيق.

<!-- START_NAVIGATION_LINKS -->
---
### 🔗 روابط التنقل السريع

| السابق (Previous) | التالي (Next) |
| :--- | :--- |
| [◀️ 📘 التبعيات الضيقة والواسعة: مفتاح تصميم Pipelines عالية الأداء](08_narrow_vs_wide_dependencies.md) | [▶️ 📘 أوضاع النشر (Deploy Modes): Client Mode vs Cluster Mode — الشبكة والإنتاج](10_deploy_modes_networking.md) |
<!-- END_NAVIGATION_LINKS -->
