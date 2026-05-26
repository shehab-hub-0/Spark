# Apache Spark Core Architecture — الدليل الشامل من الصفر للسينيور

> هذا الدليل يجمع وحدة **Core Architecture** في مسار واحد مترابط. الهدف ليس تلخيص الملفات، بل بناء فهم تدريجي: لماذا وُجد Spark، كيف يفكر، كيف ينفذ، أين يبطؤ، وكيف تتخذ قرارات إنتاجية صحيحة.

---

## المحتويات

1. الحوسبة الموزعة و MapReduce
2. طوبولوجيا Spark: Driver وExecutors وWorkers
3. مدراء الموارد: YARN وKubernetes وStandalone
4. دورة حياة SparkSession وPy4J
5. RDD والـ Lineage والتسامح مع الأعطال
6. DataFrames وDatasets وCatalyst وTungsten
7. نموذج التنفيذ: DAG وStages وTasks
8. Narrow vs Wide Dependencies
9. Lazy Evaluation والـ Caching
10. Deploy Modes والشبكات في الإنتاج
11. Roadmap لما بعد هذه الوحدة

---

## مقدمة: لماذا يجب أن تفهم المعمارية؟

Spark ليس مجرد مكتبة تكتب بها `filter` و`groupBy`. هو نظام موزع يحول الكود البسيط إلى آلاف المهام التي تعمل على أجهزة مختلفة، وتقرأ من التخزين، وتنقل بيانات عبر الشبكة، وتكتب ملفات مؤقتة، وتعيد المحاولة عند الفشل.

لو تعاملت معه كأنه Pandas كبير، ستقع في مشاكل مثل:

- `collect()` على بيانات ضخمة فينهار الـ Driver.
- `groupByKey()` فينتقل حجم هائل عبر الشبكة.
- `cache()` بدون `unpersist()` فتتآكل ذاكرة الـ Executors تدريجياً.
- تشغيل Jobs إنتاجية بـ Client Mode فتفشل بسبب Firewall أو انقطاع شبكة جهاز الإرسال.

التشبيه البسيط: Spark مثل مصنع كبير. الكود الذي تكتبه هو أمر الإنتاج، لكن الأداء الحقيقي يعتمد على ترتيب خط الإنتاج، عدد العمال، مسارات النقل، المخازن المؤقتة، والمكان الذي يقف فيه المدير. هذا الدليل يشرح المصنع من الداخل.

---

## 1. الحوسبة الموزعة و MapReduce

### لماذا نحتاج الحوسبة الموزعة؟

لو لديك 10 تيرابايت بيانات، لا يمكنك غالباً تحميلها على جهاز واحد. الحل القديم كان شراء جهاز أكبر: RAM أكثر، CPU أكثر، قرص أسرع. هذا يسمى **Scale-Up**. لكنه يصطدم بثلاث مشكلات: تكلفة عالية، حدود فيزيائية، ونقطة فشل واحدة.

الحل العملي هو **Scale-Out**: قسّم البيانات والعمل على عشرات أو مئات الأجهزة.

```
بدلاً من:
  جهاز واحد يحاول حمل 10 TB

نستخدم:
  100 جهاز، كل جهاز يعالج جزءاً صغيراً

لو فشل جهاز:
  يعاد تشغيل الجزء الخاص به على جهاز آخر
```

التشبيه: لا تنقل بيتاً كاملاً بشاحنة واحدة ضخمة؛ وزّع الأثاث على شاحنات كثيرة. لو تعطلت شاحنة، لا تتوقف العملية كلها.

### MapReduce: الثورة الأولى

MapReduce قدّم نموذجاً بسيطاً لمعالجة البيانات الضخمة:

- **Map:** حوّل كل سجل إلى أزواج مفتاح/قيمة.
- **Shuffle & Sort:** اجمع القيم التي لها نفس المفتاح.
- **Reduce:** لخّص قيم كل مفتاح.

مثال عد الكلمات:

```text
"Hello World"  -> ("Hello", 1), ("World", 1)
"Hello Spark"  -> ("Hello", 1), ("Spark", 1)

بعد الـ Shuffle:
"Hello" -> [1, 1]
"World" -> [1]
"Spark" -> [1]

بعد الـ Reduce:
"Hello" -> 2
"World" -> 1
"Spark" -> 1
```

### أين كانت المشكلة؟

MapReduce قوي، لكنه مكلف لأنه يعتمد على القرص بين المراحل.

```
Input -> Map -> Disk -> Shuffle -> Disk -> Reduce -> HDFS
```

أي Pipeline متعدد الخطوات يصبح سلسلة Jobs منفصلة، وكل Job يقرأ ويكتب على القرص. لو العملية فيها 4 مراحل، قد تقرأ وتكتب نفس البيانات مرات كثيرة.

المشكلة الأخرى هي الحاجز الصارم: لا يبدأ الـ Reduce قبل انتهاء كل الـ Map tasks. Task بطيئة واحدة يمكنها حبس المرحلة كلها.

### Spark: الفكرة التي غيّرت اللعبة

Spark لا ينفذ كل خطوة فوراً. هو يبني **DAG**: رسم بياني لكل العمليات، ثم يحسّنه، ثم ينفذه بأقل عدد ممكن من المرور على البيانات.

```
قراءة Parquet
  -> Filter
  -> Partial Aggregate
  -> Shuffle عند الضرورة
  -> Final Aggregate
  -> Write
```

العمليات الضيقة مثل `filter` و`map` يمكن دمجها في Task واحدة، داخل الذاكرة، بدون قرص أو شبكة. Spark يكتب على القرص غالباً عند الـ Shuffle أو عند الكتابة النهائية.

### مثال كود وشرحه

```python
df = spark.read.parquet("s3a://logs/raw_traffic") \
    .filter("status == 'ERROR'") \
    .groupBy("service") \
    .count()

df.explain(mode="formatted")
```

شرح السطور:

- `spark.read.parquet(...)`: يبني خطة قراءة من ملفات Parquet. لا يقرأ فعلياً بعد.
- `.filter(...)`: يضيف شرطاً للخطة. Catalyst قد يدفعه لقارئ Parquet عبر Predicate Pushdown.
- `.groupBy("service")`: يطلب تجميع البيانات حسب الخدمة. هذا غالباً يحتاج Shuffle لأن نفس الخدمة قد توجد في Partitions مختلفة.
- `.count()`: يعرّف التجميع المطلوب.
- `explain(...)`: يعرض خطة التنفيذ، وفيها تبحث عن `Exchange` لمعرفة مكان الـ Shuffle.

### ASCII Diagram

```text
MapReduce:
  Data -> Map -> Disk -> Shuffle -> Disk -> Reduce -> HDFS

Spark:
  Data -> Filter -> Map -> Partial Agg -> Shuffle -> Final Agg -> Output
                ^^^^^^^^^^^^^^^^^^^^^
                غالباً داخل نفس الـ Stage
```

### ملاحظة السينيور

لا تقل فقط "Spark أسرع لأنه يستخدم الذاكرة". الجملة الأدق: Spark أسرع لأنه يبني خطة كاملة، يدمج الـ Narrow transformations، يقلل المرور على القرص، ويؤخر الـ Shuffle لما يكون ضرورياً. الذاكرة جزء من القصة، وليست القصة كلها.

---

## 2. طوبولوجيا Spark: Driver وExecutors وWorkers

### لماذا الطوبولوجيا مهمة؟

عندما تكتب:

```python
df.groupBy("country").count().collect()
```

يبدو الأمر كسطر واحد. لكن خلفه:

1. الـ Driver يبني الخطة.
2. الـ Driver يقسمها إلى Stages وTasks.
3. الـ Executors تنفذ Tasks على Partitions.
4. النتائج الجزئية ترجع للـ Driver.

إذا لم تفهم من هو الـ Driver، لن تعرف لماذا `collect()` خطر.

### المكونات الأساسية

| المكون | دوره |
| :--- | :--- |
| Driver | العقل المدبر: يبني DAG، يرسل Tasks، يجمع النتائج الصغيرة |
| Executor | عملية JVM تنفذ المهام وتخزن Cache وShuffle files |
| Worker | خدمة/خادم يستضيف Executors، ولا ينفذ كودك مباشرة |
| Cluster Manager | يخصص الموارد ويطلق Executors |

التشبيه: الـ Driver هو مدير الورشة، الـ Executors هم العمال، الـ Worker هو المبنى الذي يعمل فيه العمال، وCluster Manager هو قسم الموارد الذي يقرر كم عامل ومكانهم.

### ما بداخل الـ Driver؟

- **DAGScheduler:** يقسم الـ DAG إلى Stages.
- **TaskScheduler:** يرسل Tasks للـ Executors ويتابع الفشل.
- **SchedulerBackend:** يتحدث مع مدير الموارد.
- **BlockManagerMaster:** يعرف أماكن الـ Cached blocks وShuffle metadata.

إذا مات الـ Driver، يموت التطبيق غالباً؛ لأنه يحمل حالة التخطيط والتنسيق.

### ما بداخل الـ Executor؟

```text
Executor JVM
├── Task Thread Pool
│   ├── Thread 1 -> Partition 0
│   ├── Thread 2 -> Partition 1
│   └── ...
├── BlockManager
│   ├── Cached Partitions
│   └── Shuffle Files
└── MemoryManager
    ├── Execution Memory
    └── Storage Memory
```

عدد المهام المتوازية داخل Executor يساوي غالباً `spark.executor.cores`.

### مثال كود وشرحه

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[4]") \
    .appName("TopologyInspector") \
    .getOrCreate()

sc = spark.sparkContext

print(sc.master)
print(sc.defaultParallelism)
```

شرح السطور:

- `master("local[4]")`: يشغل Spark محلياً بأربعة Threads.
- `appName(...)`: يحدد اسم التطبيق في Spark UI.
- `getOrCreate()`: ينشئ أو يعيد SparkSession موجودة.
- `spark.sparkContext`: نقطة الوصول للطبقة الأساسية.
- `sc.defaultParallelism`: غالباً عدد الـ cores/threads المتاحة افتراضياً.

### حساب موارد Executor

لو الخادم لديه 16 Core و64 GB RAM:

```text
اختيار شائع:
  4 cores لكل Executor

عدد Executors لكل خادم:
  16 / 4 = 4
  احجز Core للنظام
  عملياً: 3 Executors

RAM لكل Executor:
  (64 - 2 للنظام) / 3 ~= 20 GB
  اترك overhead
  executor.memory ~= 18g
```

### ملاحظة السينيور

Executor واحد بـ 16 cores وذاكرة ضخمة ليس دائماً أفضل. قد يسبب GC pauses طويلة، فتتأخر الـ Heartbeats ويظن الـ Driver أن الـ Executor مات. قاعدة 4-5 cores لكل Executor ليست قانوناً مقدساً، لكنها نقطة بداية جيدة.

---

## 3. مدراء الموارد: YARN وKubernetes وStandalone

### لماذا يحتاج Spark مدير موارد؟

Spark محرك تنفيذ، وليس نظام إدارة عنقود كامل. يحتاج جهة تجيب عن أسئلة مثل:

- أين توجد موارد فارغة؟
- كم ذاكرة وCPU يحصل عليها كل تطبيق؟
- كيف نطلق Executor جديد؟
- ماذا يحدث عند موت خادم؟

هذه مهمة **Cluster Manager**.

### YARN

YARN شائع في بيئات Hadoop التقليدية.

```text
ResourceManager
  -> يخصص Container للـ ApplicationMaster
ApplicationMaster
  -> يطلب Containers للـ Executors
Executors
  -> تتحدث مباشرة مع Spark Driver
```

في Cluster Mode على YARN، الـ ApplicationMaster هو غالباً الـ Driver نفسه.

### Kubernetes

في Kubernetes، كل شيء Pods:

```text
spark-submit -> K8s API Server -> Driver Pod
Driver Pod -> ينشئ Executor Pods
Executor Pods -> تنفذ Tasks
```

الميزة الكبرى: عزل بالحاويات، تكامل مع CI/CD، Secrets، IAM، Observability.

الشرط المهم: الـ Driver Pod يحتاج ServiceAccount بصلاحيات إنشاء ومراقبة وحذف Pods.

### Standalone

Standalone هو أبسط وضع: Spark Master وSpark Workers فقط. مناسب للتعلم أو لعناقيد مخصصة لـ Spark.

```text
Standalone Master
  -> يراقب Workers
  -> يأمر Workers بإطلاق Executors
```

### مشكلة Dynamic Allocation والـ Shuffle

Dynamic Allocation يحذف Executors الخاملة. لكن ماذا لو كان Executor الخامل يحتفظ بملفات Shuffle يحتاجها Stage لاحق؟

```text
Stage 0 يكتب Shuffle على Executor 3
Executor 3 يصبح idle
Dynamic Allocation يحذفه
Stage 1 يحتاج ملفات Executor 3
FetchFailedException
```

الحلول:

- YARN: External Shuffle Service.
- Kubernetes: Shuffle Tracking أو Remote Shuffle Service.
- Standalone: يعتمد على الإعدادات والدعم المتاح.

### مثال إعداد Kubernetes مختصر

```bash
spark-submit \
  --master k8s://https://kubernetes.default.svc \
  --deploy-mode cluster \
  --conf spark.kubernetes.namespace=data-platform \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa \
  --conf spark.executor.instances=10 \
  app.py
```

شرح السطور:

- `--master k8s://...`: يوجه Spark إلى Kubernetes API.
- `--deploy-mode cluster`: يجعل الـ Driver داخل العنقود.
- `namespace`: أين تنشأ Pods.
- `serviceAccountName`: الصلاحيات التي يستخدمها الـ Driver لإنشاء Executors.
- `executor.instances`: عدد Executors المطلوب.

### ملاحظة السينيور

اختيار مدير الموارد قرار منصة، لا قرار Spark فقط. لو لديك Hadoop قائم، YARN منطقي. لو تبني منصة سحابية حديثة، Kubernetes غالباً أفضل. لو تريد عنقود Spark بسيطاً ومغلقاً، Standalone يكفي. لا تختر بناءً على الموضة؛ اختر بناءً على التشغيل والمراقبة والأمان المتاح لديك.

---

## 4. دورة حياة SparkSession وPy4J

### ماذا يحدث عند `getOrCreate()`؟

هذا السطر:

```python
spark = SparkSession.builder.appName("MyApp").getOrCreate()
```

لا ينشئ مجرد كائن Python. في PySpark يحدث الآتي:

1. Python يبحث عن SparkSession موجودة.
2. إذا لم توجد، يطلق JVM للـ Driver.
3. ينشأ Py4J Gateway بين Python والـ JVM.
4. JVM يقرأ SparkConf.
5. ينشئ SparkEnv ومكوناته الداخلية.
6. يبدأ Spark UI.
7. يتصل بمدير الموارد ويطلب Executors.

### Python وJVM: عمليتان لا واحدة

```text
Python Process
  df.filter(...)
      |
      | Py4J Socket
      v
Driver JVM
  Catalyst, DAGScheduler, Spark Core
```

هذا مهم جداً في UDFs. Python UDF لا تعمل داخل JVM مباشرة، بل تحتاج نقل بيانات بين JVM وعملية Python، لذلك هي أبطأ من Spark SQL expressions.

### ترتيب أولوية الإعدادات

تقريبياً، الإعداد الأحدث والأقرب للكود يغلب:

```text
Spark defaults
-> spark-defaults.conf
-> spark-submit --conf
-> builder.config(...)
```

### مثال إنتاجي وشرحه

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ProductionETL") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memoryOverhead", "1g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()
```

شرح السطور:

- `executor.memory`: ذاكرة JVM Heap لكل Executor.
- `executor.cores`: عدد المهام المتوازية داخل Executor.
- `memoryOverhead`: ذاكرة خارج الـ Heap، مهمة لـ PySpark وNIO buffers.
- `adaptive.enabled`: يفعّل AQE لإعادة تحسين الخطة أثناء التنفيذ.
- `KryoSerializer`: غالباً أسرع وأصغر من Java Serializer.

### خطأ شائع: `getOrCreate()` بإعدادات مختلفة

```python
spark1 = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark2 = SparkSession.builder \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()
```

`spark2` غالباً سيعيد نفس الجلسة الأولى، ولن تحصل على 8g. إذا أردت إعداداً جديداً، أوقف الجلسة القديمة أولاً.

### ملاحظة السينيور

في Airflow أو notebooks طويلة العمر، جلسات Spark القديمة قد تظل موجودة وتبتلع الإعدادات الجديدة. عند تشخيص سلوك غريب، اطبع `spark.sparkContext.getConf().getAll()` ولا تثق أن الكود الأخير هو ما يعمل فعلاً.

---

## 5. RDD والـ Lineage والتسامح مع الأعطال

### ما هو RDD؟

RDD اختصار لـ **Resilient Distributed Dataset**. الفكرة العميقة ليست أنه "بيانات موزعة" فقط، بل أنه يعرف كيف يعيد بناء نفسه.

بدلاً من حفظ كل نتيجة وسيطة على القرص، Spark يحفظ **الوصفة**:

```text
اقرأ من المصدر
ثم filter
ثم map
ثم reduceByKey
```

إذا فقد Partition، يطبق Spark الوصفة على هذا الجزء فقط أو يعيد Stage حسب نوع التبعية.

### مكونات RDD داخلياً

```text
RDD
├── Partitions
├── Compute function
├── Dependencies
├── Partitioner اختياري
└── Preferred locations اختياري
```

### Lineage مثال وشرح

```python
base = sc.parallelize([1, 2, 3, 4, 5, 6], 2)
doubled = base.map(lambda x: x * 2)
filtered = doubled.filter(lambda x: x > 6)
paired = filtered.map(lambda x: (x % 3, x))
grouped = paired.groupByKey()

print(grouped.toDebugString().decode("utf-8"))
```

شرح السطور:

- `parallelize(..., 2)`: ينشئ RDD من قائمة، مقسماً إلى Partitionين.
- `map`: Dependency ضيقة؛ كل Partition ينتج Partition مقابلاً.
- `filter`: ضيقة أيضاً.
- `map` إلى أزواج: ما زالت ضيقة.
- `groupByKey`: Wide dependency؛ يحتاج تجميع القيم حسب المفتاح عبر Partitions.
- `toDebugString`: يكشف Lineage وستجد `ShuffledRDD`.

### Narrow vs Wide في التعافي

```text
Narrow failure:
  فقد Partition واحد
  -> أعد حساب هذا الـ Partition غالباً

Wide failure:
  فقد Shuffle output
  -> قد تضطر لإعادة Stage كاملة
```

### Cache vs Checkpoint

| المعيار | Cache | Checkpoint |
| :--- | :--- | :--- |
| الهدف | تسريع إعادة الاستخدام | قطع Lineage |
| التخزين | Executor memory/disk | HDFS/S3 أو تخزين موثوق |
| يبقى بعد التطبيق؟ | لا | نعم غالباً |
| يستخدم عند | بيانات تستخدم عدة مرات | Lineage طويل أو ML loops |

### ملاحظة السينيور

RDD API يعطيك تحكماً عالياً، لكنه يحرم Catalyst من معرفة الأعمدة والأنواع. استخدم RDD عندما تحتاج تحكماً منخفض المستوى أو عمليات غير مناسبة لـ DataFrame. في أغلب ETL والتحليلات، DataFrame هو الاختيار الأفضل.

---

## 6. DataFrames وDatasets وCatalyst وTungsten

### لماذا DataFrame أسرع من RDD؟

RDD يرى بيانات عامة. DataFrame يعرف Schema: أسماء الأعمدة وأنواعها. هذا يسمح لـ Catalyst بتحسين الخطة.

```python
df = spark.read.parquet("s3://sales/")
result = df.filter(df.amount > 1000) \
           .groupBy("store_id") \
           .sum("amount")
```

Spark يمكنه هنا:

- قراءة الأعمدة المطلوبة فقط.
- دفع الفلتر لقارئ Parquet.
- اختيار Broadcast Join عند الإمكان.
- توليد Java code سريع عبر Whole-Stage Codegen.

### Catalyst Pipeline

```text
User Code
  -> Unresolved Logical Plan
  -> Analyzed Logical Plan
  -> Optimized Logical Plan
  -> Physical Plans
  -> Selected Physical Plan
  -> Execution
```

### Tungsten وUnsafeRow

بدلاً من تخزين كل صف ككائن Java مليء بالـ headers والـ references، Tungsten يخزنه بصيغة ثنائية مضغوطة.

```text
Java Objects:
  Object header + references + padding + GC pressure

UnsafeRow:
  bytes متجاورة + null bitmap + offsets
```

الفوائد:

- ذاكرة أقل.
- GC أقل.
- CPU cache locality أفضل.

### Whole-Stage Codegen

بدلاً من سلسلة Operators منفصلة، Spark يولّد Java loop واحدة تجمع الفلترة والإسقاط والتجميع قدر الإمكان.

```text
بدون Codegen:
  Row -> Filter object -> Project object -> Aggregate object

مع Codegen:
  while rows:
    اقرأ value
    لو الشرط صحيح
    حدّث hash aggregate
```

### مثال خطة تنفيذ

```python
result = spark.read.parquet("s3://sales/") \
    .filter("amount > 1000") \
    .select("store_id", "amount") \
    .groupBy("store_id") \
    .sum("amount")

result.explain(mode="formatted")
```

ابحث عن:

- `PushedFilters`: الفلتر وصل لقارئ الملفات.
- `Exchange`: Shuffle.
- `BroadcastExchange`: Broadcast.
- `*` قبل Operator: Codegen مفعّل.

### Dataset vs DataFrame

في Scala:

- DataFrame = `Dataset[Row]`
- Dataset[T] = strongly typed

لكن Dataset[T] قد يكون أبطأ إذا استخدمت عمليات تحول الصفوف إلى Objects ثم تعيدها إلى Tungsten. في PySpark، DataFrame هو الطريق الأساسي.

### ملاحظة السينيور

لا تكتب Python UDF إلا عند الحاجة. استخدم built-in functions قدر الإمكان. كلما بقيت داخل Spark SQL expressions، بقي Catalyst قادراً على التحسين وبقي التنفيذ داخل JVM/Tungsten.

---

## 7. نموذج التنفيذ: DAG وStages وTasks

### من كود إلى مهام موزعة

عندما تكتب:

```python
result = orders \
    .filter("amount > 500") \
    .groupBy("city") \
    .sum("amount") \
    .orderBy("city")
```

Spark لا ينفذ فوراً. هو يبني خطة. عند Action مثل `show()` أو `write.parquet()` يبدأ التنفيذ.

### Lazy Evaluation في قلب التنفيذ

```python
df = spark.read.parquet("s3://data/orders")
filtered = df.filter("amount > 500")
grouped = filtered.groupBy("city").count()

# لا تنفيذ حقيقي حتى الآن

grouped.show()
# الآن يبدأ التنفيذ
```

### تقسيم DAG إلى Stages

قاعدة مهمة:

```text
كل Shuffle = حد Stage جديد
```

مثال:

```text
Stage 0:
  Scan -> Filter -> Partial Aggregate -> Shuffle Write

Stage 1:
  Shuffle Read -> Final Aggregate -> Shuffle Write for Sort

Stage 2:
  Shuffle Read -> Sort -> Output
```

### Tasks وPartitions

كل Stage تتحول إلى Tasks بعدد Partitions.

```text
Stage مع 200 Partitions
  -> 200 Tasks

كل Task:
  تعالج Partition واحدة
```

### Spark UI: ماذا تراقب؟

| المقياس | ماذا يعني |
| :--- | :--- |
| Input Size | حجم البيانات المقروءة |
| Shuffle Read/Write | حجم البيانات المنقولة عبر الشبكة |
| GC Time | وقت ضائع في Garbage Collection |
| Task Duration Max vs Median | مؤشر Data Skew أو خادم بطيء |
| Spill Disk | الذاكرة لم تكفِ، فكتب Spark على القرص |

### AQE

Adaptive Query Execution في Spark 3+ يعدل الخطة أثناء التنفيذ:

- يدمج Partitions صغيرة بعد Shuffle.
- يحول Join إلى Broadcast إذا اكتشف أن الجدول صغير.
- يتعامل مع بعض حالات Data Skew.

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### ملاحظة السينيور

لا تحلل الأداء من الكود فقط. افتح Spark UI. الكود يخبرك بما طلبته، لكن UI يخبرك بما حدث فعلياً: أين الـ Shuffle، أين الـ Spill، وأي Task حبست الـ Stage.

---

## 8. Narrow vs Wide Dependencies

### السؤال الأهم

قبل أي عملية اسأل:

> هل تحتاج هذه العملية نقل بيانات بين Executors؟

لو لا، فهي Narrow غالباً. لو نعم، فهي Wide وتحتاج Shuffle.

### Narrow Dependencies

أمثلة:

- `map`
- `filter`
- `flatMap`
- `select`
- `withColumn` غالباً
- `coalesce` عند تقليل Partitions

```text
Parent Partition 0 -> Child Partition 0
Parent Partition 1 -> Child Partition 1
```

### Wide Dependencies

أمثلة:

- `groupBy`
- `reduceByKey`
- `join` غالباً
- `distinct`
- `repartition`
- `orderBy`

```text
Parent Partition 0 -> Child Partitions كثيرة
Parent Partition 1 -> Child Partitions كثيرة
```

### تكلفة الـ Shuffle

الـ Shuffle ليس "شبكة" فقط:

1. Shuffle Write على قرص الـ Executor.
2. Driver يحتفظ بMetadata لمواقع الملفات.
3. Shuffle Read يسحب الملفات عبر الشبكة.
4. Sort/Merge/Aggregate قد يسبب Spill.

### groupByKey vs reduceByKey

```python
# سيئ غالباً
rdd.groupByKey().mapValues(sum)

# أفضل
rdd.reduceByKey(lambda a, b: a + b)
```

`groupByKey` ينقل كل القيم ثم يجمعها. `reduceByKey` يجمع محلياً أولاً ثم ينقل النتائج الجزئية، فيقلل حجم الشبكة.

### Data Skew

Data Skew يعني أن مفتاحاً واحداً يحمل جزءاً ضخماً من البيانات.

```python
df.groupBy("city").count().orderBy("count", ascending=False).show(10)
```

لو وجدت `Cairo` مثلاً تحمل 70% من السجلات، ستظهر Task واحدة بطيئة جداً.

حلول:

- AQE.
- Salting.
- Broadcast Join عندما يكون أحد الجداول صغيراً.
- إعادة تصميم المفتاح أو تقسيم المعالجة.

### Salting مثال وشرح

```python
from pyspark.sql.functions import col, floor, rand, concat_ws

salted = df.withColumn(
    "city_salted",
    concat_ws("_", col("city"), floor(rand() * 10).cast("string"))
)

partial = salted.groupBy("city_salted").sum("amount")
```

شرح السطور:

- `rand() * 10`: ينتج رقماً عشوائياً من 0 إلى أقل من 10.
- `floor(...)`: يحوله إلى 0..9.
- `concat_ws`: يضيف الملح للمفتاح، مثل `Cairo_3`.
- `groupBy("city_salted")`: يوزع مفتاح Cairo على 10 مجموعات بدلاً من واحدة.

بعدها تحتاج تجميعاً ثانياً لإزالة الملح ودمج النتائج.

### ملاحظة السينيور

تقليل عدد الـ Shuffles أهم من حفظ أسماء الإعدادات. كل `join` و`groupBy` و`orderBy` قرار معماري. فلتر واختر الأعمدة قبلها، وفكر في Broadcast أو Partitioning إذا كان الـ Pipeline حساساً للأداء.

---

## 9. Lazy Evaluation والـ Caching

### لماذا Spark كسول؟

Spark يؤجل التنفيذ حتى يرى الخطة كلها. هذا يسمح له بتحسين الترتيب ودفع الفلاتر وتقليل الأعمدة.

```python
df = spark.read.parquet("s3://sales")
filtered = df.filter("amount > 1000")
result = filtered.groupBy("city").sum("amount")

# لا شيء يعمل فعلياً

result.count()
# الآن يعمل
```

### Transformations vs Actions

Transformations تبني خطة:

- `filter`
- `select`
- `withColumn`
- `groupBy`
- `join`

Actions تطلق التنفيذ:

- `count`
- `show`
- `collect`
- `write`
- `take`

### cache لا يخزن فوراً

```python
clean_df = raw_df.filter("is_valid = true").cache()

# لم يتم التخزين بعد

clean_df.count()
# الآن يُحسب clean_df ويُخزن

clean_df.groupBy("city").count().show()
# يستخدم المخزن غالباً
```

### متى تستخدم Cache؟

استخدمه عندما:

- نفس DataFrame/RDD يستخدم أكثر من مرة.
- يوجد تدريب ML تكراري.
- نتيجة تحويل مكلف ستستخدم في عدة تقارير.

تجنبه عندما:

- البيانات تستخدم مرة واحدة.
- حجمها أكبر كثيراً من الذاكرة.
- أنت في Streaming pipeline.
- لا تستطيع تنظيفه بعد الاستخدام.

### Storage Levels

| المستوى | متى يستخدم |
| :--- | :--- |
| MEMORY_ONLY | بيانات صغيرة أو متوسطة وتحتاج سرعة عالية |
| MEMORY_AND_DISK | الخيار الآمن الافتراضي |
| MEMORY_ONLY_SER | تقليل الذاكرة مقابل كلفة serialization |
| MEMORY_AND_DISK_SER | إنتاجياً عند احتمال نقص الذاكرة |
| OFF_HEAP | عند إعداد off-heap بعناية |

### مثال آمن وشرحه

```python
from pyspark import StorageLevel

clean_df = raw_df.filter("is_valid = true") \
    .persist(StorageLevel.MEMORY_AND_DISK)

clean_df.count()

report1 = clean_df.groupBy("city").count()
report2 = clean_df.groupBy("category").sum("amount")

report1.write.parquet("s3://out/report1")
report2.write.parquet("s3://out/report2")

clean_df.unpersist()
```

شرح السطور:

- `persist(...)`: يحدد مستوى التخزين صراحة.
- `count()`: يجسّد التخزين حتى لا تعيد التقارير الحساب من المصدر.
- `report1/report2`: يعيدان استخدام نفس البيانات النظيفة.
- `unpersist()`: يحرر الذاكرة بعد الانتهاء.

### ملاحظة السينيور

أسوأ Cache هو الذي لا تعرف لماذا وضعته. كل Cache يجب أن يجيب عن سؤال: "ما الحساب الذي أتجنب إعادته؟" ويجب أن يكون له مكان تنظيف واضح. راقب Storage tab في Spark UI للتأكد من الحجم ونسبة الـ cached partitions.

---

## 10. Deploy Modes والشبكات في الإنتاج

### السؤال الحاسم: أين يعمل الـ Driver؟

هذا هو الفرق بين Client Mode وCluster Mode.

```text
Client Mode:
  Driver على جهاز الإرسال أو Airflow Worker
  Executors داخل العنقود

Cluster Mode:
  Driver داخل العنقود
  جهاز الإرسال يرسل الطلب فقط
```

### Client Mode

مناسب لـ:

- التطوير المحلي.
- PySpark shell.
- Jupyter notebooks.
- التجارب التي تحتاج logs مباشرة.

مشاكله في الإنتاج:

- Driver يعتمد على شبكة جهاز الإرسال.
- Executors تحتاج الاتصال بجهاز الإرسال، وقد يمنعها Firewall.
- Airflow Worker قد يحمل Drivers كثيرة.
- `collect()` يرسل البيانات إلى جهاز بعيد عن العنقود.

### Cluster Mode

مناسب لـ:

- Production ETL.
- Airflow scheduled jobs.
- CI/CD.
- Jobs طويلة.

الميزة: Driver داخل شبكة العنقود، قريب من Executors، ولا يتوقف إذا أُغلق جهاز الإرسال.

### Firewall في Client Mode

في Client Mode، يجب على Executors الاتصال بالـ Driver. لذلك تحتاج منافذ ثابتة:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf spark.driver.host=192.168.1.100 \
  --conf spark.driver.port=40000 \
  --conf spark.blockManager.port=40001 \
  my_app.py
```

شرح السطور:

- `spark.driver.host`: العنوان الذي يعلنه الـ Driver للـ Executors.
- `spark.driver.port`: منفذ RPC الرئيسي.
- `spark.blockManager.port`: منفذ تبادل blocks.
- يجب فتح هذه المنافذ من شبكة العنقود إلى جهاز الـ Driver.

### لا تستخدم collect للإخراج الكبير

```python
# خطر
rows = df.collect()

# أفضل
df.write.mode("overwrite").parquet("s3://output/path")
```

`collect()` يعيد كل البيانات إلى Driver memory. أما `write` فيجعل Executors تكتب مباشرة للتخزين.

### جدول قرار سريع

| السيناريو | الاختيار |
| :--- | :--- |
| Notebook تفاعلي | Client Mode |
| PySpark shell | Client Mode |
| Airflow production job | Cluster Mode |
| CI/CD batch job | Cluster Mode |
| تجربة محلية | Client Mode أو local |
| Job طويل وحساس للاستقرار | Cluster Mode |

### ملاحظة السينيور

Client Mode ليس "سيئاً"؛ هو فقط في المكان الخطأ يصبح خطراً. للتفاعل ممتاز. للإنتاج المجدول غالباً Cluster Mode هو القاعدة. لو وجدت Airflow Workers تستهلك RAM هائلة، فتش عن Spark jobs تعمل Client Mode.

---

## 11. قراءة خطة التنفيذ: مهارة تربط كل شيء

استخدم دائماً:

```python
df.explain(mode="formatted")
```

واقرأ من الأسفل للأعلى:

```text
Scan parquet
  -> Filter
  -> Partial HashAggregate
  -> Exchange
  -> Final HashAggregate
```

معاني الكلمات المهمة:

| الكلمة | معناها |
| :--- | :--- |
| Scan | قراءة من مصدر |
| PushedFilters | الفلتر وصل للمصدر |
| Exchange | Shuffle |
| BroadcastExchange | إرسال جدول صغير لكل Executors |
| HashAggregate partial | تجميع محلي قبل Shuffle |
| HashAggregate final | تجميع نهائي بعد Shuffle |
| * قبل Operator | Whole-Stage Codegen |

### Workflow تشخيص عملي

1. شغّل `explain`.
2. ابحث عن `Exchange`.
3. اسأل: هل هذا الـ Shuffle ضروري؟
4. ابحث عن `PushedFilters`.
5. افتح Spark UI بعد التشغيل.
6. قارن Median Task Duration مع Max.
7. راقب Spill وGC وShuffle sizes.
8. عدّل: filter/select قبل join/groupBy، فعّل AQE، اضبط partitions، عالج skew.

---

## 12. إعدادات أساسية تحفظها

```python
# AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")

# Serializer
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

قاعدة partitions كبداية:

```text
shuffle_partitions ~= total_executor_cores * 2 إلى 4
```

ومع AQE يمكنك البدء بقيمة أكبر والسماح له بدمج Partitions الصغيرة.

---

## 13. أخطاء إنتاجية متكررة وحلولها

### 1. Driver OOM

السبب الشائع:

```python
df.collect()
```

الحل:

```python
df.write.parquet("s3://output")
```

أو استخدم `limit`, `take`, `show` للعينة فقط.

### 2. Executor OOM بسبب Partition كبير

الأعراض:

- `Java heap space`
- Disk Spill كبير
- Task واحدة بطيئة جداً

الحلول:

- زيادة `spark.sql.shuffle.partitions`.
- معالجة Data Skew.
- تقليل الأعمدة قبل الـ Shuffle.
- ضبط موارد Executor.

### 3. بطء شديد بسبب UDF

الحل:

- استبدل Python UDF بـ Spark SQL functions.
- استخدم Pandas UDF عند الحاجة وبحذر.
- راقب اختفاء `PushedFilters` أو Codegen.

### 4. Cache يسبب نفاد الذاكرة

الحل:

- استخدم Cache فقط عند إعادة الاستخدام.
- اختر StorageLevel مناسب.
- نفذ `unpersist()`.
- راقب Storage tab.

### 5. Client Mode يفشل خلف Firewall

الحل:

- للإنتاج: Cluster Mode.
- لو مضطر Client Mode: ثبت `spark.driver.host`, `spark.driver.port`, وBlockManager port.

---

## 14. تمارين عملية موحدة

### تمرين 1: راقب Lazy Evaluation

```python
df = spark.read.parquet("/path/does/not/exist")
df2 = df.filter("id > 10")

print("No error yet")

df2.count()
```

المطلوب: لاحظ أن الخطأ يظهر عند `count()` فقط.

### تمرين 2: ابحث عن الـ Shuffle

```python
df = spark.range(1, 1_000_000) \
    .selectExpr("id", "id % 100 as key")

result = df.groupBy("key").count()
result.explain(mode="formatted")
```

المطلوب: ابحث عن `Exchange`.

### تمرين 3: قارن Cache

```python
expensive = spark.range(1, 10_000_000) \
    .selectExpr("id", "id * 2 as value") \
    .filter("value > 1000")

expensive.cache()
expensive.count()
expensive.groupByExpr("id % 10").count().show()
expensive.unpersist()
```

المطلوب: افتح Spark UI Storage tab وشاهد التخزين.

### تمرين 4: اكتشف Data Skew

```python
df.groupBy("key_column").count() \
  .orderBy("count", ascending=False) \
  .show(20)
```

المطلوب: لو أعلى مفتاح أكبر من المتوسط بعشرات المرات، صمم علاجاً بـ AQE أو Salting.

---

## 15. Roadmap بعد هذه الوحدة

بعد فهم Core Architecture، انتقل بالترتيب التالي:

1. **DataFrame Transformations:** اكتب pipelines فعالة دون UDFs غير ضرورية.
2. **Joins بعمق:** Broadcast, Sort-Merge, Shuffle Hash, Skew joins.
3. **File formats:** Parquet, ORC, Delta Lake، وPartition pruning.
4. **Spark SQL tuning:** Statistics, CBO, AQE details.
5. **Structured Streaming:** State, watermarks, checkpointing.
6. **Production operations:** Monitoring, logs, retries, SLAs, cost control.

---

## الخلاصة

Spark يصبح واضحاً عندما تراه كطبقات:

```text
User Code
  -> Logical Plan
  -> Catalyst Optimization
  -> Physical Plan
  -> DAG
  -> Stages
  -> Tasks
  -> Executors
  -> Storage/Network/Memory
```

أفضل مهندس Spark لا يحفظ كل config فقط، بل يعرف متى يتحول سطر صغير إلى Shuffle كبير، ومتى يكون Driver نقطة خطر، ومتى يكون Cache مكسباً أو عبئاً، وكيف يقرأ Spark UI ليحوّل التخمين إلى تشخيص.

لو خرجت من هذا الدليل بثلاث عادات فقط، فاجعلها:

1. اقرأ `explain()` قبل تحسين الأداء.
2. راقب Spark UI بعد التنفيذ.
3. قلل البيانات قبل أي `join`, `groupBy`, أو `orderBy`.

