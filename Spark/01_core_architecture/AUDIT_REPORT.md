# Spark Core Architecture Markdown Audit Report

## نطاق التدقيق

تمت مراجعة جميع ملفات Markdown داخل هذا المجلد:

- `01_distributed_computing_mapreduce.md`
- `02_spark_cluster_topology.md`
- `03_cluster_resource_managers.md`
- `04_spark_session_lifecycle.md`
- `05_resilient_distributed_datasets.md`
- `06_dataframes_and_datasets.md`
- `07_execution_model_dag.md`
- `08_narrow_vs_wide_dependencies.md`
- `09_lazy_evaluation_caching.md`
- `10_deploy_modes_networking.md`
- `spark_core_architecture_complete_guide.md`

## أخطاء تقنية تم تصحيحها

| الملف | المشكلة | التصحيح |
| :--- | :--- | :--- |
| `01_distributed_computing_mapreduce.md` | شرح MapReduce ذكر أن Reducers لا تبدأ إطلاقاً قبل انتهاء كل Mappers. | تم توضيح أن Reducers قد تبدأ copy/fetch لمخرجات Map مبكراً، لكن Reduce النهائي ينتظر اكتمال المخرجات المطلوبة. |
| `01_distributed_computing_mapreduce.md` | وصف Spark بأنه "الذاكرة أولاً" بشكل يوحي أنه لا يكتب على القرص إلا نادراً. | تم توضيح أن Spark يقلل I/O عبر pipelining، لكنه يكتب Shuffle files وقد يحدث Spill. |
| `01_distributed_computing_mapreduce.md` | `PushedFilters` وWhole-Stage Codegen تم شرحهما بعبارات مطلقة. | تم تصحيح الشرح: Pushdown يقلل I/O عبر metadata ولا يعني قراءة الصفوف المطابقة فقط دائماً، وCodegen يقلل allocation/virtual calls ولا يحبس كل البيانات في registers. |
| `02_spark_cluster_topology.md` | Cluster Manager قُدم كأنه يختفي تماماً بعد تخصيص الموارد. | تم توضيح أنه لا ينسق Tasks، لكنه يستمر في إدارة الحاويات/العمليات على مستوى المنصة. |
| `02_spark_cluster_topology.md` | MemoryManager عُرض بتقسيم ثابت 60/40. | تم استبداله بشرح Unified Memory الديناميكي. |
| `03_cluster_resource_managers.md` | أرقام إطلاق Executors وnetwork overhead قُدمت كحقائق عامة. | تم تحويلها إلى اتجاهات تشغيلية تعتمد على البيئة، لأن الأرقام تختلف حسب Scheduler وImage Pull وCNI والضغط. |
| `03_cluster_resource_managers.md` | External Shuffle Service نُسب إلى NodeManager نفسه. | تم تصحيحه إلى خدمة Node-level مستقلة تخدم Shuffle files بعد موت Executor. |
| `04_spark_session_lifecycle.md` | مثال SparkListener من Python غير قابل للتطبيق كما هو. | تم استبداله بتوجيه صحيح: JVM listener عبر `spark.extraListeners` أو event logs/metrics. |
| `05_resilient_distributed_datasets.md` | نص نصح بـ Checkpoint بعد كل Wide Dependency مكلفة. | تم تصحيحه: Checkpoint يستخدم عند Lineage طويل أو تكلفة تعاف عالية، لأنه يضيف I/O كبيراً. |
| `06_dataframes_and_datasets.md` | `select("*")` قيل إنه يعطل Column Pruning. | تم تصحيح ذلك: Spark يستطيع حذف الأعمدة غير المستخدمة أحياناً، لكن تحديد الأعمدة مبكراً أفضل خصوصاً مع UDFs أو writes. |
| `07_execution_model_dag.md` | كود استخدم `df.read.parquet` بدلاً من `spark.read.parquet`. | تم تصحيحه. |
| `08_narrow_vs_wide_dependencies.md` | قيل إن pre-repartitioning قبل join يجعل DataFrame join ضيقاً تلقائياً. | تم توضيح أن Catalyst قد يضيف Exchange؛ يجب التحقق من `explain()`. |
| `09_lazy_evaluation_caching.md` | Lazy Evaluation قُدمت كأن كل أخطاء القراءة مؤجلة دائماً. | تم توضيح أن بعض المصادر قد تتحقق من metadata/schema مبكراً. |
| `10_deploy_modes_networking.md` | Client Mode قُدم ككارثة حتمية وCluster Mode كضمان إعادة تشغيل. | تم تصحيح النبرة: Client Mode مناسب للتفاعل، وخطر في الإنتاج المجدول؛ إعادة تشغيل Driver تعتمد على الإعدادات والمنصة. |

## مشاكل Distributed Systems

- تم تعزيز شرح Shuffle كعملية تجمع بين disk write، network transfer، metadata lookup، merge/sort، وspill المحتمل.
- تم تصحيح الافتراضات المطلقة حول network overhead في Kubernetes؛ الأداء يعتمد على CNI وMTU وnetwork policies والسحابة.
- تم توضيح أن Driver هو نقطة فشل حرجة، لكن سلوك إعادة المحاولة ليس موحداً بين YARN وKubernetes وStandalone.
- تم تصحيح استخدام `HTTP` كشرح عام لنقل Shuffle إلى Spark block transfer المبني على Netty.

## مشاكل كود تم إصلاحها

- أزيلت تعليقات بعد backslash في PySpark snippets لأنها تكسر syntax.
- تم تصحيح `spark.builder...` إلى `SparkSession.builder...`.
- تم تصحيح `df.read.parquet` إلى `spark.read.parquet`.
- تم تصحيح `groupByExpr` غير الموجودة في PySpark إلى `groupBy(expr(...))`.
- تم استبدال `model.predict(...)` بنمط Spark ML الصحيح `model.transform(...)`.

## ممارسات خطرة تم تقييدها

- `collect()` على بيانات ضخمة: بقي التحذير، مع التشديد أن `write` يتم من Executors مباشرة.
- `groupByKey()`: بقي التحذير مع شرح pre-aggregation في `reduceByKey`.
- `cache()` بدون `unpersist()`: بقي التحذير مع ربطه بـ Storage tab وLRU/eviction.
- `repartition()` و`coalesce()`: تم تعديل الشرح ليكون أقل إطلاقاً وأكثر اعتماداً على الخطة الفعلية.
- `Checkpoint`: تم منعه من أن يصبح نصيحة افتراضية بعد كل Shuffle.

## مشاكل تنسيق ولغة

- تم الحفاظ على المصطلحات الهندسية الأساسية بالإنجليزية: `Driver`, `Executor`, `Shuffle`, `DAG`, `Stage`, `Task`, `Partition`, `Catalyst`, `AQE`.
- تم إصلاح أمثلة Markdown/Python التي كانت تتأثر باتجاه النص المختلط بسبب التعليقات بعد `\`.
- تم تحسين بعض العبارات العربية التي كانت تعطي يقيناً زائداً أو تعميماً غير آمن.

## ملاحظات متبقية

- توجد بعض الأرقام التعليمية التقريبية في الملفات. تم تخفيف الأكثر خطورة، لكن أي benchmark إنتاجي يجب قياسه على البيانات والعنقود الفعليين.
- بعض الروابط التنقلية تشير إلى وحدة لاحقة خارج هذا المجلد؛ لم يتم تعديلها لأنها جزء من هيكل المسار التعليمي.
- لم يتم تشغيل أمثلة PySpark فعلياً لأن ذلك يتطلب بيئة Spark وJava مهيأة محلياً. تم إجراء مراجعة syntax/semantic يدوية وتصحيح الأخطاء الواضحة.
