# 📘 تنظيف وتحضير البيانات (Data Wrangling): Select، Filter، Cast، وأخطاء الـ Null الخفية

> [!IMPORTANT]
> **هدف هذا الدليل:**
> بنهاية هذا الملف، ستفهم لماذا `withColumn` في حلقة يُحطم الـ Driver، كيف تختفي بيانات صامتة بسبب قيم Null، وكيف تكتب Pipeline تنظيف بيانات لا تُسبب OOM مع 200 عمود.

---

## 1. 🎯 Data Wrangling: 80% من كود الـ Pipeline

قبل أي تحليل أو ML، البيانات تحتاج:
- اختيار الأعمدة المطلوبة فقط
- تغيير أسمائها لتتوافق مع الـ Schema
- تحويل أنواع البيانات (casting)
- تنظيف القيم الخاطئة والـ Null

```python
# Pipeline نموذجي لتنظيف بيانات مبيعات
df_raw = spark.read.csv("s3://raw/sales_2025.csv", header=True)
# أعمدة خام: id, SALE_AMOUNT, user-id, date_str, STATUS

df_clean = df_raw \
    .select(
        col("id").cast("long").alias("sale_id"),
        col("SALE_AMOUNT").cast("double").alias("amount"),
        col("user-id").cast("long").alias("user_id"),  # توحيد الاسم
        col("date_str").cast("date").alias("sale_date"),
        col("STATUS").alias("status")
    ) \
    .filter(
        col("amount").isNotNull() &
        (col("amount") > 0) &
        col("status").isin("COMPLETE", "PENDING")
    )
```

---

## 2. 🏗️ كيف يُعالج Catalyst عمليات الـ Wrangling؟

### قاعدة CollapseProject: دمج الـ Projections

عندما تكتب عدة `withColumn` أو `select` متتالية، Catalyst يدمجها تلقائياً:

```python
df.withColumn("a", col("x") * 2) \
  .withColumn("b", col("y") + 10) \
  .select("a", "b")
```

**ما يبنيه Catalyst (قبل التحسين):**
```
Project [a, b]
  Project [x, y, x*2 as a, y+10 as b]
    Project [x, y]
      Scan
```

**بعد قاعدة CollapseProject:**
```
Project [x*2 as a, y+10 as b]  ← طبقة واحدة فقط!
  Scan [x, y]
```

### 🔴 ولكن: withColumn في حلقة يُكسر كل شيء

> [!CAUTION]
> **Common Mistake الأكثر خطورة:**
>
> ```python
> # ❌ Pattern كارثي — يُسبب Driver StackOverflow!
> for col_name in list_of_200_columns:
>     df = df.withColumn(col_name, transform_fn(col(col_name)))
>
> # كل withColumn يُضيف Node للـ Logical Plan
> # بعد 200 iteration: شجرة عمق 200 طبقة!
> # Catalyst يحاول تحليلها recursively → StackOverflowError
> ```
>
> ```text
> ERROR DAGScheduler: Job aborted due to stage failure
> java.lang.StackOverflowError
>   at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan...
>   at ... (تكرار 500+ مرة)
> ```

**المقارنة الكمية:**

| عدد الأعمدة | `withColumn` في حلقة | `select()` موحد |
| :--- | :--- | :--- |
| 10 أعمدة | 0.2 ثانية | 0.05 ثانية |
| 100 عمود | 4.8 ثانية | 0.12 ثانية |
| 500 عمود | **StackOverflow!** | 0.45 ثانية |

---

## 3. ⚡ الحلول الصحيحة: select بدلاً من withColumn في الحلقات

### البديل 1: select مع list comprehension

```python
from pyspark.sql.functions import col, upper, trim, when

# قائمة الأعمدة التي تحتاج تحويلاً
text_columns = ["name", "city", "region", "category"]

# ✅ طريقة صحيحة: تحضير كل التعبيرات أولاً، ثم select واحد
transformed_exprs = [
    upper(trim(col(c))).alias(c) if c in text_columns else col(c)
    for c in df.columns
]
df_clean = df.select(*transformed_exprs)
# نتيجة: Project واحد في الـ Plan، بدون تراكم!
```

### البديل 2: selectExpr للتعبيرات البسيطة

```python
# ✅ selectExpr — SQL expressions مباشرة
df_clean = df.selectExpr(
    "id",
    "CAST(SALE_AMOUNT AS DOUBLE) AS amount",
    "UPPER(TRIM(status)) AS status",
    "TO_DATE(date_str, 'yyyy-MM-dd') AS sale_date",
    "COALESCE(discount, 0.0) AS discount"
)
```

### البديل 3: withColumnsRenamed للتسمية الجماعية

```python
# ✅ في Spark 3.4+: تغيير عدة أسماء دفعة واحدة
rename_map = {
    "SALE_AMOUNT": "amount",
    "user-id": "user_id",
    "STATUS": "status"
}
df = df.withColumnsRenamed(rename_map)
```

---

## 4. 🕳️ خطر الـ Null: بيانات تختفي صامتة

### منطق القيمة الثلاثية في Spark (Three-Valued Logic)

SQL يستخدم منطقاً ثلاثياً: True / False / NULL (مجهول)

```
NULL != 'INACTIVE'  → NULL (مجهول، لا True ولا False!)
NULL > 0            → NULL
NULL = NULL         → NULL  ← حتى NULL لا تساوي NULL!

الـ Filter يحتفظ فقط بالصفوف التي تُقيّم إلى TRUE
→ الصفوف التي تُقيّم إلى NULL تُحذف صامتة!
```

**مثال على فقدان بيانات صامت:**
```python
# ❌ كود يفقد بيانات بصمت!
active_users = df.filter("status != 'INACTIVE'")
# المستخدمون الذين status = NULL يُحذفون!
# لأن: NULL != 'INACTIVE' = NULL → محذوف

df.count()          # 10,000 سجل
active_users.count() # 6,000 سجل  ← أين الـ 4,000؟!
```

```python
# ✅ معالجة NULL بشكل صحيح
active_users = df.filter(
    (col("status") != "INACTIVE") | col("status").isNull()
)
# يحتفظ بالمستخدمين الفعّالين والذين status = NULL

# أو باستخدام NULL-safe equality (<=>)
inactive_users = df.filter(col("status") <=> "INACTIVE")
# <=> يُعيد TRUE إذا كلاهما NULL، FALSE إذا أحدهما NULL فقط
active_users = df.filter(~(col("status") <=> "INACTIVE"))
```

### جدول مقارنة عمليات الـ NULL

| التعبير | عندما `status = NULL` | النتيجة |
| :--- | :--- | :--- |
| `status != 'INACTIVE'` | NULL | ❌ يُحذف! |
| `status = 'ACTIVE'` | NULL | ❌ يُحذف! |
| `status.isNull()` | TRUE | ✅ يُبقى |
| `status.isNotNull()` | FALSE | ❌ يُحذف |
| `status <=> 'ACTIVE'` | FALSE | ❌ يُحذف (صحيح سلوكياً) |
| `status <=> NULL` | TRUE | ✅ يُبقى |

---

## 5. 🔄 Type Casting: المزالق الخفية

### Casting الآمن مقابل الخطر

```python
# ✅ Casting آمن — يُعيد NULL عند الفشل
df = df.withColumn("amount", col("amount_str").cast("double"))
# إذا "amount_str" = "N/A" → amount = null (لا انهيار)

# ❌ Casting غير آمن مع قيم خاطئة
# "abc".cast("int") = null  ← بصمت وبدون تحذير!

# ✅ للاكتشاف المبكر: اختبر بعد الـ Cast
df_cast = df.withColumn("amount", col("amount_str").cast("double"))
invalid_count = df_cast.filter(
    col("amount_str").isNotNull() & col("amount").isNull()
).count()
print(f"سجلات فشل فيها الـ Casting: {invalid_count}")
```

### أنواع الـ Cast المهمة

```python
from pyspark.sql.functions import col, to_date, to_timestamp, unix_timestamp

# أعداد
col("price_str").cast("double")       # "10.5" → 10.5
col("count_str").cast("integer")      # "42" → 42
col("big_num_str").cast("long")       # "9999999999" → 9999999999L

# تواريخ (مهم تحديد الصيغة!)
to_date(col("date_str"), "yyyy-MM-dd")         # "2025-01-15" → Date
to_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss")  # Timestamp
col("epoch_str").cast("long").cast("timestamp")     # Unix timestamp

# Booleans
when(col("flag_str") == "Y", True).otherwise(False).alias("flag")
```

---

## 6. 🧹 عمليات التنظيف الشائعة

```python
from pyspark.sql.functions import (
    col, trim, upper, lower, regexp_replace, 
    when, coalesce, lit
)

df_cleaned = df.select(
    # تنظيف النصوص
    trim(col("name")).alias("name"),                    # إزالة المسافات
    upper(trim(col("status"))).alias("status"),         # توحيد الحالة
    regexp_replace(col("phone"), r"[^0-9]", "").alias("phone"),  # إزالة غير الأرقام
    
    # معالجة Nulls
    coalesce(col("discount"), lit(0.0)).alias("discount"),  # استبدال NULL بـ 0
    when(col("category").isNull(), "UNKNOWN")
        .otherwise(col("category")).alias("category"),
    
    # تحويل الأنواع
    col("amount_str").cast("double").alias("amount"),
    to_date(col("date_str"), "dd/MM/yyyy").alias("sale_date"),
    
    # أعمدة مشتقة
    (col("price") * col("quantity")).alias("total"),
    when(col("amount") > 1000, "HIGH").otherwise("LOW").alias("tier")
)
```

---

## 7. 🚨 سيناريوهات الفشل وكيفية التشخيص

### حادثة: StackOverflow في بنك مع 200 عمود

```text
ERROR DAGScheduler: Job aborted
java.lang.StackOverflowError
  at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.collectLeaves(LogicalPlan.scala:134)
  at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.collectLeaves(LogicalPlan.scala:134)
  ... (repeated 500+ times)
```

**الكود المُسبّب:**
```python
# ❌ 200 عمود عملة تحتاج تطبيع
for currency_col in currency_columns:  # 200 عمود
    df = df.withColumn(currency_col, df[currency_col].cast("double") * exchange_rate)
# → 200 مستوى في الـ Logical Plan → StackOverflow أثناء التحليل!
```

**الحل:**
```python
# ✅ قاموس الـ Exchange Rates + select واحد
exchange_rates = {"USD": 1.0, "EUR": 1.1, "EGP": 0.02}

exprs = []
for c in df.columns:
    if c in currency_columns:
        rate = exchange_rates.get(c.split("_")[0], 1.0)
        exprs.append((col(c).cast("double") * rate).alias(c))
    else:
        exprs.append(col(c))

df_normalized = df.select(*exprs)
# → Plan بطبقة واحدة → يعمل بسرعة مهما كان عدد الأعمدة!
```

---

## 8. 🧪 التمارين العملية

### التمرين 1: مقارنة Plan مع withColumn مقابل select

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[4]").appName("WranglingLab").getOrCreate()

df = spark.range(1, 100).selectExpr("id", "id * 1.5 as price", "cast(id % 5 as string) as category")

# الطريقة 1: withColumn متسلسلة
df_chain = df \
    .withColumn("price_vat", col("price") * 1.14) \
    .withColumn("price_usd", col("price") * 0.02) \
    .withColumn("category_upper", col("category"))

print("=== withColumn متسلسلة ===")
df_chain.explain()

# الطريقة 2: select موحد
df_single = df.select(
    col("id"),
    col("price"),
    (col("price") * 1.14).alias("price_vat"),
    (col("price") * 0.02).alias("price_usd"),
    col("category")
)

print("\n=== select موحد ===")
df_single.explain()
# قارن عمق الـ Plan — الـ select يجب أن يكون أبسط بكثير
```

### التمرين 2: اكتشاف فقدان البيانات بسبب Null

```python
from pyspark.sql.functions import col, when, lit

# بيانات تحتوي على Nulls
data = [
    (1, "ACTIVE"),
    (2, "INACTIVE"),
    (3, None),       # ← هذا سيُفقد!
    (4, "ACTIVE"),
    (5, None),       # ← هذا سيُفقد!
]
df = spark.createDataFrame(data, ["user_id", "status"])

print(f"إجمالي السجلات: {df.count()}")

# ❌ فلتر خاطئ يُفقد Nulls
wrong_filter = df.filter(col("status") != "INACTIVE")
print(f"❌ مع فلتر خاطئ: {wrong_filter.count()} سجل")
wrong_filter.show()

# ✅ فلتر صحيح يحتفظ بالـ Nulls
correct_filter = df.filter(
    (col("status") != "INACTIVE") | col("status").isNull()
)
print(f"✅ مع فلتر صحيح: {correct_filter.count()} سجل")
correct_filter.show()
```

### التمرين 3: Pipeline تنظيف شامل

```python
from pyspark.sql.functions import *

# بيانات "قذرة"
raw_data = [
    ("1", " ALICE ", "25", "1500.50", "2025/01/15", "ACTIVE"),
    ("2", "Bob", "invalid_age", "0", "2025/02/01", "INACTIVE"),
    ("3", " CHARLIE ", "30", "2300.75", "invalid_date", None),
    ("4", "diana", "28", "1100.00", "2025/01/20", "ACTIVE"),
]
df_raw = spark.createDataFrame(raw_data, ["id", "name", "age_str", "amount_str", "date_str", "status"])

print("=== البيانات الخام ===")
df_raw.show()

# Pipeline تنظيف احترافي
df_clean = df_raw.select(
    col("id").cast("int").alias("user_id"),
    upper(trim(col("name"))).alias("name"),
    col("age_str").cast("int").alias("age"),        # invalid → NULL
    col("amount_str").cast("double").alias("amount"),
    to_date(col("date_str"), "yyyy/MM/dd").alias("sale_date"),  # invalid → NULL
    when(col("status").isNull(), "UNKNOWN")
        .otherwise(upper(col("status"))).alias("status")
) \
.filter(
    col("user_id").isNotNull() &
    col("amount").isNotNull() &
    (col("amount") > 0)
)

print("=== البيانات بعد التنظيف ===")
df_clean.show()

# إحصاءات الجودة
total = df_raw.count()
clean = df_clean.count()
print(f"\nتقرير جودة البيانات:")
print(f"  الإجمالي: {total} سجل")
print(f"  الصالح:   {clean} سجل ({clean/total*100:.1f}%)")
print(f"  المحذوف:  {total-clean} سجل ({(total-clean)/total*100:.1f}%)")
```

---

## 9. 🎓 أسئلة المقابلات التقنية

### سؤال 1: لماذا `withColumn` في حلقة يُعدّ Anti-Pattern؟

**الإجابة النموذجية:**
كل استدعاء لـ `withColumn` يُضيف Node جديداً للـ Logical Plan في الذاكرة. عند استدعائه في حلقة بـ N iteration، ينتج عنها شجرة عمقها N طبقة. أثناء تحليل Catalyst لهذه الشجرة (Analysis + Optimization)، يستدعي دوال recursive قد تتجاوز حد الـ JVM Stack (عادة 500 مستوى). الحل: تجميع كل التعبيرات في list وتنفيذها بـ `select()` واحد → Node واحد في الـ Plan.

### سؤال 2: ما الفرق بين `==` و`<=>` في Spark SQL؟

**الإجابة النموذجية:**
| التعبير | `NULL == 'A'` | `NULL == NULL` |
| :--- | :--- | :--- |
| `==` (Standard Equality) | NULL → محذوف | NULL → محذوف |
| `<=>` (Null-Safe Equality) | FALSE | TRUE |

`==` يتبع SQL Three-Valued Logic: أي مقارنة مع NULL تُعطي NULL، والـ Filter يحذف النتائج غير TRUE. `<=>` يتعامل مع NULL كقيمة معروفة ويُعيد Boolean حقيقي.

### سؤال 3 (متقدم): ماذا يحدث عند `cast("date")` على قيمة غير صالحة؟

**الإجابة النموذجية:**
في Spark (الـ Default Mode)، الـ Cast الفاشل يُعيد `NULL` بصمت. هذا السلوك يُسمى **Permissive Casting** وهو الافتراضي لأن Spark مُصمَّم للعمل على بيانات ضخمة قد تحتوي على أخطاء. إذا أردت الانهيار عند أول Cast فاشل:
```python
spark.conf.set("spark.sql.ansi.enabled", "true")
# الآن Cast الفاشل يُطلق Exception فوراً
```

---

## 10. 📋 ورقة الغش السريعة

### عمليات الـ Wrangling الأساسية

```python
# ── اختيار الأعمدة ──────────────────────────────────────────────
df.select("col1", "col2")                    # بالاسم
df.select(col("id"), col("name").alias("n")) # مع تغيير الاسم
df.selectExpr("id", "upper(name) as name")   # SQL expressions

# ── إعادة التسمية ─────────────────────────────────────────────────
df.withColumnRenamed("old", "new")           # عمود واحد
df.toDF(*new_columns_list)                   # كل الأعمدة دفعة واحدة

# ── إضافة أعمدة ──────────────────────────────────────────────────
df.withColumn("new", col("old") * 2)         # عمود واحد (بسيط)
df.select(*exprs_list)                       # عدة أعمدة (أفضل)

# ── الفلترة ───────────────────────────────────────────────────────
df.filter("amount > 1000")                   # SQL String
df.filter(col("amount") > 1000)              # Column expression
df.filter(col("status").isin("A", "B"))      # قائمة قيم
df.filter(col("name").isNotNull())           # NULL-safe
df.filter((col("status") != "X") | col("status").isNull())  # NULL-aware

# ── Type Casting ─────────────────────────────────────────────────
col("str").cast("int")           # String → Int
col("str").cast("double")        # String → Double
to_date(col("str"), "yyyy-MM-dd")  # String → Date
```

> [!TIP]
> **الخطوة القادمة:** انتقل للملف `14_advanced_aggregations.md` لتتعلم التجميعات المتقدمة وكيف تُحسب Window Functions دون Shuffle زائد.

<!-- START_NAVIGATION_LINKS -->
---
### 🔗 روابط التنقل السريع

| السابق (Previous) | التالي (Next) |
| :--- | :--- |
| [◀️ 📘 صيغ التخزين: Parquet vs Avro — الهيكل الداخلي والاختيار الأمثل](12_storage_layouts_parquet_avro.md) | [▶️ 📘 التجميعات المتقدمة: GroupBy، Rollup، Cube، وسر HashAggregate](14_advanced_aggregations.md) |
<!-- END_NAVIGATION_LINKS -->
