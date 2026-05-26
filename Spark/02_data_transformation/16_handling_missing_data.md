# 📘 معالجة البيانات الناقصة (Missing Data): استراتيجيات الـ NULL في الإنتاج

> [!IMPORTANT]
> **هدف هذا الدليل:**
> بنهاية هذا الملف، ستفهم الفرق بين استراتيجيات التعامل مع الـ NULL، متى تحذف صفاً ومتى تملأه، وكيف تبني Pipeline يتعامل مع البيانات الناقصة دون فقدان صامت للبيانات.

---

## 1. 🎯 لماذا البيانات الناقصة مشكلة خطيرة؟

```
حادثة حقيقية: تقرير إيرادات ربع سنوي
  أصل البيانات: 10,000,000 طلب
  بعد Filter خاطئ يتجاهل NULLs: 6,000,000 طلب
  نسبة الفقدان: 40%!
  
  القرارات المبنية على هذا التقرير: خاطئة بنسبة 40%!
```

**الـ NULL في قواعد البيانات يعني "مجهول"** — وليس صفراً أو فراغاً.

مصادر الـ NULL:
- حقول اختيارية لم يُعبَّأ بها
- انضمام (Join) فاشل (LEFT JOIN مع صفوف لا تتطابق)
- Casting فاشل لقيمة غير صالحة
- بيانات تالفة أثناء النقل

---

## 2. 🏗️ تشخيص الـ NULL: اعرف مشكلتك أولاً

### الأمر الأول: فحص النسب

```python
from pyspark.sql.functions import col, count, isnan, when, sum as _sum

def null_report(df):
    """تقرير شامل عن القيم الناقصة في كل عمود"""
    total = df.count()
    
    null_counts = df.select([
        _sum(
            when(col(c).isNull() | isnan(col(c)), 1).otherwise(0)
        ).alias(c)
        for c in df.columns
    ]).collect()[0]
    
    print(f"{'العمود':<25} {'النوع':<15} {'NULL':<10} {'NaN':<10} {'النسبة':<10}")
    print("-" * 70)
    for field in df.schema.fields:
        c = field.name
        null_c = df.filter(col(c).isNull()).count()
        nan_c = df.filter(isnan(col(c))).count() if str(field.dataType) in ["DoubleType", "FloatType"] else 0
        pct = (null_c + nan_c) / total * 100
        flag = "⚠️" if pct > 5 else ("🔴" if pct > 20 else "✅")
        print(f"{flag} {c:<23} {str(field.dataType):<15} {null_c:<10} {nan_c:<10} {pct:.1f}%")

null_report(df)
```

---

## 3. ⚡ استراتيجيات معالجة الـ NULL

### الاستراتيجية 1: الحذف (dropna)

```python
# حذف أي صف يحتوي على NULL في أي عمود
df.dropna()

# حذف فقط إذا كانت NULL في أعمدة محددة
df.dropna(subset=["user_id", "amount"])

# حذف فقط إذا كانت نسبة الـ NULL عالية (أكثر من N عمود فارغ)
df.dropna(thresh=3)  # احتفظ بالصف فقط إذا كان 3 أعمدة على الأقل غير NULL
```

> [!WARNING]
> **Common Mistake:** استخدام `dropna()` بدون تفكير يُفقد بيانات ثمينة.
>
> **متى يكون الحذف مناسباً:**
> - عندما الـ NULL في عمود جوهري (مثل `user_id`) يجعل الصف عديم الفائدة
> - عندما نسبة الـ NULL صغيرة (< 5%) ولا توجد أنماط
> - عندما بيانات التدريب (ML) لا تقبل الـ NULL
>
> **متى لا تحذف:**
> - عندما الـ NULL تحمل معنى (مثل NULL في `discount` = لا خصم)
> - عندما نسبة الـ NULL كبيرة (> 20%) → فقدان بيانات ضخم

### الاستراتيجية 2: الاستبدال (fillna / coalesce)

```python
from pyspark.sql.functions import coalesce, lit

# استبدال بقيمة ثابتة
df.fillna(0, subset=["amount", "discount"])      # أرقام
df.fillna("UNKNOWN", subset=["category"])         # نصوص
df.fillna(False, subset=["is_premium"])           # Boolean

# استبدال بقواميس (مختلف لكل عمود)
df.fillna({
    "discount": 0.0,
    "category": "UNCATEGORIZED",
    "country": "Unknown",
    "age": df.agg({"age": "avg"}).first()[0]  # المتوسط
})

# coalesce — استخدام أول قيمة غير NULL من قائمة
df.withColumn(
    "effective_price",
    coalesce(col("sale_price"), col("list_price"), lit(0.0))
)
# إذا sale_price = NULL، استخدم list_price
# إذا كلاهما NULL، استخدم 0.0
```

### الاستراتيجية 3: الإحلال (Imputation) بالمتوسط/الوسيط

```python
from pyspark.sql.functions import avg, median, col, when

# احسب المتوسط لكل مجموعة
avg_by_region = df.groupBy("region").agg(avg("amount").alias("avg_amount"))

# استخدم متوسط المجموعة لملء الـ NULL
df_imputed = df.join(avg_by_region, "region", "left") \
               .withColumn(
                   "amount_filled",
                   coalesce(col("amount"), col("avg_amount"))
               ).drop("avg_amount")

# الوسيط (Median) — أكثر مقاومة للقيم الشاذة
medians = df.agg({
    "age": "percentile_approx(age, 0.5)"
}).first()

df.fillna({"age": medians[0]})
```

### الاستراتيجية 4: Forward Fill / Backward Fill (للـ Time Series)

```python
from pyspark.sql import Window
from pyspark.sql.functions import last, first, col

# ترتيب البيانات بالوقت
w_forward = Window.orderBy("timestamp").rowsBetween(Window.unboundedPreceding, 0)
w_backward = Window.orderBy("timestamp").rowsBetween(0, Window.unboundedFollowing)

# Forward Fill: استخدام آخر قيمة غير NULL السابقة
df_ff = df.withColumn(
    "temperature_ff",
    last(col("temperature"), ignorenulls=True).over(w_forward)
)

# Backward Fill: استخدام أول قيمة غير NULL التالية
df_bf = df.withColumn(
    "temperature_bf",
    first(col("temperature"), ignorenulls=True).over(w_backward)
)
```

---

## 4. 🔢 NaN مقابل NULL: فرق جوهري

```python
from pyspark.sql.functions import isnan, isnull, col

# NaN (Not a Number): فقط للأعداد العشرية (Float/Double)
# ينتج من: 0.0/0.0، sqrt(-1)، inf - inf
# NULL: غياب القيمة تماماً، لأي نوع

# فحص النوعين معاً
df.withColumn(
    "is_problematic",
    col("amount").isNull() | isnan(col("amount"))
)

# الفلتر الصحيح للأرقام
df.filter(col("amount").isNotNull() & ~isnan(col("amount")))

# استبدال NaN وNULL معاً
from pyspark.sql.functions import nanvl
df.withColumn(
    "amount_clean",
    coalesce(nanvl(col("amount"), lit(None)), lit(0.0))
    # nanvl يُحوّل NaN → NULL، ثم coalesce يُحوّل NULL → 0.0
)
```

| المعيار | NULL | NaN |
| :--- | :--- | :--- |
| **المعنى** | قيمة مجهولة/غائبة | نتيجة حسابية غير صالحة |
| **الأنواع** | أي نوع | Double/Float فقط |
| **`isNull()`** | True | False |
| **`isnan()`** | خطأ (Exception) | True |
| **في الـ Aggregation** | يُتجاهل | يُلوّث النتيجة (sum = NaN) |

> [!WARNING]
> **Common Mistake — NaN يُلوّث النتائج:**
>
> ```python
> from pyspark.sql.functions import sum
>
> # إذا كان عمود amount يحتوي حتى على NaN واحد:
> df.agg(sum("amount")).show()
> # النتيجة: NaN! (لا صفر، بل NaN يُلوّث كل الـ Sum)
>
> # ✅ الحل: نظّف NaN قبل أي Aggregation
> df_clean = df.withColumn("amount", nanvl(col("amount"), lit(0.0)))
> df_clean.agg(sum("amount")).show()  # النتيجة صحيحة الآن
> ```

---

## 5. 🏭 Pipeline تنظيف البيانات الناقصة في الإنتاج

```python
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

def clean_missing_data(df, config):
    """
    Pipeline شامل لمعالجة البيانات الناقصة
    
    config مثال:
    {
        "drop_if_null": ["user_id", "order_id"],  # أعمدة حرجة
        "fill_with_zero": ["discount", "bonus"],
        "fill_with_mode": ["category", "status"],
        "fill_with_avg": ["age", "score"],
        "drop_nan": ["amount", "price"]
    }
    """
    
    # 1. تقرير قبل التنظيف
    before_count = df.count()
    
    # 2. حذف إذا كانت NULL في الأعمدة الحرجة
    if config.get("drop_if_null"):
        df = df.dropna(subset=config["drop_if_null"])
    
    # 3. تنظيف NaN من الأعمدة العددية
    if config.get("drop_nan"):
        for c in config["drop_nan"]:
            df = df.filter(~isnan(col(c)))
    
    # 4. ملء بالصفر
    if config.get("fill_with_zero"):
        df = df.fillna(0.0, subset=config["fill_with_zero"])
    
    # 5. ملء بالمتوسط
    if config.get("fill_with_avg"):
        avgs = df.agg({c: "avg" for c in config["fill_with_avg"]}).first()
        fill_dict = {c: avgs[f"avg({c})"] for c in config["fill_with_avg"]}
        df = df.fillna(fill_dict)
    
    # 6. تقرير بعد التنظيف
    after_count = df.count()
    dropped = before_count - after_count
    print(f"تقرير التنظيف:")
    print(f"  قبل: {before_count:,} سجل")
    print(f"  بعد: {after_count:,} سجل")
    print(f"  محذوف: {dropped:,} سجل ({dropped/before_count*100:.1f}%)")
    
    return df

# الاستخدام
config = {
    "drop_if_null": ["user_id", "order_id"],
    "fill_with_zero": ["discount"],
    "fill_with_avg": ["age"]
}
df_clean = clean_missing_data(df_raw, config)
```

---

## 6. 🚨 سيناريوهات الفشل والتشخيص

### حادثة: فقدان بيانات صامت في Pipeline

```python
# ❌ Pipeline يُفقد بياناً صامتاً
pipeline_result = raw_data \
    .filter("status != 'CANCELLED'") \   # يحذف status=NULL صامتاً!
    .filter("amount > 0") \              # يحذف amount=NULL صامتاً!
    .groupBy("region") \
    .sum("amount")

# التحقق الصحيح:
before = raw_data.count()
after = pipeline_result_source.count()
print(f"تم معالجة {after/before*100:.1f}% من البيانات")
# إذا كانت النسبة < 95% → تحقق من الـ NULL handling
```

**الحل الجذري:**

```python
# ✅ وثّق وعالج الـ NULLs صراحةً
df_documented = raw_data \
    .withColumn("status", 
        when(col("status").isNull(), "UNKNOWN").otherwise(col("status"))) \
    .withColumn("amount", 
        when(col("amount").isNull(), lit(0.0)).otherwise(col("amount"))) \
    .withColumn("has_missing", 
        when(col("user_id").isNull(), True).otherwise(False))

# الآن Filter آمن:
df_documented \
    .filter("status != 'CANCELLED'") \  # UNKNOWN لن يُحذف
    .filter("amount > 0") \              # 0.0 لن يُحذف
    .groupBy("region") \
    .sum("amount")
```

---

## 7. 🧪 التمارين العملية

### التمرين 1: تشخيص وتقرير الـ NULL

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[4]").appName("MissingDataLab").getOrCreate()

# بيانات تحتوي على أنواع مختلفة من القيم الناقصة
data = [
    (1, "Ali",    25,    1500.0, "ACTIVE",  "Cairo"),
    (2, "Sara",   None,  2000.0, "INACTIVE","Alex"),
    (3, None,     30,    None,   "ACTIVE",  None),
    (4, "Omar",   28,    float("nan"), None, "Giza"),
    (5, "Diana",  22,    500.0,  "ACTIVE",  "Cairo"),
    (6, None,     None,  None,   None,      None),  # سجل فارغ تقريباً
]
df = spark.createDataFrame(data, ["id", "name", "age", "amount", "status", "city"])

print("=== البيانات الخام ===")
df.show()

# فحص الـ NULL
print("\n=== تقرير الـ NULL ===")
total = df.count()
for c in df.columns:
    null_count = df.filter(col(c).isNull()).count()
    print(f"  {c}: {null_count}/{total} NULL ({null_count/total*100:.0f}%)")

# فحص الـ NaN (فقط الأعداد العشرية)
print("\n=== تقرير الـ NaN ===")
nan_count = df.filter(isnan(col("amount"))).count()
print(f"  amount NaN: {nan_count}")
```

### التمرين 2: مقارنة استراتيجيات التعامل مع NULL

```python
from pyspark.sql.functions import avg

# الاستراتيجية 1: الحذف
df_dropped = df.dropna(subset=["id", "amount"])
print(f"بعد الحذف: {df_dropped.count()} سجل")

# الاستراتيجية 2: ملء بالمتوسط
avg_age = df.filter(col("age").isNotNull()).agg(avg("age")).first()[0]
avg_amount = df.filter(col("amount").isNotNull() & ~isnan(col("amount"))).agg(avg("amount")).first()[0]

df_filled = df \
    .fillna({"name": "UNKNOWN", "city": "UNKNOWN", "status": "UNKNOWN"}) \
    .withColumn("age", when(col("age").isNull(), avg_age).otherwise(col("age"))) \
    .withColumn("amount", 
        when(col("amount").isNull() | isnan(col("amount")), avg_amount)
        .otherwise(col("amount")))

print(f"\nبعد الملء:")
df_filled.show()
```

### التمرين 3: Forward Fill لبيانات Time Series

```python
from pyspark.sql import Window
from pyspark.sql.functions import last, col, lit

# بيانات درجات حرارة متقطعة (sensor data)
temp_data = [
    ("2025-01-01 00:00", 25.0),
    ("2025-01-01 01:00", None),   # قراءة مفقودة
    ("2025-01-01 02:00", None),   # قراءة مفقودة
    ("2025-01-01 03:00", 27.0),
    ("2025-01-01 04:00", None),   # قراءة مفقودة
    ("2025-01-01 05:00", 26.5),
]
df_temp = spark.createDataFrame(temp_data, ["timestamp", "temperature"])

# Forward Fill
w = Window.orderBy("timestamp").rowsBetween(Window.unboundedPreceding, 0)

df_ff = df_temp.withColumn(
    "temperature_filled",
    last(col("temperature"), ignorenulls=True).over(w)
)

print("=== Forward Fill لبيانات الـ Sensor ===")
df_ff.show()
```

---

## 8. 🎓 أسئلة المقابلات التقنية

### سؤال 1: ما الفرق بين NULL وNaN في Spark وكيف تتعامل مع كل منهما؟

**الإجابة النموذجية:**
- **NULL:** يمثّل قيمة غائبة/مجهولة، يمكن أن يكون في أي نوع بيانات. يُتجاهل في معظم دوال الـ Aggregation (مثلاً `SUM` يتجاهل الـ NULLs). يُكتشف بـ `isNull()` أو `col IS NULL`.
- **NaN (Not a Number):** ينتج عن عمليات حسابية غير صالحة (0.0/0.0)، فقط في Float/Double. **يُلوّث نتائج الـ Aggregation** — أي `SUM` يحتوي NaN يُعطي NaN! يُكتشف بـ `isnan()`.

**استراتيجية التعامل:**
```python
# فحص شامل ومعالجة متكاملة
df.withColumn("amount",
    when(col("amount").isNull() | isnan(col("amount")), lit(0.0))
    .otherwise(col("amount")))
```

### سؤال 2: متى تختار الحذف ومتى الاستبدال؟

**الإجابة النموذجية:**
- **احذف (dropna)** عندما: الصف لا معنى له بدون القيمة (مثل NULL في `order_id`)، أو نسبة الـ NULL صغيرة جداً (< 2%) وعشوائية.
- **استبدل (fillna/impute)** عندما: الـ NULL يحمل معنى (NULL في `discount` = لا خصم)، أو نسبة الـ NULL كبيرة وتمثّل حالة حقيقية، أو في ML حيث الحذف يُفقد بيانات تدريب ثمينة.
- **علّم (add indicator column)** عندما: الـ NULL نفسه معلومة مفيدة (مثل "هذا المستخدم لم يُكمل ملفه الشخصي"):
```python
df.withColumn("has_age", col("age").isNotNull().cast("int")) \
  .fillna({"age": -1})
```

### سؤال 3 (متقدم): ما هو Forward Fill وكيف تُنفّذه بكفاءة في Spark؟

**الإجابة النموذجية:**
Forward Fill (أو LOCF — Last Observation Carried Forward) يُعبّئ الـ NULLs بآخر قيمة صالحة سابقة. مفيد لبيانات Time Series مثل بيانات الـ Sensors.

في Spark، يُنفَّذ باستخدام `Window Function` مع `last(col, ignorenulls=True)`:
```python
w = Window.orderBy("timestamp").rowsBetween(Window.unboundedPreceding, 0)
df.withColumn("value_ff", last(col("value"), ignorenulls=True).over(w))
```

**تحذير:** يُنشئ `unboundedPreceding` Wide Dependency وقد يكون بطيئاً مع بيانات ضخمة. تقسيم البيانات بـ Sensor ID أو Device ID يُحسّن الأداء بشكل كبير:
```python
w = Window.partitionBy("sensor_id").orderBy("timestamp") \
          .rowsBetween(Window.unboundedPreceding, 0)
```

---

## 9. 📋 ورقة الغش السريعة

### قرار المعالجة

```
هل NULL في عمود حرج (PK أو Key للـ Join)؟
  ← نعم → احذف الصف (dropna)

هل NULL يحمل معنى (مثل لا خصم، لا ضامن)؟
  ← نعم → ملء بقيمة Sentinel (0، "NONE"، False)

هل البيانات Time Series؟
  ← نعم → Forward Fill أو Backward Fill

هل هي ML Features؟
  ← استخدم Imputation (متوسط، وسيط) + أضف indicator column
```

### الدوال الأهم

```python
# الفحص
col("x").isNull()                      # هل NULL؟
isnan(col("x"))                        # هل NaN؟ (float/double فقط)
col("x").isNull() | isnan(col("x"))    # كلاهما معاً

# المعالجة
df.dropna(subset=["critical_col"])     # حذف
df.fillna(0, subset=["numeric_cols"])  # ملء بقيمة
coalesce(col("a"), col("b"), lit(0))   # أول غير-NULL
nanvl(col("x"), lit(0.0))             # NaN → قيمة بديلة

# Time Series
last(col("x"), ignorenulls=True).over(w)   # Forward Fill
first(col("x"), ignorenulls=True).over(w)  # Backward Fill

# إحصاءات
df.agg({"col": "avg"}).first()[0]      # متوسط
df.stat.approxQuantile("col", [0.5], 0.01)  # وسيط تقريبي
```

> [!TIP]
> **الخطوة القادمة:** انتقل للملف `17_date_time_engineering.md` لتتعلم كيف تتعامل مع التواريخ والمناطق الزمنية في PySpark وتجنب أخطاء الـ Timezone الشهيرة.

<!-- START_NAVIGATION_LINKS -->
---
### 🔗 روابط التنقل السريع

| السابق (Previous) | التالي (Next) |
| :--- | :--- |
| [◀️ 📘 الـ Joins: خوارزميات الربط، Broadcast، ومعالجة Data Skew](15_relational_joins.md) | [▶️ 📘 هندسة التواريخ والأوقات: Timestamps، Timezones، وعمليات الـ Time Series](17_date_time_engineering.md) |
<!-- END_NAVIGATION_LINKS -->
