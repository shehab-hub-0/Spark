# 📘 هندسة التواريخ والأوقات: Timestamps، Timezones، وعمليات الـ Time Series

> [!IMPORTANT]
> **هدف هذا الدليل:**
> بنهاية هذا الملف، ستفهم لماذا نفس الـ Timestamp يُعطي نتائج مختلفة على خوادم في مناطق زمنية مختلفة، وكيف تبني حسابات وقت موثوقة لبيئات الإنتاج الموزعة.

---

## 1. 🎯 لماذا التواريخ والأوقات صعبة في الأنظمة الموزعة؟

```
حادثة إنتاجية شهيرة:
  Pipeline يُحسب "طلبات الأمس"
  Server 1 (على AWS us-east-1): UTC-5 → يعتقد أن "اليوم" = 2025-01-15
  Server 2 (على AWS eu-west-1): UTC+0 → يعتقد أن "اليوم" = 2025-01-16

  نفس الـ Query، نتائج مختلفة على خوادم مختلفة!
  التقرير النهائي: 30% من البيانات في "اليوم الخطأ"!
```

**المشكلة الجوهرية:** Timestamp يُخزَّن كـ Unix epoch (ثوانٍ منذ 1970-01-01 UTC)، لكن العرض والتحويل يعتمدان على الـ Timezone المحلي للخادم.

---

## 2. 🏗️ أنواع التواريخ في Spark

```python
from pyspark.sql.functions import current_timestamp, current_date
from pyspark.sql.types import DateType, TimestampType

# الأنواع المتاحة:
# DateType:      YYYY-MM-DD (بدون وقت) — دقة يوم
# TimestampType: YYYY-MM-DD HH:MM:SS (مع وقت) — دقة microsecond
# StringType:    نص — يحتاج تحويل صريح

# الفرق الجوهري:
spark.sql("SELECT current_date(), current_timestamp()").show(truncate=False)
# +------------+----------------------------+
# |current_date|current_timestamp           |
# +------------+----------------------------+
# |2025-01-15  |2025-01-15 14:30:25.123456  |
```

---

## 3. ⚡ تحويل النصوص لتواريخ: to_date وto_timestamp

```python
from pyspark.sql.functions import to_date, to_timestamp, col

# ✅ دائماً حدّد الصيغة صراحة!
df = df.withColumn("sale_date",
    to_date(col("date_str"), "yyyy-MM-dd")
)

# صيغ شائعة
to_date(col("d"), "dd/MM/yyyy")           # 15/01/2025
to_date(col("d"), "MM-dd-yyyy")           # 01-15-2025
to_date(col("d"), "dd MMM yyyy")          # 15 Jan 2025
to_timestamp(col("t"), "yyyy-MM-dd HH:mm:ss")  # 2025-01-15 14:30:00
to_timestamp(col("t"), "yyyy-MM-dd'T'HH:mm:ssZ")  # ISO 8601 مع Timezone
```

> [!WARNING]
> **Common Mistake — بدون تحديد الصيغة:**
>
> ```python
> # ❌ خطر! Spark يُخمّن الصيغة
> df.withColumn("date", to_date(col("date_str")))
>
> # إذا كانت البيانات "01/15/2025" (أمريكي MM/DD/YYYY)
> # Spark يُفسّرها كـ DD/MM/YYYY → 2025-01-01 (خطأ!)
> # أو يُعيد NULL إذا لم يتعرف عليها
>
> # ✅ صحيح دائماً
> df.withColumn("date", to_date(col("date_str"), "MM/dd/yyyy"))
> ```

---

## 4. 🌍 Timezones: أكبر مصدر للأخطاء الصامتة

### المشكلة: spark.sql.session.timeZone

```python
# الإعداد الافتراضي: يستخدم Timezone الخادم (خطر في بيئات موزعة!)
# التحقق من الـ Timezone الحالي:
print(spark.conf.get("spark.sql.session.timeZone"))
# قد يُعطي: "UTC", "America/New_York", "Africa/Cairo"...

# الخطر: إذا كانت Executors على خوادم في مناطق زمنية مختلفة
# نفس الـ Timestamp سيُعطي نتائج مختلفة!
```

**الحل: اضبط UTC دائماً في الإنتاج**

```python
# ✅ في أول سطر من كل Job إنتاجي
spark.conf.set("spark.sql.session.timeZone", "UTC")

# أو في spark-submit:
# --conf spark.sql.session.timeZone=UTC
```

### التحويل بين Timezones

```python
from pyspark.sql.functions import convert_tz, from_utc_timestamp, to_utc_timestamp, col

# تحويل من UTC لتوقيت القاهرة
df.withColumn(
    "cairo_time",
    from_utc_timestamp(col("utc_timestamp"), "Africa/Cairo")
)
# 2025-01-15 12:00:00 UTC → 2025-01-15 14:00:00 Cairo (UTC+2)

# تحويل من توقيت محلي لـ UTC
df.withColumn(
    "utc_time",
    to_utc_timestamp(col("local_timestamp"), "Africa/Cairo")
)

# تحويل بين أي Timezone و Timezone
from pyspark.sql.functions import convert_tz
df.withColumn(
    "ny_time",
    convert_tz(col("cairo_time"), "Africa/Cairo", "America/New_York")
)
```

---

## 5. 📅 حسابات الفترات الزمنية

### الفرق بين تاريخين

```python
from pyspark.sql.functions import datediff, months_between, col

# الفرق بالأيام
df.withColumn("days_since_order",
    datediff(current_date(), col("order_date"))
)

# الفرق بالأشهر
df.withColumn("months_active",
    months_between(current_date(), col("signup_date")).cast("int")
)

# الفرق بالساعات (Timestamps)
from pyspark.sql.functions import unix_timestamp

df.withColumn("hours_to_deliver",
    (unix_timestamp(col("delivery_ts")) - unix_timestamp(col("order_ts"))) / 3600
)
```

### إضافة/طرح فترات زمنية

```python
from pyspark.sql.functions import date_add, date_sub, add_months

df.withColumn("due_date", date_add(col("order_date"), 30))    # +30 يوم
df.withColumn("prev_month", add_months(col("sale_date"), -1)) # -1 شهر
df.withColumn("week_ago", date_sub(col("event_date"), 7))     # -7 أيام
```

---

## 6. 🔧 استخراج مكونات التاريخ (Feature Engineering)

```python
from pyspark.sql.functions import (
    year, month, dayofmonth, dayofweek, dayofyear,
    hour, minute, second,
    quarter, weekofyear,
    date_trunc, date_format, col
)

df_features = df.withColumn("year",       year(col("order_date"))) \
               .withColumn("month",      month(col("order_date"))) \
               .withColumn("day",        dayofmonth(col("order_date"))) \
               .withColumn("day_of_week", dayofweek(col("order_date"))) \
               # 1=Sunday, 2=Monday, ..., 7=Saturday
               .withColumn("quarter",    quarter(col("order_date"))) \
               .withColumn("week_num",   weekofyear(col("order_date"))) \
               .withColumn("hour",       hour(col("order_ts"))) \
               .withColumn("is_weekend", 
                   dayofweek(col("order_date")).isin([1, 7]))  # 1=Sun, 7=Sat
```

### Truncation: تقريب للفترة المطلوبة

```python
from pyspark.sql.functions import date_trunc

# date_trunc يُدوّر للأسفل لبداية الفترة
df.withColumn("start_of_day",   date_trunc("day",    col("ts")))  # 2025-01-15 00:00:00
df.withColumn("start_of_hour",  date_trunc("hour",   col("ts")))  # 2025-01-15 14:00:00
df.withColumn("start_of_week",  date_trunc("week",   col("ts")))  # 2025-01-13 (Monday)
df.withColumn("start_of_month", date_trunc("month",  col("ts")))  # 2025-01-01
df.withColumn("start_of_year",  date_trunc("year",   col("ts")))  # 2025-01-01
```

**متى تستخدم `date_trunc`؟**
```python
# تجميع المبيعات اليومية (بغض النظر عن الوقت)
df.withColumn("day_key", date_trunc("day", col("order_ts"))) \
  .groupBy("day_key") \
  .sum("amount")
# ← كل الطلبات من نفس اليوم تُجمع معاً
```

---

## 7. ⏱️ Window Functions للـ Time Series

```python
from pyspark.sql import Window
from pyspark.sql.functions import lag, lead, avg, col

# Window مرتب بالوقت لكل منتج
w = Window.partitionBy("product_id").orderBy("sale_date")

df_timeseries = df \
    .withColumn("prev_day_sales", lag("amount", 1).over(w)) \
    .withColumn("next_day_sales", lead("amount", 1).over(w)) \
    .withColumn("sales_growth", 
        col("amount") - col("prev_day_sales")) \
    .withColumn("growth_pct",
        (col("amount") - col("prev_day_sales")) / col("prev_day_sales") * 100)

# المتوسط المتحرك (7 أيام)
w_rolling = Window.partitionBy("product_id") \
                  .orderBy("sale_date") \
                  .rowsBetween(-6, 0)  # 7 أيام: اليوم + 6 السابقة

df_with_ma = df.withColumn("moving_avg_7d",
    avg("amount").over(w_rolling)
)
```

---

## 8. 🚨 سيناريوهات الفشل وكيفية التشخيص

### حادثة: تقارير "أمس" تُعطي نتائج خاطئة

```python
# ❌ كود يعتمد على Timezone الخادم (خطر!)
from pyspark.sql.functions import current_date, date_sub

yesterday = date_sub(current_date(), 1)
df_yesterday = df.filter(col("order_date") == yesterday)
# إذا تغيّر الخادم أو Timezone الـ Server → نتائج مختلفة!
```

```python
# ✅ حل محدد بـ Timezone صريح
from datetime import datetime, timezone, timedelta

# حساب "أمس" بـ UTC صريح
utc_now = datetime.now(timezone.utc)
yesterday_utc = (utc_now - timedelta(days=1)).date().isoformat()
print(f"أمس UTC: {yesterday_utc}")  # "2025-01-14"

df_yesterday = df.filter(col("order_date") == yesterday_utc)
```

### حادثة: Timestamps تُعطي قيماً غريبة بعد تغيير Server

```python
# التشخيص: تحقق من Timezone الـ JVM
import subprocess
tz_check = spark.sql("SELECT current_timestamp(), current_timezone()").collect()
print(tz_check)
# إذا لم يكن UTC → مشكلة!

# الإصلاح
spark.conf.set("spark.sql.session.timeZone", "UTC")
# أعد تشغيل الـ Query
```

---

## 9. 🧪 التمارين العملية

### التمرين 1: تحويل التواريخ وفحص الـ Timezones

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("DateTimeLab").getOrCreate()

# بيانات تحتوي على صيغ مختلفة
data = [
    (1, "2025-01-15",           "2025-01-15 14:30:00"),
    (2, "15/01/2025",           "2025-01-15T10:00:00Z"),
    (3, "January 15, 2025",     None),
    (4, "2025-01-16",           "2025-01-16 09:15:30"),
]
df = spark.createDataFrame(data, ["id", "date_str", "ts_str"])

# تحويل كل الصيغ
df_converted = df \
    .withColumn("date_iso", 
        coalesce(
            to_date(col("date_str"), "yyyy-MM-dd"),
            to_date(col("date_str"), "dd/MM/yyyy"),
            to_date(col("date_str"), "MMMM dd, yyyy")
        )
    ) \
    .withColumn("timestamp_utc",
        coalesce(
            to_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("ts_str"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )
    )

df_converted.show(truncate=False)
```

### التمرين 2: تحليل مبيعات بالمتوسط المتحرك

```python
from pyspark.sql import Window
from pyspark.sql.functions import avg, col, date_add, lit
import random

spark.conf.set("spark.sql.session.timeZone", "UTC")

# بيانات مبيعات يومية
from datetime import date, timedelta

sales_data = []
base_date = date(2025, 1, 1)
for i in range(30):
    d = (base_date + timedelta(days=i)).isoformat()
    sales_data.append((d, random.randint(1000, 5000)))

df_sales = spark.createDataFrame(sales_data, ["sale_date", "revenue"]) \
               .withColumn("sale_date", to_date(col("sale_date")))

# حساب المتوسط المتحرك 7 أيام
w7 = Window.orderBy("sale_date").rowsBetween(-6, 0)

df_with_ma = df_sales \
    .withColumn("ma_7day", avg("revenue").over(w7).cast("int")) \
    .withColumn("prev_day", lag("revenue", 1).over(Window.orderBy("sale_date"))) \
    .withColumn("daily_growth_pct",
        ((col("revenue") - col("prev_day")) / col("prev_day") * 100).cast("int"))

print("=== تحليل المبيعات مع المتوسط المتحرك ===")
df_with_ma.show(30)
```

### التمرين 3: تجميع بالفترات الزمنية

```python
# تجميع المبيعات بمستويات زمنية مختلفة
df_ts = spark.createDataFrame([
    ("2025-01-15 08:30:00", 500),
    ("2025-01-15 14:15:00", 750),
    ("2025-01-15 20:00:00", 300),
    ("2025-01-16 09:00:00", 600),
    ("2025-01-16 16:30:00", 900),
], ["ts_str", "amount"]) \
.withColumn("ts", to_timestamp(col("ts_str")))

# تجميع يومي
daily = df_ts.withColumn("day", date_trunc("day", col("ts"))) \
             .groupBy("day").sum("amount")

# تجميع بالساعة
hourly = df_ts.withColumn("hour", date_trunc("hour", col("ts"))) \
              .groupBy("hour").sum("amount")

print("=== مبيعات يومية ===")
daily.orderBy("day").show()

print("=== مبيعات بالساعة ===")
hourly.orderBy("hour").show()
```

---

## 10. 🎓 أسئلة المقابلات التقنية

### سؤال 1: لماذا Timestamp يُعطي نتائج مختلفة على خوادم مختلفة؟

**الإجابة النموذجية:**
الـ `TimestampType` في Spark يُخزَّن داخلياً كـ **Unix epoch microseconds** (بدون معلومات Timezone). عند العرض أو التحويل (مثل `to_date`، `hour()`، `date_format`)، يستخدم Spark الـ Timezone المُعيَّن في `spark.sql.session.timeZone`. إذا كانت الـ Executors أو المراحل المختلفة على خوادم بـ Timezones مختلفة، ستُعطي العمليات نفسها نتائج مختلفة.

**الحل الجذري:** دائماً اضبط `spark.conf.set("spark.sql.session.timeZone", "UTC")` في أول Job، وخزّن الـ Timestamps بـ UTC دائماً.

### سؤال 2: ما الفرق بين `date_trunc` و`date_format`؟

**الإجابة النموذجية:**
- **`date_trunc("month", col)`:** يُعيد Timestamp بداية الفترة المحددة كـ `TimestampType`. مثال: `date_trunc("month", "2025-01-15")` → `2025-01-01 00:00:00`. مفيد للـ Aggregation لأن النتيجة تُحافظ على النوع.
- **`date_format(col, pattern)`:** يُعيد **String** حسب الصيغة المحددة. مثال: `date_format("2025-01-15", "yyyy-MM")` → `"2025-01"`. مفيد للعرض أو توليد مفاتيح Partition.

### سؤال 3 (متقدم): كيف تُنفَّذ `lag()` و`lead()` فيزيائياً في Spark؟

**الإجابة النموذجية:**
`lag()` و`lead()` هما Window Functions تتطلب **Sort + Shuffle** قبل التنفيذ:
1. Shuffle: البيانات تُجمَّع حسب `partitionBy` clause (مثل `product_id`)
2. Sort: داخل كل Partition، البيانات تُرتَّب حسب `orderBy` clause (مثل `sale_date`)
3. Iteration: Spark يمر على الصفوف المرتبة ويُعيد القيمة النسبية (N صف قبل أو بعد)

**التحسين:** تحديد `partitionBy` مناسب يُقسّم البيانات ويُقلل الذاكرة المطلوبة لكل Task. بدون `partitionBy`، كل البيانات تذهب لـ Partition واحد → OOM!

---

## 11. 📋 ورقة الغش السريعة

```python
# ── قاعدة أولى: UTC في كل مكان ─────────────────────────────────
spark.conf.set("spark.sql.session.timeZone", "UTC")

# ── تحويل النصوص ─────────────────────────────────────────────────
to_date(col("d"), "yyyy-MM-dd")          # String → Date
to_timestamp(col("t"), "yyyy-MM-dd HH:mm:ss")  # String → Timestamp
unix_timestamp(col("t"))                 # Timestamp → Unix epoch (seconds)
col("epoch").cast("timestamp")           # Unix epoch → Timestamp

# ── استخراج المكونات ─────────────────────────────────────────────
year(c) / month(c) / dayofmonth(c)       # مكونات التاريخ
hour(c) / minute(c) / second(c)         # مكونات الوقت
dayofweek(c)                             # 1=Sun, 7=Sat
quarter(c)                               # 1, 2, 3, 4
weekofyear(c)                            # 1-53

# ── حسابات ───────────────────────────────────────────────────────
datediff(end, start)                     # فرق بالأيام
months_between(end, start)              # فرق بالأشهر
date_add(col, n)                         # +N يوم
date_sub(col, n)                         # -N يوم
add_months(col, n)                       # +N شهر

# ── Truncation ────────────────────────────────────────────────────
date_trunc("day"/"hour"/"week"/"month"/"year", col)  # تقريب للأسفل

# ── Timezone ─────────────────────────────────────────────────────
from_utc_timestamp(col, "Africa/Cairo")  # UTC → Cairo
to_utc_timestamp(col, "Africa/Cairo")    # Cairo → UTC
```

> [!TIP]
> **الخطوة القادمة:** انتقل للملف `18_advanced_string_manipulation.md` لتتعلم التعامل مع النصوص والتعبيرات النمطية (Regex) ودوال التلاعب بالنصوص في الإنتاج.

<!-- START_NAVIGATION_LINKS -->
---
### 🔗 روابط التنقل السريع

| السابق (Previous) | التالي (Next) |
| :--- | :--- |
| [◀️ 📘 معالجة البيانات الناقصة (Missing Data): استراتيجيات الـ NULL في الإنتاج](16_handling_missing_data.md) | [▶️ 📘 معالجة النصوص المتقدمة: Regex، String Functions، وتحليل البيانات النصية](18_advanced_string_manipulation.md) |
<!-- END_NAVIGATION_LINKS -->
