# 📘 معالجة النصوص المتقدمة: Regex، String Functions، وتحليل البيانات النصية

> [!IMPORTANT]
> **هدف هذا الدليل:**
> بنهاية هذا الملف، ستتقن أهم دوال معالجة النصوص في PySpark، وكيف تكتب Regex فعّالة، ومتى تُفضّل دوال Spark المُدمجة على Python UDFs (الأبطأ بـ 10x).

---

## 1. 🎯 لماذا معالجة النصوص مختلفة في Spark؟

```python
# في Python العادية:
text = "  Hello World  "
clean = text.strip().lower().replace("world", "spark")

# في Spark: لا تستطيع استخدام Python methods مباشرة على الأعمدة!
# يجب استخدام دوال Spark المُدمجة (Built-in Functions)
# أو Python UDFs (بطيئة!)

# ✅ الطريقة الصحيحة
from pyspark.sql.functions import trim, lower, regexp_replace, col

df.withColumn("clean",
    regexp_replace(lower(trim(col("text"))), "world", "spark")
)
```

**لماذا دوال Spark أسرع من Python UDFs بـ 10x؟**

```
Python UDF:
  Spark Executor (JVM) → Py4J Bridge → Python Process → Process → JVM
  لكل صف: تسلسل البيانات + نقلها عبر الجسر + تسلسل النتيجة

Built-in Functions:
  Spark Executor (JVM) → Tungsten Bytecode
  لكل صف: عملية مباشرة في الـ JVM بدون أي نقل!
```

---

## 2. 🔧 الدوال الأساسية لمعالجة النصوص

```python
from pyspark.sql.functions import (
    # تنظيف النص
    trim, ltrim, rtrim,            # إزالة المسافات
    lower, upper, initcap,          # تحويل الحالة
    
    # تحليل النص
    length, char_length,            # طول النص
    substring, substr,              # جزء من النص
    left, right,                    # من اليسار/اليمين
    
    # البحث والاستبدال
    contains, startswith, endswith, # يحتوي؟ يبدأ بـ؟ ينتهي بـ؟
    instr, locate,                  # موضع نص داخل نص
    replace, regexp_replace,        # استبدال نص أو Pattern
    regexp_extract,                 # استخراج باستخدام Regex
    
    # تقسيم ودمج
    split, concat, concat_ws,       # تقسيم ودمج النصوص
    array_join,                     # دمج Array لـ String
    
    # تحويلات
    encode, decode,                 # تحويل الترميز
    lpad, rpad,                     # إضافة padding
    translate,                      # استبدال حرف بحرف
)
```

---

## 3. ⚡ أمثلة عملية على الدوال المهمة

### التنظيف الأساسي

```python
from pyspark.sql.functions import trim, upper, lower, regexp_replace, col

# تنظيف شامل لعمود اسم
df.withColumn("name_clean",
    upper(                              # ALICE JOHNSON
        trim(                           # إزالة مسافات البداية والنهاية
            regexp_replace(
                col("name"),
                r"[^a-zA-Z\s]",        # إزالة كل ما ليس حرفاً أو مسافة
                ""
            )
        )
    )
)

# تنظيف رقم هاتف (الاحتفاظ بالأرقام فقط)
df.withColumn("phone_clean",
    regexp_replace(col("phone"), r"[^0-9]", "")
)
# "+20 10-1234-5678" → "201012345678"
```

### استخراج معلومات بالـ Regex

```python
from pyspark.sql.functions import regexp_extract

# استخراج البريد الإلكتروني
df.withColumn("email",
    regexp_extract(col("text"), r"[\w.]+@[\w.]+\.\w+", 0)
)

# استخراج رقم الهاتف
df.withColumn("phone",
    regexp_extract(col("text"), r"\+?[\d\s\-]{10,15}", 0)
)

# استخراج أجزاء محددة (Groups)
# مثال: استخراج الدولة والمدينة من "Cairo, Egypt"
df.withColumn("city",    regexp_extract(col("location"), r"^([^,]+),", 1)) \
  .withColumn("country", regexp_extract(col("location"), r",\s*(.+)$", 1))
```

### تقسيم النصوص (split)

```python
from pyspark.sql.functions import split, col, getItem, size

# تقسيم بفاصل
df.withColumn("tags_array", split(col("tags_str"), ","))
# "python,spark,data" → ["python", "spark", "data"]

# الوصول للعناصر
df.withColumn("first_tag",  split(col("tags_str"), ",").getItem(0))
df.withColumn("tag_count",  size(split(col("tags_str"), ",")))

# تقسيم URL
df.withColumn("parts",    split(col("url"), "/")) \
  .withColumn("protocol", col("parts").getItem(0)) \
  .withColumn("domain",   col("parts").getItem(2))
```

### دمج النصوص (concat, concat_ws)

```python
from pyspark.sql.functions import concat, concat_ws, col, lit

# دمج عدة أعمدة
df.withColumn("full_name",
    concat(col("first_name"), lit(" "), col("last_name"))
)

# دمج مع فاصل (concat with separator)
df.withColumn("address",
    concat_ws(", ",          # الفاصل
        col("street"),
        col("city"),
        col("country")
    )
)
# "123 Main St, Cairo, Egypt"
```

---

## 4. 🔍 Regex في Spark: دليل عملي

### الـ Patterns الأكثر استخداماً

```python
# ── الاختبار (هل يطابق؟) ────────────────────────────────────────
from pyspark.sql.functions import col

# contains: هل يحتوي على النص؟ (بدون Regex)
df.filter(col("email").contains("@gmail.com"))

# rlike: هل يطابق الـ Regex؟
df.filter(col("phone").rlike(r"^\+\d{12}$"))  # +201234567890

# ── الاستخراج (extract) ──────────────────────────────────────────
from pyspark.sql.functions import regexp_extract

# استخراج أول تطابق (group 0 = التطابق الكامل)
df.withColumn("year", regexp_extract(col("date_str"), r"(\d{4})", 1))

# ── الاستبدال (replace) ──────────────────────────────────────────
from pyspark.sql.functions import regexp_replace

# إزالة HTML tags
df.withColumn("plain_text",
    regexp_replace(col("html_content"), r"<[^>]+>", " ")
)

# توحيد المسافات المتعددة
df.withColumn("normalized",
    regexp_replace(col("text"), r"\s+", " ")
)

# إزالة الأحرف الخاصة
df.withColumn("alphanumeric",
    regexp_replace(col("text"), r"[^a-zA-Z0-9\s]", "")
)
```

### Regex Patterns مرجعية

```
.     → أي حرف (ما عدا newline)
\d    → أي رقم [0-9]
\w    → حرف أو رقم [a-zA-Z0-9_]
\s    → مسافة (space, tab, newline)
^     → بداية النص
$     → نهاية النص
+     → مرة أو أكثر
*     → صفر مرة أو أكثر
?     → صفر أو مرة واحدة
{n}   → بالضبط n مرة
{n,m} → من n إلى m مرة
[abc] → أي حرف من القائمة
[^abc]→ أي حرف ليس في القائمة
(...)  → Capture Group
```

---

## 5. 🔄 Window Functions للنصوص

```python
from pyspark.sql import Window
from pyspark.sql.functions import collect_list, concat_ws, col

# تجميع النصوص لكل مجموعة (كـ string واحد)
w = Window.partitionBy("user_id").orderBy("timestamp")

df.withColumn(
    "conversation_history",
    concat_ws(" | ", collect_list(col("message")).over(w))
)
# "مرحبا | كيف حالك | بخير شكراً"
```

---

## 6. ⚠️ Python UDFs: متى تستخدمها ومتى تتجنبها

```python
# ❌ UDF بطيء (يتطلب تسلسل البيانات ذهاباً وإياباً بين JVM وPython)
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def clean_text_udf(text):
    if text is None:
        return None
    import re
    return re.sub(r"[^a-zA-Z\s]", "", text.strip().lower())

df.withColumn("clean", clean_text_udf(col("text")))  # بطيء!

# ✅ نفس الوظيفة بدوال Spark المُدمجة (أسرع 10x)
from pyspark.sql.functions import lower, trim, regexp_replace

df.withColumn("clean",
    regexp_replace(lower(trim(col("text"))), r"[^a-z\s]", "")
)
```

**متى تُستخدم UDFs بشكل مقبول؟**
1. منطق معقد جداً لا يمكن التعبير عنه بدوال Spark
2. استدعاء مكتبة Python خاصة (مثل `spacy` للـ NLP)
3. في هذه الحالات: استخدم **Pandas UDFs** (Vectorized UDFs) — أسرع بـ 5x من Python UDFs

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

@pandas_udf(StringType())
def advanced_clean(series: pd.Series) -> pd.Series:
    """UDF متجه (Vectorized) — يعالج Batch من الصفوف معاً"""
    import re
    return series.fillna("") \
                 .str.strip() \
                 .str.lower() \
                 .str.replace(r"[^a-z\s]", "", regex=True) \
                 .str.replace(r"\s+", " ", regex=True)

df.withColumn("clean", advanced_clean(col("text")))
# ← أسرع بكثير من Python UDF العادية لأنه يعالج Pandas Series كاملة
```

---

## 7. 🌐 التعامل مع النصوص متعددة اللغات

```python
from pyspark.sql.functions import col, regexp_replace, lower

# الكشف عن اللغة العربية
df.filter(col("text").rlike(r"[\u0600-\u06FF]"))  # أحرف عربية

# إزالة الحركات العربية (تشكيل)
df.withColumn("text_no_tashkeel",
    regexp_replace(col("text"), r"[\u064B-\u065F\u0670]", "")
)

# توحيد الألف (أ، إ، آ → ا)
df.withColumn("text_normalized",
    regexp_replace(
        regexp_replace(col("text"), r"[أإآ]", "ا"),
        r"[ىئ]", "ي"
    )
)

# الكشف عن Emoji
df.filter(col("text").rlike(r"[\U0001F600-\U0001F64F]"))
# إزالة Emoji
df.withColumn("text_no_emoji",
    regexp_replace(col("text"), r"[^\x00-\x7F]", " ")
)
```

---

## 8. 🚨 سيناريوهات الفشل وكيفية التشخيص

### حادثة: Python UDF يُبطّئ الـ Pipeline بمقدار 10x

```python
# التشخيص:
# افتح Spark UI → SQL Plan
# إذا رأيت: "BatchEvalPython" أو "ArrowEvalPython"
# → هذا UDF يعمل بـ Python Serialization!

# الأعراض:
# Task Duration طويل جداً
# CPU Usage مرتفع على Python Process
# Memory: تسرب بين JVM وPython
```

**الحل:**
```python
# Step 1: هل يمكن استبداله بدوال Spark؟ → نعم في معظم الحالات
# Step 2: إذا لم يمكن → حوّله لـ Pandas UDF

# Step 3: إذا كان لابد من Python UDF:
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
# Arrow يُسرّع نقل البيانات بين JVM وPython بـ 5-10x
```

### حادثة: regexp_replace يُعيد NULL للصفوف الصحيحة

```python
# السبب: الـ Regex Pattern خاطئ أو يحتوي على أحرف تحتاج Escaping

# ❌ خاطئ: النقطة (.) في Regex تعني "أي حرف"
df.withColumn("clean",
    regexp_replace(col("email"), "@gmail.com", "")
)
# ".com" يطابق "@gmailXcom" أيضاً!

# ✅ صحيح: استخدم \. لتعني النقطة الحرفية
df.withColumn("clean",
    regexp_replace(col("email"), r"@gmail\.com", "")
)
```

---

## 9. 🧪 التمارين العملية

### التمرين 1: تنظيف بيانات مستخدمين خام

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[4]").appName("StringLab").getOrCreate()

raw_data = [
    (1, "  Alice JOHNSON  ", "+20-10-1234-5678", "alice@GMAIL.COM", "Python, Spark, SQL"),
    (2, "BOB O'Brien",      "010 9876 5432",    "bob.smith@company.co.uk", "Java"),
    (3, " charlie  davis ", "+1 (555) 123-4567", "charlie123@yahoo.com", "R, SAS, Python"),
    (4, "DIANA  ",          "N/A",               "diana@",              ""),
]
df = spark.createDataFrame(raw_data, ["id", "name", "phone", "email", "skills"])

# تطبيق Pipeline تنظيف شامل
df_clean = df.select(
    col("id"),
    
    # تنظيف الاسم: إزالة مسافات زائدة + توحيد الحالة
    initcap(regexp_replace(trim(col("name")), r"\s+", " ")).alias("name"),
    
    # تنظيف الهاتف: الاحتفاظ بالأرقام + فحص الصلاحية
    regexp_replace(col("phone"), r"[^0-9]", "").alias("phone_digits"),
    
    # تنظيف البريد: lowercase + فحص صلاحية
    lower(trim(col("email"))).alias("email"),
    
    # تحويل Skills لـ Array
    split(regexp_replace(col("skills"), r"\s*,\s*", ","), ",").alias("skills_array")
) \
.withColumn("phone_valid", length(col("phone_digits")).between(10, 15)) \
.withColumn("email_valid", col("email").rlike(r"^[\w.+]+@[\w.]+\.[a-z]{2,}$"))

df_clean.show(truncate=False)
```

### التمرين 2: استخراج معلومات من سجلات (Log Parsing)

```python
# بيانات سجلات خادم ويب
log_data = [
    "2025-01-15 14:30:22 ERROR user_id=12345 action=login ip=192.168.1.1 status=FAILED",
    "2025-01-15 14:31:00 INFO  user_id=67890 action=purchase ip=10.0.0.5 status=SUCCESS amount=250.00",
    "2025-01-15 14:31:45 WARN  user_id=11111 action=view ip=172.16.0.1 status=SUCCESS",
]
df_logs = spark.createDataFrame([(l,) for l in log_data], ["log_line"])

# استخراج المعلومات بالـ Regex
df_parsed = df_logs.select(
    regexp_extract(col("log_line"), r"^(\d{4}-\d{2}-\d{2})", 1).alias("date"),
    regexp_extract(col("log_line"), r"(\d{2}:\d{2}:\d{2})", 1).alias("time"),
    regexp_extract(col("log_line"), r"(ERROR|INFO|WARN)", 1).alias("level"),
    regexp_extract(col("log_line"), r"user_id=(\d+)", 1).alias("user_id"),
    regexp_extract(col("log_line"), r"action=(\w+)", 1).alias("action"),
    regexp_extract(col("log_line"), r"ip=([\d.]+)", 1).alias("ip"),
    regexp_extract(col("log_line"), r"status=(\w+)", 1).alias("status"),
    regexp_extract(col("log_line"), r"amount=([\d.]+)", 1).alias("amount"),
)

print("=== سجلات مُحلَّلة ===")
df_parsed.show(truncate=False)
```

### التمرين 3: مقارنة أداء UDF مقابل Built-in Functions

```python
import time

# بيانات اختبار كبيرة
df_large = spark.range(1, 1_000_001) \
    .selectExpr("cast(id as string) as text", "concat('  Hello World ', cast(id as string)) as message")

# الطريقة 1: Python UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def clean_udf(text):
    if text is None:
        return None
    import re
    return re.sub(r"\s+", " ", text.strip().lower())

start = time.time()
df_large.withColumn("clean", clean_udf(col("message"))).count()
udf_time = time.time() - start
print(f"Python UDF:            {udf_time:.2f}s")

# الطريقة 2: Built-in Functions
start = time.time()
df_large.withColumn("clean",
    regexp_replace(lower(trim(col("message"))), r"\s+", " ")
).count()
builtin_time = time.time() - start
print(f"Built-in Functions:    {builtin_time:.2f}s")
print(f"تسريع: {udf_time/builtin_time:.1f}x")
```

---

## 10. 🎓 أسئلة المقابلات التقنية

### سؤال 1: لماذا Python UDFs أبطأ من Built-in Functions؟

**الإجابة النموذجية:**
Built-in Functions تُنفَّذ مباشرةً في الـ JVM باستخدام Tungsten Bytecode — لا overhead إضافي. Python UDFs تتطلب:
1. تسلسل (Serialize) البيانات من JVM format لـ Python format (عبر Py4J/Arrow)
2. إرسالها لـ Python Process
3. تنفيذ الدالة في Python
4. تسلسل النتيجة وإرسالها للـ JVM

هذا الـ Roundtrip يحدث لكل Partition وهو مكلف جداً. **الحل:** استخدام Pandas UDFs (Vectorized) التي تُرسل Batch من الصفوف دفعة واحدة عبر Apache Arrow، مما يُقلل عدد الـ Roundtrips بشكل كبير.

### سؤال 2: ما الفرق بين `regexp_extract` و`regexp_replace`؟

**الإجابة النموذجية:**
- **`regexp_extract(col, pattern, groupIndex)`:** يُعيد **جزءاً من النص** يطابق الـ Pattern. الـ `groupIndex` يحدد أي Capture Group يُعاد (0 = الكامل، 1 = الأول، ...). مفيد لاستخراج معلومات محددة.
- **`regexp_replace(col, pattern, replacement)`:** يُعيد **نص جديد** حيث كل تطابق للـ Pattern يُستبدل بـ `replacement`. مفيد لتنظيف النصوص.

### سؤال 3 (متقدم): متى تستخدم Pandas UDF بدلاً من Python UDF؟

**الإجابة النموذجية:**
استخدم Pandas UDF (Vectorized UDF) عندما:
1. تحتاج منطقاً معقداً لا يمكن تعبيره بدوال Spark
2. تحتاج مكتبات Python مثل `spacy`, `transformers`, `scikit-learn`
3. تحتاج أداءً أفضل من Python UDF

Pandas UDF تعالج `pd.Series` كاملة في كل استدعاء (بدلاً من صف صف). تنتقل البيانات عبر Apache Arrow (بدلاً من Pickle) — أسرع بـ 5-10x من Python UDF العادية.

```python
@pandas_udf(StringType())
def my_vectorized_udf(s: pd.Series) -> pd.Series:
    # يعمل على كل الـ Partition دفعة واحدة!
    return s.str.lower().str.strip()
```

---

## 11. 📋 ورقة الغش السريعة

```python
# ── التنظيف ───────────────────────────────────────────────────────
trim/ltrim/rtrim(col)                    # إزالة مسافات
lower(col) / upper(col)                  # تحويل الحالة
initcap(col)                             # كل كلمة بحرف كبير
regexp_replace(col, r"[^a-z]", "")      # إزالة أحرف معينة

# ── الاستخراج ─────────────────────────────────────────────────────
substring(col, start, length)            # جزء من النص
regexp_extract(col, r"(\d+)", 1)        # استخراج بـ Regex
split(col, ",").getItem(0)              # العنصر الأول بعد التقسيم

# ── الفحص ────────────────────────────────────────────────────────
col.contains("text")                     # يحتوي على؟
col.startswith("prefix")                # يبدأ بـ؟
col.endswith("suffix")                  # ينتهي بـ؟
col.rlike(r"^\d+$")                     # يطابق Regex؟
length(col) > 0                         # ليس فارغاً؟

# ── الدمج ────────────────────────────────────────────────────────
concat(col1, lit(" "), col2)             # دمج بدون فاصل
concat_ws(", ", col1, col2, col3)       # دمج مع فاصل

# ── أداء ─────────────────────────────────────────────────────────
# الأسرع: Built-in Functions (Tungsten)
# ثم: Pandas UDF (Arrow vectorized)
# الأبطأ: Python UDF (Py4J serialization)
```

> [!TIP]
> **الخطوة القادمة:** انتقل للملف `19_custom_udfs.md` لتتعلم كيف تكتب UDFs احترافية مع Type Checking، Error Handling، واختبار الأداء.

<!-- START_NAVIGATION_LINKS -->
---
### 🔗 روابط التنقل السريع

| السابق (Previous) | التالي (Next) |
| :--- | :--- |
| [◀️ 📘 هندسة التواريخ والأوقات: Timestamps، Timezones، وعمليات الـ Time Series](17_date_time_engineering.md) | [▶️ 📘 الـ UDFs الاحترافية: Python UDFs، Pandas UDFs، وكيفية كتابة دوال آمنة وسريعة](19_custom_udfs.md) |
<!-- END_NAVIGATION_LINKS -->
