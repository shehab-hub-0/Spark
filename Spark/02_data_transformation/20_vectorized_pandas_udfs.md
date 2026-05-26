# 📘 Pandas UDFs المتجهة (Vectorized): ML Inference، GroupedData، وWindow Functions

> [!IMPORTANT]
> **هدف هذا الدليل:**
> بنهاية هذا الملف، ستتقن أنواع Pandas UDFs الأربعة، وكيف تستخدمها لتطبيق ML Models على ملايين السجلات بكفاءة، وكيف تُجري حسابات Window متقدمة لا تستطيع Built-in Functions تنفيذها.

---

## 1. 🎯 لماذا Pandas UDFs غيّرت قواعد اللعبة؟

قبل Spark 2.3 (2018)، كان الاختيار بين:
- Built-in Functions: سريعة لكن محدودة
- Python UDFs: مرنة لكن بطيئة جداً (Python سطر بسطر)

Pandas UDFs جمعت الأفضل من العالمين:
```
Python UDF:     صف واحد ← Python Process ← صف واحد
Pandas UDF:     Batch كامل → Apache Arrow → pd.Series → نتيجة كاملة
```

**Apache Arrow:** صيغة بيانات عمودية في الذاكرة مُشتركة بين JVM وPython. **لا تسلسل (Serialization)!** البيانات تنتقل بالـ Zero-Copy مباشرة — بدون نسخ في الذاكرة.

---

## 2. 🏗️ الأنواع الأربعة لـ Pandas UDFs

### النوع 1: Scalar UDF (الأكثر شيوعاً)

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import DoubleType, StringType

@pandas_udf(DoubleType())
def normalize_score(scores: pd.Series) -> pd.Series:
    """تطبيع الدرجات بين 0 و1"""
    min_val = scores.min()
    max_val = scores.max()
    if max_val == min_val:
        return pd.Series([0.5] * len(scores))
    return (scores - min_val) / (max_val - min_val)

# استخدام مثل أي دالة Spark
df.withColumn("score_normalized", normalize_score(col("score")))
```

**كيف تعمل داخلياً:**
```
DataFrame Column (JVM Tungsten) 
    → Arrow Buffer (مشترك بين JVM وPython)
    → pd.Series في Python
    → normalize_score(series) تُعالج الـ Batch كاملاً
    → pd.Series النتيجة
    → Arrow Buffer
    → Column في الـ DataFrame
```

### النوع 2: Iterator of Series UDF

```python
from typing import Iterator

@pandas_udf(StringType())
def predict_with_model(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """
    يُحمَّل الـ Model مرة واحدة لكل Partition
    مثالي لـ ML Inference!
    """
    import pickle
    
    # تحميل ثقيل مرة واحدة!
    with open("/models/classifier.pkl", "rb") as f:
        model = pickle.load(f)
    
    for series in iterator:
        # تطبيق على كل Batch
        predictions = model.predict(series.fillna("").tolist())
        yield pd.Series(predictions)

df.withColumn("prediction", predict_with_model(col("text")))
```

### النوع 3: Iterator of Multiple Series UDF

```python
from pyspark.sql.functions import struct

@pandas_udf(DoubleType())
def fraud_score(
    iterator: Iterator[pd.DataFrame]
) -> Iterator[pd.Series]:
    """
    يستقبل عدة أعمدة كـ DataFrame
    مثالي لـ Multi-feature ML Models
    """
    import numpy as np
    model = load_fraud_model()
    
    for df_batch in iterator:
        features = np.column_stack([
            df_batch["amount"].fillna(0),
            df_batch["merchant_risk"].fillna(0.5),
            df_batch["hour"].fillna(12)
        ])
        probabilities = model.predict_proba(features)[:, 1]
        yield pd.Series(probabilities)

# عند الاستخدام: مرّر عدة أعمدة عبر struct
df.withColumn(
    "fraud_prob",
    fraud_score(struct(col("amount"), col("merchant_risk"), col("hour")))
)
```

### النوع 4: Grouped Map UDF

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql import GroupedData

# يُطبَّق على كل مجموعة (Partition بعد groupBy) كـ pd.DataFrame
# ويُعيد pd.DataFrame

output_schema = "user_id long, product_id long, score double, rank int"

@pandas_udf(output_schema)
def rank_recommendations(df: pd.DataFrame) -> pd.DataFrame:
    """
    ترتيب التوصيات لكل مستخدم
    يُستدعى مرة لكل user_id (مجموعة كاملة في DataFrame)
    """
    df_sorted = df.sort_values("score", ascending=False).reset_index(drop=True)
    df_sorted["rank"] = range(1, len(df_sorted) + 1)
    return df_sorted[["user_id", "product_id", "score", "rank"]]

# تطبيق على كل مجموعة
result = recommendations_df.groupBy("user_id").applyInPandas(
    rank_recommendations, 
    schema=output_schema
)
```

---

## 3. ⚡ ML Inference على نطاق واسع

### Pattern الإنتاجي: Batch Prediction

```python
from pyspark.sql.functions import pandas_udf, struct, col
from pyspark.sql.types import DoubleType
from typing import Iterator
import pandas as pd

# الـ Model path على HDFS/S3 (متاح لكل الـ Executors)
MODEL_PATH = "s3://models/sentiment_model/"

@pandas_udf(DoubleType())
def predict_sentiment(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """
    نموذج تحليل مشاعر يعمل على ملايين التعليقات
    
    Iterator Pattern يضمن:
    1. تحميل الـ Model مرة واحدة لكل Executor Partition
    2. معالجة البيانات على Batches من 10000 صف
    3. استخدام GPU إذا كانت متاحة
    """
    # تحميل mرة واحدة
    from transformers import pipeline
    sentiment_pipe = pipeline(
        "sentiment-analysis",
        model=MODEL_PATH,
        device=0 if torch.cuda.is_available() else -1  # GPU إذا متاح
    )
    
    for text_series in iterator:
        # معالجة Batch كامل
        texts = text_series.fillna("").tolist()
        results = sentiment_pipe(texts, batch_size=32, truncation=True)
        
        # استخراج الدرجة (positive probability)
        scores = [
            r["score"] if r["label"] == "POSITIVE" else 1 - r["score"]
            for r in results
        ]
        yield pd.Series(scores)

# التطبيق على ملايين التعليقات
reviews_scored = reviews_df.withColumn(
    "sentiment_score",
    predict_sentiment(col("review_text"))
)
```

---

## 4. 🪟 Pandas UDFs مع Window Functions

```python
from pyspark.sql import Window
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import DoubleType
import pandas as pd
import numpy as np

# Pandas UDF لحساب Moving Average مُخصَّص
# (مختلف عن avg().over() المُدمجة — يمكنك كتابة أي منطق)

@pandas_udf(DoubleType())
def exponential_moving_avg(values: pd.Series, alphas: pd.Series) -> pd.Series:
    """
    Exponential Moving Average مع Alpha مُخصَّص لكل صف
    هذا غير ممكن بـ Built-in Window Functions!
    """
    result = pd.Series(index=values.index, dtype=float)
    ema = values.iloc[0]
    for i, (val, alpha) in enumerate(zip(values, alphas)):
        ema = alpha * val + (1 - alpha) * ema
        result.iloc[i] = ema
    return result

# تطبيق
w = Window.partitionBy("product_id").orderBy("date")

df.withColumn(
    "ema_price",
    exponential_moving_avg(col("price"), col("smoothing_factor")).over(w)
)
```

---

## 5. 📊 applyInPandas: العملية الأكثر مرونة

```python
# applyInPandas يُطبّق دالة Python كاملة على كل مجموعة

def detect_anomalies(user_df: pd.DataFrame) -> pd.DataFrame:
    """
    كشف شذوذات في مشتريات مستخدم معين
    يُستدعى مرة واحدة لكل user_id
    """
    # حساب Z-Score لكل مشترى
    from scipy import stats
    
    user_df = user_df.sort_values("timestamp")
    amounts = user_df["amount"]
    
    # Z-score
    z_scores = np.abs(stats.zscore(amounts))
    user_df["is_anomaly"] = z_scores > 3  # أكثر من 3 انحرافات معيارية
    user_df["z_score"] = z_scores
    
    return user_df

# Schema الإخراج
output_schema = """
    user_id long, timestamp string, amount double, 
    is_anomaly boolean, z_score double
"""

result = transactions \
    .groupBy("user_id") \
    .applyInPandas(detect_anomalies, schema=output_schema)
```

### mapInPandas: للعمليات على مستوى الـ Partition

```python
def enrich_with_external_api(df_iter):
    """
    إثراء البيانات بـ API خارجي
    mapInPandas تُعطيك كل الـ Partition كـ Iterator من DataFrames
    """
    import requests
    
    # Session واحدة للـ Partition كلها (أفضل من فتح connection لكل صف)
    session = requests.Session()
    
    for df_batch in df_iter:
        # استدعاء API على Batch
        ids = df_batch["user_id"].tolist()
        response = session.post("https://api.example.com/enrich", 
                               json={"ids": ids})
        enrichment = response.json()
        
        df_batch["credit_score"] = df_batch["user_id"].map(
            lambda uid: enrichment.get(str(uid), {}).get("credit_score", None)
        )
        yield df_batch

result = df.mapInPandas(enrich_with_external_api, schema=df.schema)
```

---

## 6. 🚨 سيناريوهات الفشل وكيفية التشخيص

### حادثة 1: OOM في Grouped Map UDF

```text
ERROR ExecutorLostFailure: Executor 5 exited with exit code 137 (OOM)
Stage: applyInPandas stage
```

**السبب:** مجموعة واحدة (مثل user_id=1 مع ملايين السجلات) لا تسع في ذاكرة Executor واحد.

**التشخيص:**
```python
# أولاً: تحقق من توزيع المجموعات
df.groupBy("user_id").count() \
  .orderBy("count", ascending=False) \
  .show(5)
# إذا كانت قيمة واحدة تحتوي على 10M+ سجل → ستنهار!
```

**الحل:**
```python
# الحل 1: تصفية المجموعات الكبيرة قبل الـ applyInPandas
df.filter(
    df.user_id.isin(manageable_user_ids)
).groupBy("user_id").applyInPandas(...)

# الحل 2: تقسيم المجموعات الكبيرة بـ Salting
df.withColumn("user_shard", 
    concat(col("user_id"), lit("_"), (col("row_num") % 10).cast("string"))
).groupBy("user_shard").applyInPandas(...)
```

### حادثة 2: Arrow Type Mismatch

```text
ERROR ArrowException: Column 'score' has type Double but UDF returned Int64
```

**الحل:**
```python
# ❌ Type mismatch
@pandas_udf(DoubleType())
def my_udf(x: pd.Series) -> pd.Series:
    return x.astype(int)  # يُعيد int64 لكن المُعلَن double!

# ✅ تأكد من توافق الأنواع
@pandas_udf(DoubleType())
def my_udf(x: pd.Series) -> pd.Series:
    return x.astype(float)  # ← مطابق للـ DoubleType!
```

---

## 7. 🧪 التمارين العملية

### التمرين 1: ML Scoring على نطاق واسع

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col, struct
from pyspark.sql.types import DoubleType
from typing import Iterator
import pandas as pd
import numpy as np

spark = SparkSession.builder.master("local[4]").appName("PandasUDFLab").getOrCreate()

# بيانات ائتمانية وهمية
credit_data = spark.range(1, 100001).selectExpr(
    "id as user_id",
    "cast(rand() * 100000 as double) as income",
    "cast(rand() * 50000 as double) as debt",
    "cast(rand() * 10 + 1 as int) as credit_history_years",
    "cast(rand() * 5 as int) as late_payments"
)

# Pandas UDF لتسجيل الائتمان (يُحاكي ML Model)
@pandas_udf(DoubleType())
def credit_score_udf(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.Series]:
    """
    حساب درجة ائتمانية باستخدام نموذج بسيط
    Iterator Pattern: يحسب الـ Coefficients مرة واحدة للـ Partition
    """
    # Coefficients (تُمثّل ML Model مُحمَّل)
    INCOME_COEF = 0.0003
    DEBT_PENALTY = -0.005
    HISTORY_BONUS = 8.0
    LATE_PENALTY = -15.0
    BASE_SCORE = 500
    
    for df_batch in iterator:
        scores = (
            BASE_SCORE +
            df_batch["income"] * INCOME_COEF +
            df_batch["debt"] * DEBT_PENALTY +
            df_batch["credit_history_years"] * HISTORY_BONUS +
            df_batch["late_payments"] * LATE_PENALTY
        ).clip(300, 850)  # FICO range
        yield scores

# تطبيق
result = credit_data.withColumn(
    "credit_score",
    credit_score_udf(struct(
        col("income"), col("debt"),
        col("credit_history_years"), col("late_payments")
    ))
).withColumn(
    "credit_grade",
    (col("credit_score") >= 750).cast("string")
)

print("=== نتائج التسجيل الائتماني ===")
result.show(5)

# إحصاءات التوزيع
result.groupBy("credit_grade").count().show()
```

### التمرين 2: applyInPandas لتحليل السلاسل الزمنية

```python
import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf

# بيانات مبيعات يومية
sales_data = [(f"Product_{i}", f"2025-{m:02d}-{d:02d}", 
               int(np.random.normal(1000, 200)))
              for i in range(1, 6)
              for m in range(1, 4)
              for d in range(1, 29)]

df_sales = spark.createDataFrame(sales_data, ["product_id", "date", "sales"])

output_schema = "product_id string, date string, sales long, ma_7 double, trend string"

def analyze_product_trend(df: pd.DataFrame) -> pd.DataFrame:
    """تحليل اتجاه المبيعات لكل منتج"""
    df = df.sort_values("date").reset_index(drop=True)
    
    # المتوسط المتحرك 7 أيام
    df["ma_7"] = df["sales"].rolling(window=7, min_periods=1).mean()
    
    # تحديد الاتجاه
    if len(df) >= 2:
        first_half = df["sales"][:len(df)//2].mean()
        second_half = df["sales"][len(df)//2:].mean()
        change_pct = (second_half - first_half) / first_half * 100
        
        if change_pct > 10:
            trend = "📈 GROWING"
        elif change_pct < -10:
            trend = "📉 DECLINING"
        else:
            trend = "➡️ STABLE"
    else:
        trend = "UNKNOWN"
    
    df["trend"] = trend
    return df[["product_id", "date", "sales", "ma_7", "trend"]]

result = df_sales.groupBy("product_id").applyInPandas(
    analyze_product_trend, 
    schema=output_schema
)

print("=== تحليل اتجاهات المبيعات ===")
result.orderBy("product_id", "date").show(15)
```

---

## 8. 🎓 أسئلة المقابلات التقنية

### سؤال 1: ما الفرق بين Scalar Pandas UDF وIterator Pandas UDF؟

**الإجابة النموذجية:**
- **Scalar Pandas UDF:** يُستدعى لكل Batch من البيانات. الدالة تستقبل `pd.Series` وتُعيد `pd.Series`. إذا كان لديك عملية initialization ثقيلة (تحميل Model)، ستتكرر لكل Batch.
- **Iterator Pandas UDF:** يُستدعى مرة واحدة لكل Partition. يستقبل `Iterator[pd.Series]`. يُتيح initialization مرة واحدة قبل Loop المعالجة. **مثالي للـ ML Inference** لأن تحميل الـ Model يحدث مرة واحدة لكل Executor Partition.

### سؤال 2: ما هو Apache Arrow ولماذا يجعل Pandas UDFs أسرع؟

**الإجابة النموذجية:**
Apache Arrow هو معيار تخزين بيانات عمودية في الذاكرة. يسمح بـ **Zero-Copy Data Sharing** بين JVM وPython Process.

بدون Arrow (Python UDF القديمة):
- JVM تُسلسل البيانات بـ Pickle → ترسلها لـ Python → Python تُفكك الـ Pickle → تعالج → تُسلسل → ترسل لـ JVM

مع Arrow (Pandas UDF):
- JVM تكتب البيانات في Arrow Buffer (في ذاكرة مشتركة)
- Python تقرأ من نفس الـ Buffer مباشرة (Zero-Copy!)
- لا Serialization، لا Copy → أسرع بكثير

### سؤال 3 (متقدم): ما الفرق بين `applyInPandas` و`mapInPandas`؟

**الإجابة النموذجية:**
- **`groupBy().applyInPandas(fn, schema)`:** يُطبّق الدالة على **مجموعة كاملة** (بعد groupBy). الدالة تستقبل `pd.DataFrame` لكل group وتُعيد `pd.DataFrame`. تُجري **Shuffle** لتجميع كل البيانات ذات نفس الـ Key. مناسب للعمليات التي تحتاج كل بيانات المجموعة معاً (مثل ترتيب، نمذجة، كشف شذوذ).
- **`mapInPandas(fn, schema)`:** يُطبّق الدالة على **كل Partition** كما هي (بدون Shuffle). الدالة تستقبل `Iterator[pd.DataFrame]` وتُعيد `Iterator[pd.DataFrame]`. مناسب للـ Stateless transformations مثل التنظيف أو الإثراء بـ APIs خارجية.

---

## 9. 📋 ورقة الغش السريعة

```python
# ── Scalar UDF (عمود واحد ← عمود واحد) ──────────────────────────
@pandas_udf(ReturnType())
def scalar_udf(s: pd.Series) -> pd.Series:
    return s.str.upper()

df.withColumn("col", scalar_udf(col("col")))

# ── Iterator (تحميل ثقيل مرة واحدة للـ Partition) ────────────────
@pandas_udf(ReturnType())
def iterator_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    model = load_model()  # مرة واحدة!
    for series in iterator:
        yield pd.Series(model.predict(series.tolist()))

# ── Multi-Column Iterator ─────────────────────────────────────────
@pandas_udf(ReturnType())
def multi_col_udf(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.Series]:
    model = load_model()
    for df_batch in iterator:
        yield pd.Series(model.predict(df_batch.values))

df.withColumn("result", multi_col_udf(struct(col("a"), col("b"))))

# ── Grouped Map ──────────────────────────────────────────────────
def grouped_fn(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values("score", ascending=False)

df.groupBy("user_id").applyInPandas(grouped_fn, schema="...")

# ── Map (بدون Shuffle) ────────────────────────────────────────────
def map_fn(iterator):
    for df_batch in iterator:
        df_batch["new_col"] = df_batch["col"] * 2
        yield df_batch

df.mapInPandas(map_fn, schema=df.schema)
```

### إعداد Arrow للأداء المثالي

```python
# تفعيل Arrow (ضروري لـ Pandas UDFs)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# حجم الـ Batch (افتراضي: 10,000 صف)
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")
```

> [!TIP]
> **🎉 أحسنت! انتهيت من مجلد `02_data_transformation` كاملاً!**
>
> لديك الآن 10 ملفات بمستوى عالمي تغطي كل جوانب تحويل البيانات في PySpark.
> الخطوة القادمة: الانتقال للمجلد التالي في المنهج.

<!-- START_NAVIGATION_LINKS -->
---
### 🔗 روابط التنقل السريع

| السابق (Previous) | التالي (Next) |
| :--- | :--- |
| [◀️ 📘 الـ UDFs الاحترافية: Python UDFs، Pandas UDFs، وكيفية كتابة دوال آمنة وسريعة](19_custom_udfs.md) | [▶️ Window Functions: Partitioning, Ordering, Frame Specifications, & Physical Execution](../03_advanced_analytics/21_window_functions.md) |
<!-- END_NAVIGATION_LINKS -->
