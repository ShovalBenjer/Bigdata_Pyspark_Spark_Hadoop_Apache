Readme.MD
# Personal Note

Authors: Shoval Benjer Adir Amar  Alon Berkovich 

![Demo](https://github.com/ShovalBenjer/ShovalBenjer/blob/main/plotly_real-time_simulation_demo.gif)

This project commenced with an exploration of advanced NLP techniques, initially drawn to the potential of transformer architectures like modern BERT variants. Early discussions with Dr. Boris Moroz were instrumental in pivoting from less defined clustering ideas towards a focused and achievable classification mission – predicting consumer complaint resolution success.

Driven by the goal of creating a robust solution, significant effort, totalling approximately 100 hours, was invested in researching architectural patterns, coding the pipeline components, and intensive debugging. While the aspiration was to align with state-of-the-art principles, a key realization emerged during implementation: the practical challenges of setting up and configuring distributed system components, such as Kafka and Zookeeper, require deep, hands-on engagement that often extends beyond readily available high-level documentation or current AI assistance (as of 2025).

This necessity to "get my hands dirty" with the underlying infrastructure proved to be a profound learning experience. The resulting pipeline, integrating Spark MLlib, simulated streaming, and a monitoring dashboard, stands as a functional end-to-end system. More importantly, it serves as a practical template and a testament to the skills developed in bridging theoretical knowledge with real-world big data implementation – a capability I believe is readily transferable to developing analytical tools for decision-makers.
yours sincerly, 

Shoval

# Solosolve AI Big Data Pipeline

## Architecture Overview
```
┌─────────────────┐        ┌─────────────────┐        ┌───────────────┐
│                 │        │ Kafka Setup/Sim │        │ Spark Batch   │
│  Data Sources   │───────▶│  - Topic Init │───────▶│ Processing    │
│  - CSV          │        │  - Producer Sim│        │  (Sampled CSV)│
│  - Simulation   │        │                 │        │               │
└─────────────────┘        └─────────────────┘        └───────┬───────┘
      │                                                       │ ▲
      │ Simulation                                            │ │ Fallback
      ▼                                                       ▼ │ Data
┌─────────────────┐        ┌───────────────┐        ┌───────────────┐
│                 │        │               │        │               │
│ Dash Dashboard  │◀───────│ Model         │◀───────│ AFE Pipeline  │
│  - Metrics      │        │ Training (GBT)│        │  - HashingTF  │
│  - Charts       │        └───────┬───────┘        │  - OHE        │
└────────┬────────┘                │                └───────────────┘
         │                         │                        ▲
         │ Simulation              │ Model Loading          │
         ▼                         │                        │
┌─────────────────┐        ┌─────────────────┐        ┌───────────────┐
│                 │        │   Simulated     │        │ Continuous    │
│  (Not Impl.)    │<───────│   Streaming     │<───────│ Learning Goal │
│  User Feedback  │        │   Inference     │        │ (Retraining)  │
└─────────────────┘        └─────────────────┘        └───────────────┘
```


## Component Breakdown

### 1. Environment Setup
-   **Spark:** Initialized via `initialize_spark()` with specific memory (`driver=3g`, `executor=2g`), parallelism, and Kryo serialization settings for optimization. Uses `SparkSession.builder`. `psutil` optionally used for memory info.
-   **Kafka:**
    -   Topics setup via `setup_kafka_topics()` using `kafka-python`'s `KafkaAdminClient` & `NewTopic` (if Kafka available). Defines topics like `consumer-complaints-raw`.
    -   Producer simulation via `kafka_producer_job()` using `KafkaProducer` (if Kafka available). *Note: Main inference uses local simulation.*
-   **MLflow:** Used for tracking (`mlflow.log_metric`, `log_param`) and model logging/registry (`mlflow.spark.log_model`) if available (`MLFLOW_AVAILABLE` flag).
-   **Dashboard:** Uses `Dash`, `Plotly`, `dcc`, `html` for UI and visualization if available (`DASH_AVAILABLE` flag). Served via `JupyterDash`. `threading.Lock` used for safe data updates.

### 2. Core Pipeline Functions

#### Data Handling & Labeling
-   **Loading:** `load_data_optimized()` reads CSV (`spark.read.csv`) using an explicit `COMPLAINT_SCHEMA`, applies sampling (`.sample()`), persists (`.persist(pyspark.StorageLevel.MEMORY_AND_DISK)`), and has a fallback to `create_simulation_data()`.
-   **Labeling:** `create_label_column()` uses `pyspark.sql.functions.when` and `col` to derive the binary `is_successful_resolution` target variable based on specific conditions.

#### Feature Engineering (AFE)
-   Implemented in `create_feature_pipeline()` and applied via `apply_feature_engineering()`. Uses `pyspark.ml.Pipeline`.
-   **Text:** `length()` calculates narrative length. `Tokenizer` splits text, `StopWordsRemover` filters common words, `HashingTF` converts tokens into fixed-size vectors.
-   **Categorical:** Uses `StringIndexer` (maps strings to indices) and `OneHotEncoder` (converts indices to sparse vectors). Attempts optimization using `Window` functions (`row_number`) to find top categories before encoding.
-   **Date:** `to_timestamp()` parses date strings, `month()` extracts the month feature.
-   **Assembly:** `VectorAssembler` combines all generated features into a single `features` vector column. Includes fallback to simpler features (`narrative_length` only) on error.

#### Model Training & Evaluation
-   **Training:** `train_model()` splits data (`.randomSplit()`), trains a `pyspark.ml.classification.GBTClassifier` (`.fit()`), and uses `persist`/`unpersist` for memory management.
-   **Evaluation:** Uses `pyspark.ml.evaluation.MulticlassClassificationEvaluator` (for Accuracy, Precision, F1) and `BinaryClassificationEvaluator` (for AUC) on validation data (`.transform()`).
-   **Persistence:** `save_model()` saves the AFE `PipelineModel` and `GBTClassificationModel` (`.write().overwrite().save()`). `load_models()` loads them back.

#### Simulated Streaming & Dashboard Updates
-   **Simulation:** `simulate_streaming_inference()` runs in a thread, *simulating* streaming by creating small Spark DataFrames (`spark.createDataFrame`) from Pandas batches in a loop. It does *not* use Spark Structured Streaming (`readStream`/`writeStream`).
-   **Prediction:** Applies loaded `afe_model` and `gbt_model` (`.transform()`) on simulated batches.
-   **Dashboard Update:** `update_dashboard_with_predictions()` takes prediction results (Pandas DataFrame), updates shared `dashboard_data` (protected by `dashboard_lock`), calculating metrics for various charts (Confusion Matrix (`go.Heatmap`), Company Success (`go.Bar`), State Success Rate (`go.Choropleth`)).

### 3. Execution Phases

#### Phase 1: Batch Processing (`run_batch_phase`)
```python
# Spark Session & Kafka Topics (Setup)
spark = initialize_spark() # Configures Spark
setup_kafka_topics() # Uses KafkaAdminClient (optional)

# Load data from CSV (Sampled)
df = load_data_optimized(spark, DATASET_PATH) # spark.read.csv, sample
filtered_df = df.filter(...) # pyspark.sql.functions.col, length
filtered_df = filtered_df.withColumn("narrative_length", length(...)) # Add feature
filtered_df = filtered_df.persist(...) # Memory optimization

# Feature Engineering
labeled_df = create_label_column(filtered_df) # when, col
afe_model, processed_df = apply_feature_engineering(labeled_df) # Pipeline, HashingTF, OHE, etc.

# Train, Evaluate, Save
gbt_model, metrics, predictions = train_model(processed_df) # GBTClassifier.fit, Evaluators
save_model(afe_model, gbt_model, metrics) # model.write().save(), MLflow (optional)

# Update dashboard with initial metrics/samples
# update_dashboard_with_predictions(predictions.limit(20).toPandas())

filtered_df.unpersist() # Release memory
```

#### Phase 2: Streaming Inference (`run_streaming_phase`, `simulate_streaming_inference`)
```python
# Load Models if needed
afe_model, gbt_model = load_models() # PipelineModel.load, GBTClassificationModel.load

# Start Dashboard (in a thread)
app = create_dashboard() # Dash, Plotly
# dashboard_thread = threading.Thread(target=lambda: app.run_server(...))
# dashboard_thread.start()

# Start Streaming Simulation (in a thread)
# Uses simulate_streaming_inference(spark, afe_model, gbt_model, ...)
streaming_thread = threading.Thread(
    target=simulate_streaming_inference, args=(...), daemon=True
)
streaming_thread.start() # Creates batches (spark.createDataFrame), predicts (.transform)
```

#### Phase 3: Feedback & Retraining (Conceptual / Not Implemented in Loop)
-   The corrected code *does not* show an active retraining loop triggered by a threshold. This remains a design goal rather than an implemented feature in the provided execution flow.

## Key Design Features

1.  **Optimized Feature Engineering**: Uses `Pipeline`, `HashingTF`, `OHE`, `VectorAssembler`.
2.  **Kafka Integration**: Utilizes `kafka-python` for topic setup and simulated production.
3.  **Simulated Real-time Inference**: Uses threaded batch processing (`spark.createDataFrame`, `.transform`) for pseudo-streaming.
4.  **Enhanced Observability**: Rich `Dash`/`Plotly` dashboard (Confusion Matrix, Company/State charts, metrics).
5.  **Continuous Learning Goal**: Pipeline designed with future retraining in mind.
6.  **MLflow Integration**: Optional experiment tracking and model registry.
7.  **Modular Design**: Functions for distinct tasks (load, featurize, train, simulate).
8.  **Error Resilience**: Includes `try...except` blocks and fallback model creation/loading logic.

Okay, let's break down the complexity and architecture.

## Time and Space Complexity Analysis (Estimates)

This analysis provides high-level estimates. Actual performance heavily depends on the Spark cluster configuration (number of nodes, cores, memory), data skew, partitioning, and specific Spark optimizations during execution.

Let:
*   `N`: Total number of records in the *sampled* dataset used for batch processing.
*   `N_full`: Total number of records in the original CSV file.
*   `F`: Number of raw features selected.
*   `F'`: Number of features after AFE (can be significantly larger due to OHE, text features).
*   `V`: Size of the vocabulary/number of hashing features for text (`MAX_TEXT_FEATURES`).
*   `C`: Number of unique categories considered per categorical feature (`MAX_CATEGORICAL_VALUES`).
*   `b`: Batch size for simulated streaming (`MAX_BATCH_SIZE`).
*   `I`: Number of iterations for GBT training (`maxIter`).
*   `D`: Max depth of GBT trees (`maxDepth`).
*   `M`: Number of nodes in the Spark cluster.

**Phase 1: Batch Processing (`run_batch_phase`)**

*   **`load_data_optimized`**:
    *   Time: O(N_full / M) for reading (depends on file size/partitions) + O(N) for sampling & processing the sample. Dominated by reading if N_full is huge, or processing if sampling is intensive. `count()` is O(N).
    *   Space: O(N * F / M) distributed across nodes for the sampled DataFrame.
*   **`apply_feature_engineering`**:
    *   Time: Multiple passes over the data.
        *   Text (Tokenizer, StopWords, HashingTF): Roughly O(N * avg_text_length / M) or O(N * V / M).
        *   Categorical (Grouping, Window, Indexer, OHE): Can be O(N log N / M) or O(N / M) depending on Spark execution for grouping/windowing, plus O(N * C / M) for encoding. `collect()` for top categories adds driver overhead.
        *   Assembler: O(N * F' / M).
        *   Overall: Likely dominated by the most expensive stage, potentially O(N log N / M) or multiple O(N / M) passes.
    *   Space: O(N * F' / M) for the transformed DataFrame. Feature vector size `F'` can be large.
*   **`train_model` (GBT)**:
    *   Time: O(I * N * F' * D / M). GBT training is computationally intensive and iterative.
    *   Space: O(Model Size) for the trained GBT model (can be significant) + O(N * F' / M) for cached training/validation data partitions.
*   **`save_model`**:
    *   Time: O(Model Size). Depends on model complexity and storage speed.
    *   Space: O(Model Size) on disk.

**Overall Batch Phase**:
*   Time: Dominated by GBT Training and potentially Feature Engineering. Can range from O(N log N / M) to O(I * N * F' * D / M).
*   Space: Dominated by persisted DataFrames (Sampled, Transformed) O(N * F' / M) and the stored Model Size.

**Phase 2: Simulated Streaming Inference (`simulate_streaming_inference`)**

*   **Per Batch (size `b`)**:
    *   Time: O(b) to create DataFrame + O(b * F') for AFE transform + O(b * F'') for GBT transform + O(b) for `toPandas` + O(b) for dashboard update logic. Dominated by model transforms: O(b * F'). `toPandas` can be a bottleneck transferring data to the driver.
    *   Space: O(b * F') for the temporary batch DataFrame and predictions. Driver memory needed for Pandas DataFrame.
*   **Overall Streaming**: Runs indefinitely. Performance metric is throughput (batches/sec or records/sec), limited by the per-batch time complexity. Space is relatively constant per batch, but dashboard state might grow slightly (e.g., keeping top N companies).

**Dashboard (`create_dashboard`, Callbacks)**

*   Time: Callbacks update periodically. Complexity depends on the data visualized. Plotting recent predictions (e.g., 50) is O(1) relative to N. Plotting aggregated data (states, companies) depends on the number of unique states/companies shown, typically small compared to N. Rendering Plotly figures takes time proportional to the complexity of the chart.
*   Space: O(constant) to store recent predictions (fixed size list), aggregated metrics per state/company, current metrics. Relatively low compared to Spark DataFrames.

**Key Complexity Factors**:

*   **Data Size (N, N_full)**: Most operations scale linearly or slightly super-linearly with the number of records in the sample.
*   **Feature Dimensionality (F')**: Especially after OHE and text vectorization, transforms and GBT training time increase.
*   **GBT Parameters (I, D)**: Directly impact training time.
*   **Cluster Size (M)**: Spark parallelizes work, reducing wall-clock time (ideally).
*   **`collect()` operations**: Used for finding top categories, brings data to the driver, can be a bottleneck.
*   `.toPandas()`: Used in streaming simulation, brings data to the driver, bottleneck for large batches.

## Architecture Class Diagram (UML in folders)

This diagram represents the logical components and their primary interactions based on the corrected code's structure. Since the code is mostly functional, classes represent modules or key responsibilities.


**Explanation of Diagram:**

1.  **Packages:** Group related classes (e.g., `SparkInfrastructure`, `ModelManagement`). `<<Frame>>` indicates a major architectural component.
2.  **Classes:** Represent key modules or responsibilities identified in the code (e.g., `SparkManager`, `DataLoader`, `AFEPipeline`, `ModelTrainer`, `StreamingSimulator`, `DashboardApp`).
3.  **Attributes/Methods:** Show essential data members (like models, configuration) and primary functions performed by each component.
4.  **Relationships:**
    *   `-->`: Association (e.g., `PipelineRunner` *uses* `SparkManager`).
    *   `..>`: Dependency (often configuration or logging).
    *   `<<Optional: ...>>`: Indicates components (Dash, MLflow, Kafka) that might not be present depending on installation. Notes provide extra context.
5.  **High-Level View:** The diagram focuses on how major components interact rather than detailing every single function or variable. It abstracts the functional code into a component-based architectural view. The `PipelineRunner` acts as the central orchestrator.

## Terminal Commands Summary

```bash
# Terminal 1: PySpark (Example Invocation)
# Ensure PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON are set or use:
PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 pyspark --driver-memory 3g --executor-memory 2g # Add --packages if using Kafka direct stream

# Terminal 2: Zookeeper (If running Kafka locally)
# Navigate to Kafka directory
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 3: Kafka Broker (If running Kafka locally)
# Navigate to Kafka directory
bin/kafka-server-start.sh config/server.properties

# Terminal 4: Run the Python Script
python your_pipeline_script.py
```
```
