Readme.MD
# Personal Note

Authors: Shoval Benjer Adir Amar  Alon Berkovich 


This project commenced with an exploration of advanced NLP techniques, initially drawn to the potential of transformer architectures like modern BERT variants. Early discussions with Dr. Boris Moroz were instrumental in pivoting from less defined clustering ideas towards a focused and achievable classification mission – predicting consumer complaint resolution success.

Driven by the goal of creating a robust solution, significant effort, totalling approximately 100 hours, was invested in researching architectural patterns, coding the pipeline components, and intensive debugging. While the aspiration was to align with state-of-the-art principles, a key realization emerged during implementation: the practical challenges of setting up and configuring distributed system components, such as Kafka and Zookeeper, require deep, hands-on engagement that often extends beyond readily available high-level documentation or current AI assistance (as of 2025).

This necessity to "get my hands dirty" with the underlying infrastructure proved to be a profound learning experience. The resulting pipeline, integrating Spark MLlib, simulated streaming, and a monitoring dashboard, stands as a functional end-to-end system. More importantly, it serves as a practical template and a testament to the skills developed in bridging theoretical knowledge with real-world big data implementation – a capability I believe is readily transferable to developing analytical tools for decision-makers.
yours sincerly, 

Shoval

# *Quick Setup* 

*1. Extract Consumer_Complaints.zip in ...\Solosolve-AI_Big_Data\data*

2. Open 3 terminals (cd to kafka directory - extracted, if on vlab enviorment you will need also to extract kafka)

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

