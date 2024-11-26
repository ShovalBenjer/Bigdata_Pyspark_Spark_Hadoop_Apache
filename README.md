Distributed Numerical Integration with Apache Spark
Description
This notebook demonstrates how to compute numerical integrals using Apache Spark for distributed processing. It evaluates the performance and accuracy of the integral approximation as the number of intervals (
𝑛
n) and Spark workers vary. The project highlights the trade-offs between precision and execution time in distributed systems.

Prerequisites
To run this notebook, ensure you have the following installed:

Python 3.x
Apache Spark
Required Python packages:
bash
Copy code
pip install pyspark pandas sympy
How to Run the Notebook
Open the notebook in your preferred environment (e.g., Jupyter Notebook, Google Colab, or VLAB).
Ensure Spark is installed and correctly configured. If using Google Colab, you may need to set up Spark using additional commands (not covered in this notebook).
Run the notebook from top to bottom without skipping any cells. The code is structured sequentially, so the outputs depend on prior computations.
Key Notes
Do not modify execution order: The notebook is designed to execute seamlessly without repeated code lines.
SparkContext is initialized at the beginning of the notebook and stopped at the end to avoid conflicts.
Results, including tables and performance metrics, are automatically displayed after running the notebook.
Output
The notebook generates:

Tables showing error and execution time for different configurations.
Simplified explanations of results.
Visual representations (if applicable).
Troubleshooting
If SparkContext conflicts occur, ensure no other Spark sessions are running.
For performance issues, adjust the number of workers or reduce the size of 
𝑛
n to test with smaller datasets.
