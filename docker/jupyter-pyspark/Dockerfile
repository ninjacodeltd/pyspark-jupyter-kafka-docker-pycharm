FROM docker.io/jupyter/pyspark-notebook:x86_64-spark-3.5.0

# Install in the default python3 environment
RUN pip install --no-cache-dir 'pytest' && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"