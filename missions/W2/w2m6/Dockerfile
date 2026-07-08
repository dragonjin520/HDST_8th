FROM quay.io/jupyter/minimal-notebook:latest

WORKDIR /home/jovyan/work

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY notebooks/ /home/jovyan/work/notebooks/
COPY data/ /home/jovyan/work/data/
COPY src/ /home/jovyan/work/src/
COPY config/ /home/jovyan/work/config/

EXPOSE 8888

CMD ["start-notebook.py", "--NotebookApp.token=softier", "--NotebookApp.password=", "--NotebookApp.allow_origin=*"]