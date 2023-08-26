# Big data project
This project assumes you have copied the repository onto your virtual HDP Hortonworks virtual machine.

This repository is a template for the final project of big data course in IU-2023. It contains the following directories:

- `data/` contains the dataset files.
- `models/` contains the Spark ML models.
- `notebooks/` has the Jupyter or Zeppelin notebooks of your project and used just for learning purposes.
- `output/` represents the output directory for storing the results of the project. It can contain `csv` files, text files. images and any other materials you returned as an ouput of the pipeline.
- `scripts/` is a place for storing `.sh` scripts and `.py` scripts of the pipeline.
- `sql/` is a folder for keeping all `.sql` and `.hql` files.

`requirements.txt` lists the Python packages needed for running your Python scripts. Feel free to add more packages when necessary.

`main.sh` is the main script that will run all scripts of the pipeline stages which will execute the full pipeline and store the results in `output/` folder. During checking your project repo, the grader will run only the main script and check the results in `output/` folder.

## RUNNING PROJECT

To run the project, copy it to HDP machine under root directory (~/bdproject). Then, use command:

```shell
sh main.sh
```

## Running streamlit

To run streamlit on your local PC, install it, unzip dataset to the same folder it is in, and then use:

```shell
streamlit run streamlit_app.py --server.port 60000
```

Created by Tishkin Pavel p.tishkin@innopolis.university