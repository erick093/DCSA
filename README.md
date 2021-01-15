# Distributed Computing and Storage Architecture VUB-2020 Project

This repository contains an implementation of the final project for the DCSA - VUB course.

In this project you will practice your skills on MapReduce algorithms in Python using MRJob. The data you will be working on includes movie titles and ratings from IMDB, annual retail exports, meta data on Arxiv3 scientic papers and large matrices. For each of the provided data you are asked to perform a set of tasks such as counting, matrix manipulation and similarity searching.

More information can be found in the project description file: Project_Outline_DCSA2021.pdf

## Installation

In order to install the needed dependencies, you'll need to install the following libraries.

```bash
pip install mrjob
pip install ntlk
pip install numpy
```

## Usage
In order to run the python files, please execute them in the following way.
```bash
python task_{task_number}.py {dataset_name}
```
All the files were tested using Python  3.7.9

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
