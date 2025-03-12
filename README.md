# PIFU_MI


## Setup

Paths to data in the lake are stored in a file called env.py. This file contains a dictionary called env, which contains the following:

```py
env = {
    "pifu_path": "abfss://path/to/the/PIFU/data/in/the/lake.",
}
```

The env.py file is gitignored.