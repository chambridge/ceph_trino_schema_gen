# ceph_trino_schema_gen
Generate Trino schema from an Ceph S3 bucket


# Usage

Start by cloning the repository:
```
git clone https://github.com/chambridge/ceph_trino_schema_gen.git
```

Switch to the new directory:
```
cd ceph_trino_schema_gen
```

Create Python 3.9 virual enviroment:
```
pipenv --python 3.9
pipenv install
```

Copy and configure connection to your Ceph bucket:
```
cp .env.example .env
```

Enter the virtual env:
```
pipenv shell
```

Execute the python script:
```
python gen_table_defs.py
```

# Workaround
_Note:_ You may encounter the following error with Python 3.9 if the dependency has not been fixed yet:
```
AttributeError: 'xml.etree.ElementTree.Element' object has no attribute 'getchildren'
```

In order to resolve the problem you need to remove the `.getchildren()` method calls in *s3cmd* locally.
To do this find the location of `s3scmd` in your virtual environment:
```
which s3cmd
```
Open a terminal to the python directory listed. Change to the S3 site-package:
```
cd lib/python3.9/site-packages/S3/
```
Remove all occurrences of `.getchildren()` from the code. Now the python script should run properly.