# NOTE

The backbone step (`03_backbone.py`) uses [cppbone](https://pypi.org/project/cppbone/), a C++ port of
netbone that installs the `netbone_cpp` module. It is listed in `requirements.txt`, so

```bash
$ pip install -r requirements.txt
```

is enough; the patched netbone install is no longer needed.
