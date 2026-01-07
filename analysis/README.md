# NOTE

This pipeline requires netbone package, which however is incompatible with current versions of pyhton. to solve this,
you need to patch the netbone package:
- clone locally netbone from ```https://gitlab.liris.cnrs.fr/coregraphie/netbone/```
- Modify ```setup.py``` in the following way:
    - delete the dependency ```"matplotlib==3.6.0"```
    - manually install matplotlib with pypi
- install the netbone package manually with 
    ```bash
    $ pip install .
    ```