#### This is to be run with `python2.7` (not compatable with `python3`)

##### (NOTE!  if `pypy` is available, then it should definitely be used instead;  
just comment out the line to use `python` in the `run.sh` file, with the line to 
use `pypy` in that file [see 1]) 


#### Also, unit-tests can be run from the root directory of this repo via:

(you might need to install the `nose` python package first with something like pip [1])

`nosetests -s -v`

Of course if not already installed, `pip` might have to be installed as well.


[1] on Ubuntu/Debian `pypy` can be installed very easily with:

`sudo apt-get install pypy`

[2] `pip install nose --user --upgrade`
