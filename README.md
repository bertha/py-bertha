Bertha Python client and CL
===========================

This is

1. a Python client library for [Bertha][bertha].
2. a commandline interface to [Bertha][bertha].

[bertha]: http://github.com/bwesterb/bertha

Python
------

### Installation

Simply execute

    easy_install bertha

or from this source distribution, run

    python setup.py install

### Example

```python
>>> from bertha import BerthaClient
>>> c = BerthaClient('serf', 1234)
>>> list(c.list())
[]
>>> key = c.put_str('Hello world')
>>> key
'64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c'
>>> c.get(key).read()
'Hello world'
>>> list(c.list())
['64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c']
>>> ctx = c.put()
>>> ctx.f.write("Do some")
>>> ctx.f.write("Streaming")
>>> ctx.finish()
'975001fb9bdc0f72a78ca6326c55af86348d4c84da7ba47b7ed785a03f6803b0'
>>> c.get('975001fb9bdc0f72a78ca6326c55af86348d4c84da7ba47b7ed785a03f6803b0').read()
'Do someStreaming'
```

Commandline interface
---------------------
### Example
```sh
$ bertha list
975001fb9bdc0f72a78ca6326c55af86348d4c84da7ba47b7ed785a03f6803b0
64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c
$ bertha get 975001fb9bdc0f72a78ca6326c55af86348d4c84da7ba47b7ed785a03f6803b0
Do someStreaming
$ echo Hi | bertha put 
c01a4cfa25cb895cdd0bb25181ba9c1622e93895a6de6f533a7299f70d6b0cfb
$ bertha get c01a4cfa25cb895cdd0bb25181ba9c1622e93895a6de6f533a7299f70d6b0cfb tmp
$ cat tmp
Hi
```

See `bertha -h`, `bertha get -h`, etc. for more help.
