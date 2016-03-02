from distutils.core import setup
import py2exe

setup(
    name='AutoTrader',
    version='',
    packages=['test', 'common', 'data_source', 'data_process'],
    url='',
    license='',
    author='lidian',
    author_email='',
    description='',
    console=[{"script": "GetHisTickData.py"}]
)
