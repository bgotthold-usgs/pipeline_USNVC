from setuptools import setup

setup(name='usnvc',
      version='0.1',
      description='usnvc pipline processing',
      url='https://github.com/bgotthold-usgs/pipeline_USNVC',
      author='Ben Gotthold, Sky Bristol',
      author_email='',
      license='MIT',
      packages=['usnvc'],
      install_requires=['pandas>=0.25.0','requests'],
      zip_safe=False)
