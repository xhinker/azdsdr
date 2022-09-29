from setuptools import setup,find_packages

setup(
    name='azdsdr',
    version='1.220929.2',
    license='Apache License',
    author="Andrew Zhu",
    author_email='xhinker@hotmail.com',
    packages=find_packages('src'),
    package_dir={'': 'src/azdsdr'},
    url='https://github.com/xhinker/azdsdr',
    keywords='DS Data Reader',
    install_requires=[
          'pandas'
          ,'pyodbc'
      ],
)