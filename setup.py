from setuptools import setup,find_packages

setup(
    name='azdsdr',
    version='1.230123.1',
    license='Apache License',
    author="Andrew Zhu",
    author_email='xhinker@hotmail.com',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    url='https://github.com/xhinker/azdsdr',
    keywords='DS Data Reader',
    install_requires=[
        'numpy'
        ,'pandas'
        ,'pyodbc'
        ,'azure-cli'
        ,'azure-kusto-data'
        ,'azure-kusto-ingest'
        ,'azure-storage-blob'
        ,'matplotlib'
        ,'jupyterlab'
        ,'ipython'
        ,'ipykernel'
    ],
    description="This package provide functions and tools for accessing data in a easy way."
)