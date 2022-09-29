from setuptools import setup,find_packages

setup(
    name='azdsdr',
    version='0.6',
    license='Apache License',
    author="Andrew Zhu",
    author_email='xhinker@hotmail.com',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    url='https://github.com/gmyrianthous/example-publish-pypi',
    keywords='example project',
    install_requires=[
          'pandas'
          ,'pyodbc'
      ],
)