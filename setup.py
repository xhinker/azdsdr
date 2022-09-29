from setuptools import setup,find_packages

setup(
    name='azdsdr',
    version='1.220929',
    license='Apache License',
    author="Andrew Zhu",
    author_email='xhinker@hotmail.com',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    url='https://github.com/xhinker/azdsdr',
    keywords='example project',
    install_requires=[
          'pandas'
          ,'pyodbc'
      ],
)