from setuptools import find_packages, setup


def readme():
    with open('README.rst') as f:
        return f.read()


setup(
    name='spark-test',
    version='0.1',
    author='Tomas Farias',
    author_email='tomasfariassantana@gmail.com',
    description='A collection of assertion functions to test Spark Collections like DataFrames.',
    long_description=readme(),
    packages=find_packages(exclude=['tests', 'docs']),
    license='MIT',
    url='https://github.com/tomasfarias/spark-test',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Topic :: Software Development :: Testing',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Implementation :: CPython',
        'Operating System :: OS Independent'
    ]
)
