from setuptools import find_packages, setup


setup(
    name='spark-test',
    author='Tomas Farias',
    author_email='tomasfariassantana@gmail.com',
    description='A collection of assertion functions to test Spark Collections like DataFrames.',
    packages=find_packages(exclude=['tests', 'docs']),
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
