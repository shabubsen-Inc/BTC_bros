from setuptools import setup, find_packages

setup(
    name='shared_functions',
    version='0.1',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'google-cloud-bigquery>=2.20.0',
        'pendulum>=2.1.2',
    ],
    python_requires='>=3.6',
)
