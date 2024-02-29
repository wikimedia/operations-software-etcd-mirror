"""
Etcd2mirror is a tool to mirror directories in one etcd v2 cluster to a
different prefix on a second cluster. It should be able to restart from its last known
position.
"""
from setuptools import find_packages, setup

setup(
    name="etcdmirror",
    version="0.0.9",
    description="Tool to create a live replica of an etcd cluster",
    author="Giuseppe Lavagetto",
    author_email="joe@wikimedia.org",
    license="GPLv3+",
    url="https://github.com/wikimedia/operations-software-etcd-mirror",
    test_suite="nose.collector",
    test_require=["mock", "nose-py3", "parameterized"],
    install_requires=["python-etcd>=0.4.3", "twisted>=14.0.0", "prometheus_client"],
    zip_safe=False,
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "etcd-mirror = etcdmirror.main:main",
        ],
    },
)
