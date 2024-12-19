from setuptools import setup, find_packages

setup(
    name="mercury",
    version="0.1.0",
    description="A data pipeline project for financial and macroeconomic analysis.",
    author="Nathan Tran",
    author_email="nh.tran@outlook.com",

    packages=find_packages(include=["mercury", "mercury.*"]),
    install_requires=[
        "pandas",
        "fredapi",
        "python-dotenv",
    ],

    entry_points={
        "console_scripts": [
            "run-pipeline=mercury.main:main",
        ],
    },


    include_package_data=True,
    package_data={
        "": ["*.txt", "*.csv"],
    },

    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
