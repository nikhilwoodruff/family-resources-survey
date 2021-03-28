from setuptools import setup, find_packages

setup(
    name="family_resources_survey",
    version="0.0.1",
    description=(
        "A Python package to manage Family Resources Survey microdata."
    ),
    url="http://github.com/nikhilwoodruff/family_resources_survey",
    author="Nikhil Woodruff",
    author_email="nikhil.woodruff@outlook.com",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "microdf @ git+https://github.com/pslmodels/microdf",
        "openpyxl",
        "xlrd"
    ],
    entry_points={
        "console_scripts": ["frs-data=family_resources_survey.main:main"],
    },
)
