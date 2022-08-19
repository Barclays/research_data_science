# Overview

`ctrlaltdata` is a data access layer written and used by Barclays Investment Bank's Research Data Science team. It is designed to work with "alternative data" for the purpose of informing financial research.

To this end, the tool has several objectives:
1.	To read data from many data sources, including flat files on s3, SQL databases, and APIs;
2.	To join data from these various sources together into a panel of securities data; and
3.	To expand the functionality available in standard tools like pandas to be appropriate for use with financial cross-sectional time series and panel data.

To support these objectives, the library is divided into submodules for each data set. The scope of our initial release is to work with Refinitiv's QAD database, with more data sets to follow. Each submodule is divided into lower-level dataset access tools, and higher-level API methods.
* The lower-level tools, which we commonly refer to as the data layer, are meant to interface with data sources directly, and include SQL queries for requesting data.
* The higher-level tools, which we commonly refer to as the API layer, are meant to provide a clean, intuitive, and abstract interface to the data for the end-user. These interface with the lower-level tools, and abstract away implementation details (e.g. whether the data comes from a SQL database or s3 bucket) in favor of keeping the analyst's focus on their analysis.

The data layer changes over time as implementation details (e.g. schemas, data stores) change. The API layer, in contrast, is meant to stay static to the greatest degree practicable. This is meant to insulate analysis code and application code against changes in the underlying data stores, and hopefully reduce the maintenance burden of keeping many analysis scripts running live. We can't guarantee that we'll always be able to achieve this in practice, but will make our best effort to put deprecation warnings in code that is scoped to be changed in a later release.

There are additional benefits to these abstractions:
* SQL queries are reviewed before being added to the data layer, reducing the chances that crucial analysis code contains bugs
* SQL queries and other pre-processing code become easily re-usable across the data team
* Queries and analysis are standardized, reducing the space for bugs to enter into analysis code
* Automation of model building based on prototypes built by the data team can use the same queries and pre-processing code that the analysts use to build prototypes, reducing dev/prod drift.

As we've built and adopted this set of tools, what we have seen in practice is a super-linear rate of increase in productivity across our team. Operations that took hundreds of lines of custom code for each project to perform can now be executed in a single line of code. Optimizations to these processes instantly propagate to all users, so runtime improvements immediately benefit all users. It's easy to build abstractions on top of these basic abstractions, so the complexity and sophistication of our work has also increased with time. We worry less about manipulating our data, and more about high-value tasks like statistical modeling, causal inference, writing research. In short, we spend a higher proportion of our time focused on the quality of key deliverables.

# Installation
Our package is available on PyPI, so you can install it simply by running

`pip install ctrlaltdata`

After installing, each data module's configuration settings (e.g. database server address, access credentials) can be found in `<your install path>/ctrlaltdata/config.py`. Storing credentials on disk in plain text is insecure, because bad actors would only need to read a plain text file to gain access to systems. We recommend you use this script as an abstraction to your own internal configuration management, and copy over a `config.py` to the appropriate location after installing packages where the module is meant to be deployed.

If you'd like to install this package for development, we recommend starting a new virtual environment before navigating into your fork of the repo and installing requirements like

`pip install -r requirements.txt`

This will help us control environment-specific issues while debugging pull requests. All contributions require test coverage. We use `pytest`, and you can run tests like

`pytest <install path>/ctrlaltdata/tests`

All tests must pass before contributions are accepted. Please see below for more detailed contributor guidelines.

# Contributions
Please see our contributor guidelines [here](CONTRIBUTE.md). 

# Disclaimer

The license for this tool/library does not extend to any of the content of the datasets with which the tool is designed to function or any such related services. An appropriate contract with the relevant data vendor is required in order to use this tool and for the tool to function. This tool is not affiliated, endorsed or in any way connected with any of the proprietors of the relevant data sets or any other third party.
