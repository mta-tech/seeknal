<html>
    <h1 align="center">
        Seeknal
    </h1>
    <h3 align="center">
        An all-in-one platform for data and AI/ML engineering
    </h3>

<div align="center">

![Static Badge](https://img.shields.io/badge/seeknal-documentation-red?style=flat&link=https%3A%2F%2Fseeknal.gitbook.io%2Fseeknal)
</div>

</html>



Seeknal is a platform that abstracts away the complexity of data transformation and AI/ML engineering. It is a collection of tools that help you transform data, store it, and use it for machine learning and data analytics.

Seeknal lets you:

- **Define** data and feature transformations from raw data sources using Pythonic APIs and YAML.
- **Register** transformations and feature groups by names and get transformed data and features for various use cases including AI/ML modeling, data engineering, business metrics calculation and more.
- **Share** transformations and feature groups across teams and company.

Seeknal is useful in multiple use cases including:

- AI/ML modeling: computes your feature transformations and incorporates them into your training data, using point-in-time joins to prevent data leakage while supporting the materialization and deployment of your features for online use in production.
- Data analytics: build data pipelines to extract features and metrics from raw data for Analytics and AI/ML modeling.

## Getting Started

### Prerequisites

- Python 3.11

### Installation

To install Seeknal, follow these steps:

1. **Download the Wheel File**:
   - Visit the [releases page](https://github.com/mta-tech/seeknal/releases) and download the latest `.whl` file for Seeknal.

2. **Extract the Downloaded File**:
   - Once the download is complete, extract the contents of the zip file to your working directory.

3. **Install Seeknal Using pip**:
   - Open your terminal and navigate to the directory where you extracted the files. Then, run the following command to install Seeknal:
     ```bash
     pip install seeknal-<version>-py3-none-any.whl
     ```
   - Replace `<version>` with the actual version number of the wheel file you downloaded.

4. **Verify the Installation**:
   - To ensure that Seeknal has been installed correctly, you can run:
     ```bash
     pip show seeknal
     ```
   - This command will display information about the installed package, confirming that the installation was successful.

## Congratulation!

Your seeknal has been installed on your machine and ready to use in your projects.

### Next Steps

- **Explore the Documentation**
    
    For detailed usage instructions, examples, and advanced features, please refer to the [Seeknal Documentation](https://seeknal.gitbook.io/seeknal/). This resource will guide you through the various functionalities and help you get the most out of Seeknal.

- **Start a New Project**
    
    Begin by creating a new project and defining your data transformations.


## Licence

This project is licensed under the Apache-2.0 License. See the [LICENSE](LICENSE) file for details.

## Support

For questions or issues, visit our [GitHub repository](https://github.com/mta-tech/seeknal) or open an issue.