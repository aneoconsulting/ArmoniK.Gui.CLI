# ArmoniK.Admin.CLI

This project is part of the [ArmoniK](https://github.com/aneoconsulting/ArmoniK) project.
In particular it is a consitutent of its [Core](https://github.com/aneoconsulting/ArmoniK.Core)
component which, among its main functionalities, implements several gRPC services aiming to
provide a user with a robust task scheduler for a high throughput computing application.

The .proto files in the directory [./Protos/V1](https://github.com/aneoconsulting/ArmoniK.Api/tree/main/Protos/V1) 
provide the core data model and functionalities for ArmoniK and are used to generate the different SDKs.

## Requirements

In order to install the ArmoniK CLI in an isolated environment, you must have python3-venv installed on your machine.

```bash
sudo apt install python3-venv
```

## Installation

### Following steps for manual installation

Clone the project from https://github.com/aneoconsulting

```bash
git clone git@github.com:aneoconsulting/ArmoniK.Admin.CLI.git
```

Navigate in the root directory

```bash
cd ArmoniK.Admin.CLI
```

Create and activate the virtual environment

```bash
python -m venv ./venv
source ./venv/bin/activate
```

Install the build module and generate the archive and .whl file to install dependencies

```bash
pip install build
python -m build
```

Install dependencies using the generated .whl file

```bash
pip install dist/<name_of_the_package>.whl
```

Verify the installation

``bash
armonik_admin_cli --version
``

## Documentation

[Documentation](https://aneoconsulting.github.io/ArmoniK.Admin.CLI/api/index.html) (TODO)


## Contributing

Contributions are always welcome!

See [CONTRIBUTING.md](https://github.com/aneoconsulting/ArmoniK.Api/blob/main/CONTRIBUTING.md) for ways to get started.
