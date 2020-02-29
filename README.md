## Table of Contents
- [Getting Started](#getting-started)
  - [Development Environment Setup](#development-environment-setup)
  - [Unit Testing](#unit-testing)
  - [Installation](#installation)
- [Architecture](#architecture)
  - [File Structure](#file-structure)
  - [Environment Variables](#environment-variables)
- [Credits](#credits)

## Getting Started
### Development Environment Setup
### Unit Testing
TODO: unit test coverage

### Installation
#### Cassandra Setup
TODO: Cassandra 3 node ring setup instructions
### Running
usage: pyton cassandra_loader.py -i <inputconfig file path>
## Python Dependencies
1. DataStax cassandra driver - https://docs.datastax.com/en/developer/python-driver/3.21/
```
pip install cassandra-driver
```

pip install tqdm
## Architecture
TODO: Add a high level arch. diagram

### File Structure

The default file structure looks like this:

```
thirdeye
├── src/                 # Python source code
   ├── lib               # libraries
   ├── model             # Object mapper
   ├── config            # Configuration File
   ├── tests             # Unit test
├── input/airlines       # Data files
└── README.md            # This file
```
### Environment Variables

## Credits
