# Avista-Conrol


## Description

This is the module providing a queing and control service for the MLearn module.

## Table of Contents

1. [Installation](#installation)
2. [Usage](#usage)
3. [Testing] (#testing)
4. [Credits](#credits)
5. [License](#license)

## Installation

To install this use the following command.

```
pip3 install git+ssh://git@github.com/isu-avista/control.git
```

## Usage

TBD...

## Testing
Using the python library pifpaf to execute tests allows us to spin up rabbitmq instances
specifically for the purpose of testing. Since, the entirety of this module requires rabbitmq
it makes sense to execute tests this way instead of relying on rabbitmq being up and running
anytime we choose to run the tests. Without the use of tox, which has good support from pycharm,
we can simply run the tests by executing this command: `pifpaf run rabbitmq -- python3 -m unittest -v`.


## Credits

The following people contributed to this module:

- Andrew Christiansen, Isaac D. Griffith

## License

[LICENSE](LICENSE)
