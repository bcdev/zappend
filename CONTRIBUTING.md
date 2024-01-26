# How to contribute

The `zappend` project welcomes contributions of any form
as long as you respect our [code of conduct](CODE_OF_CONDUCT.md) and stay 
in line with the following instructions and guidelines.

If you have suggestions, ideas, feature requests, or if you have identified
a malfunction or error, then please 
[post an issue](https://github.com/bcdev/zappend/issues). 

If you'd like to submit code or documentation changes, we ask you to provide a 
pull request (PR) 
[here](https://github.com/bcdev/zappend/pulls). 
For code and configuration changes, your PR must be linked to a 
corresponding issue. 

To ensure that your code contributions are consistent with our project’s
coding guidelines, please make sure all applicable items of the following 
checklist are addressed in your PR.  

**PR checklist**

* Format code using [black](https://black.readthedocs.io/) with default settings.
  Check also section [code style](#code-style) below.
* Your change shall not break existing unit tests.
  `pytest` must run without errors.
* Add unit tests for any new code not yet covered by tests.
* Make sure test coverage is close to 100% for any change.
  Use `pytest --cov=zappend --cov-report=html` to verify.
* If your change affects the current project documentation,
  please adjust it and include the change in the PR.
  Run `mkdocs serve` to verify.

## Code style

The `zappend` code compliant to [PEP-8](https://pep8.org/) except for a line 
length of 88 characters as recommended by [black](https://black.readthedocs.io/).
Since black is un-opinionated regarding the order of imports, 
we use the following three import blocks separated by an empty 
line:

1. Python standard library imports, e.g., `os`, `typing`, etc
2. 3rd-party imports, e.g., `xarray`, `zarr`, etc
3. Relative `zappend` module imports using prefix `.`, but 
   avoid `..` prefix.

