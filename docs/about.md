# About zappend

## Changelog

You can find the complete `zappend` changelog 
[here](https://github.com/bcdev/zappend/blob/main/CHANGES.md). 

## Reporting

If you have suggestions, ideas, feature requests, or if you have identified
a malfunction or error, then please 
[post an issue](https://github.com/bcdev/zappend/issues). 

## Contributions

The `zappend` project welcomes contributions of any form
as long as you respect our 
[code of conduct](https://github.com/bcdev/zappend/blob/main/CODE_OF_CONDUCT.md)
and follow our 
[contribution guide](https://github.com/bcdev/zappend/blob/main/CONTRIBUTING.md).

If you'd like to submit code or documentation changes, we ask you to provide a 
pull request (PR) 
[here](https://github.com/bcdev/zappend/pulls). 
For code and configuration changes, your PR must be linked to a 
corresponding issue. 

## Development

Setup development environment:

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
pip install -r requirements-docs.txt
```

### Testing and Coverage

`zappend` uses [pytest](https://docs.pytest.org/) for unit-level testing 
and code coverage analysis.

```bash
pytest --cov=zappend tests
```

### Code Style

`zappend` source code is formatted using the [black](https://black.readthedocs.io/) tool.

```bash
black zappend
```

### Documentation

`zappend` documentation is built using the [mkdocs](https://www.mkdocs.org/) tool.

```bash
pip install -r requirements-doc.txt

mkdocs build
mkdocs serve
mkdocs gh-deploy
```

If the configuration JSON schema in `zappend/config/schema.py` changes
then the configuration reference documentation `docs/config.md` must be 
regenerated:

```bash
zappend --help-config md > docs/config.md
```

## License

`zappend` is open source made available under the terms and conditions of the 
[MIT License](https://github.com/bcdev/zappend/blob/main/LICENSE).

Copyright © 2024 Brockmann Consult Development
