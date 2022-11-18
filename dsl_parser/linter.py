import logging

from yamllint import linter as yaml_linter
from yamllint.config import YamlLintConfig

from dsl_parser import exceptions


yamllint_conf = YamlLintConfig('''
extends: default
rules:
  braces:
    level: warning
  brackets:
    level: warning
  colons:
    level: warning
  commas:
    level: warning
  document-start: disable
  empty-lines: disable
  hyphens:
    level: warning
  indentation:
    level: error
  key-duplicates:
    level: error
  line-length:
    level: warning
  new-line-at-end-of-file:
    level: warning
  new-lines:
    level: warning
  trailing-spaces:
    level: warning
''')


def validate(document: str, import_url: str):
    """Runs yamllint on the document"""
    logger = logging.getLogger('dsl_parser')
    problems = yaml_linter.run(document, yamllint_conf)
    linting_error = []
    for p in problems:
        if p.rule is None:
            raise exceptions.DSLParsingFormatException(-1, p.message)
        if p.level == 'warning':
            logger.warning(f'linter {p}')
        elif p.level == 'error':
            logger.error(f'linter {p}')
            linting_error += [str(p)]

    if linting_error:
        raise exceptions.DSLParsingException(
            exceptions.ERROR_YAMLLINT_ERROR,
            f'{import_url} is not properly formatted yaml:\n' +
            '\n'.join(linting_error)
        )
