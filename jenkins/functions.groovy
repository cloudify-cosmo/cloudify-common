void test1(String target) {
      var = target
      echo 'running pytest << var >>'
      sh '''#!/bin/bash
        pytest \
              -n 4 \
              --cov-report term-missing \
              --cov=<< parameters.target >> \
              << parameters.target >> /tests \
              --junitxml=test-results/<< parameters.target >>.xml
      '''
  }
