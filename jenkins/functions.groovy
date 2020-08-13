void test1(String target) {
      echo "running pytest << ${target} >>"
      sh '''#!/bin/bash
        pytest \
              -n 4 \
              --cov-report term-missing \
              --cov=<< parameters.target >> \
              << parameters.target >> /tests \
              --junitxml=test-results/<< parameters.target >>.xml
      '''
  }
