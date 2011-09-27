#!/bin/bash

# run lint with minimal output so that it's obvious
# when you have problems

pylint -i y -r n -d I0011 *py
