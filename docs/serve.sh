#!/bin/bash

sphinx-autobuild -b html . _build/html -p 8063 --ignore "*.swp" -B
