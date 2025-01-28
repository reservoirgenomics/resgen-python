#!/bin/bash

sphinx-autobuild -b html . _build/html --port 8062 --ignore "*.swp"
