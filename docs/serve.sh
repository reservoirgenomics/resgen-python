#!/bin/bash

sphinx-autobuild -b html . _build/html --port 8063 --ignore "*.swp"
