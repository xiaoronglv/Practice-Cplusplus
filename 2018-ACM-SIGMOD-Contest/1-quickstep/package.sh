#!/bin/bash

tar --dereference --exclude='build' --exclude='submission.tar.gz' --exclude='release' -czf submission.tar.gz *
