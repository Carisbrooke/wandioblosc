#!/bin/bash

make distclean
./configure
make -j7 && sudo make install
