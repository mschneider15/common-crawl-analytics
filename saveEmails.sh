#!/bin/bash

for i in {0..7}; do echo part-0000$i >> ../0002-emails.txt; cat part-0000$i >> ../0002-emails.txt; done

