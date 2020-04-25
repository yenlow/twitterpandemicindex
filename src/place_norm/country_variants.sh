#!/usr/bin/env bash
# This gets the alternate names of countries in various languages
# and loads them into place_norm/dict_places.py

# Get the required files from http://download.geonames.org/export/dump
# countryInfo.txt with countries and geonameids (~300 rows)
# alternateNamesV2.txt with alternate names of every place (800M rows!)
# filter to alternateCountries.txt with 3rd field having a 2-3 char lang code
awk '/[0-9]+\t[0-9]+\t[a-zA-Z]{2,3}\t/{print}' alternateNamesV2.txt > alternateCountries.txt

# sort countryInfo.txt by geonameid (field 17)
# must use -b to do alphanumeric sort (not numeric sort) on mac
sort -t$'\t' -k 17 -b countryInfo.txt > countryInfo_sorted

# generate alternate names
join -1 17 -2 2 -t$'\t' -o 2.2,2.4,1.1,1.5,2.3 countryInfo_sorted.txt <(sort -t$'\t' -k 2 -b alternateCountries.txt) > country_synonymns.txt

# 36K country synonymns
wc country_synonymns.txt
