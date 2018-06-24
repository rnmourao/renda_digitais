################################################################################
# Converte arquivo csv gerado por SQL para formato esperado pelo Pyspark.      #
################################################################################

#!/bin/bash

# create a copy from the original file
cp $1 temp 

# remove all brazilian's thousands separators
sed -i 's/\.//g' temp

# replace brazilian's decimal separators to english ones
sed -i 's/,/./g' temp

# replace column separators
sed -i 's/;/,/g' temp

# remove all quotes
sed -i 's/"//g' temp

# remove null characters
tr < temp -d '\000' > temp2

# remove duplicated spaces
tr -s ' ' < temp2 > temp3

# remove spaces near to column separators
sed -i 's/ ,/,/g' temp3

# convert all text to lower-case
tr '[:upper:]' '[:lower:]' < temp3 > temp4

# save new file
mv temp4 $2

# remove temporary files
rm temp*
