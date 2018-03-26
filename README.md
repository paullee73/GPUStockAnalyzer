To use, make a Kinetica account to initialize your own running database. You can get a free  
90-day key at: https://www.kinetica.com/trial/. Install a locally running instance using  
the directions in the README here: https://github.com/kineticadb/kinetica-api-cpp/tree/release/v6.0.0.  
After following all the installation directions, clone this repository into the folder that is made.  
Run the makefile as follows:  
  
./test your_local_instance "EXPR_1" "TARGET_DATE" "EXPR_2" "TARGET_PRICE"  
  
EXPR_1 and EXPR_2 can be any of the operators: > < >= <= == = != <>  
TARGET_DATE must be in the format MM/DD/YYYY  
TARGET_PRICE can be a numerical value (Ex: 180000)  
  
The parameters parses the oil prices dataset by the specified value. An example is:  
  
./test http://127.0.0.1:9191 "<" "04/11/2012" ">" "180000"  
  
which will get all the days before 04/11/2012 where the price of oil was less than $180000.  
