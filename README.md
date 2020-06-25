# Store data received from Orion To a Postgresql database

This script receives all messages from a FIWARE Orion Context Broker and save it to a Postgresql database.
It is an adaptation of the simple accumulator provided by FIWARE in:
https://github.com/telefonicaid/fiware-orion/blob/master/scripts/accumulator-server.py

To do its job the script:
* listens for new data arriving from Orion
* elaborates incoming data and save it into a database

How to use it:
1. Install Postgresql
   * `sudo apt-get install postgresql libpq-dev postgresql-client postgresql-client-common`
   * `pip install psycopg2`
   * Create the database:
      * `sudo -i -u postgres`
      * `createuser abcuser -P --interactive`  
         Answer "y" to all the questions and set a password for the created user
      * `createdb orion-data-db`
      * To interact with the database type:  
         * `sudo -i -u postgres`
         * `psql orion-data-db`
         * To exit `\q`
         
2. Set parameters inside the "config.json" file
3. Run "pip install" inside the folder of the script to install all required packages reported in requirements.txt
4. To run the script:
    python3 saveOrionToDB.py