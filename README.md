# finEngine
An Engine for collecting financial data in realtime to your prefered database

### Compiling Project
 1. To compile project you need to import the project into Intellij Idea as a Gradle project
 2. Edit the /drivers.conf , comment out any exchange you wish not to use
 3. Each exchange has it own configuration in /config/drivers folder, edit each configuration according to your needs

 4. Edit /config/app.conf to edit the data access websocket port and host , also you can edit the http data access api 
 port and host to enable you have access to the retrieved data 

 5. the project uses mongo db as backend database, kindly check config folder to change the database credentials

## Ruuning 
 After compiling jar file say app.jar , run app using this command 
#### java -jar ./app.jar 

## Accessing Data 
 connect to the server using the websocket configuration in step #2 or http api configuration
 
 
