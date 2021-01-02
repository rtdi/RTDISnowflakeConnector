# Snowflake Loader 

ATTENTION: PoC only!! It is not supposed to be used unless the limitations are clearly understood!

_Take data from a topic (different schemas are supported) and load the (nested) data into relational tables of Snowflake. The VARIANT column is not used._

Source code available here: [github](https://github.com/rtdi/SnowflakeLoader)


## Installation and testing

On any computer install the Docker Daemon - if it is not already - and download this docker image with

    docker pull rtdi/snowflakeloader

Then start the image via docker run. For a quick test this command is sufficient

    docker run -d -p 80:8080 --rm --name snowflakeloader  rtdi/snowflakeloader

to expose a webserver at port 80 on the host running the container. Make sure to open the web page via the http prefix, as https needs more configuration.
For example [http://localhost:80/](http://localhost:80/) might do the trick of the container is hosted on the same computer.

The default login for this startup method is: **rtdi / rtdi!io**

The probably better start command is to mount two host directories into the container. In this example the host's /data/files contains all files to be loaded into Kafka and the /data/config is an (initially) empty directory where all settings made when configuring the connector will be stored permanently.

    docker run -d -p 80:8080 --rm -v /data/files:/data/ -v /data/config:/usr/local/tomcat/conf/security \
        --name snowflakeloader  rtdi/snowflakeloader


For proper start commands, especially https and security related, see the [ConnectorRootApp](https://github.com/rtdi/ConnectorRootApp) project, this application is based on.

  

### Connect the Pipeline to Kafka

The first step is to connect the application to a Kafka server, in this example Confluent Cloud.

<img src="https://github.com/rtdi/SnowflakeLoader/raw/master/docs/media/SnowflakeLoader-PipelineConfig.png" width="50%">


### Define a Connection

A Connection holds all information about the connection to the Snowflake instance.


### Define the consumer

### Data content

One topic can contain multiple schemas and each schema might have nested data. The consumer creates for each schema the required tables. For example for the schema SalesOrder with the nested schema of name Items two tables will be created, one called SalesOrder, the other SalesOrder_Items.


## Licensing

This application is provided as dual license. For all users with less than 100'000 messages created per month, the application can be used free of charge and the code falls under a Gnu Public License. Users with more than 100'000 messages are asked to get a commercial license to support further development of this solution. The commercial license is on a monthly pay-per-use basis.


## Data protection and privacy

Every ten minutes the application does send the message statistics via a http call to a central server where the data is stored for information along with the public IP address (usually the IP address of the router). It is just a count which service was invoked how often, no information about endpoints, users, data or URL parameters. This information is collected to get an idea about the adoption.
To disable that, set the environment variable HANAAPPCONTAINERSTATISTICS=FALSE.
