FROM rtdi/connectorrootapp

LABEL maintainer="info@rtdi.io"

ADD https://raw.githubusercontent.com/rtdi/RTDISnowflakeConnector/master/docker/global.properties conf/rtdiconfig/SnowflakeLoader/
ADD https://github.com/rtdi/RTDISnowflakeConnector/releases/latest/download/snowflakeloader.war ./webapps/

EXPOSE 8080
EXPOSE 8443


CMD ["appcontainer.sh"]
