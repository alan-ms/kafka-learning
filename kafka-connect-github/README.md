Github issues connector source

# Running in development


## Build do projeto
```
mvn clean package
export CLASSPATH="$(find target/ -type f -name '*.jar'| grep '\-package' | tr '\n' ':')"
$CONFLUENT_HOME/bin/connect-standalone $CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties config/MySourceConnector.properties
```

## Execução via docker compose:
```
docker compose up
```

**Obs:** Execução do docker compose está quebrada, problema ao executar o connector na UID do landoop.
