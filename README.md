#Dataset
kaggle netflix-uk dataset suggests that it has 7925 unique values. rowId starts at 58773, ends with 730508

#Data Quality
Movie Release Date field can have a value of 'NOT AVAILABLE'. Therefore, Schema will treat this field value as a String


# Next Steps

1. Confluent Cloud, establish kafka topic https://developer.confluent.io/quickstart/kafka-on-confluent-cloud/
2. Schema-registry `confluent schema-registry schema create --subject netflix-uk-click-event --schema ./Documents/alex-workspace/confluent-tmm/netflix-uk-insights/src/main/resources/schemas/NetflixUkClickEvent.avsc --type avro
Successfully registered schema with ID "100001".`
2. Confluent Cloud, spin up flink compute (do i need a service account and api key first? or will it do it automagically? svc-confluent-netflix-insights)  



Observations / Questions
* Created schema via cli but UI/console only allowed me to create a new data contract, rather than associate an \
existing schema (within registry) to an already created topic. So, I deleted both again to do all in one via UI/console.\
Documentation references "Schema tab, within topics section".
* Speed Bump: trying to run with the java client/producer timing out waitOnMetadata()\
java 23? -Djava.security.manager=allow https://github.com/microsoft/mssql-jdbc/issues/2524
