# Overview

The purpose of this service is to filter unchanged specimen and media events from the source system.
While changed data should progress through the ingestion pipeline, unchanged events only need their
`last_checked` value to be updated.

This service sits between the translator and the name usage service (or processing service) in the
ingestion pipeline.

The translator's purpose is to translate incoming data from the source system to openDS. It includes
the original data in the specimen and media events, as well as the translated openDS version.

This service first checks if an incoming specimen/media exists or not, using the object's unique
local identifier. For specimens, it is the normalised physical specimen id. For media, it is the
access URI.

If the relevant objects exist in the database, we need to check if it is an update from the source
system, or if the data has not changed. This service assesses if data has been changed by comparing
the current values in the `original_data` column in the database. This column is updated every time
the source system data changes; it is the raw data from the source system as captured by the
translator. If the incoming `original_data` differs from what is in the database, we consider this
an update.

## RabbitMQ Queues

**Consumes from:** `source-system-data-checker-queue` (from translator)

**Publishes to:** One of:

* `name-usage-service-queue` - regular ingestion process, when specimen and media are new/changed
* `digital-media-queue` - when only media needs to be updated, send directly to processing service
  and skip NU service

## Distinguishing Between Changes in Specimens and Media

A specimen event may include zero or more media objects. An unchanged specimen may not necessarily
mean its media are unchanged. We address three scenarios here:

1. **Specimen and Media are unchanged**: Update last checked on all objects. The ingestion process
   stops here.
2. **Specimen is changed**: Publish the whole SpecimenEvent to the next step in the
   ingestion pipeline, `name-usage-service`.
    * Note: Media may be unchanged in this event. This is addressed in the processing service.
3. **Specimen is unchanged, media are changed**: update last_checked on specimen. Publish the
   changed media to the `media-queue`, bypassing the name usage service
    - Note: media entity relationships will not be changed when ingested through this queue

## Scheduling Forced MAS Events

Requested MASs in the Event are typically scheduled only if the specimen is new or updated; however,
the `forceMasSchedule` can be set to also schedule MASs on unchanged specimens. The SSDC will
schedule MASs for unchanged specimens with this flag set to true.

# Running Locally

## Requirements

Running locally requires:

- Access to the rabbitmq cluster via localhost:5672
    - `kubectl port-forward -n rabbitmq rabbitmq-cluster-server-0 5672`
- Access to the relational database (IP address is whitelisted)

## Domain Object generation

DiSSCo uses JSON schemas to generate domain objects (e.g. Digital Specimens, Digital Media, etc)
based on the openDS specification. These files are stored in the
`/target/generated-sources/jsonschema2pojo directory`, and must be generated before running locally.
The following steps indicate how to generate these objects.

### Importing Up To-Date JSON Schemas

The JSON schemas are stored in `/resources/json-schemas`. The source of truth for JSON schemas is
the [DiSSCO Schemas Site](https://schemas.dissco.tech/schemas/fdo-type/). If the JSON schema has
changed, the changes can be downloaded using the maven runner script.

1. **Update the pom.xml**: The exec-maven-plugin in the pom indicated which version of the schema to
   download. If the version has changed, update the pom.
2. **Run the exec plugin**: Before the plugin can be run, the code must be compiled. Run the
   following in the terminal (or via the IDE interface):

```
mvn compile 
mvn exec:java
```

### Building POJOs

DiSSCo uses the [JsonSchema2Pojo](https://github.com/joelittlejohn/jsonschema2pojo) plugin to
generate domain objects based on our JSON Schemas. Once the JSON schemas have been updated, you can
run the following from the terminal (or via the IDE interface):

```
mvn clean
mvn jsonschema2pojo:generate
```

## Application Properties

### Mandatory properties

`spring.rabbitmq.username=` RabbitMQ username
`spring.rabbitmq.password=` RabbitMQ password
`spring.rabbitmq.host=` localhost (default)
`spring.datasource.url=`database url (starting with `jdbc:postgresql://`)
`spring.datasource.username=` database username
`spring.datasource.password=` database password

### Optional Properties

Users can manually set exchanges and routing keys for:

* Republish events (queue this service consumes from)
* Media events
* Name Usage events

However, these are already defined in the code, and do not need to be set. 


