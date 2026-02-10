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

## Distinguishing between changes in Specimens and Media

A specimen event may include zero or more media objects. An unchanged specimen may not necessarily
mean its media are unchanged, and vice versa. To address this, we may publish objects to one of
three queues:

1. **Specimen and Media are unchanged**: Update last checked on all objects. The ingestion process
   stops here.
2. **Specimen and all media are changed**: Publish the whole SpecimenEvent to the next step in the
   ingestion pipeline, `name-usage-service`
3. **Specimen is unchanged, media are changed**: update last_checked on specimen. Publish the media
   to the `media-queue`, bypassing the name usage service
    - Note: media entity relationships will not be changed when ingested through this queue
4. **Specimen is changed, media are unchanged**: update last_checked on media. Publish specimen to
   the `name-usage-service-specimen` queue.
    - Planned for future release

# Run Locally

Running locally requires: 
- Access to the rabbitmq cluster via localhost:5672 
  - `kubectl port-forward -n rabbitmq rabbitmq-cluster-server-0 5672`
- Access to the database (including whitelisted IP address)

## RabbitMQ Queues

**Consumes from:** `source-system-data-checker-queue` (from translator)
**Publishes to:** `name-usage-service-queue` (to name usage service)

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


