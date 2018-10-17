CREATE TABLE temperature_by_day (
   weatherstation_id text,
   date text,
   event_time timestamp,
   temperature float,
   PRIMARY KEY ((weatherstation_id,date),event_time)
);
