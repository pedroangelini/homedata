use raw_data;

create
or replace table events as
select
    'ha_first' as ha_instance,
    state_id,
    to_timestamp(last_updated_ts) as last_updated,
    entity_id,
    split_part(entity_id, '.', 1) as entity_type,
    split_part(entity_id, '.', 2) as entity_name,
    state as state_value,
    to_json(shared_attrs),
    old_state_id,
    ingested_at,
    current_localtimestamp () as loaded_at
from
    staging.home_assistant_events;

alter table raw_data.events
add primary key (ha_instance, state_id);
