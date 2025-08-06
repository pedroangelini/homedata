use raw_data;

insert into
    events
select
    'ha_first' as ha_instance,
    state_id,
    to_timestamp(last_updated_ts) as last_updated,
    entity_id,
    split_part(entity_id, '.', 1) as entity_type,
    split_part(entity_id, '.', 1) as entity_name,
    state as state_value,
    to_json(shared_attrs),
    old_state_id,
    ingested_at,
    current_localtimestamp () as loaded_at
from
    staging.home_assistant_events
where
    last_updated > (
        select
            max(last_updated)
        from
            events
    );