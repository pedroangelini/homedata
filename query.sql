with all_states as (
select state_id, state, DATETIME(last_updated_ts,'unixepoch') last_updated, old_state_id, shared_attrs, states_meta.entity_id
from states left join state_attributes on state_attributes.attributes_id = states.attributes_id
left join states_meta on states_meta.metadata_id = states.metadata_id
)
select entity_id, min(last_updated), count(*) cnt from all_states group by entity_id order by cnt desc;
-- select count(*) from all_states;