-- attach sqlite home assistant db from staging
attach 'staging/home-assistant_v2.db' as ha_staging_db (
    type sqlite
);

use homedata;

use staging;

create
or replace table home_assistant_events as
select
    state_id,
    state,
    last_updated_ts,
    old_state_id,
    shared_attrs,
    states_meta.entity_id
from
    ha_staging_db.states
    left join ha_staging_db.state_attributes on state_attributes.attributes_id = states.attributes_id
    left join ha_staging_db.states_meta on states_meta.metadata_id = states.metadata_id;

detach ha_staging_db;