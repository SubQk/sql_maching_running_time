WITH abc as (
select 'computing'       as kind,
            a.project_key,
            a.slot,
            a.account,
            a.id,
            a.start_time,
            a.end_time
     from (
            select ra.project_key
                 , ra.slot
                 , ra.account
                 , ra.id
                 , (
              CASE
                WHEN {{start}} >= CURRENT_TIMESTAMP and {{end}} >= CURRENT_TIMESTAMP THEN CURRENT_TIMESTAMP
                WHEN ra.start_time < {{start}} THEN {{start}}
                WHEN ra.start_time >= {{start}} THEN ra.start_time
                END
              ) as start_time
                 , (
              CASE
                WHEN ra.end_time is null and {{end}} >= CURRENT_TIMESTAMP THEN CURRENT_TIMESTAMP
                WHEN ra.end_time is null and {{end}} < CURRENT_TIMESTAMP THEN {{end}}
                WHEN {{start}} >= CURRENT_TIMESTAMP and {{end}} >= CURRENT_TIMESTAMP THEN CURRENT_TIMESTAMP
                WHEN {{start}} < CURRENT_TIMESTAMP and {{end}} > CURRENT_TIMESTAMP THEN ra.end_time
                WHEN ra.end_time < {{end}} THEN ra.end_time
                WHEN ra.end_time >= {{end}} THEN {{end}}
                END
              ) as end_time
            from deployment_usage ra
            where ra.kind = 'computing'
              and ((ra.start_time >= {{start}}
              and ra.end_time <= {{end}})
               or (ra.start_time <= {{start}}
              and ra.end_time >= {{end}})
               or (ra.end_time >= {{end}}
              and ra.start_time <= {{end}})
               or (ra.end_time >= {{start}}
              and ra.start_time <= {{start}})
               or (ra.start_time >= {{start}}
              and ra.start_time <= {{end}}
              and ra.end_time is null)
               or (ra.start_time <= {{start}}
              and ra.end_time is null))
          ) a
     order by a.project_key
),efg as (
SELECT id
	 , account
	 , slot
     , start_time
     , end_time 
FROM   abc 
WHERE  end_time  <= start_time ::date + 1  
UNION ALL
SELECT id
	 , account
	 , slot
     , CASE WHEN start_time::date = d THEN start_time ELSE d END
     , CASE WHEN end_time::date = d THEN end_time ELSE d + 1 END 
FROM (
   SELECT id
   	    , slot
   		, account
        , start_time
        , end_time
        , generate_series(start_time::date, end_time::date, interval '1d')::date AS d
   FROM   abc
   WHERE  end_time > start_time::date + 1
   ) sub
ORDER  BY id, start_time
)
select EXTRACT(day from start_time) as day_st, 
	 sum(greatest(round(cast(extract('epoch' from efg.end_time - efg.start_time) / 3600 as numeric), 1),
                         .1)) as node_hours1, efg.account,o.plan
from efg
inner JOIN organizations o ON efg.account = o."key"
group by day_st 
order by day_st ;