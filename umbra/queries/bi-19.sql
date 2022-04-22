/* Q19. Interaction path between cities
\set city1Id 608
\set city1Id 1148
 */
with recursive
qs(f, t) as (
    select p1.id, p2.id
    from Person p1, Person p2
    where p1.locationcityid = :city1id and p2.locationcityid = :city2id
),
weights(src, dst, w) as (
    select least(m1.creatorpersonid, m2.creatorpersonid) as src,
           greatest(m1.creatorpersonid, m2.creatorpersonid) as dst,
           1.0::double precision / count(*)
    from Person_knows_person pp, Message m1, Message m2
    where pp.person1id = m1.creatorpersonid and pp.person2id = m2.creatorpersonid and m1.parentmessageid = m2.messageid and m1.creatorpersonid <> m2.creatorpersonid
    group by src, dst
),
path(src, dst, w) as (
    select src, dst, w from weights
    union all
    select dst, src, w from weights
),
shorts(gsrc, dst, w, dead, iter) as (
    select distinct f, f, 0, false, 0 from qs
    union all
    (
        with
        ss as (select * from shorts),
        toExplore as (select * from ss where dead = false order by w limit 1000),
        newPoints as (
            select e.gsrc as gsrc, p.dst as dst, min(e.w + p.w) as w, false as dead, min(e.iter) + 1 as iter
            from path p join toExplore e on forceorder(e.dst = p.src)
            group by e.gsrc, p.dst
        ),
        updated as (
            select n.gsrc, n.dst, n.w, false as dead, iter
            from newPoints n
            where not exists (select * from ss o where n.gsrc = o.gsrc and n.dst = o.dst and o.w <= n.w)
        ),
        found as (
            select q.f, q.t, n.w
            from qs q left join updated n on n.dst = q.t and n.gsrc = q.f
        ),
        fullTable as (
            select coalesce(n.gsrc, o.gsrc) as gsrc,
                   coalesce(n.dst, o.dst) as dst,
                   coalesce(n.w, o.w) as w,
                   coalesce(n.dead, o.dead or (exists (select * from toExplore e where e.gsrc = o.gsrc and e.dst = o.dst))) as dead,
                   coalesce(n.iter, o.iter + 1) as iter
            from ss o full join updated n on o.gsrc = n.gsrc and o.dst = n.dst
        )
        select gsrc,
               dst,
               w,
               dead or (exists (select * from found f where f.w < t.w)),
               iter
        from fullTable t
        where exists (select * from toExplore limit 1)
    )
),
ss(gsrc, dst, w, iter) as (
    select gsrc, dst, w, iter from shorts where iter = (select max(iter) from shorts)
),
results(f, t, w) as (
    select qs.f, qs.t , ss.w from qs left join ss on qs.f = ss.gsrc and qs.t = ss.dst
)
select * from results where w = (select min(w) from results) order by f, t;
