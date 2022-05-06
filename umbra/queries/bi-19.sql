/* Q19. Interaction path between cities
\set city1Id 608
\set city2Id 1148
 */
with recursive
qs(f, t) as (
    select p1.id, p2.id
    from Person p1, Person p2
    where p1.locationcityid = :city1Id and p2.locationcityid = :city2Id
),
weights(src, dst, c) as (
    select least(m1.creatorpersonid, m2.creatorpersonid) as src,
           greatest(m1.creatorpersonid, m2.creatorpersonid) as dst,
           count(*) as c
    from Person_knows_person pp, Message m1, Message m2
    where pp.person1id = m1.creatorpersonid and pp.person2id = m2.creatorpersonid and m1.parentmessageid = m2.messageid and m1.creatorpersonid <> m2.creatorpersonid
    group by src, dst
),
path(src, dst, w) as (
    select src, dst, 1.0::double precision / c from weights
    union all
    select dst, src, 1.0::double precision / c from weights
),
shorts(dir, gsrc, dst, w, dead, iter) as (
    (
        with
        srcs as (select distinct f from qs),
        dsts as (select distinct t from qs)
        (
            select false, f, f, 0, false, 0 from srcs
            union all
            select true, t, t, 0, false, 0 from dsts
        )
    )
    union all
    (
        with
        toExplore as (select * from shorts where dead = false order by w limit 1000),
        -- assumes graph is undirected
        newPoints as (
            select e.dir, e.gsrc as gsrc, p.dst as dst, min(e.w + p.w) as w, false as dead, min(e.iter) + 1 as iter
            from path p join toExplore e on forceorder(e.dst = p.src)
            group by e.dir, e.gsrc, p.dst
        ),
        updated as (
            select n.dir, n.gsrc, n.dst, n.w, false as dead, iter
            from newPoints n
            where not exists (select * from shorts o where n.gsrc = o.gsrc and n.dst = o.dst and o.w <= n.w)
        ),
        found as (
            select min(l.w + r.w) as w
            from shorts l, shorts r
            where l.dir = false and r.dir = true and l.dst = r.dst
        ),
        ss2 as (
            select o.dir, o.gsrc, o.dst, o.w, o.dead or (exists (select * from toExplore e where e.dir = o.dir and e.gsrc = o.gsrc and e.dst = o.dst)) as dead, o.iter + 1 as iter
            from shorts o
        ),
        fullTable as (
            select coalesce(n.dir, o.dir) as dir,
                   coalesce(n.gsrc, o.gsrc) as gsrc,
                   coalesce(n.dst, o.dst) as dst,
                   coalesce(n.w, o.w) as w,
                   coalesce(n.dead, o.dead) as dead,
                   coalesce(n.iter, o.iter) as iter
            from ss2 o full join updated n on o.dir = n.dir and o.gsrc = n.gsrc and o.dst = n.dst
        )
        select dir,
               gsrc,
               dst,
               w,
               dead or (coalesce(t.w > (select f.w/2 from found f), false)),
               iter
        from fullTable t
        where exists (select * from toExplore limit 1)
    )
),
ss(dir, gsrc, dst, w, iter) as (
    select dir, gsrc, dst, w, iter from shorts where iter = (select max(iter) from shorts)
),
results(f, t, w) as (
    select l.gsrc, r.gsrc, min(l.w + r.w)
    from ss l, ss r
    where l.dir = false and r.dir = true and l.dst = r.dst
    group by l.gsrc, r.gsrc
)
select * from results where w = (select min(w) from results) order by f, t;
