/* Q15. Trusted connection paths through forums created in a given timeframe
\set person1Id 21990232564808
\set person2Id 26388279076936
\set startDate '\'2010-11-01\''::timestamp
\set endDate '\'2010-12-01\''::timestamp
 */
with recursive
qs(f, t) as (
    select :person1Id, :person2Id
),
myForums(id) as (
    select id from Forum f where f.creationDate between :startDate and :endDate
),
mm as (
    select least(msg.CreatorPersonId, reply.CreatorPersonId) as src, greatest(msg.CreatorPersonId, reply.CreatorPersonId) as dst, sum(case when msg.ParentMessageId is null then 10 else 5 end) as w
    from Person_knows_Person pp, Message msg, Message reply
    where true
          and pp.person1id = msg.CreatorPersonId 
          and pp.person2id = reply.CreatorPersonId
          and reply.ParentMessageId = msg.MessageId
          and exists (select * from myForums f where f.id = msg.containerforumid)
          and exists (select * from myForums f where f.id = reply.containerforumid)
    group by src, dst
),
path(src, dst, w) as (
    select pp.person1id, pp.person2id, 10::double precision / (coalesce(w, 0) + 10)
    from Person_knows_Person pp left join mm on least(pp.person1id, pp.person2id) = mm.src and greatest(pp.person1id, pp.person2id) = mm.dst
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
select coalesce(min(w), -1) from results;
