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
shorts(gsrc, dst, w, dead, iter) as (
    (
        with srcs as (select distinct f from qs)
        select f, f, 0, true, 0 from srcs
        union all
        select ss.f, p.dst, min(p.w), false, 0
        from srcs ss, path p
        where ss.f = p.src and p.dst not in (select ss2.f from srcs ss2)
        group by ss.f, p.dst
    )
    union all
    (
        with
        toExplore as (select * from shorts where dead = false order by w limit 1000),
        newPoints as (
            select e.gsrc as gsrc, p.dst as dst, min(e.w + p.w) as w, false as dead, min(e.iter) + 1 as iter
            from path p join toExplore e on forceorder(e.dst = p.src)
            group by e.gsrc, p.dst
        ),
        updated as (
            select n.gsrc, n.dst, n.w, false as dead, iter
            from newPoints n
            where not exists (select * from shorts o where n.gsrc = o.gsrc and n.dst = o.dst and o.w <= n.w)
        ),
        found as (
            select q.f, q.t, n.w
            from qs q left join updated n on n.dst = q.t and n.gsrc = q.f
        ),
        ss2 as (
            select o.gsrc, o.dst, o.w, o.dead or (exists (select * from toExplore e where e.gsrc = o.gsrc and e.dst = o.dst)) as dead, o.iter + 1 as iter
            from shorts o
        ),
        fullTable as (
            select coalesce(n.gsrc, o.gsrc) as gsrc,
                   coalesce(n.dst, o.dst) as dst,
                   coalesce(n.w, o.w) as w,
                   coalesce(n.dead, o.dead) as dead,
                   coalesce(n.iter, o.iter) as iter
            from ss2 o full join updated n on o.gsrc = n.gsrc and o.dst = n.dst
        )
        select gsrc,
               dst,
               w,
               dead or (t.w > coalesce((select min(w) from found f), t.w)),
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
select coalesce(min(w), -1) from results;
