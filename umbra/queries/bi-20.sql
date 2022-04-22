/* Q20. Recruitment
\set person2Id 32985348834889
\set company 'Express_Air'
 */
with recursive
qs(f, t) as (
    select :person2Id, personid
    from Person_workat_company pwc, Company c
    where pwc.companyid = c.id and c.name=:company
),
path(src, dst, w) as (
    select p1.personid, p2.personid, min(abs(p1.classYear - p2.classYear)) + 1
    from Person_knows_person pp, Person_studyAt_University p1, Person_studyAt_University p2
    where pp.person1id = p1.personid and pp.person2id = p2.personid and p1.universityid = p2.universityid
    group by p1.personid, p2.personid
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
select t, w from results where w = (select min(w) from results) order by t;
