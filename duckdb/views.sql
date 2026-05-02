.sh rm -rf ldbc/

attach 'ldbc.duckdb' as ldbc;
use ldbc;
export database 'ldbc_parquets' (format parquet);

use memory;
detach ldbc;

create view city                      as from read_parquet('ldbc_parquets/city.parquet');
create view comment_hastag_tag        as from read_parquet('ldbc_parquets/comment_hastag_tag.parquet');
create view comment                   as from read_parquet('ldbc_parquets/comment.parquet');
create view company                   as from read_parquet('ldbc_parquets/company.parquet');
create view country                   as from read_parquet('ldbc_parquets/country.parquet');
create view forum_hasmember_person    as from read_parquet('ldbc_parquets/forum_hasmember_person.parquet');
create view forum_hastag_tag          as from read_parquet('ldbc_parquets/forum_hastag_tag.parquet');
create view forum                     as from read_parquet('ldbc_parquets/forum.parquet');
create view message_hastag_tag        as from read_parquet('ldbc_parquets/message_hastag_tag.parquet');
create view message                   as from read_parquet('ldbc_parquets/message.parquet');
create view organisation              as from read_parquet('ldbc_parquets/organisation.parquet');
create view person_hasinterest_tag    as from read_parquet('ldbc_parquets/person_hasinterest_tag.parquet');
create view person_knows_person       as from read_parquet('ldbc_parquets/person_knows_person.parquet');
create view person_likes_comment      as from read_parquet('ldbc_parquets/person_likes_comment.parquet');
create view person_likes_message      as from read_parquet('ldbc_parquets/person_likes_message.parquet');
create view person_likes_post         as from read_parquet('ldbc_parquets/person_likes_post.parquet');
create view person_studyat_university as from read_parquet('ldbc_parquets/person_studyat_university.parquet');
create view person_workat_company     as from read_parquet('ldbc_parquets/person_workat_company.parquet');
create view person                    as from read_parquet('ldbc_parquets/person.parquet');
create view place                     as from read_parquet('ldbc_parquets/place.parquet');
create view post_hastag_tag           as from read_parquet('ldbc_parquets/post_hastag_tag.parquet');
create view post                      as from read_parquet('ldbc_parquets/post.parquet');
create view tag                       as from read_parquet('ldbc_parquets/tag.parquet');
create view tagclass                  as from read_parquet('ldbc_parquets/tagclass.parquet');
create view university                as from read_parquet('ldbc_parquets/university.parquet');
