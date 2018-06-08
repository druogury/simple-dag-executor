-- SELECT * FROM apps LIMIT 5 ;

DROP TABLE if EXISTS req2a;
CREATE temp TABLE req2a(
  tname CHARACTER VARYING (32)
) diststyle ALL;

INSERT INTO req2a SELECT 'req2a' ;
