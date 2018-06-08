-- SELECT * FROM apps LIMIT 5 ;

DROP TABLE if EXISTS req2b;
CREATE temp TABLE req2b(
  tname CHARACTER VARYING (32)
) diststyle ALL;

INSERT INTO req2b SELECT 'req2b' ;
