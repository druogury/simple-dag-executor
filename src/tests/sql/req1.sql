-- SELECT * FROM apps LIMIT 5 ;

DROP TABLE if EXISTS req1;
CREATE temp TABLE req1(
  tname CHARACTER VARYING (32)
) diststyle ALL;

INSERT INTO req1 SELECT 'req1' ;
