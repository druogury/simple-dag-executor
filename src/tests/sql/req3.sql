-- SELECT * FROM apps LIMIT 5 ;

DROP TABLE if EXISTS req3;
CREATE temp TABLE req3(
  tname CHARACTER VARYING (32)
) diststyle ALL;

INSERT INTO req3 SELECT 'req3' ;
