-- SELECT * FROM apps LIMIT 5 ;

DROP TABLE if EXISTS req4;
CREATE temp TABLE req4(
  tname CHARACTER VARYING (32)
) diststyle ALL;

INSERT INTO req4 SELECT 'req4' ;
