## Utils querys

``` sql
SELECT  GROUP_CONCAT(nit), name, COUNT(*)
FROM entity
GROUP BY name
HAVING count(*) > 1;

SELECT  nit, GROUP_CONCAT(name), COUNT(*)
FROM entity
GROUP BY nit
HAVING count(*) > 1;
```
