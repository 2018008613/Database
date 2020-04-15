#1

SELECT name
from Pokemon
where type = 'Grass'
Order by name;

#2

SELECT name
from Trainer
where hometown = 'Brown City' or hometown = 'Rainbow City'
Order by name;

#3

SELECT DISTINCT type
FROM Pokemon
Order by type;

#4

SELECT name
FROM City
WHERE name LIKE 'B%'
Order by name;

#5

SELECT hometown
FROM Trainer
WHERE not name LIKE 'M%'
Order by hometown;

#6

SELECT nickname
FROM CatchedPokemon as c1,
(
  SELECT MAX(level) as MAX_LEVEL
  FROM CatchedPokemon
 ) as c2
WHERE c1.level = c2.MAX_LEVEL
Order by nickname;

#7

SELECT name
FROM Pokemon
WHERE name LIKE 'A%' OR name LIKE 'E%' OR name LIKE 'I%' OR name LIKE 'O%' OR name LIKE 'U%'
Order by name;

#8

SELECT AVG(level)
FROM CatchedPokemon;

#9

SELECT MAX(level)
FROM CatchedPokemon, Trainer
WHERE Trainer.name = 'Yellow' AND Trainer.id = CatchedPokemon.owner_id
Group by owner_id;

#10

SELECT DISTINCT hometown
FROM Trainer
Order by hometown;

#11

SELECT t.name, c.nickname
FROM CatchedPokemon as c, Trainer as t
WHERE nickname LIKE BINARY 'A%' AND t.id = c.owner_id
Order by t.name;

#12

SELECT T.name
FROM City as c, Trainer as t, Gym as g
WHERE c.description = 'Amazon' AND c.name = g.city AND g.leader_id = t.id;

#13

SELECT owner_id, COUNT(pid)
FROM CatchedPokemon as c, Pokemon as p
WHERE c.pid = p.id AND p.type = 'Fire'
GROUP BY owner_id;

#14

SELECT DISTINCT type
FROM Pokemon
WHERE id < 10
ORDER BY id desc;

#15

SELECT COUNT(*)
FROM Pokemon
WHERE type != 'Fire';

#16

SELECT p.name
FROM Evolution as e, Pokemon as p
WHERE e.after_id < e.before_id AND p.id = e.before_id
Order by p.name;

#17

SELECT AVG(level)
FROM CatchedPokemon as c, Pokemon as p
WHERE c.pid = p.id AND p.type = 'Water';

#18

SELECT c1.nickname
FROM CatchedPokemon as c1, Gym as g,
(
  SELECT MAX(level) as MAX_level
  FROM CatchedPokemon as c, Gym as g
  WHERE c.owner_id = g.leader_id
  ) as c2
WHERE c1.owner_id = g.leader_id AND c1.level = c2.MAX_level;

#19

SELECT t.name
FROM Trainer as t,
(SELECT owner_id, AVG(level) as AVG_level
FROM CatchedPokemon as c, Trainer as t
WHERE t.hometown = 'Blue city' AND t.id = c.owner_id
Group by owner_id
 ) as c1,
(SELECT MAX(c1.AVG_level) as MAX_AVG_level
FROM (SELECT owner_id, AVG(level) as AVG_level
FROM CatchedPokemon as c, Trainer as t
WHERE t.hometown = 'Blue city' AND t.id = c.owner_id
Group by owner_id
 ) as c1
 ) as c2
WHERE MAX_AVG_level = AVG_level AND c1.owner_id = t.id
Order by t.name;

#20

SELECT p.name
FROM CatchedPokemon as c, Evolution as e, Pokemon p,
(
  SELECT t.id as t2_id
  FROM Trainer as t,
  (
  SELECT hometown, COUNT(*) as COUNT_NUM
  FROM Trainer as t
  Group by hometown
  ) as t1
  WHERE t1.COUNT_NUM = 1 AND t1.hometown = t.hometown
) as t2
WHERE t2_id = c.owner_id AND c.pid = p.id AND e.before_id = c.pid AND p.type = 'Electric';

#21

SELECT t.name, c1.SUM_level
FROM Trainer as t,
(
  SELECT owner_id, SUM(level) as SUM_level
  FROM CatchedPokemon as c, Gym as g
  WHERE c.owner_id = g.leader_id
  Group by owner_id
  ) as c1
WHERE t.id = c1.owner_id
Order by c1.SUM_level desc;

#22

SELECT t1.hometown
FROM
(
  SELECT hometown, COUNT(*) as COUNT_NUM
  FROM Trainer as t
  Group by hometown
  ) as t1,
(
SELECT MAX(COUNT_NUM) as MAX_COUNT_NUM
FROM 
(
  SELECT hometown, COUNT(*) as COUNT_NUM
  FROM Trainer as t
  Group by hometown
  ) as t1
) as t2
WHERE t1.COUNT_NUM = t2.MAX_COUNT_NUM;

#23

SELECT DISTINCT p.name
FROM Pokemon as p,
(
  SELECT hometown, pid
  FROM Trainer as t, CatchedPokemon as c
  WHERE t.hometown = 'Sangnok City' AND t.id = c.owner_id
) as t1,
(
  SELECT hometown, pid
  FROM Trainer as t, CatchedPokemon as c
  WHERE t.hometown = 'Brown City' AND t.id = c.owner_id
) as t2
WHERE p.id = t1.pid AND p.id = t2.pid
Order by p.name;

#24

SELECT t.name
FROM Trainer t,
(
  SELECT owner_id
  FROM Pokemon as p, CatchedPokemon as c
  WHERE name LIKE BINARY 'P%' AND p.id = c.pid
 ) as c1
WHERE hometown = 'Sangnok City' AND c1.owner_id = t.id
Order by t.name;

#25

SELECT t.name, p.name
FROM Trainer as t, CatchedPokemon as c, Pokemon as p
WHERE p.id = c.pid AND c.owner_id = t.id
Order by t.name, p.name;

#26

SELECT p.name
FROM Pokemon as p,
(
SELECT p1.id as id, SUM(COUNT_NUM) as before_sum
FROM Evolution as e,
(
  SELECT p.id as id, COUNT(*) as COUNT_NUM
  FROM Evolution as e, Pokemon as p
  WHERE p.id = e.before_id OR p.id = e.after_id
  Group by p.id
) as p1
WHERE p1.id = e.before_id
Group by p1.id
) as p2,
(
SELECT e.before_id as id, SUM(COUNT_NUM) as after_sum
FROM Evolution as e,
(
  SELECT p.id as id, COUNT(*) as COUNT_NUM
  FROM Evolution as e, Pokemon as p
  WHERE p.id = e.before_id OR p.id = e.after_id
  Group by p.id
) as p1
WHERE p1.id = e.after_id
Group by p1.id
) as p3
WHERE p.id = p2.id AND p.id = p3.id AND p2.before_sum = 1 AND p3.after_sum = 1
Order by p.name;

#27

SELECT c.nickname
FROM CatchedPokemon as c, Pokemon as p, Gym as g
WHERE g.city = 'Sangnok City' AND g.leader_id = c.owner_id AND c.pid = p.id AND p.type = 'WATER'
ORDER BY c.nickname;

#28

SELECT t.name
FROM Trainer as t,
(
SELECT owner_id, COUNT(*) as COUNT_NUM
FROM CatchedPokemon as c, Evolution as e
WHERE e.after_id = c.pid
Group by owner_id
) as c1
WHERE c1.COUNT_NUM >= 3 AND c1.owner_id = t.id
Order by t.name;

#29

SELECT p.name
FROM Pokemon as p,
(
SELECT id, IFNULL(b.COUNT_NUM, 0) AS COUNT_NUM
FROM Pokemon as p
  LEFT OUTER JOIN(
    SELECT pid, COUNT(*) AS COUNT_NUM
    FROM CatchedPokemon as c
    GROUP BY pid
    ) as b on(p.id = b.pid)
Group by p.id
) as p1
WHERE p.id = p1.id AND p1.COUNT_NUM = 0
Order by p.name;

#30

SELECT t2.level
FROM
(
SELECT hometown, MAX(level) as level
FROM
(
SELECT t.hometown, level
FROM Trainer as t, CatchedPokemon as c
WHERE t.id = c.owner_id
) as t1
group by hometown
)as t2
Order by t2.level desc;

#31

SELECT e1.before_id, p1.name, p2.name, p3.name
FROM Evolution as e1, Evolution as e2, Pokemon as p1, Pokemon as p2, Pokemon as p3
WHERE e1.after_id = e2.before_id AND p1.id = e1.before_id AND p2.id = e1.after_id AND p3.id = e2.after_id
ORDER BY e1.before_id;