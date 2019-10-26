/*QUESTION 1
Select the first name, last name, and given name of players who are taller than 6 ft
[hint]: Use "People"
*/
select nameFirst, nameLast, nameGiven from People where height > 72;
/*QUESTION 2
Create a Table of all the distinct players with a first name of John who were born in the United States and
played at Fordham university
Include their first name, last name, playerID, and birth state
Add a column called nameFull that is a concatenated version of first and last
[hint] Use a Join between People and CollegePlaying
*/
create table JOHNS as
	select nameFirst, nameLast, people.playerID, people.birthState,CONCAT(nameFirst, ' ', nameLast) nameFull
    from people inner join collegeplaying on people.playerID = collegeplaying.playerID
    where nameFirst= "John" and collegeplaying.schoolID="fordham" and people.birthCountry="USA";
/*QUESTION 3
Delete all Johns from the above table whose total career runs batted in is less than 2
[hint] use a subquery to select these johns from people by playerid
[hint] you may have to set sql_safe_updates = 1 to delete without a key
*/
SET SQL_SAFE_UPDATES = 0;
delete from JOHNS
where exists(
	select people.playerID
	from people join batting on people.playerID = batting.playerID
	where batting.HR < 10
	and JOHNS.plyaerID = people.playerID
);
SET SQL_SAFE_UPDATES = 1;
/*QUESTION 4
Group together players with the same birth year, and report the year, 
 the number of players in the year, and average height for the year
 Order the resulting by year in descending order. Put this in a view
 [hint] height will be NULL for some of these years
*/
create view AverageHeight(birthYear, player_count, average_height)
as
  select birthYear, count(playerID), avg(height) from People group by birthYear order by birthYear desc;
/*QUESTION 5
Using Question 3, only include groups with an average weight >180 lbs,
also return the average weight of the group. This time, order by ascending
*/
create view AverageHeightWeight(birthYear, player_count, average_height, average_weight)
as
	select avg_w.birthYear, player_count, average_height, avg_w.average_weight
	from (select birthYear, avg(weight) average_weight from people group by birthYear) avg_w
	join AverageHeight on avg_w.birthYear = AverageHeight.birthYear
	where avg_w.average_weight > 180;

/*QUESTION 6
Find the players who made it into the hall of fame who played for a college located in NY
return the player ID, first name, last name, and school ID. Order the players by School alphabetically.
Update all entries with full name Columbia University to 'Columbia University!' in the schools table
*/
select people.playerID, nameFirst, nameLast, temp2.schoolID
from people  inner join  (
	select halloffame.playerID as playerID, temp1.schoolID as schoolID
	from halloffame
	inner join (select collegeplaying.playerID as playerID, schools.schoolID as schoolID
			from collegeplaying join schools on collegeplaying.schoolID = schools.schoolID
			where schools.state = 'NY' group by playerID) temp1
	on temp1.playerID = halloffame.playerID group by playerID) temp2
on temp2.playerID = people.playerID order by schoolID;
SET SQL_SAFE_UPDATES = 0;
UPDATE Schools set name_full = "Columbia University!" where name_full = "Columbia University";
SET SQL_SAFE_UPDATES = 1;
/*QUESTION 7
Find the team id, yearid and average HBP for each team using a subquery.
Limit the total number of entries returned to 100
group the entries by team and year and order by descending values
[hint] be careful to only include entries where AB is > 0
*/
select teamID, yearID, avg(HBP) a
from (select teamID, yearID, HBP from TEAMS where AB>0) t
group by teamID, yearID
order by a desc
limit 100;