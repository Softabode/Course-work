-- Databricks notebook source
---CREATE CLINICALTRIAL_2021 TABLE

CREATE TABLE IF NOT EXISTS clinicaltrial_2021(
)
USING CSV
OPTIONS (path "dbfs:/FileStore/tables/clinicaltrial_2021.csv", delimiter "|", header="True", inferSchema="True")


-- COMMAND ----------

---CREATE MESH TABLE

CREATE TABLE IF NOT EXISTS Mesh_(
)
USING CSV
OPTIONS (path "dbfs:/FileStore/tables/mesh.csv", delimiter ",", header="True", inferSchema="True")

-- COMMAND ----------

---CREATE PHARMA TABLE

CREATE TABLE IF NOT EXISTS Pharma_(
)
USING CSV
OPTIONS (path "dbfs:/FileStore/tables/pharma.csv", delimiter ",", header="True", inferSchema="True")

-- COMMAND ----------

-- DBTITLE 1,Problem 1
---Selecting distinct count from clinicaltrial_2021
SELECT distinct COUNT(*) AS Count FROM clinicaltrial_2021;

-- COMMAND ----------

-- DBTITLE 1,Problem 2
---Retrieving the types of studies in the clinicaltrial_2021 dataset
select Type, COUNT(Id) As Counts
from clinicaltrial_2021
group by Type
order by Counts DESC;

-- COMMAND ----------

-- DBTITLE 1,Problem 3
--- Retrieving the top 5 conditions and the frequencies

select Conditions, COUNT(Id) AS Counts
from(
  select Id, explode(split(Conditions, ',')) 
  as Conditions 
  from clinicaltrial_2021 
  where Conditions!=''
  )
group by Conditions order by Counts DESC
LIMIT 5

-- COMMAND ----------

-- DBTITLE 1,PROBLEM 4
---CREATING A VIEW TO SPLIT AND EXPLODE CONDITIONS COLUMN

CREATE VIEW IF NOT EXISTS split_conditions AS
select Id, explode(split(Conditions, ',')) 
  as Conditions 
  from clinicaltrial_2021 
  where Conditions!=''

-- COMMAND ----------

select LEFT(tree,3), COUNT(*) AS Counts
from Mesh_
join split_conditions
on (Mesh_.term = split_conditions.Conditions)
GROUP BY LEFT(tree,3)
ORDER BY Counts DESC
LIMIT 5;

-- COMMAND ----------

-- DBTITLE 1,Problem 5
--The 10 most common sponsors that are not Pharmaceutical companies
--CREATING A VIEW THAT JOINS SPONSOR AND PARENT_COMPANY COLUMNS

CREATE VIEW IF NOT EXISTS pharma_sponsor AS 
Select Sponsor
from clinicaltrial_2021
LEFT ANTI JOIN Pharma_ ON Sponsor = Parent_Company

-- COMMAND ----------

--Selecting the 10 most common sponsors that are not Pharmaceutical companies

select Sponsor, count(*) AS Counts
FROM pharma_sponsor
Group By Sponsor
ORDER BY Counts DESC
LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,Problem 6
---showing the completed datasets monthly in 2021
---Create a view thst filters the Status Column to show the rows of completed clinicaltrials and those Completed in 2021


select LEFT(Completion,3) AS Completion, Count(*) As Counts
from clinicaltrial_2021
where Status='Completed' AND Completion like '%2021'
Group By Completion
Order By (Unix_timestamp(Completion,'MMM'),'MMM');

-- COMMAND ----------


