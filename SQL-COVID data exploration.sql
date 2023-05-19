
/* Portfolio Project on two large data sets of COVID data */ 

--get the number of rows (there are 85171 rows in each data set)

SELECT COUNT(*)
FROM PortfolioProject_COVID..CovidDeaths

SELECT COUNT(*)
FROM PortfolioProject_COVID..CovidVaccinations


--look at the death data 

SELECT *
FROM PortfolioProject_COVID..CovidDeaths


--look at a sebset of the death table

SELECT location, date, new_cases, total_cases, total_deaths, population
FROM PortfolioProject_COVID..CovidDeaths
order by 1,2

--calculate the percentage of death per total cases by date
--a.k.a. likelihood of dying if someone catches COVID 
--this likelihood is different per location

SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS death_percentage
FROM PortfolioProject_COVID..CovidDeaths
WHERE location = 'United States'
order by 1,2


--calculate the total cases per population per date
--likelihood of catching COVID
--this likelihood is different per location

SELECT location, date, total_cases, population, (total_cases/population)*100 AS infection_percentage
FROM PortfolioProject_COVID..CovidDeaths
WHERE location = 'United States'
order by 1,2


--find the countries with the overall highest infection rate per population 

SELECT location, population, max(total_cases) AS latest_total_cases, (max(total_cases)/population)*100 AS infection_percentage
FROM PortfolioProject_COVID..CovidDeaths
GROUP BY location, population
ORDER BY infection_percentage desc


--find the countries with the overall highest death rate per total_cases
--NOTE: for the aggregation function (max function in this case) I need to cast the variable type of the column "total_deaths"
--otherwise, this statement will return incorrect numbers

SELECT location, max(cast(total_deaths AS INT)) AS latest_total_deaths, max(total_cases) AS latest_total_cases, (max(cast(total_deaths AS INT))/max(total_cases))*100 AS death_percentage
FROM PortfolioProject_COVID..CovidDeaths
--WHERE location = 'United States'  /* put in in the query to see only United States */
GROUP BY location
ORDER BY death_percentage desc


--find the total_death per continent 
--if you look at the data, there is a column named "continent" and a column named "location", which is the country 
--in a closer look, when "location" is a continent name (e.g., Asia), "continent" is NULL

SELECT location, max(cast(total_deaths AS INT)) AS NumberOfDeaths
FROM PortfolioProject_COVID..CovidDeaths
WHERE continent IS NULL
GROUP BY location 
ORDER BY NumberOfDeaths desc

--in the previouw output, there are rows that are not continents (e.g., World, European Union, etc.)
--to remove the unwanted locations (i.e., World, European Union, Intenational) from the previous output 

SELECT continent, location, max(cast(total_deaths AS INT)) AS NumberOfDeaths
		FROM PortfolioProject_COVID..CovidDeaths
		WHERE continent IS NULL AND location != 'World' AND location !='European Union' AND location != 'International'
		GROUP BY continent, location
			

--here is another way to get the death count per continent without having the unwanted locations in the output	
--this is because the sume of all new_deaths is equal to the latest total_death for each continent 

SELECT continent, SUM(cast(new_deaths AS INT)) AS NumberOfDeaths
FROM PortfolioProject_COVID..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY continent


--find the overall death percentage per total cases globally

SELECT SUM(new_cases) AS global_total_cases, SUM(cast(new_deaths AS INT)) AS global_total_deaths, SUM(cast(new_deaths AS INT))/SUM(new_cases)*100 AS GlobalDeathPercentage
FROM PortfolioProject_COVID..CovidDeaths


----now start looking at the vaccnination file and geting some info from it

SELECT *
FROM PortfolioProject_COVID..CovidVaccinations



--find the total number of people who were vaccinated across the world
--throughout this project, the assumption is that the number in the column of "new_vaccinations" is the number of new people who got vaccinated (regarless of the required doses for full vaccination)
--new_vaccinations should be converted (or cast) as intiger

SELECT SUM(CONVERT(INT, new_vaccinations))
FROM PortfolioProject_COVID..CovidVaccinations
WHERE continent IS NOT NULL


--now join the death and the vaccination tables

SELECT *
FROM PortfolioProject_COVID..CovidDeaths death
JOIN PortfolioProject_COVID..CovidVaccinations vac
ON death.location = vac.location 
AND death.continent = vac.continent
AND death.date = vac.date


--find the total number of vaccinated people per day in each location
--for this, add up the numbers of people vaccinated in the preceding days up to each date

SELECT death.location, death.date, vac.new_vaccinations,
SUM(CAST(vac.new_vaccinations AS int)) OVER (PARTITION BY death.location ORDER BY death.location, death.date) AS RollingTotalVaccination
FROM PortfolioProject_COVID..CovidDeaths death
JOIN PortfolioProject_COVID..CovidVaccinations vac
ON death.location = vac.location
AND death.continent = vac.continent
AND death.date = vac.date
WHERE death.continent IS NOT NULL


--find the total percentage of vaccinated people in each country (i.e. location)
--for this, need to use CTE (or temptable or subquery)
--I first use a CTE and then in the next queries, I use a subquery and then a temptable to get the same result in different ways

/* CTE */
WITH CTE_global AS
(
SELECT death.location, death.date, vac.new_vaccinations, death.population,
SUM(CAST(vac.new_vaccinations AS int)) OVER (PARTITION BY death.location ORDER BY death.location, death.date) AS RollingTotalVaccination
FROM PortfolioProject_COVID..CovidDeaths death
JOIN PortfolioProject_COVID..CovidVaccinations vac
ON death.location = vac.location
AND death.continent = vac.continent
AND death.date = vac.date
WHERE death.continent IS NOT NULL
)
SELECT location, population, MAX(RollingTotalVaccination) AS LatestTotalVaccination,
MAX(RollingTotalVaccination)/population*100 AS PercentageOfTotalVaccnivation
FROM CTE_global
Group BY location, population
ORDER BY location


/* Subquery */
SELECT subtable_global.location, subtable_global.population, 
MAX(subtable_global.RollingTotalVaccination) AS LatestTotalVaccination,
MAX(subtable_global.RollingTotalVaccination)/subtable_global.population*100 AS PercentageOfTotalVaccnivation
FROM 
(
SELECT death.location, death.date, vac.new_vaccinations, death.population,
SUM(CAST(vac.new_vaccinations AS int)) OVER (PARTITION BY death.location ORDER BY death.location, death.date) AS RollingTotalVaccination
FROM PortfolioProject_COVID..CovidDeaths death
JOIN PortfolioProject_COVID..CovidVaccinations vac
ON death.location = vac.location
AND death.continent = vac.continent
AND death.date = vac.date
WHERE death.continent IS NOT NULL
) AS subtable_global
GROUP BY subtable_global.location, subtable_global.population
ORDER BY location


/* Temptable */
DROP TABLE IF EXISTS #temptable_global
CREATE TABLE #temptable_global
(
location nvarchar(250), 
date datetime, 
new_vaccination numeric, 
population numeric, 
RollingTotalVaccination numeric
)


INSERT INTO #temptable_global
SELECT death.location, death.date, vac.new_vaccinations, death.population,
SUM(CAST(vac.new_vaccinations AS int)) OVER (PARTITION BY death.location ORDER BY death.location, death.date) AS RollingTotalVaccination
FROM PortfolioProject_COVID..CovidDeaths death
JOIN PortfolioProject_COVID..CovidVaccinations vac
ON death.location = vac.location
AND death.continent = vac.continent
AND death.date = vac.date
WHERE death.continent IS NOT NULL


SELECT location, population, MAX(RollingTotalVaccination) AS LatestTotalVaccination,
MAX(RollingTotalVaccination)/population*100 AS PercentageOfTotalVaccnivation
FROM #temptable_global
Group BY location, population
ORDER BY location


--finally make VIEW of some information that I already queried
--these virtual tables can be used later on for visualization and other purposes

/* VIEW for the  total percentage of vaccinated people in each country */

CREATE VIEW VaccinationPercentPerCountry AS
WITH CTE_global AS
(
SELECT death.location, death.date, vac.new_vaccinations, death.population,
SUM(CAST(vac.new_vaccinations AS int)) OVER (PARTITION BY death.location ORDER BY death.location, death.date) AS RollingTotalVaccination
FROM PortfolioProject_COVID..CovidDeaths death
JOIN PortfolioProject_COVID..CovidVaccinations vac
ON death.location = vac.location
AND death.continent = vac.continent
AND death.date = vac.date
WHERE death.continent IS NOT NULL
)
SELECT location, population, MAX(RollingTotalVaccination) AS LatestTotalVaccination,
MAX(RollingTotalVaccination)/population*100 AS PercentageOfTotalVaccnivation
FROM CTE_global
Group BY location, population


/* VIEW for the  total infection rate (i.e., percentage of population that got infected) in each country */

CREATE VIEW InfectionRatePerCountry AS
SELECT location, population, max(total_cases) AS latest_total_cases, (max(total_cases)/population)*100 AS infection_percentage
FROM PortfolioProject_COVID..CovidDeaths
GROUP BY location, population








