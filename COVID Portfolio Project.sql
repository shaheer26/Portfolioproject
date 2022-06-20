--- CREATING TABLE FOR CovidDeaths

CREATE TABLE CovidDeaths (
	iso_code varchar,
	continent varchar,
	location varchar,
	date date,
	population numeric,
	total_cases numeric,
	new_cases numeric,
	new_cases_smoothed numeric,
	total_deaths numeric,
	new_deaths numeric,
	new_deaths_smoothed numeric,
	total_cases_per_million numeric,
	new_cases_per_million numeric,
	new_cases_smoothed_per_million numeric,
	total_deaths_per_million numeric,
	new_deaths_per_million numeric,
	new_deaths_smoothed_per_million numeric,
	reproduction_rate numeric,
	icu_patients numeric,
	icu_patients_per_million numeric,
	hosp_patients numeric,
	hosp_patients_per_million numeric,
	weekly_icu_admissions numeric,
	weekly_icu_admissions_per_million numeric,
	weekly_hosp_admissions numeric,
	weekly_hosp_admissions_per_million numeric
);

---COPY THE DATA FROM THE CSV FILE CovidDeaths

COPY CovidDeaths FROM
'C:\Program Files\PostgreSQL\14\data\Data\CovidDeaths.csv' CSV HEADER;


SELECT * FROM CovidDeaths;

---CREATING TABLE FOR CovidVaccination

CREATE TABLE CovidVaccination (
	iso_code varchar,
	continent varchar,
	location varchar,
	date date,
	new_tests numeric,
	total_tests numeric,
	total_tests_per_thousand numeric,
	new_tests_per_thousand numeric,
	new_tests_smoothed numeric,
	new_tests_smoothed_per_thousand numeric,
	positive_rate numeric,
	tests_per_case numeric,
	tests_units varchar,
	total_vaccinations numeric,
	people_vaccinated numeric,
	people_fully_vaccinated numeric,
	total_boosters numeric,
	new_vaccinations numeric,
	new_vaccinations_smoothed numeric,
	total_vaccinations_per_hundred numeric,
	people_vaccinated_per_hundred numeric,
	people_fully_vaccinated_per_hundred numeric,
	total_boosters_per_hundred numeric,
	new_vaccinations_smoothed_per_million numeric,
	new_people_vaccinated_smoothed numeric,
	new_people_vaccinated_smoothed_per_hundred numeric,
	stringency_index numeric,
	population_density numeric,
	median_age numeric,
	aged_65_older numeric,
	aged_70_older numeric,
	gdp_per_capita numeric,
	extreme_poverty numeric,
	cardiovasc_death_rate numeric,
	diabetes_prevalence numeric,
	female_smokers numeric,
	male_smokers numeric,
	handwashing_facilities numeric,
	hospital_beds_per_thousand numeric,
	life_expectancy numeric,
	human_development_index numeric,
	excess_mortality_cumulative_absolute numeric,
	excess_mortality_cumulative numeric,
	excess_mortality numeric,
	excess_mortality_cumulative_per_million numeric
);

---COPY THE DATA FROM CSV FILE CovidVaccination

COPY CovidVaccination FROM
'C:\Program Files\PostgreSQL\14\data\Data\CovidVaccinations.csv' CSV HEADER;

SELECT * FROM CovidVaccination WHERE continent;

SELECT location, date,
	total_cases, new_cases, total_deaths,
	population
FROM CovidDeaths
ORDER BY location,date;

--- SUMMARIZING CovidDeaths TABLE WITH RELEVANT COLUMNS

SELECT location, date,
	total_cases, new_cases, total_deaths,
	population
FROM CovidDeaths
ORDER BY location,date;

--- ANALYZING TOTAL DEATHS PER TOTAL CASES WITH RESPECT TO LOCATION AND DATE

SELECT location, date,
	total_cases, total_deaths,
	(total_deaths/total_cases)
FROM CovidDeaths
ORDER BY location,date;

---- ANALYZING TOTAL CASES PER POPULATION (Percentage of population infected with COVID) WITH RESPECT TO LOCATION AND DATE

SELECT location, date, Population, total_cases,
(total_cases/Population)*100 AS CovidPercentage
FROM CovidDeaths
ORDER BY location, date;

--- ANALYZING MAXIMUM INFECTION RATE IN EACH COUNTRY
--- MAXIMUM INFECTION RATE = MAXIMUM TOTAL CASES / POPULATION * 100%

SELECT location, Population, 
MAX(total_cases) AS Highest_Infection_Count,
MAX((total_cases/population))*100 AS Highest_Infection_Rate
FROM CovidDeaths
GROUP BY location, Population
ORDER BY Highest_Infection_Rate DESC;

--- ANALYZING COUNTRIES WITH HIGHEST DEATH COUNT PER POPULATION

SELECT location, 
MAX(cast(total_deaths as int)) AS TotalDeathCount
FROM CovidDeaths
GROUP BY location
ORDER BY TotalDeathCount DESC;

--- HOWEVER, SOME CONTINENT NAMES ARE PUT INTO THE location COLUMN,
--- TO MAKE THE DATA MORE ACCURATE, WE HAVE TO DENY THE null INPUT IN THE continent COLUMN:

SELECT location, 
MAX(cast(total_deaths as int)) AS TotalDeathCount
FROM CovidDeaths
WHERE continent is not null
GROUP BY location
ORDER BY TotalDeathCount DESC;

--- ANALYZING TOTAL DEATHS WITH RESPECT TO EACH CONTINENT

SELECT continent,
	MAX(cast(Total_deaths AS int)) AS TotalDeathCount
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY TotalDeathCount DESC; 

--- ANALYZING TOTAL DEATHS AND DEATH PERCENTAGE WITH RESPECT TO EACH CONTINENT

SELECT date, SUM(new_cases) AS Total_cases, SUM(new_deaths),
SUM(new_deaths)/SUM(new_cases) AS DeathPercentage
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY 1,2;

--- JOINING TABLES CovidDeaths AND CovidVaccination GROUPED ACCORDING TO ITS LOCATION AND DATE

SELECT *
	FROM CovidDeaths AS dea
JOIN CovidVaccination AS vac
ON dea.location=vac.location AND dea.date=vac.date;

--- ANALYZING ACCUMULATED VACCINATIONS WITH RESPECT TO EACH LOCATION AND DATE
 
SELECT dea.continent, dea.location, dea.date,
	dea.population, vac.new_vaccinations, 
	(SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date))
	AS AccumulatedVaccinations
FROM CovidDeaths AS dea
JOIN CovidVaccination AS vac
ON dea.location=vac.location
AND dea.date=vac.date
WHERE dea.continent IS NOT NULL
ORDER BY 2,3; 

--- CREATING CTE TO FIND ACCUMULATED VACCINATION RATE WITH RESPECT TO LOCATION AND DATE

WITH PopvsVac (continent, location, date, population, new_vaccinations, AccumulatedVaccinations)
AS
(SELECT dea.continent, dea.location, dea.date,
	dea.population, vac.new_vaccinations, 
	SUM(new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location) AS AccumulatedVaccinations
FROM CovidDeaths AS dea
JOIN CovidVaccination AS vac
ON dea.location=vac.location
AND dea.date=vac.date
WHERE dea.continent IS NOT NULL
)
SELECT *, (AccumulatedVaccinations/population)*100 As AccumulatedVaccinationRate FROM PopvsVac;

--- CREATING TABLE ON PERCENTAGE OF POPULATION VACCINATED

CREATE TABLE PercentPopulationVaccinated
(
continent VARCHAR(255),
location VARCHAR(255),
date date,
population numeric,
new_vaccinations numeric,
AccumulatedPeopleVaccinated numeric);

INSERT INTO PercentPopulationVaccinated
SELECT dea.continent, dea.location, dea.date,
	dea.population, vac.new_vaccinations, 
	SUM(new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location) AS AccumulatedVaccinations
FROM CovidDeaths AS dea
JOIN CovidVaccination AS vac
ON dea.location=vac.location
AND dea.date=vac.date

SELECT *, (accumulatedpeoplevaccinated/population)*100 AS Vaccination_per_population
FROM PercentPopulationVaccinated;
















