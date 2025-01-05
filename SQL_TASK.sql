CREATE DATABASE EDA_Database;

USE EDA_Database;

create table table_1(
Date1 int
);

create table table_2(
Date1 int
);

USE EDA_Database;

SELECT * FROM data_set;

SELECT
    AVG(Coal_RB_4800_FOB_London_Close_USD) AS avg_Coal_RB_4800_FOB_London_Close_USD,
    AVG(Coal_RB_5500_FOB_London_Close_USD) AS avg_Coal_RB_5500_FOB_London_Close_USD,
    AVG(Coal_RB_6000_FOB_CurrentWeek_Avg_USD) AS avg_Coal_RB_6000_FOB_CurrentWeek_Avg_USD,
    AVG(Coal_India_5500_CFR_London_Close_USD) AS avg_Coal_India_5500_CFR_London_Close_USD,
    AVG(Price_WTI) AS avg_Price_WTI,
    AVG(Price_Brent_Oil) AS avg_Price_Brent_Oil,
    AVG(Price_Dubai_Brent_Oil) AS avg_Price_Dubai_Brent_Oil,
    AVG(Price_ExxonMobil) AS avg_Price_ExxonMobil,
    AVG(Price_Shenhua) AS avg_Price_Shenhua,
    AVG(Price_All_Share) AS avg_Price_All_Share,
    AVG(Price_Mining) AS avg_Price_Mining,
    AVG(Price_LNG_Japan_Korea_Marker_PLATTS) AS avg_Price_LNG_Japan_Korea_Marker_PLATTS,
    AVG(Price_ZAR_USD) AS avg_Price_ZAR_USD,
    AVG(Price_Natural_Gas) AS avg_Price_Natural_Gas,
    AVG(Price_ICE) AS avg_Price_ICE,
    AVG(Price_Dutch_TTF) AS avg_Price_Dutch_TTF
FROM data_set;


WITH OrderedData AS (
    SELECT 
        Coal_RB_4800_FOB_London_Close_USD,
        ROW_NUMBER() OVER (ORDER BY Coal_RB_4800_FOB_London_Close_USD) AS RowNum4800,
        COUNT(*) OVER () AS TotalRows4800,
        Coal_RB_5500_FOB_London_Close_USD,
        ROW_NUMBER() OVER (ORDER BY Coal_RB_5500_FOB_London_Close_USD) AS RowNum5500,
        COUNT(*) OVER () AS TotalRows5500,
        Coal_RB_6000_FOB_CurrentWeek_Avg_USD,
        ROW_NUMBER() OVER (ORDER BY Coal_RB_6000_FOB_CurrentWeek_Avg_USD) AS RowNum6000,
        COUNT(*) OVER () AS TotalRows6000,
        Coal_India_5500_CFR_London_Close_USD,
        ROW_NUMBER() OVER (ORDER BY Coal_India_5500_CFR_London_Close_USD) AS RowNumIndia5500,
        COUNT(*) OVER () AS TotalRowsIndia5500,
        Price_WTI,
        ROW_NUMBER() OVER (ORDER BY Price_WTI) AS RowNumWTI,
        COUNT(*) OVER () AS TotalRowsWTI,
        Price_Brent_Oil,
        ROW_NUMBER() OVER (ORDER BY Price_Brent_Oil) AS RowNumBrent,
        COUNT(*) OVER () AS TotalRowsBrent,
        Price_Dubai_Brent_Oil,
        ROW_NUMBER() OVER (ORDER BY Price_Dubai_Brent_Oil) AS RowNumDubai,
        COUNT(*) OVER () AS TotalRowsDubai,
        Price_ExxonMobil,
        ROW_NUMBER() OVER (ORDER BY Price_ExxonMobil) AS RowNumExxon,
        COUNT(*) OVER () AS TotalRowsExxon,
        Price_Shenhua,
        ROW_NUMBER() OVER (ORDER BY Price_Shenhua) AS RowNumShenhua,
        COUNT(*) OVER () AS TotalRowsShenhua,
        Price_All_Share,
        ROW_NUMBER() OVER (ORDER BY Price_All_Share) AS RowNumAllShare,
        COUNT(*) OVER () AS TotalRowsAllShare,
        Price_Mining,
        ROW_NUMBER() OVER (ORDER BY Price_Mining) AS RowNumMining,
        COUNT(*) OVER () AS TotalRowsMining,
        Price_LNG_Japan_Korea_Marker_PLATTS,
        ROW_NUMBER() OVER (ORDER BY Price_LNG_Japan_Korea_Marker_PLATTS) AS RowNumLNG,
        COUNT(*) OVER () AS TotalRowsLNG,
        Price_ZAR_USD,
        ROW_NUMBER() OVER (ORDER BY Price_ZAR_USD) AS RowNumZAR,
        COUNT(*) OVER () AS TotalRowsZAR,
        Price_Natural_Gas,
        ROW_NUMBER() OVER (ORDER BY Price_Natural_Gas) AS RowNumGas,
        COUNT(*) OVER () AS TotalRowsGas,
        Price_ICE,
        ROW_NUMBER() OVER (ORDER BY Price_ICE) AS RowNumICE,
        COUNT(*) OVER () AS TotalRowsICE,
        Price_Dutch_TTF,
        ROW_NUMBER() OVER (ORDER BY Price_Dutch_TTF) AS RowNumDutch,
        COUNT(*) OVER () AS TotalRowsDutch
    FROM data_set
)
SELECT 
     -- Median for each column
    (SELECT AVG(Coal_RB_4800_FOB_London_Close_USD) FROM OrderedData WHERE RowNum4800 IN (FLOOR((TotalRows4800 + 1) / 2), CEIL((TotalRows4800 + 1) / 2))) AS Median_Coal_RB_4800_FOB_London_Close_USD,
    (SELECT AVG(Coal_RB_5500_FOB_London_Close_USD) FROM OrderedData WHERE RowNum5500 IN (FLOOR((TotalRows5500 + 1) / 2), CEIL((TotalRows5500 + 1) / 2))) AS Median_Coal_RB_5500_FOB_London_Close_USD,
    (SELECT AVG(Coal_RB_6000_FOB_CurrentWeek_Avg_USD) FROM OrderedData WHERE RowNum6000 IN (FLOOR((TotalRows6000 + 1) / 2), CEIL((TotalRows6000 + 1) / 2))) AS Median_Coal_RB_6000_FOB_CurrentWeek_Avg_USD,
    (SELECT AVG(Coal_India_5500_CFR_London_Close_USD) FROM OrderedData WHERE RowNumIndia5500 IN (FLOOR((TotalRowsIndia5500 + 1) / 2), CEIL((TotalRowsIndia5500 + 1) / 2))) AS Median_Coal_India_5500_CFR_London_Close_USD,
    (SELECT AVG(Price_WTI) FROM OrderedData WHERE RowNumWTI IN (FLOOR((TotalRowsWTI + 1) / 2), CEIL((TotalRowsWTI + 1) / 2))) AS Median_Price_WTI,
    (SELECT AVG(Price_Brent_Oil) FROM OrderedData WHERE RowNumBrent IN (FLOOR((TotalRowsBrent + 1) / 2), CEIL((TotalRowsBrent + 1) / 2))) AS Median_Price_Brent_Oil,
    (SELECT AVG(Price_Dubai_Brent_Oil) FROM OrderedData WHERE RowNumDubai IN (FLOOR((TotalRowsDubai + 1) / 2), CEIL((TotalRowsDubai + 1) / 2))) AS Median_Price_Dubai_Brent_Oil,
    (SELECT AVG(Price_ExxonMobil) FROM OrderedData WHERE RowNumExxon IN (FLOOR((TotalRowsExxon + 1) / 2), CEIL((TotalRowsExxon + 1) / 2))) AS Median_Price_ExxonMobil,
    (SELECT AVG(Price_Shenhua) FROM OrderedData WHERE RowNumShenhua IN (FLOOR((TotalRowsShenhua + 1) / 2), CEIL((TotalRowsShenhua + 1) / 2))) AS Median_Price_Shenhua,
    (SELECT AVG(Price_All_Share) FROM OrderedData WHERE RowNumAllShare IN (FLOOR((TotalRowsAllShare + 1) / 2), CEIL((TotalRowsAllShare + 1) / 2))) AS Median_Price_All_Share,
    (SELECT AVG(Price_Mining) FROM OrderedData WHERE RowNumMining IN (FLOOR((TotalRowsMining + 1) / 2), CEIL((TotalRowsMining + 1) / 2))) AS Median_Price_Mining,
    (SELECT AVG(Price_LNG_Japan_Korea_Marker_PLATTS) FROM OrderedData WHERE RowNumLNG IN (FLOOR((TotalRowsLNG + 1) / 2), CEIL((TotalRowsLNG + 1) / 2))) AS Median_Price_LNG_Japan_Korea_Marker_PLATTS,
    (SELECT AVG(Price_ZAR_USD) FROM OrderedData WHERE RowNumZAR IN (FLOOR((TotalRowsZAR + 1) / 2), CEIL((TotalRowsZAR + 1) / 2))) AS Median_Price_ZAR_USD,
    (SELECT AVG(Price_Natural_Gas) FROM OrderedData WHERE RowNumGas IN (FLOOR((TotalRowsGas + 1) / 2), CEIL((TotalRowsGas + 1) / 2))) AS Median_Price_Natural_Gas,
    (SELECT AVG(Price_ICE) FROM OrderedData WHERE RowNumICE IN (FLOOR((TotalRowsICE + 1) / 2), CEIL((TotalRowsICE + 1) / 2))) AS Median_Price_ICE,
    (SELECT AVG(Price_Dutch_TTF) FROM OrderedData WHERE RowNumDutch IN (FLOOR((TotalRowsDutch + 1) / 2), CEIL((TotalRowsDutch + 1) / 2))) AS Median_Price_Dutch_TTF;


SELECT 
    (SELECT Coal_RB_4800_FOB_London_Close_USD FROM data1 GROUP BY Coal_RB_4800_FOB_London_Close_USD ORDER BY COUNT(*) DESC LIMIT 1) AS Mode_Coal_RB_4800_FOB_London_Close_USD,
    (SELECT Coal_RB_5500_FOB_London_Close_USD FROM data1 GROUP BY Coal_RB_5500_FOB_London_Close_USD ORDER BY COUNT(*) DESC LIMIT 1) AS Mode_Coal_RB_5500_FOB_London_Close_USD,
    (SELECT Coal_RB_6000_FOB_CurrentWeek_Avg_USD FROM data1 GROUP BY Coal_RB_6000_FOB_CurrentWeek_Avg_USD ORDER BY COUNT(*) DESC LIMIT 1) AS Mode_Coal_RB_6000_FOB_CurrentWeek_Avg_USD,
    (SELECT Coal_India_5500_CFR_London_Close_USD FROM data1 GROUP BY Coal_India_5500_CFR_London_Close_USD ORDER BY COUNT(*) DESC LIMIT 1) AS Mode_Coal_India_5500_CFR_London_Close_USD,
    (SELECT Price_WTI FROM data1 GROUP BY Price_WTI ORDER BY COUNT(*) DESC LIMIT 1) AS Mode_Price_WTI,
    (SELECT Price_Brent_Oil FROM data1 GROUP BY Price_Brent_Oil ORDER BY COUNT(*) DESC LIMIT 1) AS Mode_Price_Brent_Oil,
    (SELECT Price_Dubai_Brent_Oil FROM data1 GROUP BY Price_Dubai_Brent_Oil ORDER BY COUNT(*) DESC LIMIT 1) AS Mode_Price_Dubai_Brent_Oil,
    (SELECT Price_ExxonMobil FROM data1 GROUP BY Price_ExxonMobil ORDER BY COUNT(*) DESC LIMIT 1) AS Mode_Price_ExxonMobil,
    (SELECT Price_Shenhua FROM data1 GROUP BY Price_Shenhua ORDER BY COUNT(*) DESC LIMIT 1) AS Mode_Price_Shenhua,
    (SELECT Price_All_Share FROM data1 GROUP BY Price_All_Share ORDER BY COUNT(*) DESC LIMIT 1) AS Mode_Price_All_Share,
    (SELECT Price_Mining FROM data1 GROUP BY Price_Mining ORDER BY COUNT(*) DESC LIMIT 1) AS Mode_Price_Mining,
    (SELECT Price_LNG_Japan_Korea_Marker_PLATTS FROM data1 GROUP BY Price_LNG_Japan_Korea_Marker_PLATTS ORDER BY COUNT(*) DESC LIMIT 1) AS Mode_Price_LNG_Japan_Korea_Marker_PLATTS,
    (SELECT Price_ZAR_USD FROM data1 GROUP BY Price_ZAR_USD ORDER BY COUNT(*) DESC LIMIT 1) AS Mode_Price_ZAR_USD,
    (SELECT Price_Natural_Gas FROM data1 GROUP BY Price_Natural_Gas ORDER BY COUNT(*) DESC LIMIT 1) AS Mode_Price_Natural_Gas,
    (SELECT Price_ICE FROM data1 GROUP BY Price_ICE ORDER BY COUNT(*) DESC LIMIT 1) AS Mode_Price_ICE,
    (SELECT Price_Dutch_TTF FROM data1 GROUP BY Price_Dutch_TTF ORDER BY COUNT(*) DESC LIMIT 1) AS Mode_Price_Dutch_TTF;


SELECT 
    VAR_POP(Coal_RB_4800_FOB_London_Close_USD) AS Variance_Coal_RB_4800_FOB_London_Close_USD,
    VAR_POP(Coal_RB_5500_FOB_London_Close_USD) AS Variance_Coal_RB_5500_FOB_London_Close_USD,
    VAR_POP(Coal_RB_6000_FOB_CurrentWeek_Avg_USD) AS Variance_Coal_RB_6000_FOB_CurrentWeek_Avg_USD,
    VAR_POP(Coal_India_5500_CFR_London_Close_USD) AS Variance_Coal_India_5500_CFR_London_Close_USD,
    VAR_POP(Price_WTI) AS Variance_Price_WTI,
    VAR_POP(Price_Brent_Oil) AS Variance_Price_Brent_Oil,
    VAR_POP(Price_Dubai_Brent_Oil) AS Variance_Price_Dubai_Brent_Oil,
    VAR_POP(Price_ExxonMobil) AS Variance_Price_ExxonMobil,
    VAR_POP(Price_Shenhua) AS Variance_Price_Shenhua,
    VAR_POP(Price_All_Share) AS Variance_Price_All_Share,
    VAR_POP(Price_Mining) AS Variance_Price_Mining,
    VAR_POP(Price_LNG_Japan_Korea_Marker_PLATTS) AS Variance_Price_LNG_Japan_Korea_Marker_PLATTS,
    VAR_POP(Price_ZAR_USD) AS Variance_Price_ZAR_USD,
    VAR_POP(Price_Natural_Gas) AS Variance_Price_Natural_Gas,
    VAR_POP(Price_ICE) AS Variance_Price_ICE,
    VAR_POP(Price_Dutch_TTF) AS Variance_Price_Dutch_TTF
FROM data_set;


-- Standard Deviation for all columns
SELECT 
    STDDEV_POP(Coal_RB_4800_FOB_London_Close_USD) AS StdDev_Coal_RB_4800_FOB_London_Close_USD,
    STDDEV_POP(Coal_RB_5500_FOB_London_Close_USD) AS StdDev_Coal_RB_5500_FOB_London_Close_USD,
    STDDEV_POP(Coal_RB_6000_FOB_CurrentWeek_Avg_USD) AS StdDev_Coal_RB_6000_FOB_CurrentWeek_Avg_USD,
    STDDEV_POP(Coal_India_5500_CFR_London_Close_USD) AS StdDev_Coal_India_5500_CFR_London_Close_USD,
    STDDEV_POP(Price_WTI) AS StdDev_Price_WTI,
    STDDEV_POP(Price_Brent_Oil) AS StdDev_Price_Brent_Oil,
    STDDEV_POP(Price_Dubai_Brent_Oil) AS StdDev_Price_Dubai_Brent_Oil,
    STDDEV_POP(Price_ExxonMobil) AS StdDev_Price_ExxonMobil,
    STDDEV_POP(Price_Shenhua) AS StdDev_Price_Shenhua,
    STDDEV_POP(Price_All_Share) AS StdDev_Price_All_Share,
    STDDEV_POP(Price_Mining) AS StdDev_Price_Mining,
    STDDEV_POP(Price_LNG_Japan_Korea_Marker_PLATTS) AS StdDev_Price_LNG_Japan_Korea_Marker_PLATTS,
    STDDEV_POP(Price_ZAR_USD) AS StdDev_Price_ZAR_USD,
    STDDEV_POP(Price_Natural_Gas) AS StdDev_Price_Natural_Gas,
    STDDEV_POP(Price_ICE) AS StdDev_Price_ICE,
    STDDEV_POP(Price_Dutch_TTF) AS StdDev_Price_Dutch_TTF
FROM data_set;

USE eda_database;

-- Step 1: Calculate the mean (averages) for all columns
WITH Averages AS (
    SELECT
     AVG(Coal_RB_4800_FOB_London_Close_USD) AS avg_Coal_RB_4800_FOB_London_Close_USD,
        AVG(Coal_RB_5500_FOB_London_Close_USD) AS avg_Coal_RB_5500_FOB_London_Close_USD,
        AVG(Coal_RB_6000_FOB_CurrentWeek_Avg_USD) AS avg_Coal_RB_6000_FOB_CurrentWeek_Avg_USD,
        AVG(Coal_India_5500_CFR_London_Close_USD) AS avg_Coal_India_5500_CFR_London_Close_USD,
        AVG(Price_WTI) AS avg_Price_WTI,
        AVG(Price_Brent_Oil) AS avg_Price_Brent_Oil,
        AVG(Price_Dubai_Brent_Oil) AS avg_Price_Dubai_Brent_Oil,
        AVG(Price_ExxonMobil) AS avg_Price_ExxonMobil,
        AVG(Price_Shenhua) AS avg_Price_Shenhua,
        AVG(Price_All_Share) AS avg_Price_All_Share,
        AVG(Price_Mining) AS avg_Price_Mining,
        AVG(Price_LNG_Japan_Korea_Marker_PLATTS) AS avg_Price_LNG_Japan_Korea_Marker_PLATTS,
        AVG(Price_ZAR_USD) AS avg_Price_ZAR_USD,
        AVG(Price_Natural_Gas) AS avg_Price_Natural_Gas,
        AVG(Price_ICE) AS avg_Price_ICE,
        AVG(Price_Dutch_TTF) AS avg_Price_Dutch_TTF
    FROM data_set
),


Skewness AS (
    SELECT
        (COUNT(*) * SUM(POWER(Coal_RB_4800_FOB_London_Close_USD - Averages.avg_Coal_RB_4800_FOB_London_Close_USD, 3))) /
        ((COUNT(*) - 1) * (COUNT(*) - 2) * POWER(STDDEV_POP(Coal_RB_4800_FOB_London_Close_USD), 3)) AS Skewness_Coal_RB_4800_FOB_London_Close_USD,

        (COUNT(*) * SUM(POWER(Coal_RB_5500_FOB_London_Close_USD - Averages.avg_Coal_RB_5500_FOB_London_Close_USD, 3))) /
        ((COUNT(*) - 1) * (COUNT(*) - 2) * POWER(STDDEV_POP(Coal_RB_5500_FOB_London_Close_USD), 3)) AS Skewness_Coal_RB_5500_FOB_London_Close_USD,

        (COUNT(*) * SUM(POWER(Coal_RB_6000_FOB_CurrentWeek_Avg_USD - Averages.avg_Coal_RB_6000_FOB_CurrentWeek_Avg_USD, 3))) /
        ((COUNT(*) - 1) * (COUNT(*) - 2) * POWER(STDDEV_POP(Coal_RB_6000_FOB_CurrentWeek_Avg_USD), 3)) AS Skewness_Coal_RB_6000_FOB_CurrentWeek_Avg_USD,

        (COUNT(*) * SUM(POWER(Coal_India_5500_CFR_London_Close_USD - Averages.avg_Coal_India_5500_CFR_London_Close_USD, 3))) /
        ((COUNT(*) - 1) * (COUNT(*) - 2) * POWER(STDDEV_POP(Coal_India_5500_CFR_London_Close_USD), 3)) AS Skewness_Coal_India_5500_CFR_London_Close_USD,

        (COUNT(*) * SUM(POWER(Price_WTI - Averages.avg_Price_WTI, 3))) /
        ((COUNT(*) - 1) * (COUNT(*) - 2) * POWER(STDDEV_POP(Price_WTI), 3)) AS Skewness_Price_WTI,

        (COUNT(*) * SUM(POWER(Price_Brent_Oil - Averages.avg_Price_Brent_Oil, 3))) /
        ((COUNT(*) - 1) * (COUNT(*) - 2) * POWER(STDDEV_POP(Price_Brent_Oil), 3)) AS Skewness_Price_Brent_Oil,

        (COUNT(*) * SUM(POWER(Price_Dubai_Brent_Oil - Averages.avg_Price_Dubai_Brent_Oil, 3))) /
        ((COUNT(*) - 1) * (COUNT(*) - 2) * POWER(STDDEV_POP(Price_Dubai_Brent_Oil), 3)) AS Skewness_Price_Dubai_Brent_Oil,

        (COUNT(*) * SUM(POWER(Price_ExxonMobil - Averages.avg_Price_ExxonMobil, 3))) /
        ((COUNT(*) - 1) * (COUNT(*) - 2) * POWER(STDDEV_POP(Price_ExxonMobil), 3)) AS Skewness_Price_ExxonMobil,

        (COUNT(*) * SUM(POWER(Price_Shenhua - Averages.avg_Price_Shenhua, 3))) /
        ((COUNT(*) - 1) * (COUNT(*) - 2) * POWER(STDDEV_POP(Price_Shenhua), 3)) AS Skewness_Price_Shenhua,

        (COUNT(*) * SUM(POWER(Price_All_Share - Averages.avg_Price_All_Share, 3))) /
        ((COUNT(*) - 1) * (COUNT(*) - 2) * POWER(STDDEV_POP(Price_All_Share), 3)) AS Skewness_Price_All_Share,

        (COUNT(*) * SUM(POWER(Price_Mining - Averages.avg_Price_Mining, 3))) /
        ((COUNT(*) - 1) * (COUNT(*) - 2) * POWER(STDDEV_POP(Price_Mining), 3)) AS Skewness_Price_Mining,

        (COUNT(*) * SUM(POWER(Price_LNG_Japan_Korea_Marker_PLATTS - Averages.avg_Price_LNG_Japan_Korea_Marker_PLATTS, 3))) /
        ((COUNT(*) - 1) * (COUNT(*) - 2) * POWER(STDDEV_POP(Price_LNG_Japan_Korea_Marker_PLATTS), 3)) AS Skewness_Price_LNG_Japan_Korea_Marker_PLATTS,

        (COUNT(*) * SUM(POWER(Price_ZAR_USD - Averages.avg_Price_ZAR_USD, 3))) /
        ((COUNT(*) - 1) * (COUNT(*) - 2) * POWER(STDDEV_POP(Price_ZAR_USD), 3)) AS Skewness_Price_ZAR_USD,

        (COUNT(*) * SUM(POWER(Price_Natural_Gas - Averages.avg_Price_Natural_Gas, 3))) /
        ((COUNT(*) - 1) * (COUNT(*) - 2) * POWER(STDDEV_POP(Price_Natural_Gas), 3)) AS Skewness_Price_Natural_Gas,

        (COUNT(*) * SUM(POWER(Price_ICE - Averages.avg_Price_ICE, 3))) /
        ((COUNT(*) - 1) * (COUNT(*) - 2) * POWER(STDDEV_POP(Price_ICE), 3)) AS Skewness_Price_ICE,

        (COUNT(*) * SUM(POWER(Price_Dutch_TTF - Averages.avg_Price_Dutch_TTF, 3))) /
        ((COUNT(*) - 1) * (COUNT(*) - 2) * POWER(STDDEV_POP(Price_Dutch_TTF), 3)) AS Skewness_Price_Dutch_TTF

    FROM data_set
    CROSS JOIN Averages
)
SELECT * FROM Skewness;


