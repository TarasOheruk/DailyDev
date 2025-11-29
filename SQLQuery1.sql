CREATE DATABASE CabTripsDb;
GO

USE CabTripsDb;
GO

CREATE TABLE dbo.Trips
(
    Id BIGINT IDENTITY(1,1) CONSTRAINT PK_Trips PRIMARY KEY,

    tpep_pickup_datetime   DATETIME2(0) NOT NULL, 
    tpep_dropoff_datetime  DATETIME2(0) NOT NULL,  

    passenger_count        TINYINT      NOT NULL,
    trip_distance          DECIMAL(9,2) NOT NULL,

    store_and_fwd_flag     VARCHAR(3)   NOT NULL,  -- 'Yes' / 'No'

    PULocationID           INT          NOT NULL,
    DOLocationID           INT          NOT NULL,

    fare_amount            DECIMAL(10,2) NOT NULL,
    tip_amount             DECIMAL(10,2) NOT NULL,

    trip_duration_minutes AS DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) PERSISTED
);
GO


CREATE NONCLUSTERED INDEX IX_Trips_PULocation_Tip
    ON dbo.Trips (PULocationID)
    INCLUDE (tip_amount);
GO

CREATE NONCLUSTERED INDEX IX_Trips_TripDistance
    ON dbo.Trips (trip_distance DESC);
GO

CREATE NONCLUSTERED INDEX IX_Trips_Duration
    ON dbo.Trips (trip_duration_minutes DESC);
GO

CREATE NONCLUSTERED INDEX IX_Trips_PULocation
    ON dbo.Trips (PULocationID);
GO
