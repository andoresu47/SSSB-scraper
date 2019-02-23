/* ---------------------------------------------------------------------- */
/* Target DBMS:           PostgreSQL 10                                   */
/* Project file:          sssb_schema.sql                                 */
/* Project name:          sssb_data                                       */
/* Author:                Andrés López Martínez                           */
/*                        anlm@kth.se                                     */
/* Script type:           Database creation script                        */
/* Created on:            2018-02-18 15:34                                */
/* Updated on:            2018-02-21 18:34                                */
/* ---------------------------------------------------------------------- */

SET timezone = 'Europe/Stockholm';

CREATE TABLE Apartment(
    nIdApartment                  SERIAL                                        ,
    name                          VARCHAR(256)                                  ,
    type                          VARCHAR(256)            NOT NULL              ,
    zone                          VARCHAR(256)            NOT NULL              ,
    price                         DECIMAL                 NOT NULL              ,
    furnitured                    BOOLEAN           DEFAULT         FALSE       ,
    electricity                   BOOLEAN           DEFAULT         FALSE       ,
    _10_month                     BOOLEAN           DEFAULT         FALSE       ,
    CONSTRAINT pk_apartment       PRIMARY KEY (nIdApartment)                    ,
    CONSTRAINT unq_apartment      UNIQUE (name)
);

CREATE TABLE Offer(
    nIdOffer                            SERIAL                                      ,
    start_date                          DATE                                        ,
    end_date                            DATE                                        ,
    CONSTRAINT pk_offer                 PRIMARY KEY (nIdOffer)                      ,
    CONSTRAINT unq_offer_dates          UNIQUE (start_date, end_date)
);

CREATE TABLE State(
    nIdState                            SERIAL                                        ,
    time_stamp                          TIMESTAMPTZ                                   ,
    nIdApartment                        INT                                           ,
    nIdOffer                            INT                                           ,
    no_applicants                       INT                   NOT NULL                ,
    top_credits                         INT                   NOT NULL                ,
    CONSTRAINT pk_state                 PRIMARY KEY (nIdState)                        ,
    CONSTRAINT fk_state_nIdApartment    FOREIGN KEY (nIdApartment)      REFERENCES Apartment(nIdApartment)     ,
    CONSTRAINT fk_state_nIdOffer        FOREIGN KEY (nIdOffer)      REFERENCES Offer(nIdOffer)                     ,
    CONSTRAINT unq_state                UNIQUE      (nIdApartment, time_stamp)
);

CREATE TABLE IsOffered(
    nIdApartment                        INT                                         ,
    nIdOffer                            INT                                         ,
    CONSTRAINT fk_offer_nIdApartment    FOREIGN KEY (nIdApartment)      REFERENCES Apartment(nIdApartment)     ,
    CONSTRAINT fk_offer_nIdOffer        FOREIGN KEY (nIdOffer)          REFERENCES Offer(nIdOffer)             ,
    CONSTRAINT unq_offer_apartment      UNIQUE      (nIdApartment, nIdOffer)
);
