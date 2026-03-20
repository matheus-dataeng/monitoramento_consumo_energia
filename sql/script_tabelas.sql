CREATE TABLE IF NOT EXISTS "dim_tempo" (
    "Id_tempo" SERIAL PRIMARY KEY,
    "Data" DATE NOT NULL,
    "Ano" INT NOT NULL,
    "Mes" INT NOT NULL,
    "Dia" INT NOT NULL,
    "Trimestre" INT NOT NULL,
    "Dia_semana" varchar(30) NOT NULL
);

CREATE TABLE IF NOT EXISTS "dim_subsistema" (
    "Id_subsistema" SERIAL PRIMARY KEY,
    "Sigla" VARCHAR(20) NOT NULL,
    "Subsistema" VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS "fato_carga_energia" (
    "Id_tempo" INT NOT NULL REFERENCES "dim_tempo"("Id_tempo"),
    "Id_subsistema" INT NOT NULL REFERENCES "dim_subsistema"("Id_subsistema"),
    "Carga_energia" FLOAT,
    "Data" DATE NOT NULL
);
