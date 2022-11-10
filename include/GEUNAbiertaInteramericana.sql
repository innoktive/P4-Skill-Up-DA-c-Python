SELECT
    rio_cuarto_interamericana.univiersities as "university",
    rio_cuarto_interamericana.carrera as "career",
    rio_cuarto_interamericana.inscription_dates as "inscription_date",
    rio_cuarto_interamericana.names as "name",
    rio_cuarto_interamericana.sexo as "gender",
    rio_cuarto_interamericana.fechas_nacimiento as "birth_date",
    rio_cuarto_interamericana.localidad as "location",
    rio_cuarto_interamericana.email as "email"
FROM public.rio_cuarto_interamericana
WHERE
    (univiersities = '-universidad-abierta-interamericana')
    AND
    (TO_DATE(inscription_dates, 'DD/Mon/YY')
    BETWEEN
    TO_DATE('01/Sep/20', 'DD/Mon/YY')
    AND
    TO_DATE('01/Feb/21', 'DD/Mon/YY'));
