SELECT
    rio_cuarto_interamericana.univiersities,
    rio_cuarto_interamericana.carrera,
    rio_cuarto_interamericana.inscription_dates,
    rio_cuarto_interamericana.names,
    rio_cuarto_interamericana.sexo,
    rio_cuarto_interamericana.fechas_nacimiento,
    rio_cuarto_interamericana.direcciones,
    rio_cuarto_interamericana.localidad,
    rio_cuarto_interamericana.email
FROM public.rio_cuarto_interamericana
WHERE
    (univiersities = '-universidad-abierta-interamericana')
    AND
    (TO_DATE(inscription_dates, 'DD/Mon/YY')
    BETWEEN
    TO_DATE('01/Sep/20', 'DD/Mon/YY')
    AND
    TO_DATE('01/Feb/21', 'DD/Mon/YY'));