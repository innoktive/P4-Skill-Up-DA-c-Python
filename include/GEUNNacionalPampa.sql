SELECT
    moron_nacional_pampa.universidad as "university",
    moron_nacional_pampa.carrerra as "career",
    moron_nacional_pampa.fechaiscripccion as "inscription_date",
    moron_nacional_pampa.nombrre as "name",
    moron_nacional_pampa.sexo as "gender",
    moron_nacional_pampa.nacimiento as "birth_date",
    moron_nacional_pampa.codgoposstal as "postal_code",
    moron_nacional_pampa.eemail as "email"
FROM public.moron_nacional_pampa
WHERE
    (universidad = 'Universidad nacional de la pampa')
    AND
    (TO_DATE(fechaiscripccion, 'DD/MM/YYYY')
    BETWEEN
    TO_DATE('01/09/2020', 'DD/MM/YYYY')
    AND
    TO_DATE('01/02/2021', 'DD/MM/YYYY'));
