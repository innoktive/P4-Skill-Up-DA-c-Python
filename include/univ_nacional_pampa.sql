SELECT
    moron_nacional_pampa.universidad,
    moron_nacional_pampa.carrerra,
    moron_nacional_pampa.fechaiscripccion,
    moron_nacional_pampa.nombrre,
    moron_nacional_pampa.sexo,
    moron_nacional_pampa.nacimiento,
    moron_nacional_pampa.codgoposstal,
    moron_nacional_pampa.direccion,
    moron_nacional_pampa.eemail
FROM public.moron_nacional_pampa
WHERE
    (universidad = 'Universidad nacional de la pampa')
    AND
    (TO_DATE(fechaiscripccion, 'DD/MM/YYYY')
    BETWEEN
    TO_DATE('01/09/2020', 'DD/MM/YYYY')
    AND
    TO_DATE('01/02/2021', 'DD/MM/YYYY'));
