SELECT flores_comahue.universidad as UNIVERSITY, 
<<<<<<< HEAD
flores_comahue.carrera as CAREER, 
flores_comahue.fecha_de_inscripcion as INSCRIPTION_DATE, 
flores_comahue.name as LAST_NAME, 
flores_comahue.sexo as GENDER, 
flores_comahue.fecha_nacimiento as BIRTH_DATE, 
flores_comahue.codigo_postal as POSTAL_CODE, 
flores_comahue.direccion as LOCATION, 
flores_comahue.correo_electronico as EMAIL
FROM public.salvador_villa_maria
WHERE
    UNIVERSITY = 'UNIV. NACIONAL DEL COMAHUE'
    AND
    INSCRIPTION_DATE BETWEEN '20-09-01' AND '21-02-01';
=======
    flores_comahue.carrera as CAREER,
    flores_comahue.fecha_de_inscripcion as INSCRIPTION_DATE,
    flores_comahue.name as LAST_NAME,
    flores_comahue.sexo as GENDER,
    flores_comahue.fecha_nacimiento as BIRTH_DATE,
    flores_comahue.codigo_postal as POSTAL_CODE,
    flores_comahue.direccion as LOCATION,
    flores_comahue.correo_electronico as EMAIL
FROM public.flores_comahue
WHERE
    universidad = 'UNIV. NACIONAL DEL COMAHUE'
    AND
    fecha_de_inscripcion BETWEEN '20-09-01' AND '21-02-01';
>>>>>>> 3637f8b510fb2064abc850d1636e6c63dbc28a05
