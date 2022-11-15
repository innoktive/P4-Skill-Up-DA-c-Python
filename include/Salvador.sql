SELECT salvador_villa_maria.universidad as UNIVERSITY, 
salvador_villa_maria.carrera as CAREER, 
salvador_villa_maria.fecha_de_inscripcion as INSCRIPTION_DATE , 
salvador_villa_maria.nombre as LAST_NAME, 
salvador_villa_maria.sexo as GENDER, 
salvador_villa_maria.fecha_nacimiento as BIRTH_DATE, 
salvador_villa_maria.localidad as LOCATION, 
salvador_villa_maria.email as EMAIL
FROM public.salvador_villa_maria
WHERE
    universidad = 'UNIVERSIDAD_DEL_SALVADOR'
    AND
<<<<<<< HEAD
    INSCRIPTION_DATE BETWEEN '20-09-01' AND '21-02-01';
=======
    fecha_de_inscripcion BETWEEN '20-09-01' AND '21-02-01';
>>>>>>> 3637f8b510fb2064abc850d1636e6c63dbc28a05
	