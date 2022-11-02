SELECT salvador_villa_maria.universidad, salvador_villa_maria.carrera, salvador_villa_maria.fecha_de_inscripcion, salvador_villa_maria.nombre, salvador_villa_maria.sexo, salvador_villa_maria.fecha_nacimiento, salvador_villa_maria.localidad, salvador_villa_maria.email 
FROM public.salvador_villa_maria
WHERE
    universidad = 'UNIVERSIDAD_DEL_SALVADOR'
    AND
    fecha_de_inscripcion BETWEEN '20-09-01' AND '21-02-01';
	