SELECT
  flores_comahue.universidad,
  flores_comahue.carrera,
  flores_comahue.fecha_de_inscripcion,
  flores_comahue.nombre,
  flores_comahue.apellido,
  flores_comahue.sexo,
  flores_comahue.edad,
  flores_comahue.codigo_postal,
  flores_comahue.direccion,
  flores_comahue.correo_electronico
FROM public.salvador_villa_maria
WHERE
    universidad = 'UNIV. NACIONAL DEL COMAHUE'
    AND
    fecha_de_inscripcion BETWEEN '20-09-01' AND '21-02-01';
