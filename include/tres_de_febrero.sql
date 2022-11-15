SELECT universidad AS university, 
	careers AS career, 
	fecha_de_inscripcion AS inscription_date, 
	names AS full_name, 
	sexo AS gender, 
	birth_dates AS age, 
	codigo_postal AS postal_code, 
	direcciones AS location, 
	correos_electronicos AS email 
FROM palermo_tres_de_febrero 
WHERE TO_DATE(fecha_de_inscripcion,'DD/Mon/YY') BETWEEN '01/Sep/20' AND '01/Feb/21' 
AND universidad = 'universidad_nacional_de_tres_de_febrero';