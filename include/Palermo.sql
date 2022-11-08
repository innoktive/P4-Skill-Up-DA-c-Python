SELECT palermo_tres_de_febrero.universidad AS UNIVERSITY,
	palermo_tres_de_febrero.CAREERS AS CAREER,
	palermo_tres_de_febrero.FECHA_DE_INSCRIPCION AS INSCRIPTION_DATE,
	palermo_tres_de_febrero.NAMES AS LAST_NAME,
	palermo_tres_de_febrero.SEXO AS GENDER,
	palermo_tres_de_febrero.BIRTH_DATES,
	NULL AS AGE,
	palermo_tres_de_febrero.CODIGO_POSTAL AS POSTAL_CODE,
	NULL AS LOCATION,
	palermo_tres_de_febrero.CORREOS_ELECTRONICOS AS EMAIL
FROM PUBLIC.palermo_tres_de_febrero
WHERE UNIVERSIDAD = '_universidad_de_palermo'
	AND TO_DATE(FECHA_DE_INSCRIPCION,'DD/Mon/YY') BETWEEN '01/Sep/20' AND '01/Feb/21';