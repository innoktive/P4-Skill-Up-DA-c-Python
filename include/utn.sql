SELECT university,
	career,
	inscription_date,
	nombre AS full_name,
	sexo AS gender,
	birth_date AS age,
	location,
	email
FROM jujuy_utn
WHERE inscription_date BETWEEN '2020/09/01' AND '2021/02/01'
	AND university = 'universidad tecnológica nacional';