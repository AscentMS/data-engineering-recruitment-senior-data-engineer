SELECT p.laua, c.sex, COUNT(DISTINCT c.user_id)
FROM customers c
JOIN postcodes p ON c.postcode = p.postcode
GROUP BY p.laua, c.sex