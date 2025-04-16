SELECT p.laua, COUNT(DISTINCT c.user_id) AS user_count
FROM customers c
JOIN postcodes p ON c.postcode = p.postcode
GROUP BY p.laua