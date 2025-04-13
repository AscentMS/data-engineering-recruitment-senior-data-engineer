-- Count of distinct users by local authority
SELECT 
    COALESCE(p.local_authority, 'Unknown') as local_authority,
    COUNT(DISTINCT c.customer_id) as user_count
FROM 
    customers c
LEFT JOIN 
    postcodes p ON c.postcode = p.postcode
GROUP BY 
    COALESCE(p.local_authority, 'Unknown')
ORDER BY 
    user_count DESC;