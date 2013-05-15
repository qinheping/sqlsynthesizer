http://forums.tutorialized.com/sql-basics-113/finding-list-of-duplicate-records-with-more-than-two-duplicates-379845.html

SELECT upedonid, count(upedonid) as nmbr
FROM pedon
WHERE upedonid != null
GROUP BY upedonid


just 1 column

