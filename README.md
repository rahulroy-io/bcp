# bcp

SELECT CHECKSUM_AGG(BINARY_CHECKSUM(TableHash)) AS TableChecksum
FROM (
  SELECT HASHBYTES('SHA2_256', CONCAT(Column1, Column2, Column3, ...)) AS TableHash
  FROM [YourTableName]
  ORDER BY Column1, Column2, Column3, ...
) AS Subquery;
