# Partition 현황조회
<PRE>
#legacySQL
SELECT
  partition_id,
  last_modified_time
FROM
  [{dataset}.{table_name}$__PARTITIONS_SUMMARY__]
</PRE>