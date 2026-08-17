pivot (
  select name, regexp_extract(filename, '(\w+).csv', 1) as storage, case when outcome = 'PASS' then 'Pass ✅' else 'Fail ❌' end as outcome from
  read_csv('results/results-*.csv', header = false, names = ['name', 'outcome'], union_by_name = true, filename = true)
)
on storage
using first(outcome)
group by name order by name;
