class Table():
    DEPARMENTS = 1
    HIRED_EMPLOYEES = 2
    JOBS = 3


class ColumnsType():
    INT = 1
    DATE = 2
    STRING = 3


class METRIC():
    QUARTER = 1
    UPPER_AVERAGE = 2


class QUERY():
    QUARTER = """
        select req.department, req.job,
        sum(CASE WHEN mquarter = 1 THEN 1 ELSE 0 END) Q1,
        sum(CASE WHEN mquarter = 2 THEN 1 ELSE 0 END) Q2,
        sum(CASE WHEN mquarter = 3 THEN 1 ELSE 0 END) Q3,
        sum(CASE WHEN mquarter = 4 THEN 1 ELSE 0 END) Q4
        from (
            select d.department, j.job , extract(quarter from he.datetime) as mquarter
            from hired_employees he
            left join departments d on d.id = he.department_id
            left join jobs j on j.id = he.job_id
            where he.datetime between '{year}-01-01' and '{year}-12-31'
        ) req group by (req.department, req.job)
        order by department, job
    """
