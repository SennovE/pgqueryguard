WITH baz AS (
    SELECT
        foo.a AS a,
        foo.c AS c
    FROM
        foo AS foo
    WHERE
        foo.a = 1
)
SELECT
    f.a AS a,
    b.b AS b,
    baz.c AS c,
    CAST(b.a AS float) AS d
FROM
    foo AS f
    JOIN bar AS b ON b.a = f.a
    LEFT JOIN baz AS baz ON baz.a = f.a