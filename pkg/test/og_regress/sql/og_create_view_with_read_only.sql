DROP TRIGGER IF EXISTS insert_readonly_view;
DROP TRIGGER IF EXISTS update_readonly_view;
DROP TRIGGER IF EXISTS delete_readonly_view;

DROP VIEW IF EXISTS read_only_view;

DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table (
                            id INT PRIMARY KEY,
                            name VARCHAR(50),
                            age INT
);

INSERT INTO test_table VALUES (1, '张三', 25);
INSERT INTO test_table VALUES (2, '李四', 30);
INSERT INTO test_table VALUES (3, '王五', 28);

CREATE VIEW read_only_view WITH READ ONLY AS
SELECT id, name, age FROM test_table;

SELECT * FROM read_only_view;

INSERT INTO read_only_view VALUES (5, '钱七', 40);

UPDATE read_only_view SET age = 31 WHERE id = 2;

DELETE FROM read_only_view WHERE id = 2;

SELECT * FROM test_table;

DROP VIEW read_only_view;

DROP TABLE test_table;