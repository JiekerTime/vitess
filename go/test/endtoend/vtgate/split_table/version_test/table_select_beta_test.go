package split_table

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestSelectBeta(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")

	for i := 1; i <= 31; i++ {
		query := fmt.Sprintf("INSERT INTO zcorder1 (order_id, customer_id, product_id, country, default_id, order_date, total_amount, quantity, unit_price, string, sequence_id) VALUES (%d, %d, %d, 'China', 63, '2023-11-11', 10.00, 5, 2.00, 'zhuchen63', %d)",
			i, i, i, i)
		mcmp.Exec(query)
	}

	for j := 15; j <= 35; j++ {
		query := fmt.Sprintf("INSERT INTO zcorder2 (order_id, customer_id, product_id, country, default_id, order_date, total_amount, quantity, unit_price, string, sequence_id) VALUES (%d, %d, %d, 'China', 63, '2023-11-11', 10.00, 5, 2.00, 'zhuchen63', %d)",
			j, j, j, j)
		mcmp.Exec(query)
	}

	mcmp.ExecAndNotEmpty("SELECT zcorder1.country FROM zcorder1 INNER JOIN zcorder2 AS o ON zcorder1.order_id = o.order_id LIMIT 100")
	mcmp.ExecAndNotEmpty("SELECT zcorder1.default_id,zcorder1.quantity FROM zcorder1 LEFT JOIN zcorder2 AS o ON zcorder1.product_id = o.product_id LIMIT 100")
	mcmp.ExecAndNotEmpty("SELECT zcorder1.default_id,zcorder1.quantity FROM zcorder1 RIGHT JOIN zcorder2 AS o ON zcorder1.product_id = o.product_id LIMIT 100")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 INNER JOIN zcorder1 AS o ON zcorder1.total_amount = o.total_amount")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 LEFT JOIN zcorder2 AS o ON zcorder1.product_id = o.product_id")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 RIGHT JOIN zcorder2 AS o ON zcorder1.country = o.country")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 CROSS JOIN zcorder2 AS o")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 INNER JOIN zcorder2 AS o ON zcorder1.order_id = o.order_id WHERE zcorder1.sequence_id = o.sequence_id ORDER BY o.order_id LIMIT 5")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 LEFT JOIN zcorder2 AS o ON zcorder1.quantity = o.quantity WHERE o.unit_price >= zcorder1.unit_price")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 RIGHT JOIN zcorder2 AS o ON zcorder1.string = o.string WHERE zcorder1.sequence_id <= o.sequence_id")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 INNER JOIN zcorder2 AS o ON zcorder1.product_id = o.product_id ORDER BY o.total_amount DESC")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 LEFT JOIN zcorder2 AS o ON zcorder1.product_id = o.product_id ORDER BY zcorder1.order_date ASC")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 RIGHT JOIN zcorder2 AS o ON zcorder1.country = o.country ORDER BY o.unit_price ASC")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 CROSS JOIN zcorder2 AS o WHERE zcorder1.unit_price <= o.unit_price")
	mcmp.ExecAndNotEmpty("SELECT zcorder1.customer_id, COUNT(*) AS order_count FROM zcorder1 INNER JOIN zcorder2 AS o ON zcorder1.sequence_id = o.sequence_id GROUP BY zcorder1.customer_id")
	mcmp.ExecAndNotEmpty("SELECT zcorder1.country, SUM(zcorder1.total_amount) AS total_sales FROM zcorder1 LEFT JOIN zcorder2 AS o ON zcorder1.product_id = o.product_id GROUP BY zcorder1.total_amount")
	//mcmp.ExecAndNotEmpty("SELECT o.product_id, AVG(zcorder1.unit_price) AS avg_price FROM zcorder1 RIGHT JOIN zcorder2 AS o ON zcorder1.country = o.country GROUP BY o.product_id")
	mcmp.ExecAndNotEmpty("SELECT zcorder1.customer_id, COUNT(*) AS order_count FROM zcorder1 INNER JOIN zcorder2 AS o ON zcorder1.sequence_id = o.sequence_id GROUP BY zcorder1.customer_id")
	mcmp.ExecAndNotEmpty("SELECT zcorder1.country, SUM(zcorder1.total_amount) AS total_sales FROM zcorder1 LEFT JOIN zcorder2 AS o ON zcorder1.product_id = o.product_id GROUP BY zcorder1.country")
	//mcmp.ExecAndNotEmpty("SELECT total_amount FROM zcorder1 LEFT JOIN (SELECT product_id FROM zcorder2 ) AS sub ON zcorder1.product_id = sub.product_id")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 RIGHT JOIN (SELECT DISTINCT country FROM zcorder1) AS sub ON zcorder2.country = sub.country")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 INNER JOIN zcorder2 AS o ON zcorder1.sequence_id = o.sequence_id WHERE EXISTS (SELECT 1 FROM zcorder1 WHERE product_id = 1)")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 LEFT JOIN zcorder2 AS o ON zcorder1.product_id = o.product_id WHERE EXISTS (SELECT 1 FROM zcorder1 WHERE customer_id = 1)")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 RIGHT JOIN zcorder2 AS o ON zcorder1.country = o.country WHERE EXISTS (SELECT 1 FROM zcorder1 WHERE order_id = 1)")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 INNER JOIN zcorder2 AS o ON zcorder1.sequence_id = o.sequence_id WHERE NOT EXISTS (SELECT 1 FROM zcorder1 WHERE product_id = 5)")
	//mcmp.ExecAndNotEmpty("SELECT z.order_id, z.total_amount FROM zcorder1 AS z INNER JOIN (SELECT customer_id, MAX(total_amount) AS max_amount FROM zcorder2 GROUP BY customer_id) AS sub ON z.customer_id = sub.customer_id AND z.total_amount = sub.max_amount")
	//mcmp.ExecAndNotEmpty("SELECT z.order_id, z.total_amount FROM zcorder1 AS z LEFT JOIN (SELECT customer_id, MAX(total_amount) AS max_amount FROM zcorder2 GROUP BY customer_id) AS sub ON z.customer_id = sub.customer_id AND z.total_amount = sub.max_amount")
	//mcmp.ExecAndNotEmpty("SELECT z.order_id, z.total_amount FROM zcorder1 AS z RIGHT JOIN (SELECT customer_id, MAX(total_amount) AS max_amount FROM zcorder2 GROUP BY customer_id) AS sub ON z.customer_id = sub.customer_id AND z.total_amount = sub.max_amount ORDER BY z.total_amount DESC")
	mcmp.ExecAndNotEmpty("SELECT z.order_id, z.total_amount FROM zcorder1 AS z INNER JOIN zcorder2 AS o ON z.product_id = o.product_id AND z.order_date = o.order_date")
	mcmp.ExecAndNotEmpty("SELECT z.order_id, z.total_amount FROM zcorder1 AS z INNER JOIN zcorder2 AS o ON z.sequence_id = o.sequence_id AND z.order_date = o.order_date")
	utils.Exec(t, mcmp.VtConn, "SELECT o.customer_id, COUNT(z.order_id) AS order_count FROM zcorder1 AS o LEFT JOIN zcorder2 AS z ON o.product_id = z.product_id AND o.order_date = z.order_date GROUP BY o.customer_id ORDER BY o.customer_id ")
	utils.Exec(t, mcmp.VtConn, "SELECT o.customer_id, COUNT(z.order_id) AS order_count FROM zcorder1 AS o LEFT JOIN zcorder2 AS z ON o.sequence_id = z.sequence_id AND o.order_date = z.order_date GROUP BY o.customer_id ORDER BY o.customer_id ")
	//mcmp.ExecAndNotEmpty("SELECT z.order_id, z.total_amount FROM zcorder1 AS z RIGHT JOIN (SELECT customer_id, AVG(total_amount) AS avg_amount FROM zcorder2 GROUP BY customer_id) AS sub ON z.customer_id = sub.customer_id AND z.total_amount > sub.avg_amount")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 AS z INNER JOIN zcorder2 AS o ON z.sequence_id = o.sequence_id WHERE z.order_id NOT IN (SELECT order_id FROM zcorder1 WHERE customer_id = 1)")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 AS z LEFT JOIN zcorder2 AS o ON z.product_id = o.product_id WHERE EXISTS (SELECT * FROM zcorder1 WHERE customer_id = 1 AND order_date = '2023-11-11')")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 AS z INNER JOIN (SELECT sequence_id FROM zcorder1 WHERE order_date = '2023-11-11' UNION SELECT product_id FROM zcorder1 WHERE total_amount > 10) AS sub ON z.sequence_id = sub.sequence_id")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 AS z LEFT JOIN zcorder2 AS o ON z.customer_id = o.customer_id WHERE NOT EXISTS (SELECT * FROM zcorder1 WHERE product_id = 1 AND order_date = '2023-11-11')")
	//mcmp.ExecAndNotEmpty("SELECT o.customer_id, COUNT(z.order_id) AS order_count FROM zcorder1 AS o RIGHT JOIN (SELECT * FROM zcorder2 WHERE total_amount > 1000) AS z ON o.customer_id = z.customer_id GROUP BY o.customer_id")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 AS z INNER JOIN (SELECT customer_id, MAX(total_amount) AS max_amount FROM zcorder2 GROUP BY customer_id) AS sub ON z.customer_id = sub.customer_id AND z.total_amount = sub.max_amount")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 AS z LEFT JOIN (SELECT customer_id, MIN(total_amount) AS min_amount FROM zcorder2 GROUP BY customer_id) AS sub ON z.customer_id = sub.customer_id AND z.total_amount = sub.min_amount")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 AS z RIGHT JOIN(SELECT product_id, COUNT(*) AS order_count FROM zcorder2 GROUP BY product_id) AS sub ON z.product_id = sub.product_id AND z.quantity > sub.order_count")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 AS z INNER JOIN (SELECT customer_id, SUM(total_amount) AS total_sale FROM zcorder2 GROUP BY customer_id) AS sub ON z.customer_id = sub.customer_id AND z.total_amount > sub.total_sales")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 AS z LEFT JOIN (SELECT customer_id, AVG(total_amount) AS avg_amount FROM zcorder2 GROUP BY customer_id) AS sub ON z.customer_id = sub.customer_id AND z.total_amount < sub.avg_amount")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 AS z RIGHT JOIN (SELECT DISTINCT product_id FROM zcorder1 WHERE total_amount > 1000) AS sub ON z.product_id = sub.product_id")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 AS z INNER JOIN (SELECT customer_id FROM zcorder2 WHERE order_date = '2022-01-01' UNION ALL SELECT customer_id FROM zcorder1 WHERE total_amount > 1000) AS sub ON z.customer_id = sub.customer_id")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 AS z LEFT JOIN (SELECT customer_id, total_amount FROM zcorder2 WHERE order_date = '2022-01-01' ORDER BY total_amount DESC) AS sub ON z.customer_id = sub.customer_id")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 AS z RIGHT JOIN (SELECT customer_id, total_amount FROM zcorder2 WHERE total_amount > 1000 LIMIT 10) AS sub ON z.customer_id = sub.customer_id")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 AS z INNER JOIN (SELECT customer_id, order_date FROM zcorder2 WHERE order_date BETWEEN '2022-01-01' AND '2022-12-31') AS sub ON z.customer_id = sub.customer_id")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 AS z LEFT JOIN (SELECT customer_id, product_name FROM zcorder2 WHERE product_name LIKE '%apple%') AS sub ON z.customer_id = sub.customer_id")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 AS z LEFT JOIN (SELECT customer_id, product_name FROM zcorder2 WHERE product_name LIKE '%apple%') AS sub ON z.customer_id = sub.customer_id")

	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 WHERE total_amount = (SELECT MAX(total_amount) FROM zcorder1)")
	mcmp.ExecAndNotEmpty("SELECT country, COUNT(*) AS order_count FROM zcorder1 GROUP BY country ORDER BY order_count DESC LIMIT 1")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 WHERE order_id IN (SELECT order_id FROM zcorder2)")
	mcmp.ExecAndNotEmpty("SELECT DISTINCT country FROM zcorder1 WHERE order_id NOT IN (SELECT order_id FROM zcorder2)")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31'")
	mcmp.ExecAndNotEmpty("SELECT COUNT(*) AS order_count FROM zcorder1 WHERE total_amount BETWEEN 1000 AND 2000")
	//mcmp.ExecAndNotEmpty("SELECT country, COUNT(*) AS order_count, AVG(total_amount) AS average_amount FROM zcorder1 GROUP BY country")
	mcmp.ExecAndNotEmpty("SELECT DATE_FORMAT(order_date, '%Y-%m') AS month, SUM(total_amount) AS monthly_total_amount FROM zcorder1 GROUP BY month")
	//mcmp.ExecAndNotEmpty("SELECT order_id, COUNT(*) AS duplicate_count FROM zcorder1 GROUP BY order_id HAVING COUNT(*) > 1")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 WHERE order_date IN (SELECT order_date FROM zcorder2)")

	_, err := mcmp.ExecAndIgnore("SELECT * FROM zcorder1 WHERE total_amount >= (SELECT AVG(total_amount) FROM zcorder1) LIMIT 1")
	require.ErrorContains(t, err, "VT12001: unsupported: in scatter query: aggregation function 'avg(total_amount)' (errno 1235) (sqlstate 42000)")

	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 WHERE total_amount > (SELECT AVG(total_amount) FROM zcorder1 WHERE zcorder1.country = country GROUP BY country)")
	utils.Exec(t, mcmp.VtConn, "SELECT customer_id, COUNT(*) AS order_count FROM zcorder1 GROUP BY customer_id ORDER BY order_count DESC LIMIT 1")
	//mcmp.ExecAndNotEmpty("SELECT customer_id, SUM(total_amount) AS total_amount, SUM(total_amount) / (SELECT SUM(total_amount) FROM zcorder1) * 100 AS percentage FROM zcorder1 GROUP BY customer_id")
	//mcmp.ExecAndNotEmpty("SELECT order_date FROM zcorder1 WHERE DATE(order_date) - INTERVAL 2 DAY IN (SELECT DATE(order_date) FROM zcorder1) GROUP BY order_date")
	//mcmp.ExecAndNotEmpty("SELECT order_id, order_date, total_amount FROM zcorder1 UNION SELECT order_id, order_date, total_amount FROM zcorder2")
	//mcmp.ExecAndNotEmpty("SELECT order_date, total_amount FROM zcorder1 where order_id = 6 UNION ALL SELECT order_date, total_amount FROM zcorder1 where order_id = 10 ORDER BY order_date DESC")
	//mcmp.ExecAndNotEmpty("SELECT order_id, order_date, total_amount FROM zcorder1 UNION ALL SELECT order_id, order_date, total_amount FROM zcorder2 ORDER BY order_date ASC")
	//mcmp.ExecAndNotEmpty("SELECT order_id, order_date, total_amount FROM zcorder1_0 UNION ALL SELECT order_id, order_date, total_amount FROM zcorder2_1 ORDER BY total_amount DESC")
	//mcmp.ExecAndNotEmpty("SELECT order_id, order_date, total_amount FROM zcorder1 WHERE order_date >= '2023-01-01' UNION ALL SELECT order_id, order_date, total_amount FROM zcorder2 WHERE order_date >= '2023-01-01'")
	//mcmp.ExecAndNotEmpty("SELECT order_id, order_date, total_amount FROM zcorder1 UNION SELECT order_id, order_date, total_amount FROM zcorder2 SELECT AVG(total_amount) AS average_amount")
	//mcmp.ExecAndNotEmpty("SELECT order_id, order_date, total_amount FROM zcorder1 UNION ALL SELECT order_id, order_date, total_amount FROM zcorder2 SELECT SUM(total_amount) AS total_sum")

	_, err = mcmp.ExecAndIgnore("SELECT * FROM zcorder1 WHERE total_amount < (SELECT AVG(total_amount) FROM zcorder2)")
	require.ErrorContains(t, err, "VT12001: unsupported: in scatter query: aggregation function 'avg(total_amount)' (errno 1235) (sqlstate 42000)")

	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 WHERE order_date IN (SELECT order_date FROM zcorder2 WHERE total_amount > 5)")
	//mcmp.ExecAndNotEmpty("SELECT customer_id, AVG(total_amount) AS average_amount FROM zcorder1 GROUP BY customer_id HAVING COUNT(*) > 1")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 WHERE country IN (SELECT country FROM zcorder2 WHERE total_amount > 1)")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 WHERE total_amount = (SELECT MAX(total_amount) FROM zcorder1)")
	mcmp.ExecAndNotEmpty("SELECT country, COUNT(*) AS order_count FROM zcorder1 GROUP BY country ORDER BY order_count DESC LIMIT 1")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 WHERE order_id IN (SELECT order_id FROM zcorder2)")
	mcmp.ExecAndNotEmpty("SELECT DISTINCT customer_id FROM zcorder1 WHERE order_id NOT IN (SELECT order_id FROM zcorder2)")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31'")
	mcmp.ExecAndNotEmpty("SELECT COUNT(*) AS order_count FROM zcorder1 WHERE total_amount BETWEEN 1000 AND 2000")
	mcmp.ExecAndNotEmpty("SELECT DATE_FORMAT(order_date, '%Y-%m') AS month, SUM(total_amount) AS monthly_total_amount FROM zcorder1 GROUP BY month")
	//mcmp.ExecAndNotEmpty("SELECT order_id, COUNT() AS duplicate_count FROM zcorder1 GROUP BY order_id HAVING COUNT() > 1")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 WHERE order_id IN (SELECT order_id FROM zcorder2)")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 WHERE total_amount > (SELECT AVG(total_amount) FROM zcorder1)")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 WHERE total_amount >= (SELECT MAX(total_amount) FROM zcorder1)")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 WHERE total_amount <= (SELECT MIN(total_amount) FROM zcorder1)")
	mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 WHERE total_amount < (SELECT SUM(total_amount) FROM zcorder1)")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 WHERE total_amount > (SELECT AVG(total_amount) FROM zcorder1 WHERE zcorder1.country = country GROUP BY country )")
	utils.Exec(t, mcmp.VtConn, "SELECT customer_id, COUNT(*) AS order_count FROM zcorder1 GROUP BY customer_id ORDER BY order_count DESC LIMIT 1")
	//mcmp.ExecAndNotEmpty("SELECT * FROM zcorder1 WHERE EXISTS (SELECT * FROM zcorder2 WHERE ABS(DATEDIFF(zcorder1.order_date, zcorder2.order_date)) > 7)")
	//mcmp.ExecAndNotEmpty("SELECT order_id, order_date, total_amount FROM zcorder1 WHERE order_date >= '2023-01-01' UNION ALL SELECT order_id, order_date, total_amount FROM zcorder2 WHERE order_date >= '2023-01-01'")
	utils.Exec(t, mcmp.VtConn, "SELECT * FROM zcorder1 WHERE product_id IN (SELECT product_id FROM zcorder2 WHERE total_amount > 1) LIMIT 10")
	//mcmp.ExecAndNotEmpty("SELECT customer_id, AVG(total_amount) AS average_amount FROM zcorder1 GROUP BY customer_id HAVING COUNT(*) > 1")

	mcmp.ExecAndNotEmpty("SELECT customer_id FROM zcorder1 WHERE country IN (SELECT country FROM zcorder2 WHERE total_amount > 1)")
	mcmp.ExecAndNotEmpty("SELECT country FROM zcorder1 WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31' AND customer_id > (SELECT MAX(total_amount) FROM zcorder1 WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31') ORDER BY total_amount DESC;")

	_, err = mcmp.ExecAndIgnore("SELECT * FROM zcorder1 where customer_id = 10 UNION ALL SELECT * FROM zcorder2;")
	require.ErrorContains(t, err, "VT12001: unsupported: statement type *sqlparser.Union in split table (errno 1235) (sqlstate 42000)")

	_, err = mcmp.ExecAndIgnore("SELECT * FROM zcorder1 UNION ALL SELECT * FROM zcorder2 where customer_id = 10;")
	require.ErrorContains(t, err, "VT12001: unsupported: statement type *sqlparser.Union in split table (errno 1235) (sqlstate 42000)")

	_, err = mcmp.ExecAndIgnore("SELECT * FROM zcorder1 where sequence_id = 5 UNION ALL SELECT * FROM zcorder2 where customer_id = 10 LIMIT 6;")
	require.ErrorContains(t, err, "VT12001: unsupported: statement type *sqlparser.Union in split table (errno 1235) (sqlstate 42000)")

	mcmp.ExecAndNotEmpty("SELECT order_id, order_date, total_amount FROM zcorder1 where customer_id = 6 UNION ALL SELECT order_id, order_date, total_amount FROM zcorder1 where customer_id = 10;")
	mcmp.ExecAndNotEmpty("SELECT order_id, product_id, country FROM zcorder1 WHERE customer_id = 1  UNION ALL SELECT order_id, product_id, country FROM zcorder2 where customer_id = 10 LIMIT 100;")
	mcmp.ExecAndNotEmpty("SELECT order_id, customer_id, product_id, country FROM zcorder1 WHERE customer_id = 1  UNION ALL SELECT order_id, customer_id, product_id, country FROM zcorder2 where customer_id = 10 LIMIT 100;")
	mcmp.ExecAndNotEmpty("select total_amount from zcorder1 where customer_id = 3  union all select total_amount from zcorder1 where customer_id = 5")
	mcmp.ExecAndNotEmpty("select country from zcorder1 where customer_id = 3  union all select country from zcorder1 WHERE customer_id = 5")
	//mcmp.ExecAndNotEmpty("SELECT order_id, customer_id, product_id, country, default_id, order_date, total_amount, quantity, unit_price, string, sequence_id FROM (SELECT order_id, customer_id, product_id, country, default_id, order_date, total_amount, quantity, unit_price, string, sequence_id FROM zcorder1 ORDER BY customer_id LIMIT 10) AS t1 UNION ALL SELECT order_id, customer_id, product_id, country, default_id, order_date, total_amount, quantity, unit_price, string, sequence_id FROM (SELECT order_id, customer_id, product_id, country, default_id, order_date, total_amount, quantity, unit_price, string, sequence_id FROM zcorder1 ORDER BY customer_id LIMIT 10) AS t2 UNION ALL SELECT order_id, customer_id, product_id, country, default_id, order_date, total_amount, quantity, unit_price, string, sequence_id FROM (SELECT order_id, customer_id, product_id, country, default_id, order_date, total_amount, quantity, unit_price, string, sequence_id FROM zcorder2 ORDER BY customer_id LIMIT 10) AS t3 UNION ALL SELECT order_id, customer_id, product_id, country, default_id, order_date, total_amount, quantity, unit_price, string, sequence_id FROM (SELECT order_id, customer_id, product_id, country, default_id, order_date, total_amount, quantity, unit_price, string, sequence_id FROM zcorder2 ORDER BY customer_id LIMIT 10) AS t4;")
	//mcmp.ExecAndNotEmpty("SELECT order_id, customer_id, product_id, country FROM (SELECT order_id, customer_id, product_id, country FROM zcorder1 ORDER BY customer_id ASC, order_id DESC LIMIT 10) AS t1 UNION ALL SELECT order_id, customer_id, product_id, country FROM (SELECT order_id, customer_id, product_id, country FROM zcorder2 ORDER BY customer_id ASC, order_id DESC LIMIT 10) AS t2;")

	//mcmp.ExecAndNotEmpty("SELECT order_id, customer_id, product_id, country FROM zcorder1 ORDER BY order_date DESC, total_amount ASC UNION ALL SELECT order_id, customer_id, product_id, country FROM zcorder2 ORDER BY unit_price DESC, quantity DESC;")
}
