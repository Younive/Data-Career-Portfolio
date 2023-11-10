SELECT
  c.country,
  c.alpha3Code,
  COUNT(fi.invoice_id) AS total_invoices,
  SUM(fi.total) AS total_revenue
FROM `online-retail-project`.`retail`.`fct_invoices` fi
JOIN `online-retail-project`.`retail`.`dim_customer` c ON fi.customer_id = c.customer_id
GROUP BY c.country, c.alpha3Code
ORDER BY total_revenue DESC
LIMIT 10