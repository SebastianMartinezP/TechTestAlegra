# TechTestAlegra
Technical test for Data Engineer position at Alegra.

## ENTREGABLES
### Documento de diseño del modelo relacional
[Documentación de diseño del Modelo Dimensional](https://docs.google.com/document/d/1xN2t9K9dk5b721JqX1mwrwtG0MGDUqq2D4DK0ZH4rYI/edit?usp=sharing).
 
### Scripts ETL:
 - `src/stagging_tables.py`: contiene la lógica de la capa stagging, mencionada en el documento.
 - muestras de csv: 
    - `docs/output_stagging/*.csv`
    - `docs/output_datawarehouse/*.csv`

### Responder preguntas:
+ ¿Cuál es el producto más vendido en cada trimestre del año?
    ```sql
    /* bigquery dialect */
    SELECT
        COUNT(DISTINCT i.invoice_id) as invoice_quantity,
        p.product_name,
        t.quarter,
        t.year
    FROM `dw_prod.fact_invoices` AS i
    INNER JOIN `dw_prod.time_dim` AS t      ON i.time_id = t.id
    INNER JOIN `dw_prod.products_dim` AS p  ON i.product_id = p.id
    GROUP BY
        p.product_name,
        t.quarter,
        t.year
    ORDER BY
        invoice_quantity DESC;
    ```
    **Respuesta**: ...

+ ¿Cuáles son las tendencias de compra de los clientes más leales?
    ```sql
    SELECT product_id, SUM(quantity) AS total_sales FROM sales GROUP BY product
    ```
    **Respuesta**: ...

+ ¿Cómo varían las ventas según las regiones geográficas durante el año?
    ```sql
    SELECT product_id, SUM(quantity) AS total_sales FROM sales GROUP BY product
    ```
    **Respuesta**: ...

