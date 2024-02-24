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
-----
+ ¿Cuál es el producto más vendido en cada trimestre del año?
    ```sql
    /* bigquery dialect */
    WITH
    all_data AS 
    (
        SELECT
            q.quarter,
            p.name as product_name,
            COUNT(invoice_id) AS num_products_per_invoice
        FROM `alegra-tech-test.dw_prd.fact_invoices` i
        INNER JOIN `alegra-tech-test.dw_prd.time_dim` q     ON q.id = i.time_id
        INNER JOIN `alegra-tech-test.dw_prd.products_dim` p ON p.id = i.product_id
        GROUP BY
            q.quarter,
            product_name
    ),
    max_per_quarter AS 
    (
        SELECT
            quarter,
            MAX(num_products_per_invoice) AS max_quantity
        FROM all_data
        GROUP BY quarter
    )

    SELECT
        A.quarter,
        U.product_name,
        A.max_quantity
    FROM max_per_quarter A
    LEFT JOIN all_data AS U
    ON
        A.quarter = U.quarter 
        AND max_quantity = U.num_products_per_invoice
    ;
    ```
    |quarter	| product_name      |	max_quantity    |
    |-----------|-------------------|-------------------|
    |       1	|  Prod_TVBPMNCHAG  |	            10  |
    |       2	|  Prod_TVBPMNCHAG  |	            10  |
    |       3	|  Prod_OMPHLJZNSU  |	            15  |
    |       4	|  Prod_VOLCAVCKJI  |	            9   |
    |       4	|  Prod_NZRFVMLYWH  |	            9   |

    **Respuesta**: Los productos más vendidos en cada trimestre son: `Prod_TVBPMNCHAG` en el primer trimestre, `Prod_TVBPMNCHAG` en el segundo, `Prod_OMPHLJZNSU` en el tercero, `Prod_VOLCAVCKJI` y `Prod_NZRFVMLYWH` en el cuarto.

-----
+ ¿Cuáles son las tendencias de compra de los clientes más leales?
    ```sql
    WITH
    top_10_customers AS 
    (
        SELECT customer_id, customer, invoices FROM (
            SELECT
            i.customer_id,
            c.name AS customer,
            COUNT(i.invoice_id) as invoices
            FROM `alegra-tech-test.dw_prd.fact_invoices` i
            INNER JOIN `alegra-tech-test.dw_prd.time_dim` q ON q.id = i.time_id
            INNER JOIN `alegra-tech-test.dw_prd.customers_dim` c ON c.id = i.customer_id
            GROUP BY i.customer_id, customer
        )
        ORDER BY invoices DESC LIMIT 10
    ),
    categories_per_customer AS
    (
        SELECT
            t.customer,
            p.category,
            COUNT(p.category) AS items_per_category
        FROM `alegra-tech-test.dw_prd.fact_invoices` i
        INNER JOIN top_10_customers t ON i.customer_id = t.customer_id
        INNER JOIN `alegra-tech-test.dw_prd.products_dim` p ON p.id = i.product_id
        GROUP BY t.customer, p.category
    )

    SELECT
    A.customer,
    A.category
    FROM categories_per_customer A
    INNER JOIN -- TOP product categories per customer
    (
        SELECT customer, MAX(items_per_category) as max_items
        FROM categories_per_customer
        GROUP BY customer
    ) B ON
        A.customer = B.customer
        AND A.items_per_category = B.max_items
    ;
    ```

    |customer	        |   category|
    |---                |---        |
    |Fernando Martinez	|   Juguete |
    |Carlos Martinez	|   Ropa    |
    |Jorge Cruz	        |   Ropa    |
    |Jorge Garcia	    |   Electronica |
    |Miguel Martinez	|   Ropa    |
    |Manuel Hernandez	|   Juguete |
    |Miguel Ramirez	    |   Juguete |
    |Jose Rodriguez	    |   Juguete |
    |Pedro Martinez	    |   Juguete |
    |Fernando Garcia	|   Hogar   |

    **Respuesta**: Se presenta a continuación una lista de los 10 clientes con más compras, junto a sus tendencias
    en las categorías de productos.
-----

+ ¿Cómo varían las ventas según las regiones geográficas durante el año?
    ```sql
    WITH
    all_data AS
    (
        SELECT
            t.year,
            t.month_string,
            COUNT(i.invoice_id) AS invoice_quantity,
            ROUND(SUM(i.total_invoice), 3) AS total_ammount
        FROM `alegra-tech-test.dw_prd.fact_invoices` i
        INNER JOIN `alegra-tech-test.dw_prd.time_dim` t ON i.time_id = t.id
        GROUP BY
            t.year,
            t.month_string
    )
    SELECT * FROM all_data

    ```
    |   year	|  month_string | invoice_quantity  |total_ammount  |
    |---        |---            | ---               | ---           |
    |   2023	|   January	    |	      76        |   42298.178   |
    |   2023	|   February	|	      78        |   43114.646   |
    |   2023	|   March	    |	      90        |   51703.628   |
    |   2023	|   April	    |	      81        |   46347.543   |
    |   2023	|   May	        |	      88        |   49238.121   |
    |   2023	|   June	    |	      82        |   47032.055   |
    |   2023	|   July	    |	      89        |   49017.333   |
    |   2023	|   August	    |	      89        |   50388.692   |
    |   2023	|   September	|	      78        |   43122.079   |
    |   2023	|   October	    |	      83        |   45961.606   |
    |   2023	|   November	|	      83        |   47158.805   |
    |   2023	|   December	|	      83        |   45367.942   |

    **Respuesta**: No hay una varianza alarmante en el número de ventas en el año. Los períodos de más
    ventas es Marzo, Mayo y Julio-Agosto.

-----