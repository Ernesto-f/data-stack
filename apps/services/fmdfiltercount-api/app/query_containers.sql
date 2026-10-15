    WITH parametros AS (
        SELECT
            NOW() - INTERVAL '0 hours' AS doze_horas
    ),

    horario AS (
        SELECT
            NOW() AS agora
    ),

    q_max_container_achid AS (
        SELECT
            MAX(ach.id) AS achid,
            ach.container
        FROM
            udi_gmes.at_container_history ach
        WHERE
            ach.transactioncode <> 'HRC'
            AND COALESCE(ach.dstwarehouselocation, '') <> ''
        GROUP BY
            ach.container
    )

    SELECT
        ROW_NUMBER() OVER (
            ORDER BY ach.id ASC
        ) AS "Rank",

        ach.id AS achid,
        ach.transactioncode,
        ach.srcproductno,

        CASE
            WHEN ach.srcwarehouselocation = 'Picking'
                AND ach.dstwarehouselocation IN (
                    'Emissores_Filtros_Domesticos',
                    'Emissores_Filtros_Importados'
                )
                THEN 'BUFFER-FMD'

            WHEN ach.srcwarehouselocation IN (
                    'Emissores_Filtros_Domesticos',
                    'Emissores_Filtros_Importados'
                )
                AND ach.dstwarehouselocation IN (
                    'Emissores_Filtros_Domesticos',
                    'Emissores_Filtros_Importados'
                )
                THEN 'Emissores'

            ELSE 'Produção FMD'
        END AS paradoem,

        CASE
            WHEN ach.transactioncode = 'INVINC'
                AND (
                    (
                        COALESCE(ach.srcwarehouselocation, '') = ''
                        AND ach.dstwarehouselocation LIKE 'FM%'
                    )
                    OR (
                        ach.srcwarehouselocation LIKE 'FM%'
                        AND ach.dstwarehouselocation LIKE 'FM%'
                    )
                )
                THEN ach.dstwarehouselocation

            ELSE ''
        END AS modulo,

        TO_CHAR(
            CASE
                WHEN ach.createdon::time < TIME '05:20:00'
                    THEN ach.createdon - INTERVAL '1 day'
                ELSE ach.createdon
            END,
            'DD/MM/YYYY'
        ) AS workday,

        ach.srcwarehouselocation,
        ach.dstwarehouselocation,
        ach.container,
        ach.incontainer,
        ach.containerclassid,
        ach.oldquantity,
        ach.newquantity,
        ach.quantityadjusted,
        ach.uomcode,
        ach.createdon,
        ach.transactioncontext,
        ach.wiporderno,
        ach.wipordertype,
        cmt.nam_comments,
        cmt.cod_id,

        p.doze_horas AS dezhoras,
        h.agora AS agora,
        NOW() - INTERVAL '0 hours' AS atualizacao

    FROM
        udi_gmes.at_container_history ach

        INNER JOIN q_max_container_achid q
            ON ach.id = q.achid

        LEFT JOIN udi_development.tb_comments cmt
            ON ach.container = cmt.val_container_number

        CROSS JOIN parametros p

        CROSS JOIN horario h

    WHERE
        ach.srcproductno LIKE '3%'
        AND ach.container LIKE '1%'
        AND p.doze_horas > ach.createdon
        AND (
            (
                ach.srcwarehouselocation IN (
                    'Emissores_Filtros_Domesticos',
                    'Emissores_Filtros_Importados'
                )
                AND ach.dstwarehouselocation IN (
                    'Emissores_Filtros_Domesticos',
                    'Emissores_Filtros_Importados'
                )
                AND ach.newquantity > 0
                AND ach.transactioncode <> 'INVDEC'
            )

            OR (
                ach.srcwarehouselocation = 'Picking'
                AND ach.dstwarehouselocation IN (
                    'Emissores_Filtros_Domesticos',
                    'Emissores_Filtros_Importados'
                )
                AND ach.newquantity > 0
            )

            OR (
                (
                    ach.transactioncode = 'INVINC'
                    AND COALESCE(ach.srcwarehouselocation, '') = ''
                    AND ach.dstwarehouselocation LIKE 'FM%'
                )

                OR (
                    ach.transactioncode = 'INVINC'
                    AND ach.srcwarehouselocation LIKE 'FM%'
                    AND ach.dstwarehouselocation LIKE 'FM%'
                )
            )
        );