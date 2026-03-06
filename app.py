SELECT r.codigo_venta, r.cant_real, r.um_salida, r.rendimiento,
       SUM(v.cantidad_vendida) as ventas
FROM recetas r
LEFT JOIN ventas v ON r.codigo_venta = v.sku_producto
    AND v.fecha_venta BETWEEN '2026-01-01' AND '2026-01-31'
WHERE r.sku_ingrediente = 'AL-FV-035'
GROUP BY 1, 2, 3, 4;
