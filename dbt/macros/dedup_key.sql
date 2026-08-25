{% macro dedup_key() %}
lot_id, wafer_id, x_coord, y_coord,
CASE WHEN x_coord = -32768 AND y_coord = -32768 THEN part_txt ELSE '' END,
test_num, pin_num
{% endmacro %}
