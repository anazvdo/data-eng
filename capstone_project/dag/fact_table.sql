SELECT p.product_id,
         p.title,
         r.rating,
         r.reviewer_id,
         p.price,
         b.brand,
         c.category,
         mc.main_cat,         
         t.year,
         t.month,
         t.day
FROM products p
LEFT JOIN brand b
    ON b.brand_id = p.brand_id
LEFT JOIN category c
    ON p.category_id = c.category_id
LEFT JOIN main_category mc
    ON p.main_cat_id = mc.main_cat_id
LEFT JOIN ratings r
    ON p.product_id = r.product_id
LEFT JOIN time t
    ON t.start_time = r.start_time 