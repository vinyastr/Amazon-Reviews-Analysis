reviews = LOAD '/Users/vinyaskaushiktr/Downloads/amazonreviewsdatasets/mergedfile' AS(marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body,review_date);

filtered1 = FILTER reviews BY helpful_votes > 3;
filtered2 = FILTER filtered1 BY star_rating >3;
filtered3 = FOREACH filtered2 GENERATE $0,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14, EqualsIgnoreCase(verified_purchase,'Y');

STORE filtered3 INTO '/Users/vinyaskaushiktr/Downloads/amazonreviewsdatasets/mergedfileforTop25';