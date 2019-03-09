reviews = LOAD '/Users/vinyaskaushiktr/Downloads/amazonreviewsdatasets/mergedfiletop25' AS(marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body,review_date);

top5 = LOAD '/Users/vinyaskaushiktr/Downloads/amazonreviewsdatasets/Top5Output/Top5ByYear/2015data-m-00000' ;

grpd = GROUP reviews BY (product_id, product_category) ;

result = FOREACH grpd GENERATE group,group.product_category;

joined = JOIN top5 BY $0 LEFT OUTER, result BY group.product_id;

finalResult = FOREACH joined GENERATE $0,$1,$2,$4;

Sorted = ORDER finalResult BY $2 DESC;

top25 = LIMIT Sorted 25;

STORE top25 INTO '/Users/vinyaskaushiktr/Downloads/amazonreviewsdatasets/Top25ByYear/2015';